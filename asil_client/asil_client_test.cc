// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// Interoperability tests: the ASIL client and the standard client share
// channels on a server running a static channel config.  Static channels keep
// their messages for the life of the server, so each test uses its own
// channel.

#include "client/test_fixture.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "asil_client/client.h"
#include "asil_client/phaser_connection.h"
#include "asil_client/publisher.h"
#include "asil_client/shm_layout.h"
#include "asil_client/split_buffer.h"
#include "asil_client/subscriber.h"
#include "server/static_config.h"
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <new>
#include <poll.h>
#include <string>
#include <utility>

namespace {
// Counts heap allocations made by this thread while counting is set.
thread_local bool counting_allocations = false;
thread_local int allocations = 0;
} // namespace

void *operator new(std::size_t size) {
  if (counting_allocations) {
    allocations++;
  }
  void *p = std::malloc(size == 0 ? 1 : size);
  if (p == nullptr) {
    throw std::bad_alloc();
  }
  return p;
}

void operator delete(void *p) noexcept { std::free(p); }
void operator delete(void *p, std::size_t) noexcept { std::free(p); }

namespace {

namespace asil = ::subspace::asil;
using ::testing::HasSubstr;

constexpr char kConfig[] = R"pb(
  multiplexers { name: "/asil/mux" slot_size: 128 num_slots: 8 }
  channels { name: "/asil/to_std" slot_size: 256 num_slots: 8 type: "T" }
  channels { name: "/asil/from_std" slot_size: 256 num_slots: 8 type: "T" }
  channels { name: "/asil/wait" slot_size: 64 num_slots: 4 }
  channels { name: "/asil/late_sub" slot_size: 64 num_slots: 4 }
  channels { name: "/asil/sum_to_std" slot_size: 128 num_slots: 4 }
  channels { name: "/asil/sum_from_std" slot_size: 128 num_slots: 4 }
  channels { name: "/asil/meta" slot_size: 64 num_slots: 8 metadata_size: 16 }
  channels { name: "/asil/newest" slot_size: 64 num_slots: 8 }
  channels { name: "/asil/drops_in" slot_size: 64 num_slots: 4 }
  channels { name: "/asil/drops_out" slot_size: 64 num_slots: 4 }
  channels { name: "/asil/activate" slot_size: 64 num_slots: 4 }
  channels { name: "/asil/cycle" slot_size: 64 num_slots: 4 }
  channels { name: "/asil/hold" slot_size: 64 num_slots: 4 }
  channels { name: "/asil/reject" slot_size: 64 num_slots: 4 }
  channels { name: "/asil/close" slot_size: 64 num_slots: 4 }
  channels { name: "/asil/v1" mux: "/asil/mux" vchan_id: 1 }
  channels { name: "/asil/v2" mux: "/asil/mux" vchan_id: 2 }
  channels { name: "/asil/rel_to_std" slot_size: 64 num_slots: 4 }
  channels { name: "/asil/rel_from_std" slot_size: 64 num_slots: 4 }
  channels { name: "/asil/rel_newest" slot_size: 64 num_slots: 4 }
  channels { name: "/asil/rel_late" slot_size: 64 num_slots: 4 }
  channels { name: "/asil/multi" slot_size: 64 num_slots: 8 }
  channels { name: "/asil/no_alloc" slot_size: 64 num_slots: 4 }
  channels { name: "/asil/oversize" slot_size: 64 num_slots: 4 }
  channels { name: "/asil/early_sub" slot_size: 64 num_slots: 4 }
  channels {
    name: "/asil/split_to_std"
    slot_size: 256
    num_slots: 4
    use_split_buffers: true
  }
  channels {
    name: "/asil/split_from_std"
    slot_size: 256
    num_slots: 4
    use_split_buffers: true
  }
  channels {
    name: "/asil/split_reject"
    slot_size: 64
    num_slots: 4
    use_split_buffers: true
  }
  multiplexers {
    name: "/asil/split_mux"
    slot_size: 128
    num_slots: 8
    use_split_buffers: true
  }
  channels { name: "/asil/split_v" mux: "/asil/split_mux" vchan_id: 4 }
)pb";

subspace::PublisherOptions FixedSize() {
  return subspace::PublisherOptions().SetFixedSize(true);
}

void StdPublish(subspace::Publisher &pub, const std::string &text) {
  absl::StatusOr<void *> buffer = pub.GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, text.data(), text.size());
  ASSERT_OK(pub.PublishMessage(static_cast<int64_t>(text.size())));
}

void AsilPublish(asil::Publisher &pub, const std::string &text) {
  void *buffer = pub.Buffer();
  ASSERT_NE(nullptr, buffer);
  memcpy(buffer, text.data(), text.size());
  ASSERT_EQ(asil::Error::kOk, pub.Publish(text.size()));
}

std::string Text(const asil::Message &msg) {
  return std::string(static_cast<const char *>(msg.data), msg.length);
}

std::string Text(const subspace::Message &msg) {
  return std::string(static_cast<const char *>(msg.buffer), msg.length);
}

// A copy of a message read by a standard subscriber.  The message itself is
// released before returning because standard subscribers default to one
// active message.
struct StdMessage {
  std::string text;
  uint64_t ordinal = 0;
  uint64_t timestamp = 0;
  bool is_activation = false;
  bool checksum_error = false;
  std::string metadata;
};

StdMessage StdRead(subspace::Subscriber &sub) {
  absl::StatusOr<subspace::Message> msg = sub.ReadMessage();
  EXPECT_TRUE(msg.ok()) << msg.status();
  StdMessage copy;
  if (!msg.ok()) {
    return copy;
  }
  copy.text = Text(*msg);
  copy.ordinal = msg->ordinal;
  copy.timestamp = msg->timestamp;
  copy.is_activation = msg->is_activation;
  copy.checksum_error = msg->checksum_error;
  absl::Span<const std::byte> metadata = sub.GetMetadata();
  copy.metadata.assign(reinterpret_cast<const char *>(metadata.data()),
                       metadata.size());
  return copy;
}

bool Readable(int fd) {
  struct pollfd p = {fd, POLLIN, 0};
  return ::poll(&p, 1, 0) == 1;
}

asil::PublisherOptions AsilPubOptions(int64_t slot_size, int32_t num_slots) {
  asil::PublisherOptions options;
  options.slot_size = slot_size;
  options.num_slots = num_slots;
  return options;
}

// A server on its own thread.  config is a static channel config, or null
// for a server that creates channels on demand.
class TestServer {
public:
  void Start(const char *config) {
#if defined(__ANDROID__)
    char socket_name_template[] = "/data/local/tmp/subspaceXXXXXX"; // NOLINT
#else
    char socket_name_template[] = "/tmp/subspaceXXXXXX"; // NOLINT
#endif
    ::close(mkstemp(&socket_name_template[0]));
    socket_ = &socket_name_template[0];

    (void)pipe(server_pipe_);

    server_ = std::make_unique<subspace::Server>(
        engine_, socket_, "", 0, 0,
        /*local=*/true, server_pipe_[1], /*initial_ordinal=*/1,
        /*wait_for_clients=*/true);
    if (config != nullptr) {
      absl::StatusOr<subspace::StaticChannelConfig> parsed =
          subspace::ParseStaticChannelConfig(config);
      ASSERT_OK(parsed);
      ASSERT_OK(server_->SetStaticChannelConfig(*std::move(parsed)));
    }

    server_thread_ = std::thread([this]() {
      absl::Status s = server_->Run();
      if (!s.ok()) {
        fprintf(stderr, "Error running Subspace server: %s\n",
                s.ToString().c_str());
        exit(1);
      }
    });

    char buf[8];
    (void)::read(server_pipe_[0], buf, 8);
  }

  void Stop() {
    server_->Stop();

    char buf[8];
    (void)::read(server_pipe_[0], buf, 8);
    server_thread_.join();
    server_->CleanupAfterSession();
    server_.reset();
    ::close(server_pipe_[0]);
    ::close(server_pipe_[1]);
    (void)remove(socket_.c_str());
  }

  const std::string &Socket() const { return socket_; }

private:
  subspace::async::RuntimeEngine engine_;
  std::string socket_;
  int server_pipe_[2];
  std::unique_ptr<subspace::Server> server_;
  std::thread server_thread_;
};

// Connects a standard client and an ASIL client to a test server.
class AsilTestBase : public ::testing::Test {
public:
  subspace::Client &Std() { return std_client_; }
  asil::Client &Asil() { return asil_client_; }
  asil::PhaserServerConnection &Connection() { return connection_; }

protected:
  void Connect(const std::string &socket) {
    signal(SIGPIPE, SIG_IGN);
    ASSERT_OK(std_client_.Init(socket));
    ASSERT_EQ(asil::Error::kOk, connection_.Connect(socket.c_str()));
    ASSERT_EQ(asil::Error::kOk, asil_client_.Init(connection_, "asil_test"));
  }

private:
  subspace::Client std_client_;
  asil::PhaserServerConnection connection_;
  asil::Client asil_client_;
};

// A server with a static channel config.
class AsilClientTest : public AsilTestBase {
public:
  static void SetUpTestSuite() { server_.Start(kConfig); }
  static void TearDownTestSuite() { server_.Stop(); }
  void SetUp() override { Connect(server_.Socket()); }

private:
  inline static TestServer server_;
};

// A server that creates channels on demand, so channels can resize and use
// subscriber queues.
class AsilDynamicTest : public AsilTestBase {
public:
  static void SetUpTestSuite() { server_.Start(nullptr); }
  static void TearDownTestSuite() { server_.Stop(); }
  void SetUp() override { Connect(server_.Socket()); }

private:
  inline static TestServer server_;
};

TEST_F(AsilClientTest, AsilPublisherStandardSubscriber) {
  absl::StatusOr<subspace::Subscriber> sub =
      Std().CreateSubscriber("/asil/to_std");
  ASSERT_OK(sub);

  asil::Publisher pub;
  asil::PublisherOptions options = AsilPubOptions(256, 8);
  options.type = "T";
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreatePublisher("/asil/to_std", options, pub));
  EXPECT_EQ(256, pub.SlotSize());
  EXPECT_EQ(8, pub.NumSlots());

  asil::PublishedMessage published;
  void *buffer = pub.Buffer();
  ASSERT_NE(nullptr, buffer);
  memcpy(buffer, "one", 3);
  ASSERT_EQ(asil::Error::kOk, pub.Publish(3, &published));
  EXPECT_NE(0u, published.ordinal);
  AsilPublish(pub, "two");
  AsilPublish(pub, "three");

  StdMessage msg = StdRead(*sub);
  EXPECT_EQ("one", msg.text);
  EXPECT_EQ(published.ordinal, msg.ordinal);
  EXPECT_EQ(published.timestamp, msg.timestamp);
  EXPECT_EQ("two", StdRead(*sub).text);
  EXPECT_EQ("three", StdRead(*sub).text);
  EXPECT_EQ("", StdRead(*sub).text);
}

TEST_F(AsilClientTest, StandardPublisherAsilSubscriber) {
  asil::Subscriber sub;
  asil::SubscriberOptions options;
  options.type = "T";
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/asil/from_std", options, sub));

  // Nothing has been published, so there is no buffer to read yet.
  asil::Message msg;
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_EQ(0u, msg.length);

  absl::StatusOr<subspace::Publisher> pub =
      Std().CreatePublisher("/asil/from_std", 256, 8, FixedSize());
  ASSERT_OK(pub);
  StdPublish(*pub, "alpha");
  StdPublish(*pub, "beta");

  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_EQ("alpha", Text(msg));
  EXPECT_EQ(0u, msg.dropped);
  const uint64_t first = msg.ordinal;
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_EQ("beta", Text(msg));
  EXPECT_EQ(first + 1, msg.ordinal);
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_EQ(0u, msg.length);
}

TEST_F(AsilClientTest, AsilToAsilWithWait) {
  asil::Subscriber sub;
  ASSERT_EQ(asil::Error::kOk, Asil().CreateSubscriber("/asil/wait", {}, sub));
  asil::Publisher pub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreatePublisher("/asil/wait", AsilPubOptions(64, 4), pub));

  // Drain the trigger set when the subscriber opened.
  asil::Message msg;
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_EQ(0u, msg.length);
  EXPECT_EQ(asil::Error::kTimeout, sub.Wait(10));

  AsilPublish(pub, "wake");
  EXPECT_EQ(asil::Error::kOk, sub.Wait(1000));
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_EQ("wake", Text(msg));
}

TEST_F(AsilClientTest, SubscriberJoinsAfterPublisher) {
  asil::Publisher pub;
  ASSERT_EQ(
      asil::Error::kOk,
      Asil().CreatePublisher("/asil/late_sub", AsilPubOptions(64, 4), pub));

  absl::StatusOr<subspace::Subscriber> sub =
      Std().CreateSubscriber("/asil/late_sub");
  ASSERT_OK(sub);
  EXPECT_EQ("", StdRead(*sub).text);
  ASSERT_FALSE(Readable(sub->GetPollFd().fd));

  // The publisher learns of the new subscriber's trigger when it publishes.
  AsilPublish(pub, "late");
  EXPECT_TRUE(Readable(sub->GetPollFd().fd));
  EXPECT_EQ("late", StdRead(*sub).text);
}

TEST_F(AsilClientTest, ChecksumToStandard) {
  absl::StatusOr<subspace::Subscriber> sub = Std().CreateSubscriber(
      "/asil/sum_to_std", subspace::SubscriberOptions().SetChecksum(true));
  ASSERT_OK(sub);
  asil::Publisher pub;
  asil::PublisherOptions options = AsilPubOptions(128, 4);
  options.checksum = true;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreatePublisher("/asil/sum_to_std", options, pub));
  AsilPublish(pub, "checked");

  StdMessage msg = StdRead(*sub);
  EXPECT_EQ("checked", msg.text);
  EXPECT_FALSE(msg.checksum_error);
}

TEST_F(AsilClientTest, ChecksumFromStandard) {
  asil::SubscriberOptions strict;
  strict.checksum = true;
  asil::Subscriber strict_sub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/asil/sum_from_std", strict, strict_sub));
  asil::SubscriberOptions lenient = strict;
  lenient.pass_checksum_errors = true;
  asil::Subscriber lenient_sub;
  ASSERT_EQ(asil::Error::kOk, Asil().CreateSubscriber("/asil/sum_from_std",
                                                      lenient, lenient_sub));

  absl::StatusOr<subspace::Publisher> pub = Std().CreatePublisher(
      "/asil/sum_from_std", 128, 4, FixedSize().SetChecksum(true));
  ASSERT_OK(pub);
  StdPublish(*pub, "good");

  // Corrupt the second message after it has been published.
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, "bad", 3);
  ASSERT_OK(pub->PublishMessage(3));
  static_cast<char *>(*buffer)[0] = 'B';

  asil::Message msg;
  ASSERT_EQ(asil::Error::kOk, strict_sub.ReadMessage(msg));
  EXPECT_EQ("good", Text(msg));
  EXPECT_FALSE(msg.checksum_error);
  EXPECT_EQ(asil::Error::kChecksumMismatch, strict_sub.ReadMessage(msg));
  EXPECT_EQ(0u, msg.length);

  ASSERT_EQ(asil::Error::kOk, lenient_sub.ReadMessage(msg));
  EXPECT_EQ("good", Text(msg));
  ASSERT_EQ(asil::Error::kOk, lenient_sub.ReadMessage(msg));
  EXPECT_EQ("Bad", Text(msg));
  EXPECT_TRUE(msg.checksum_error);
}

TEST_F(AsilClientTest, Metadata) {
  asil::Subscriber asil_sub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/asil/meta", {}, asil_sub));
  absl::StatusOr<subspace::Subscriber> std_sub =
      Std().CreateSubscriber("/asil/meta");
  ASSERT_OK(std_sub);

  asil::Publisher asil_pub;
  asil::PublisherOptions options = AsilPubOptions(64, 8);
  options.metadata_size = 16;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreatePublisher("/asil/meta", options, asil_pub));
  ASSERT_EQ(16u, asil_pub.MetadataSize());
  memcpy(asil_pub.Metadata(), "asil-metadata!!", 16);
  AsilPublish(asil_pub, "from asil");

  StdMessage std_msg = StdRead(*std_sub);
  EXPECT_EQ("from asil", std_msg.text);
  EXPECT_EQ(std::string("asil-metadata!!", 16), std_msg.metadata);

  absl::StatusOr<subspace::Publisher> std_pub = Std().CreatePublisher(
      "/asil/meta", 64, 8, FixedSize().SetMetadataSize(16));
  ASSERT_OK(std_pub);
  absl::StatusOr<void *> buffer = std_pub->GetMessageBuffer();
  ASSERT_OK(buffer);
  absl::Span<std::byte> meta = std_pub->GetMetadata();
  ASSERT_EQ(16u, meta.size());
  memcpy(meta.data(), "std-metadata!!!", 16);
  memcpy(*buffer, "from std", 8);
  ASSERT_OK(std_pub->PublishMessage(8));

  asil::Message msg;
  ASSERT_EQ(asil::Error::kOk, asil_sub.ReadMessage(msg));
  EXPECT_EQ("from asil", Text(msg));
  ASSERT_EQ(16u, msg.metadata_length);
  EXPECT_EQ(0, memcmp(msg.metadata, "asil-metadata!!", 16));
  ASSERT_EQ(asil::Error::kOk, asil_sub.ReadMessage(msg));
  EXPECT_EQ("from std", Text(msg));
  ASSERT_EQ(16u, msg.metadata_length);
  EXPECT_EQ(0, memcmp(msg.metadata, "std-metadata!!!", 16));
}

TEST_F(AsilClientTest, ReadNewest) {
  asil::Subscriber sub;
  ASSERT_EQ(asil::Error::kOk, Asil().CreateSubscriber("/asil/newest", {}, sub));
  absl::StatusOr<subspace::Publisher> pub =
      Std().CreatePublisher("/asil/newest", 64, 8, FixedSize());
  ASSERT_OK(pub);
  StdPublish(*pub, "old");
  StdPublish(*pub, "older");
  StdPublish(*pub, "newest");

  asil::Message msg;
  ASSERT_EQ(asil::Error::kOk,
            sub.ReadMessage(msg, asil::ReadMode::kReadNewest));
  EXPECT_EQ("newest", Text(msg));
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_EQ(0u, msg.length);

  StdPublish(*pub, "next");
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_EQ("next", Text(msg));
}

TEST_F(AsilClientTest, AsilSubscriberCountsDrops) {
  asil::Subscriber sub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/asil/drops_in", {}, sub));
  absl::StatusOr<subspace::Publisher> pub =
      Std().CreatePublisher("/asil/drops_in", 64, 4, FixedSize());
  ASSERT_OK(pub);
  StdPublish(*pub, "first");
  asil::Message msg;
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_EQ("first", Text(msg));
  const uint64_t first = msg.ordinal;
  sub.ReleaseMessage();

  for (int i = 0; i < 10; i++) {
    StdPublish(*pub, "m" + std::to_string(i));
  }
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_GT(msg.dropped, 0u);
  EXPECT_EQ(first + 1 + msg.dropped, msg.ordinal);
  uint64_t last = msg.ordinal;
  std::string last_text = Text(msg);
  int received = 1;
  for (;;) {
    ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
    if (msg.length == 0) {
      break;
    }
    EXPECT_EQ(last + 1, msg.ordinal);
    EXPECT_EQ(0u, msg.dropped);
    last = msg.ordinal;
    last_text = Text(msg);
    received++;
  }
  EXPECT_EQ("m9", last_text);
  EXPECT_EQ(first + 10, last);
  EXPECT_LT(received, 10);
}

TEST_F(AsilClientTest, StandardSubscriberSeesAsilDrops) {
  absl::StatusOr<subspace::Subscriber> sub =
      Std().CreateSubscriber("/asil/drops_out");
  ASSERT_OK(sub);
  int64_t dropped = 0;
  ASSERT_OK(sub->RegisterDroppedMessageCallback(
      [&dropped](subspace::Subscriber *, int64_t n) { dropped += n; }));

  asil::Publisher pub;
  ASSERT_EQ(
      asil::Error::kOk,
      Asil().CreatePublisher("/asil/drops_out", AsilPubOptions(64, 4), pub));
  AsilPublish(pub, "first");
  EXPECT_EQ("first", StdRead(*sub).text);

  for (int i = 0; i < 10; i++) {
    AsilPublish(pub, "m" + std::to_string(i));
  }
  int received = 0;
  std::string last;
  for (;;) {
    const StdMessage msg = StdRead(*sub);
    if (msg.text.empty()) {
      break;
    }
    last = msg.text;
    received++;
  }
  EXPECT_EQ("m9", last);
  EXPECT_GT(dropped, 0);
  EXPECT_EQ(10, received + dropped);
}

TEST_F(AsilClientTest, Activation) {
  absl::StatusOr<subspace::Subscriber> std_sub = Std().CreateSubscriber(
      "/asil/activate", subspace::SubscriberOptions().SetPassActivation(true));
  ASSERT_OK(std_sub);
  asil::Subscriber asil_sub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/asil/activate", {}, asil_sub));

  asil::Publisher pub;
  asil::PublisherOptions options = AsilPubOptions(64, 4);
  options.activate = true;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreatePublisher("/asil/activate", options, pub));
  AsilPublish(pub, "payload");

  EXPECT_TRUE(StdRead(*std_sub).is_activation);
  StdMessage msg = StdRead(*std_sub);
  EXPECT_FALSE(msg.is_activation);
  EXPECT_EQ("payload", msg.text);

  asil::Message asil_msg;
  ASSERT_EQ(asil::Error::kOk, asil_sub.ReadMessage(asil_msg));
  EXPECT_FALSE(asil_msg.is_activation);
  EXPECT_EQ("payload", Text(asil_msg));
  EXPECT_EQ(0u, asil_msg.dropped);
}

TEST_F(AsilClientTest, ManyMessagesThroughFewSlots) {
  asil::Subscriber asil_sub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/asil/cycle", {}, asil_sub));
  absl::StatusOr<subspace::Subscriber> std_sub =
      Std().CreateSubscriber("/asil/cycle");
  ASSERT_OK(std_sub);
  asil::Publisher pub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreatePublisher("/asil/cycle", AsilPubOptions(64, 4), pub));

  for (int i = 0; i < 200; i++) {
    const std::string text = "message " + std::to_string(i);
    AsilPublish(pub, text);
    asil::Message msg;
    ASSERT_EQ(asil::Error::kOk, asil_sub.ReadMessage(msg));
    ASSERT_EQ(text, Text(msg));
    ASSERT_EQ(0u, msg.dropped);
    ASSERT_EQ(text, StdRead(*std_sub).text);
  }
}

TEST_F(AsilClientTest, HeldMessageIsNotOverwritten) {
  asil::Subscriber sub;
  ASSERT_EQ(asil::Error::kOk, Asil().CreateSubscriber("/asil/hold", {}, sub));
  absl::StatusOr<subspace::Publisher> pub =
      Std().CreatePublisher("/asil/hold", 64, 4, FixedSize());
  ASSERT_OK(pub);
  StdPublish(*pub, "held");
  asil::Message held;
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(held));
  ASSERT_EQ("held", Text(held));

  for (int i = 0; i < 20; i++) {
    StdPublish(*pub, "overwrite " + std::to_string(i));
  }
  EXPECT_EQ("held", Text(held));
  sub.ReleaseMessage();

  asil::Message msg;
  ASSERT_EQ(asil::Error::kOk,
            sub.ReadMessage(msg, asil::ReadMode::kReadNewest));
  EXPECT_EQ("overwrite 19", Text(msg));
}

TEST_F(AsilClientTest, Rejections) {
  asil::Publisher pub;
  EXPECT_EQ(
      asil::Error::kServerRejected,
      Asil().CreatePublisher("/asil/reject", AsilPubOptions(128, 4), pub));
  EXPECT_THAT(Asil().LastServerError(), HasSubstr("has 4 slots of 64 bytes"));
  EXPECT_FALSE(pub.IsOpen());

  EXPECT_EQ(
      asil::Error::kServerRejected,
      Asil().CreatePublisher("/asil/unknown", AsilPubOptions(64, 4), pub));
  EXPECT_THAT(Asil().LastServerError(),
              HasSubstr("isn't in the static channel config"));

  asil::Subscriber sub;
  EXPECT_EQ(asil::Error::kServerRejected,
            Asil().CreateSubscriber("/asil/unknown", {}, sub));

  EXPECT_EQ(asil::Error::kInvalidArgument,
            Asil().CreatePublisher("/asil/reject", AsilPubOptions(0, 4), pub));
  EXPECT_EQ(asil::Error::kCapacityExceeded,
            Asil().CreatePublisher(std::string(300, 'x').c_str(),
                                   AsilPubOptions(64, 4), pub));

  ASSERT_EQ(asil::Error::kOk,
            Asil().CreatePublisher("/asil/reject", AsilPubOptions(64, 4), pub));
  EXPECT_EQ(asil::Error::kAlreadyInitialized,
            Asil().CreatePublisher("/asil/reject", AsilPubOptions(64, 4), pub));
  EXPECT_EQ(asil::Error::kMessageTooLarge, pub.Publish(65));

  asil::Client uninitialized;
  asil::Publisher other;
  EXPECT_EQ(asil::Error::kNotInitialized,
            uninitialized.CreatePublisher("/asil/reject", AsilPubOptions(64, 4),
                                          other));
}

TEST_F(AsilClientTest, CloseRemovesFromServer) {
  asil::Publisher pub;
  asil::Subscriber sub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreatePublisher("/asil/close", AsilPubOptions(64, 4), pub));
  ASSERT_EQ(asil::Error::kOk, Asil().CreateSubscriber("/asil/close", {}, sub));
  absl::StatusOr<const subspace::ChannelInfo> info =
      Std().GetChannelInfo("/asil/close");
  ASSERT_OK(info);
  EXPECT_EQ(1, info->num_publishers);
  EXPECT_EQ(1, info->num_subscribers);

  EXPECT_EQ(asil::Error::kOk, pub.Close());
  EXPECT_EQ(asil::Error::kOk, sub.Close());
  EXPECT_FALSE(pub.IsOpen());
  EXPECT_EQ(asil::Error::kOk, pub.Close());
  absl::StatusOr<const subspace::ChannelInfo> closed_info =
      Std().GetChannelInfo("/asil/close");
  ASSERT_OK(closed_info);
  EXPECT_EQ(0, closed_info->num_publishers);
  EXPECT_EQ(0, closed_info->num_subscribers);

  // The channel can be used again.
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreatePublisher("/asil/close", AsilPubOptions(64, 4), pub));
  ASSERT_EQ(asil::Error::kOk, Asil().CreateSubscriber("/asil/close", {}, sub));
  AsilPublish(pub, "again");
  asil::Message msg;
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_EQ("again", Text(msg));
}

TEST_F(AsilClientTest, HandshakeMakesNoHeapAllocations) {
  asil::Publisher pub;
  asil::Subscriber sub;
  allocations = 0;
  counting_allocations = true;
  int *volatile counted = new int(1);
  counting_allocations = false;
  delete counted;
  ASSERT_EQ(1, allocations);

  allocations = 0;
  counting_allocations = true;
  const asil::Error pub_error =
      Asil().CreatePublisher("/asil/no_alloc", AsilPubOptions(64, 4), pub);
  const asil::Error sub_error =
      Asil().CreateSubscriber("/asil/no_alloc", {}, sub);
  const asil::Error pub_close = pub.Close();
  const asil::Error sub_close = sub.Close();
  counting_allocations = false;

  EXPECT_EQ(asil::Error::kOk, pub_error);
  EXPECT_EQ(asil::Error::kOk, sub_error);
  EXPECT_EQ(asil::Error::kOk, pub_close);
  EXPECT_EQ(asil::Error::kOk, sub_close);
  EXPECT_EQ(0, allocations);
}

TEST_F(AsilClientTest, OversizedRequestKeepsTheConnection) {
  const std::string huge(asil::PhaserServerConnection::kWireBufferSize, 'x');
  asil::TriggersReply reply;
  EXPECT_EQ(asil::Error::kCapacityExceeded,
            Connection().GetTriggers(huge.c_str(), reply));
  EXPECT_TRUE(Connection().Connected());

  asil::TriggerFdList subscribers;
  reply.subscriber_triggers = &subscribers;
  EXPECT_EQ(asil::Error::kOk,
            Connection().GetTriggers("/asil/oversize", reply));
}

// Publishes "<prefix><n>" for n from first until the reliable publisher has
// no free slot, and returns the number published.
int FillReliable(asil::Publisher &pub, const std::string &prefix, int first) {
  int n = first;
  for (; n < first + 100; n++) {
    void *buffer = pub.Buffer();
    if (buffer == nullptr) {
      break;
    }
    const std::string text = prefix + std::to_string(n);
    memcpy(buffer, text.data(), text.size());
    EXPECT_EQ(asil::Error::kOk, pub.Publish(text.size()));
  }
  return n - first;
}

asil::PublisherOptions ReliablePubOptions(int64_t slot_size,
                                          int32_t num_slots) {
  asil::PublisherOptions options = AsilPubOptions(slot_size, num_slots);
  options.reliable = true;
  return options;
}

TEST_F(AsilClientTest, ReliableAsilPublisherStandardSubscriber) {
  absl::StatusOr<subspace::Subscriber> sub = Std().CreateSubscriber(
      "/asil/rel_to_std", subspace::SubscriberOptions().SetReliable(true));
  ASSERT_OK(sub);
  asil::Publisher pub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreatePublisher("/asil/rel_to_std",
                                   ReliablePubOptions(64, 4), pub));
  EXPECT_TRUE(pub.IsReliable());

  // The activation message and the unread messages fill the channel.
  const int published = FillReliable(pub, "r", 0);
  EXPECT_EQ(3, published);
  EXPECT_EQ(asil::Error::kTimeout, pub.Wait(10));

  // Reading frees slots and wakes the publisher.
  EXPECT_EQ("r0", StdRead(*sub).text);
  EXPECT_EQ(asil::Error::kOk, pub.Wait(1000));
  const int more = FillReliable(pub, "r", published);
  EXPECT_GT(more, 0);
  for (int i = 1; i < published + more; i++) {
    EXPECT_EQ("r" + std::to_string(i), StdRead(*sub).text);
  }
  EXPECT_EQ("", StdRead(*sub).text);
}

TEST_F(AsilClientTest, StandardReliablePublisherAsilSubscriber) {
  asil::Subscriber sub;
  asil::SubscriberOptions options;
  options.reliable = true;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/asil/rel_from_std", options, sub));
  EXPECT_TRUE(sub.IsReliable());
  absl::StatusOr<subspace::Publisher> pub = Std().CreatePublisher(
      "/asil/rel_from_std", 64, 4, FixedSize().SetReliable(true));
  ASSERT_OK(pub);

  int published = 0;
  for (; published < 100; published++) {
    absl::StatusOr<void *> buffer = pub->GetMessageBuffer();
    ASSERT_OK(buffer);
    if (*buffer == nullptr) {
      break;
    }
    const std::string text = "s" + std::to_string(published);
    memcpy(*buffer, text.data(), text.size());
    ASSERT_OK(pub->PublishMessage(static_cast<int64_t>(text.size())));
  }
  EXPECT_EQ(3, published);
  EXPECT_FALSE(Readable(pub->GetPollFd().fd));

  asil::Message msg;
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_EQ("s0", Text(msg));
  EXPECT_FALSE(msg.is_activation);
  EXPECT_TRUE(Readable(pub->GetPollFd().fd));
  StdPublish(*pub, "s3");

  uint64_t last = msg.ordinal;
  for (int i = 1; i <= published; i++) {
    ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
    EXPECT_EQ("s" + std::to_string(i), Text(msg));
    EXPECT_EQ(last + 1, msg.ordinal);
    EXPECT_EQ(0u, msg.dropped);
    last = msg.ordinal;
  }
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_EQ(0u, msg.length);
}

TEST_F(AsilClientTest, ReliableReadNewestFreesSkippedSlots) {
  asil::SubscriberOptions options;
  options.reliable = true;
  asil::Subscriber sub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/asil/rel_newest", options, sub));
  asil::Publisher pub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreatePublisher("/asil/rel_newest",
                                   ReliablePubOptions(64, 4), pub));
  ASSERT_EQ(3, FillReliable(pub, "n", 0));

  asil::Message msg;
  ASSERT_EQ(asil::Error::kOk,
            sub.ReadMessage(msg, asil::ReadMode::kReadNewest));
  EXPECT_EQ("n2", Text(msg));
  EXPECT_EQ(asil::Error::kOk, pub.Wait(1000));

  // The skipped messages no longer hold back the publisher.  The held message
  // keeps its slot.
  EXPECT_EQ(3, FillReliable(pub, "m", 0));
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
    EXPECT_EQ("m" + std::to_string(i), Text(msg));
  }
}

TEST_F(AsilClientTest, ReliablePublisherWaitsForSubscriber) {
  asil::Publisher pub;
  ASSERT_EQ(
      asil::Error::kOk,
      Asil().CreatePublisher("/asil/rel_late", ReliablePubOptions(64, 4), pub));
  EXPECT_EQ(nullptr, pub.Buffer());

  absl::StatusOr<subspace::Subscriber> sub = Std().CreateSubscriber(
      "/asil/rel_late", subspace::SubscriberOptions().SetReliable(true));
  ASSERT_OK(sub);
  AsilPublish(pub, "hello");
  EXPECT_EQ("hello", StdRead(*sub).text);
}

TEST_F(AsilClientTest, VirtualChannels) {
  asil::Publisher rejected;
  EXPECT_EQ(
      asil::Error::kServerRejected,
      Asil().CreatePublisher("/asil/v1", AsilPubOptions(128, 8), rejected));

  asil::Subscriber mux_sub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/asil/mux", {}, mux_sub));
  asil::SubscriberOptions v2_options;
  v2_options.mux = "/asil/mux";
  asil::Subscriber v2_sub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/asil/v2", v2_options, v2_sub));
  EXPECT_EQ(2, v2_sub.VirtualChannelId());
  absl::StatusOr<subspace::Subscriber> v1_sub = Std().CreateSubscriber(
      "/asil/v1", subspace::SubscriberOptions().SetMux("/asil/mux"));
  ASSERT_OK(v1_sub);

  asil::PublisherOptions v1_options = AsilPubOptions(128, 8);
  v1_options.mux = "/asil/mux";
  asil::Publisher v1_pub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreatePublisher("/asil/v1", v1_options, v1_pub));
  EXPECT_EQ(1, v1_pub.VirtualChannelId());
  absl::StatusOr<subspace::Publisher> v2_pub = Std().CreatePublisher(
      "/asil/v2", 128, 8, FixedSize().SetMux("/asil/mux"));
  ASSERT_OK(v2_pub);

  AsilPublish(v1_pub, "a1");
  StdPublish(*v2_pub, "b1");
  AsilPublish(v1_pub, "a2");

  EXPECT_EQ("a1", StdRead(*v1_sub).text);
  EXPECT_EQ("a2", StdRead(*v1_sub).text);
  EXPECT_EQ("", StdRead(*v1_sub).text);

  asil::Message msg;
  ASSERT_EQ(asil::Error::kOk, v2_sub.ReadMessage(msg));
  EXPECT_EQ("b1", Text(msg));
  EXPECT_EQ(2, msg.vchan_id);
  ASSERT_EQ(asil::Error::kOk, v2_sub.ReadMessage(msg));
  EXPECT_EQ(0u, msg.length);

  // Each virtual channel has its own ordinals, so interleaving is no drop.
  const struct {
    const char *text;
    int vchan_id;
  } expected[] = {{"a1", 1}, {"b1", 2}, {"a2", 1}};
  for (const auto &e : expected) {
    ASSERT_EQ(asil::Error::kOk, mux_sub.ReadMessage(msg));
    EXPECT_EQ(e.text, Text(msg));
    EXPECT_EQ(e.vchan_id, msg.vchan_id);
    EXPECT_EQ(0u, msg.dropped);
  }
  ASSERT_EQ(asil::Error::kOk, mux_sub.ReadMessage(msg));
  EXPECT_EQ(0u, msg.length);
}

TEST_F(AsilClientTest, MultipleActiveMessages) {
  asil::SubscriberOptions options;
  options.max_active_messages = 3;
  asil::Subscriber sub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/asil/multi", options, sub));
  absl::StatusOr<subspace::Publisher> pub =
      Std().CreatePublisher("/asil/multi", 64, 8, FixedSize());
  ASSERT_OK(pub);
  for (const char *text : {"a", "b", "c", "d"}) {
    StdPublish(*pub, text);
  }

  asil::Message a, b, c, d;
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(a));
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(b));
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(c));
  EXPECT_EQ(3, sub.NumActiveMessages());
  EXPECT_EQ(asil::Error::kActiveMessageLimit, sub.ReadMessage(d));
  EXPECT_EQ(0u, d.length);

  // Held messages survive the publisher cycling through the other slots.
  for (int i = 0; i < 20; i++) {
    StdPublish(*pub, "x" + std::to_string(i));
  }
  EXPECT_EQ("a", Text(a));
  EXPECT_EQ("b", Text(b));
  EXPECT_EQ("c", Text(c));

  EXPECT_EQ(asil::Error::kOk, sub.ReleaseMessage(b));
  EXPECT_EQ(2, sub.NumActiveMessages());
  EXPECT_NE(asil::Error::kOk, sub.ReleaseMessage(b));
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(d));
  EXPECT_FALSE(Text(d).empty());
  EXPECT_EQ(3, sub.NumActiveMessages());
  EXPECT_EQ("a", Text(a));
  sub.ReleaseMessage();
  EXPECT_EQ(0, sub.NumActiveMessages());
}

TEST_F(AsilClientTest, SubscriberOpensBeforeAnyPublisher) {
  // The server created the channel's buffer, so the subscriber maps it now.
  asil::Subscriber sub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/asil/early_sub", {}, sub));
  asil::Message msg;
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_EQ(0u, msg.length);

  absl::StatusOr<subspace::Publisher> pub =
      Std().CreatePublisher("/asil/early_sub", 64, 4, FixedSize());
  ASSERT_OK(pub);
  StdPublish(*pub, "early");
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_EQ("early", Text(msg));
}

asil::PublisherOptions SplitPubOptions(int64_t slot_size, int32_t num_slots,
                                       asil::SplitSlot *slots) {
  asil::PublisherOptions options = AsilPubOptions(slot_size, num_slots);
  options.use_split_buffers = true;
  options.split_slots = slots;
  options.split_slot_capacity = num_slots;
  return options;
}

asil::SubscriberOptions SplitSubOptions(asil::SplitSlot *slots,
                                        int32_t capacity) {
  asil::SubscriberOptions options;
  options.split_slots = slots;
  options.split_slot_capacity = capacity;
  return options;
}

TEST_F(AsilClientTest, SplitAsilPublisherStandardSubscriber) {
  absl::StatusOr<subspace::Subscriber> std_sub =
      Std().CreateSubscriber("/asil/split_to_std");
  ASSERT_OK(std_sub);
  EXPECT_TRUE(std_sub->UsesSplitBuffers());
  asil::SplitSlot sub_slots[4];
  asil::Subscriber asil_sub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/asil/split_to_std",
                                    SplitSubOptions(sub_slots, 4), asil_sub));

  asil::SplitSlot pub_slots[4];
  asil::Publisher pub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreatePublisher("/asil/split_to_std",
                                   SplitPubOptions(256, 4, pub_slots), pub))
      << Asil().LastServerError();
  EXPECT_GE(pub.SlotSize(), 256);
  for (const asil::SplitSlot &slot : pub_slots) {
    EXPECT_NE(nullptr, slot.mapping.address);
    EXPECT_FALSE(slot.allocator_mapped);
  }

  asil::Message msg;
  for (int i = 0; i < 10; i++) {
    const std::string text = "split " + std::to_string(i);
    AsilPublish(pub, text);
    EXPECT_EQ(text, StdRead(*std_sub).text);
    ASSERT_EQ(asil::Error::kOk, asil_sub.ReadMessage(msg));
    EXPECT_EQ(text, Text(msg));
    EXPECT_EQ(sub_slots[msg.slot_id].mapping.address, msg.data);
  }
  pub.Close();
  asil_sub.Close();
  for (const asil::SplitSlot &slot : sub_slots) {
    EXPECT_EQ(nullptr, slot.mapping.address);
  }
}

TEST_F(AsilClientTest, SplitStandardPublisherAsilSubscriber) {
  asil::SplitSlot slots[4];
  asil::SubscriberOptions options = SplitSubOptions(slots, 4);
  options.checksum = true;
  asil::Subscriber sub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/asil/split_from_std", options, sub));
  absl::StatusOr<subspace::Publisher> pub = Std().CreatePublisher(
      "/asil/split_from_std", 256, 4,
      FixedSize().SetUseSplitBuffers(true).SetChecksum(true));
  ASSERT_OK(pub);

  asil::Message msg;
  for (int i = 0; i < 10; i++) {
    const std::string text = "from std " + std::to_string(i);
    StdPublish(*pub, text);
    ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
    EXPECT_EQ(text, Text(msg));
    EXPECT_FALSE(msg.checksum_error);
  }
}

TEST_F(AsilClientTest, SplitVirtualChannel) {
  asil::SplitSlot sub_slots[8];
  asil::SubscriberOptions sub_options = SplitSubOptions(sub_slots, 8);
  sub_options.mux = "/asil/split_mux";
  asil::Subscriber sub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/asil/split_v", sub_options, sub));
  EXPECT_EQ(4, sub.VirtualChannelId());

  asil::SplitSlot pub_slots[8];
  asil::PublisherOptions pub_options = SplitPubOptions(128, 8, pub_slots);
  pub_options.mux = "/asil/split_mux";
  asil::Publisher pub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreatePublisher("/asil/split_v", pub_options, pub))
      << Asil().LastServerError();
  AsilPublish(pub, "vchan");
  asil::Message msg;
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_EQ("vchan", Text(msg));
  EXPECT_EQ(4, msg.vchan_id);
}

TEST_F(AsilClientTest, SplitRejections) {
  asil::Subscriber sub;
  EXPECT_EQ(asil::Error::kInvalidArgument,
            Asil().CreateSubscriber("/asil/split_reject", {}, sub));
  asil::SplitSlot slots[4];
  EXPECT_EQ(asil::Error::kCapacityExceeded,
            Asil().CreateSubscriber("/asil/split_reject",
                                    SplitSubOptions(slots, 3), sub));
  EXPECT_FALSE(sub.IsOpen());

  asil::Publisher pub;
  EXPECT_EQ(
      asil::Error::kServerRejected,
      Asil().CreatePublisher("/asil/split_reject", AsilPubOptions(64, 4), pub));
  EXPECT_THAT(Asil().LastServerError(), HasSubstr("uses split buffers"));
  EXPECT_EQ(asil::Error::kServerRejected,
            Asil().CreatePublisher("/asil/reject",
                                   SplitPubOptions(64, 4, slots), pub));
  EXPECT_THAT(Asil().LastServerError(), HasSubstr("doesn't use split buffers"));
}

TEST_F(AsilDynamicTest, AsilPublisherCreatesChannel) {
  asil::Publisher pub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreatePublisher("/dyn/create", AsilPubOptions(64, 8), pub));
  asil::Subscriber asil_sub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/dyn/create", {}, asil_sub));
  absl::StatusOr<subspace::Subscriber> std_sub =
      Std().CreateSubscriber("/dyn/create");
  ASSERT_OK(std_sub);
  asil::Publisher second;
  ASSERT_EQ(asil::Error::kOk, Asil().CreatePublisher(
                                  "/dyn/create", AsilPubOptions(64, 8), second))
      << Asil().LastServerError();
  absl::StatusOr<subspace::Publisher> std_pub =
      Std().CreatePublisher("/dyn/create", 64, 8, FixedSize());
  ASSERT_OK(std_pub);

  AsilPublish(pub, "one");
  AsilPublish(second, "two");
  StdPublish(*std_pub, "three");
  asil::Message msg;
  for (const char *text : {"one", "two", "three"}) {
    EXPECT_EQ(text, StdRead(*std_sub).text);
    ASSERT_EQ(asil::Error::kOk, asil_sub.ReadMessage(msg));
    EXPECT_EQ(text, Text(msg));
  }
}

TEST_F(AsilDynamicTest, SubscriberNeedsBuffers) {
  asil::Subscriber sub;
  EXPECT_EQ(asil::Error::kNoBuffers,
            Asil().CreateSubscriber("/dyn/placeholder", {}, sub));
  EXPECT_FALSE(sub.IsOpen());

  absl::StatusOr<subspace::Publisher> pub =
      Std().CreatePublisher("/dyn/placeholder", 64, 4);
  ASSERT_OK(pub);
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/dyn/placeholder", {}, sub));
  StdPublish(*pub, "first");
  EXPECT_EQ(asil::Error::kOk, sub.Wait(1000));
  asil::Message msg;
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_EQ("first", Text(msg));
}

TEST_F(AsilDynamicTest, SubscriberSkipsMessagesAfterResize) {
  asil::Subscriber sub;
  absl::StatusOr<subspace::Publisher> pub =
      Std().CreatePublisher("/dyn/resize", 64, 4);
  ASSERT_OK(pub);
  ASSERT_EQ(asil::Error::kOk, Asil().CreateSubscriber("/dyn/resize", {}, sub));
  StdPublish(*pub, "small");

  const std::string big(1000, 'B');
  absl::StatusOr<void *> buffer = pub->GetMessageBuffer(1000);
  ASSERT_OK(buffer);
  memcpy(*buffer, big.data(), big.size());
  ASSERT_OK(pub->PublishMessage(static_cast<int64_t>(big.size())));
  StdPublish(*pub, "after");

  // The subscriber maps only the buffer that existed when it opened.
  asil::Message msg;
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_EQ("small", Text(msg));
  EXPECT_EQ(asil::Error::kBufferNotMapped, sub.ReadMessage(msg));
  EXPECT_EQ(0u, msg.length);
  EXPECT_EQ(asil::Error::kBufferNotMapped, sub.ReadMessage(msg));
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_EQ(0u, msg.length);

  // A subscriber that opens after the resize maps the new buffer.
  asil::Subscriber later;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/dyn/resize", {}, later));
  EXPECT_EQ(asil::Error::kBufferNotMapped, later.ReadMessage(msg));
  ASSERT_EQ(asil::Error::kOk, later.ReadMessage(msg));
  EXPECT_EQ(big, Text(msg));
  ASSERT_EQ(asil::Error::kOk, later.ReadMessage(msg));
  EXPECT_EQ("after", Text(msg));
  for (int i = 0; i < 10; i++) {
    StdPublish(*pub, "later " + std::to_string(i));
    ASSERT_EQ(asil::Error::kOk, later.ReadMessage(msg));
    ASSERT_EQ("later " + std::to_string(i), Text(msg));
  }
}

TEST_F(AsilDynamicTest, AsilPublisherJoinsResizedChannel) {
  asil::Subscriber sub;
  {
    absl::StatusOr<subspace::Publisher> std_pub =
        Std().CreatePublisher("/dyn/rejoin", 64, 4);
    ASSERT_OK(std_pub);
    StdPublish(*std_pub, "small");
    absl::StatusOr<void *> buffer = std_pub->GetMessageBuffer(1000);
    ASSERT_OK(buffer);
    ASSERT_OK(std_pub->PublishMessage(1000));
    ASSERT_EQ(asil::Error::kOk,
              Asil().CreateSubscriber("/dyn/rejoin", {}, sub));
  }

  // The publisher maps the newest buffer, which the subscriber also mapped.
  asil::Publisher pub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreatePublisher("/dyn/rejoin", AsilPubOptions(64, 4), pub));
  EXPECT_GE(pub.SlotSize(), 1000);
  AsilPublish(pub, "asil");

  asil::Message msg;
  EXPECT_EQ(asil::Error::kBufferNotMapped, sub.ReadMessage(msg));
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_EQ(1000u, msg.length);
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
  EXPECT_EQ("asil", Text(msg));
}

TEST_F(AsilDynamicTest, AsilPublisherFillsSubscriberQueues) {
  asil::PublisherOptions options = AsilPubOptions(64, 8);
  options.subscriber_queue_arena_size = 4096;
  asil::Publisher pub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreatePublisher("/dyn/queue", options, pub));
  absl::StatusOr<subspace::Subscriber> sub = Std().CreateSubscriber(
      "/dyn/queue", subspace::SubscriberOptions().SetSubscriberQueueSize(2));
  ASSERT_OK(sub);
  asil::Subscriber asil_sub;
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/dyn/queue", {}, asil_sub));

  for (int i = 0; i < 5; i++) {
    AsilPublish(pub, "q" + std::to_string(i));
  }
  // A two entry queue keeps the newest two messages, so they are read first.
  EXPECT_EQ("q3", StdRead(*sub).text);
  EXPECT_EQ("q4", StdRead(*sub).text);

  asil::Message msg;
  for (int i = 0; i < 5; i++) {
    ASSERT_EQ(asil::Error::kOk, asil_sub.ReadMessage(msg));
    EXPECT_EQ("q" + std::to_string(i), Text(msg));
  }

  asil::Publisher wrong_arena;
  EXPECT_EQ(
      asil::Error::kServerRejected,
      Asil().CreatePublisher("/dyn/queue", AsilPubOptions(64, 8), wrong_arena));
}

// Payloads that a standard publisher's custom allocator put on the heap.
// Clients in one process can share them by address.
struct HeapAllocations {
  std::map<uintptr_t, std::unique_ptr<char[]>> memory;
  uintptr_t next_handle = 0;
  int maps = 0;
  int unmaps = 0;
};

subspace::SplitBufferCallbacks HeapCallbacks(HeapAllocations &heap) {
  subspace::SplitBufferCallbacks callbacks;
  callbacks.allocate = [&heap](const subspace::SplitBufferMetadata &metadata)
      -> absl::StatusOr<subspace::SplitBufferMapping> {
    const uintptr_t handle = ++heap.next_handle;
    auto &memory = heap.memory[handle];
    memory = std::make_unique<char[]>(metadata.allocation_size);
    subspace::SplitBufferMapping mapping;
    mapping.handle = handle;
    mapping.address = memory.get();
    mapping.size = static_cast<size_t>(metadata.allocation_size);
    return mapping;
  };
  callbacks.unmap = [](const subspace::SplitBufferMetadata &,
                       const subspace::SplitBufferMapping &) {
    return absl::OkStatus();
  };
  callbacks.free = [](const subspace::SplitBufferMetadata &,
                      const subspace::SplitBufferMapping &) {
    return absl::OkStatus();
  };
  return callbacks;
}

asil::Error HeapMap(void *context, const asil::SplitBufferInfo &info,
                    asil::SplitBufferMapping &mapping) {
  auto *heap = static_cast<HeapAllocations *>(context);
  auto it = heap->memory.find(static_cast<uintptr_t>(info.handle));
  if (it == heap->memory.end()) {
    return asil::Error::kInvalidArgument;
  }
  heap->maps++;
  mapping.address = it->second.get();
  mapping.size = info.allocation_size;
  return asil::Error::kOk;
}

void HeapUnmap(void *context, const asil::SplitBufferInfo &,
               const asil::SplitBufferMapping &) {
  static_cast<HeapAllocations *>(context)->unmaps++;
}

TEST_F(AsilDynamicTest, SplitCustomAllocator) {
  HeapAllocations heap;
  asil::SplitSlot slots[4];
  asil::SubscriberOptions options;
  options.split_slots = slots;
  options.split_slot_capacity = 4;
  options.split_allocator.map = HeapMap;
  options.split_allocator.unmap = HeapUnmap;
  options.split_allocator.context = &heap;

  // There is nothing to map until the publisher creates the buffers.
  asil::Subscriber sub;
  EXPECT_EQ(asil::Error::kNoBuffers,
            Asil().CreateSubscriber("/dyn/custom", options, sub));

  absl::StatusOr<subspace::Publisher> pub = Std().CreatePublisher(
      "/dyn/custom", subspace::PublisherOptions()
                         .SetSlotSize(64)
                         .SetNumSlots(4)
                         .SetFixedSize(true)
                         .SetUseSplitBuffers(true)
                         .SetSplitBufferCallbacks(HeapCallbacks(heap)));
  ASSERT_OK(pub);

  asil::Subscriber no_allocator;
  asil::SubscriberOptions without = options;
  without.split_allocator = asil::SplitBufferAllocator();
  EXPECT_EQ(asil::Error::kUnsupported,
            Asil().CreateSubscriber("/dyn/custom", without, no_allocator));

  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/dyn/custom", options, sub));
  EXPECT_EQ(4, heap.maps);
  for (const asil::SplitSlot &slot : slots) {
    EXPECT_TRUE(slot.allocator_mapped);
    EXPECT_EQ(asil::BufferAllocator::kSplitCallback, slot.info.allocator);
  }

  asil::Message msg;
  for (int i = 0; i < 8; i++) {
    const std::string text = "heap " + std::to_string(i);
    StdPublish(*pub, text);
    ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg));
    EXPECT_EQ(text, Text(msg));
  }
  sub.Close();
  EXPECT_EQ(4, heap.unmaps);
}

} // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
