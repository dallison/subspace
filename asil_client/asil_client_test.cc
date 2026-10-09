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
#include "asil_client/subscriber.h"
#include "server/static_config.h"
#include <cstdlib>
#include <cstring>
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
  channels { name: "/asil/no_alloc" slot_size: 64 num_slots: 4 }
  channels { name: "/asil/oversize" slot_size: 64 num_slots: 4 }
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

class AsilClientTest : public ::testing::Test {
public:
  static void SetUpTestSuite() {
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
    absl::StatusOr<subspace::StaticChannelConfig> config =
        subspace::ParseStaticChannelConfig(kConfig);
    ASSERT_OK(config);
    ASSERT_OK(server_->SetStaticChannelConfig(*std::move(config)));

    server_thread_ = std::thread([]() {
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

  static void TearDownTestSuite() {
    server_->Stop();

    char buf[8];
    (void)::read(server_pipe_[0], buf, 8);
    server_thread_.join();
    server_->CleanupAfterSession();
    (void)remove(socket_.c_str());
  }

  void SetUp() override {
#if ASIL_SHM_MODE == ASIL_SHM_MEMFD
    GTEST_SKIP() << "The ASIL client does not support the memfd backend";
#endif
    signal(SIGPIPE, SIG_IGN);
    ASSERT_OK(std_client_.Init(socket_));
    ASSERT_EQ(asil::Error::kOk, connection_.Connect(socket_.c_str()));
    ASSERT_EQ(asil::Error::kOk, asil_client_.Init(connection_, "asil_test"));
  }

  subspace::Client &Std() { return std_client_; }
  asil::Client &Asil() { return asil_client_; }
  asil::PhaserServerConnection &Connection() { return connection_; }

private:
  inline static subspace::async::RuntimeEngine engine_;
  inline static std::string socket_;
  inline static int server_pipe_[2];
  inline static std::unique_ptr<subspace::Server> server_;
  inline static std::thread server_thread_;

  subspace::Client std_client_;
  asil::PhaserServerConnection connection_;
  asil::Client asil_client_;
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
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/asil/wait", {}, sub));
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
  ASSERT_EQ(asil::Error::kOk, Asil().CreatePublisher(
                                  "/asil/late_sub", AsilPubOptions(64, 4), pub));

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
  ASSERT_EQ(asil::Error::kOk, Asil().CreateSubscriber("/asil/sum_from_std",
                                                      strict, strict_sub));
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
  ASSERT_EQ(asil::Error::kOk,
            Asil().CreateSubscriber("/asil/newest", {}, sub));
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
  ASSERT_EQ(asil::Error::kOk, Asil().CreatePublisher(
                                  "/asil/drops_out", AsilPubOptions(64, 4), pub));
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
  ASSERT_EQ(asil::Error::kOk, sub.ReadMessage(msg, asil::ReadMode::kReadNewest));
  EXPECT_EQ("overwrite 19", Text(msg));
}

TEST_F(AsilClientTest, Rejections) {
  asil::Publisher pub;
  EXPECT_EQ(asil::Error::kServerRejected,
            Asil().CreatePublisher("/asil/reject", AsilPubOptions(128, 4), pub));
  EXPECT_THAT(Asil().LastServerError(), HasSubstr("has 4 slots of 64 bytes"));
  EXPECT_FALSE(pub.IsOpen());

  EXPECT_EQ(asil::Error::kServerRejected,
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
            uninitialized.CreatePublisher("/asil/reject",
                                          AsilPubOptions(64, 4), other));
}

TEST_F(AsilClientTest, VirtualChannelsAreUnsupported) {
  asil::Subscriber sub;
  const asil::Error e = Asil().CreateSubscriber("/asil/v1", {}, sub);
  EXPECT_NE(asil::Error::kOk, e);
  EXPECT_FALSE(sub.IsOpen());
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

} // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
