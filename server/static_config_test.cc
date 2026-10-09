// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// Tests for a server started with a static channel config: the configured
// channels exist before any client connects, they are the only channels
// clients can use, and their layout is fixed.

#include "client/test_fixture.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "server/static_config.h"
#include <chrono>
#include <cstring>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

using ::testing::HasSubstr;

constexpr char kConfig[] = R"pb(
  multiplexers { name: "/static/mux" slot_size: 128 num_slots: 8 type: "Mux" }
  channels { name: "/static/a" slot_size: 256 num_slots: 8 type: "A" }
  channels { name: "/static/meta" slot_size: 64 num_slots: 4 metadata_size: 16 }
  channels { name: "/static/v1" mux: "/static/mux" vchan_id: 3 }
  channels { name: "/static/v2" mux: "/static/mux" vchan_id: 7 }
  channels {
    name: "/static/split"
    slot_size: 256
    num_slots: 4
    use_split_buffers: true
  }
  multiplexers {
    name: "/static/split_mux"
    slot_size: 128
    num_slots: 4
    use_split_buffers: true
  }
  channels { name: "/static/sv" mux: "/static/split_mux" vchan_id: 1 }
)pb";

subspace::PublisherOptions FixedSize() {
  return subspace::PublisherOptions().SetFixedSize(true);
}

void Publish(subspace::Publisher &pub, const std::string &text) {
  absl::StatusOr<void *> buffer = pub.GetMessageBuffer();
  ASSERT_OK(buffer);
  memcpy(*buffer, text.data(), text.size());
  ASSERT_OK(pub.PublishMessage(static_cast<int64_t>(text.size())));
}

class StaticConfigTest : public ::testing::Test {
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
    signal(SIGPIPE, SIG_IGN);
    ASSERT_OK(client_.Init(socket_));
  }

  subspace::Client &Client() { return client_; }

private:
  inline static subspace::async::RuntimeEngine engine_;
  inline static std::string socket_;
  inline static int server_pipe_[2];
  inline static std::unique_ptr<subspace::Server> server_;
  inline static std::thread server_thread_;

  subspace::Client client_;
};

TEST(StaticConfigParseTest, ParsesValidConfig) {
  absl::StatusOr<subspace::StaticChannelConfig> config =
      subspace::ParseStaticChannelConfig(kConfig);
  ASSERT_OK(config);
  EXPECT_EQ(2, config->multiplexers_size());
  EXPECT_EQ(6, config->channels_size());
}

TEST(StaticConfigParseTest, RejectsInvalidConfigs) {
  const std::vector<std::pair<std::string, std::string>> cases = {
      {R"pb(channels { nme: "/x" })pb", "invalid protobuf text format"},
      {R"pb(channels { slot_size: 64 num_slots: 4 })pb", "needs a name"},
      {R"pb(channels { name: "/subspace/x" slot_size: 64 num_slots: 4 })pb",
       "reserved prefix"},
      {R"pb(channels { name: "/x" slot_size: 64 num_slots: 4 }
            channels { name: "/x" slot_size: 64 num_slots: 4 })pb",
       "more than once"},
      {R"pb(multiplexers { name: "/x" slot_size: 64 num_slots: 4 }
            channels { name: "/x" slot_size: 64 num_slots: 4 })pb",
       "more than once"},
      {R"pb(channels { name: "/x" num_slots: 4 })pb",
       "greater than 0"},
      {R"pb(channels { name: "/x" slot_size: 64 })pb", "greater than 0"},
      {R"pb(channels { name: "/x" slot_size: 64 num_slots: 4
                       checksum_size: -1 })pb",
       "checksum_size -1"},
      {R"pb(channels { name: "/x" slot_size: 64 num_slots: 4
                       metadata_size: 100000 })pb",
       "metadata_size 100000"},
      {R"pb(channels { name: "/x" slot_size: 64 num_slots: 4 vchan_id: 2 })pb",
       "vchan_id but no mux"},
      {R"pb(channels { name: "/v" mux: "/m" })pb",
       "isn't a configured multiplexer"},
      {R"pb(multiplexers { name: "/m" slot_size: 64 num_slots: 4 }
            channels { name: "/v" mux: "/m" slot_size: 64 })pb",
       "leave them unset"},
      {R"pb(multiplexers { name: "/m" slot_size: 64 num_slots: 4 }
            channels { name: "/v" mux: "/m" use_split_buffers: true })pb",
       "leave them unset"},
      {R"pb(multiplexers { name: "/m" slot_size: 64 num_slots: 4 }
            channels { name: "/v" mux: "/m" vchan_id: 1023 })pb",
       "vchan_id 1023"},
      {R"pb(multiplexers { name: "/m" slot_size: 64 num_slots: 4 }
            channels { name: "/v1" mux: "/m" vchan_id: 1 }
            channels { name: "/v2" mux: "/m" vchan_id: 1 })pb",
       "reuses vchan_id 1"},
  };
  for (const auto &[text, error] : cases) {
    absl::StatusOr<subspace::StaticChannelConfig> config =
        subspace::ParseStaticChannelConfig(text);
    ASSERT_FALSE(config.ok()) << text;
    EXPECT_THAT(config.status().message(), HasSubstr(error)) << text;
  }
}

TEST(StaticConfigParseTest, ReadMissingFile) {
  absl::StatusOr<subspace::StaticChannelConfig> config =
      subspace::ReadStaticChannelConfig("/no/such/static_config.textproto");
  ASSERT_FALSE(config.ok());
  EXPECT_THAT(config.status().message(), HasSubstr("can't open"));
}

TEST_F(StaticConfigTest, ChannelsExistBeforeAnyClient) {
  absl::StatusOr<const subspace::ChannelInfo> info =
      Client().GetChannelInfo("/static/a");
  ASSERT_OK(info);
  EXPECT_EQ(256u, info->slot_size);
  EXPECT_EQ(8, info->num_slots);
  EXPECT_EQ("A", info->type);
  EXPECT_EQ(0, info->num_publishers);
  EXPECT_EQ(0, info->num_subscribers);

  absl::StatusOr<const subspace::ChannelInfo> vchan_info =
      Client().GetChannelInfo("/static/v2");
  ASSERT_OK(vchan_info);
  EXPECT_EQ(128u, vchan_info->slot_size);
  EXPECT_EQ(8, vchan_info->num_slots);
  EXPECT_EQ("Mux", vchan_info->type);
}

TEST_F(StaticConfigTest, SubscriberBeforePublisher) {
  absl::StatusOr<subspace::Subscriber> sub =
      Client().CreateSubscriber("/static/a");
  ASSERT_OK(sub);
  EXPECT_EQ(8, sub->NumSlots());

  absl::StatusOr<subspace::Publisher> pub =
      Client().CreatePublisher("/static/a", 256, 8, FixedSize());
  ASSERT_OK(pub);
  Publish(*pub, "hello");

  absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  ASSERT_EQ(5, msg->length);
  EXPECT_EQ("hello",
            std::string(static_cast<const char *>(msg->buffer), msg->length));
}

TEST_F(StaticConfigTest, SplitBuffersCreatedByServer) {
  absl::StatusOr<subspace::Subscriber> sub =
      Client().CreateSubscriber("/static/split");
  ASSERT_OK(sub);
  EXPECT_TRUE(sub->UsesSplitBuffers());

  absl::StatusOr<subspace::Publisher> pub = Client().CreatePublisher(
      "/static/split", 256, 4, FixedSize().SetUseSplitBuffers(true));
  ASSERT_OK(pub);
  EXPECT_TRUE(pub->UsesSplitBuffers());
  for (int i = 0; i < 6; i++) {
    std::string text = absl::StrFormat("split %d", i);
    Publish(*pub, text);
    absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
    ASSERT_OK(msg);
    EXPECT_EQ(text,
              std::string(static_cast<const char *>(msg->buffer), msg->length));
  }
}

TEST_F(StaticConfigTest, SplitBufferVirtualChannel) {
  absl::StatusOr<subspace::Subscriber> sub = Client().CreateSubscriber(
      "/static/sv", subspace::SubscriberOptions().SetMux("/static/split_mux"));
  ASSERT_OK(sub);
  EXPECT_TRUE(sub->UsesSplitBuffers());

  absl::StatusOr<subspace::Publisher> pub = Client().CreatePublisher(
      "/static/sv", 128, 4,
      FixedSize().SetMux("/static/split_mux").SetUseSplitBuffers(true));
  ASSERT_OK(pub);
  Publish(*pub, "vchan");
  absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  EXPECT_EQ("vchan",
            std::string(static_cast<const char *>(msg->buffer), msg->length));
}

TEST_F(StaticConfigTest, RejectsMismatchedSplitBuffers) {
  absl::StatusOr<subspace::Publisher> unsplit =
      Client().CreatePublisher("/static/split", 256, 4, FixedSize());
  ASSERT_FALSE(unsplit.ok());
  EXPECT_THAT(unsplit.status().message(), HasSubstr("uses split buffers"));

  absl::StatusOr<subspace::Publisher> split = Client().CreatePublisher(
      "/static/a", 256, 8, FixedSize().SetUseSplitBuffers(true));
  ASSERT_FALSE(split.ok());
  EXPECT_THAT(split.status().message(),
              HasSubstr("doesn't use split buffers"));
}

TEST_F(StaticConfigTest, UnconfiguredChannelsAreRejected) {
  absl::StatusOr<subspace::Publisher> pub =
      Client().CreatePublisher("/not/configured", 64, 4, FixedSize());
  ASSERT_FALSE(pub.ok());
  EXPECT_THAT(pub.status().message(),
              HasSubstr("isn't in the static channel config"));

  absl::StatusOr<subspace::Subscriber> sub =
      Client().CreateSubscriber("/not/configured");
  ASSERT_FALSE(sub.ok());
  EXPECT_THAT(sub.status().message(),
              HasSubstr("isn't in the static channel config"));

  // A virtual channel on a configured multiplexer must be configured too.
  pub = Client().CreatePublisher("/static/v9", 128, 8,
                                 FixedSize().SetMux("/static/mux"));
  ASSERT_FALSE(pub.ok());
  EXPECT_THAT(pub.status().message(),
              HasSubstr("isn't in the static channel config"));
}

TEST_F(StaticConfigTest, PublisherLayoutMustMatch) {
  absl::StatusOr<subspace::Publisher> pub = Client().CreatePublisher(
      "/static/a", 256, 8, subspace::PublisherOptions());
  ASSERT_FALSE(pub.ok());
  EXPECT_THAT(pub.status().message(), HasSubstr("must be fixed size"));

  for (auto [slot_size, num_slots] :
       std::vector<std::pair<int, int>>{{512, 8}, {128, 8}, {256, 16},
                                        {256, 4}}) {
    pub = Client().CreatePublisher("/static/a", slot_size, num_slots,
                                   FixedSize());
    ASSERT_FALSE(pub.ok()) << slot_size << "/" << num_slots;
    EXPECT_THAT(pub.status().message(),
                HasSubstr("has 8 slots of 256 bytes"));
  }

  pub = Client().CreatePublisher("/static/meta", 64, 4, FixedSize());
  ASSERT_FALSE(pub.ok());
  EXPECT_THAT(pub.status().message(),
              HasSubstr("checksum_size 4 and metadata_size 16, not 4 and 0"));
  pub = Client().CreatePublisher("/static/meta", 64, 4,
                                 FixedSize().SetMetadataSize(16));
  ASSERT_OK(pub);

  // A matching second publisher is accepted after the first has created the
  // channel's buffers.
  absl::StatusOr<subspace::Publisher> first =
      Client().CreatePublisher("/static/a", 256, 8, FixedSize());
  ASSERT_OK(first);
  Publish(*first, "x");
  absl::StatusOr<subspace::Publisher> second =
      Client().CreatePublisher("/static/a", 256, 8, FixedSize());
  ASSERT_OK(second);
}

TEST_F(StaticConfigTest, ChannelsOutliveTheirUsers) {
  {
    absl::StatusOr<subspace::Publisher> pub =
        Client().CreatePublisher("/static/a", 256, 8, FixedSize());
    ASSERT_OK(pub);
    absl::StatusOr<subspace::Subscriber> sub =
        Client().CreateSubscriber("/static/a");
    ASSERT_OK(sub);
    Publish(*pub, "before");
  }
  absl::StatusOr<const subspace::ChannelInfo> info =
      Client().GetChannelInfo("/static/a");
  ASSERT_OK(info);
  EXPECT_EQ(0, info->num_publishers);
  EXPECT_EQ(0, info->num_subscribers);
  EXPECT_EQ(256u, info->slot_size);
  EXPECT_EQ(8, info->num_slots);

  absl::StatusOr<subspace::Publisher> pub =
      Client().CreatePublisher("/static/a", 256, 8, FixedSize());
  ASSERT_OK(pub);
  absl::StatusOr<subspace::Subscriber> sub =
      Client().CreateSubscriber("/static/a");
  ASSERT_OK(sub);
  Publish(*pub, "after");
  absl::StatusOr<subspace::Message> msg =
      sub->ReadMessage(subspace::ReadMode::kReadNewest);
  ASSERT_OK(msg);
  EXPECT_EQ("after",
            std::string(static_cast<const char *>(msg->buffer), msg->length));
}

TEST_F(StaticConfigTest, VirtualChannels) {
  absl::StatusOr<subspace::Subscriber> sub = Client().CreateSubscriber(
      "/static/v1", subspace::SubscriberOptions().SetMux("/static/mux"));
  ASSERT_OK(sub);
  EXPECT_EQ(3, sub->VirtualChannelId());

  absl::StatusOr<subspace::Publisher> pub = Client().CreatePublisher(
      "/static/v1", 256, 8, FixedSize().SetMux("/static/mux"));
  ASSERT_FALSE(pub.ok());
  EXPECT_THAT(pub.status().message(), HasSubstr("has 8 slots of 128 bytes"));

  pub = Client().CreatePublisher("/static/v1", 128, 8,
                                 FixedSize().SetMux("/static/mux"));
  ASSERT_OK(pub);
  EXPECT_EQ(3, pub->VirtualChannelId());
  Publish(*pub, "virtual");

  absl::StatusOr<subspace::Message> msg = sub->ReadMessage();
  ASSERT_OK(msg);
  EXPECT_EQ("virtual",
            std::string(static_cast<const char *>(msg->buffer), msg->length));

  // Publishers on other virtual channels of the multiplexer share its
  // buffers, so they are also fixed size.
  absl::StatusOr<subspace::Publisher> other = Client().CreatePublisher(
      "/static/v2", 128, 8, FixedSize().SetMux("/static/mux"));
  ASSERT_OK(other);
  EXPECT_EQ(7, other->VirtualChannelId());
}

TEST_F(StaticConfigTest, ServerChannelsArePublished) {
  for (const char *name :
       {"/subspace/ChannelDirectory", "/subspace/Statistics"}) {
    int num_publishers = 0;
    for (int i = 0; i < 100 && num_publishers == 0; i++) {
      absl::StatusOr<const subspace::ChannelInfo> info =
          Client().GetChannelInfo(name);
      if (info.ok()) {
        num_publishers = info->num_publishers;
      }
      if (num_publishers == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      }
    }
    EXPECT_EQ(1, num_publishers) << name;
  }
}

} // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
