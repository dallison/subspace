// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// Tests that exercise the server's ClientHandler directly via the raw
// protobuf wire protocol.  Each test opens a Unix socket to the server,
// sends hand-crafted Request protos, and verifies Response fields and
// error strings — covering server-side validation that the client library
// would normally prevent.

#include "client/test_fixture.h"

#include "proto/subspace.pb.h"
#include "toolbelt/fd.h"
#include "toolbelt/sockets.h"
#include <string>

// Helper to send raw Request protos and receive Response protos + FDs,
// using the same wire format as the real client (4-byte length prefix,
// then protobuf body, then SCM_RIGHTS FDs).
class RawConnection {
public:
  absl::Status Connect(const std::string &socket_path) {
    return socket_.Connect(socket_path);
  }

  absl::Status Init(const std::string &client_name = "raw_test") {
    subspace::Request req;
    req.mutable_init()->set_client_name(client_name);
    auto result = Send(req);
    if (!result.ok()) {
      return result.status();
    }
    session_id_ = result->first.init().session_id();
    return absl::OkStatus();
  }

  absl::StatusOr<
      std::pair<subspace::Response, std::vector<toolbelt::FileDescriptor>>>
  Send(const subspace::Request &req,
       const std::vector<toolbelt::FileDescriptor> &send_fds = {}) {
    size_t msg_len = req.ByteSizeLong();
    std::vector<char> buf(sizeof(int32_t) + msg_len);
    char *payload = buf.data() + sizeof(int32_t);
    if (!req.SerializeToArray(payload, msg_len)) {
      return absl::InternalError("Failed to serialize request");
    }
    auto n = socket_.SendMessage(payload, msg_len);
    if (!n.ok()) {
      return n.status();
    }
    if (!send_fds.empty()) {
      if (auto s = socket_.SendFds(send_fds); !s.ok()) {
        return s;
      }
    }

    auto recv = socket_.ReceiveVariableLengthMessage();
    if (!recv.ok()) {
      return recv.status();
    }
    subspace::Response resp;
    if (!resp.ParseFromArray(recv->data(), static_cast<int>(recv->size()))) {
      return absl::InternalError("Failed to parse response");
    }

    std::vector<toolbelt::FileDescriptor> fds;
    if (auto s = socket_.ReceiveFds(fds); !s.ok()) {
      return s;
    }
    return std::make_pair(std::move(resp), std::move(fds));
  }

  absl::Status SendOneWay(
      const subspace::Request &req,
      const std::vector<toolbelt::FileDescriptor> &send_fds = {}) {
    size_t msg_len = req.ByteSizeLong();
    std::vector<char> buf(sizeof(int32_t) + msg_len);
    char *payload = buf.data() + sizeof(int32_t);
    if (!req.SerializeToArray(payload, msg_len)) {
      return absl::InternalError("Failed to serialize request");
    }
    auto n = socket_.SendMessage(payload, msg_len);
    if (!n.ok()) {
      return n.status();
    }
    if (!send_fds.empty()) {
      return socket_.SendFds(send_fds);
    }
    return absl::OkStatus();
  }

  uint64_t SessionId() const { return session_id_; }

  // Convenience: create a publisher and return the response.
  std::pair<subspace::Response, std::vector<toolbelt::FileDescriptor>>
  CreatePublisher(const std::string &channel, int slot_size = 64,
                  int num_slots = 4, const std::string &type = "",
                  bool reliable = false, bool is_local = true,
                  bool fixed_size = false, const std::string &mux = "",
                  int vchan_id = 0, bool for_tunnel = false,
                  bool notify_retirement = false, int checksum_size = 0,
                  int metadata_size = 0, int max_publishers = 0) {
    subspace::Request req;
    auto *cmd = req.mutable_create_publisher();
    cmd->set_channel_name(channel);
    cmd->set_slot_size(slot_size);
    cmd->set_num_slots(num_slots);
    cmd->set_type(type);
    cmd->set_is_reliable(reliable);
    cmd->set_is_local(is_local);
    cmd->set_is_fixed_size(fixed_size);
    cmd->set_mux(mux);
    cmd->set_vchan_id(vchan_id);
    cmd->set_for_tunnel(for_tunnel);
    cmd->set_notify_retirement(notify_retirement);
    cmd->set_checksum_size(checksum_size);
    cmd->set_metadata_size(metadata_size);
    cmd->set_max_publishers(max_publishers);
    cmd->set_publisher_id(-1);
    auto result = Send(req);
    return std::move(*result);
  }

  // Convenience: create a subscriber and return the response.
  std::pair<subspace::Response, std::vector<toolbelt::FileDescriptor>>
  CreateSubscriber(const std::string &channel,
                   const std::string &type = "", bool reliable = false,
                   int max_active_messages = 4, const std::string &mux = "",
                   int vchan_id = 0, bool for_tunnel = false) {
    subspace::Request req;
    auto *cmd = req.mutable_create_subscriber();
    cmd->set_channel_name(channel);
    cmd->set_type(type);
    cmd->set_is_reliable(reliable);
    cmd->set_max_active_messages(max_active_messages);
    cmd->set_subscriber_id(-1);
    cmd->set_mux(mux);
    cmd->set_vchan_id(vchan_id);
    cmd->set_for_tunnel(for_tunnel);
    auto result = Send(req);
    return std::move(*result);
  }

private:
  toolbelt::UnixSocket socket_;
  uint64_t session_id_ = 0;
};

class ServerTest : public SubspaceTestBase {};

// ---------------------------------------------------------------------------
// Protocol-level tests
// ---------------------------------------------------------------------------

TEST_F(ServerTest, InitSuccess) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));

  subspace::Request req;
  req.mutable_init()->set_client_name("test_init");
  auto result = conn.Send(req);
  ASSERT_OK(result);
  auto &[resp, fds] = *result;
  ASSERT_TRUE(resp.has_init());
  ASSERT_GT(resp.init().session_id(), 0);
  ASSERT_FALSE(fds.empty());
}

TEST_F(ServerTest, CreatePublisherSuccess) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  auto [resp, fds] = conn.CreatePublisher("srv_pub_ok");
  ASSERT_TRUE(resp.has_create_publisher());
  ASSERT_TRUE(resp.create_publisher().error().empty());
  ASSERT_GE(resp.create_publisher().publisher_id(), 0);
  ASSERT_GE(static_cast<int>(fds.size()), 4);
}

TEST_F(ServerTest, CreateSubscriberSuccess) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  auto [resp, fds] = conn.CreateSubscriber("srv_sub_ok");
  ASSERT_TRUE(resp.has_create_subscriber());
  ASSERT_TRUE(resp.create_subscriber().error().empty());
  ASSERT_GE(resp.create_subscriber().subscriber_id(), 0);
  ASSERT_GE(static_cast<int>(fds.size()), 4);
}

// ---------------------------------------------------------------------------
// CreatePublisher error paths
// ---------------------------------------------------------------------------

TEST_F(ServerTest, PubTypeMismatch) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("type_mismatch_ch", 64, 4, "typeA");
  auto [resp, fds] = conn.CreatePublisher("type_mismatch_ch", 64, 4, "typeB");
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("Inconsistent channel types"));
}

TEST_F(ServerTest, PubFixedSizeMismatch) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("fixmis_ch", 64, 4, "", false, true, /*fixed=*/true);
  auto [resp, fds] =
      conn.CreatePublisher("fixmis_ch", 64, 4, "", false, true, /*fixed=*/false);
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("fixed size"));
}

TEST_F(ServerTest, PubNumSlotsIncrease) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("slots_ch", 64, 4);
  auto [resp, fds] = conn.CreatePublisher("slots_ch", 64, 8);
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("more slots"));
}

TEST_F(ServerTest, PubSlotSizeIncreaseOnFixedSize) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("fixslot_ch", 64, 4, "", false, true, /*fixed=*/true);
  auto [resp, fds] =
      conn.CreatePublisher("fixslot_ch", 128, 4, "", false, true, /*fixed=*/true);
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("fixed size channel"));
}

TEST_F(ServerTest, PubLocalMismatch) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("local_ch", 64, 4, "", false, /*local=*/true);
  auto [resp, fds] =
      conn.CreatePublisher("local_ch", 64, 4, "", false, /*local=*/false);
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("local or not"));
}

TEST_F(ServerTest, PubChecksumSizeTooLarge) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  auto [resp, fds] = conn.CreatePublisher(
      "cs_large_ch", 64, 4, "", false, true, false, "", 0, false, false,
      /*checksum_size=*/0x10000);
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("checksum_size"));
}

TEST_F(ServerTest, PubMetadataSizeTooLarge) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  auto [resp, fds] = conn.CreatePublisher(
      "ms_large_ch", 64, 4, "", false, true, false, "", 0, false, false,
      0, /*metadata_size=*/0x10000);
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("metadata_size"));
}

TEST_F(ServerTest, PubChecksumSizeInconsistent) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("cs_incon_ch", 64, 4, "", false, true, false, "", 0,
                        false, false, /*checksum_size=*/8);
  auto [resp, fds] = conn.CreatePublisher(
      "cs_incon_ch", 64, 4, "", false, true, false, "", 0, false, false,
      /*checksum_size=*/16);
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("Inconsistent checksum_size"));
}

TEST_F(ServerTest, PubMetadataSizeInconsistent) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("ms_incon_ch", 64, 4, "", false, true, false, "", 0,
                        false, false, 0, /*metadata_size=*/16);
  auto [resp, fds] = conn.CreatePublisher(
      "ms_incon_ch", 64, 4, "", false, true, false, "", 0, false, false,
      0, /*metadata_size=*/32);
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("Inconsistent metadata_size"));
}

TEST_F(ServerTest, PubMaxPublishersInconsistent) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("max_pub_incon_ch", 64, 4, "", false, true, false, "",
                       0, false, false, 0, 0, /*max_publishers=*/2);
  auto [resp, fds] = conn.CreatePublisher(
      "max_pub_incon_ch", 64, 4, "", false, true, false, "", 0, false, false,
      0, 0, /*max_publishers=*/3);
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("Inconsistent max_publishers"));
}

TEST_F(ServerTest, PubMaxPublishersLimit) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("max_pub_limit_ch", 64, 4, "", false, true, false, "",
                       0, false, false, 0, 0, /*max_publishers=*/1);
  auto [resp, fds] = conn.CreatePublisher(
      "max_pub_limit_ch", 64, 4, "", false, true, false, "", 0, false, false,
      0, 0, /*max_publishers=*/1);
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("maximum number of publishers"));
}

TEST_F(ServerTest, PubMaxPublishersNegativeRejected) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  auto [resp, fds] = conn.CreatePublisher(
      "max_pub_negative_ch", 64, 4, "", false, true, false, "", 0, false,
      false, 0, 0, /*max_publishers=*/-1);
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("Invalid max_publishers"));
}

TEST_F(ServerTest, PubToMuxChannelWithoutMuxName) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  // First create a virtual publisher to establish the mux.
  conn.CreatePublisher("vchan_for_mux", 64, 4, "", false, true, false,
                        "/mux_test");
  // Now try to publish directly to the mux name without specifying mux.
  auto [resp, fds] = conn.CreatePublisher("/mux_test", 64, 4);
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("multiplexer channel"));
}

TEST_F(ServerTest, PubWithNonExistentMux) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  // Create a normal (non-mux) channel first, then try to use it as mux.
  conn.CreatePublisher("normal_ch", 64, 4);
  auto [resp, fds] = conn.CreatePublisher("vchan_bad_mux", 64, 4, "", false,
                                           true, false, "normal_ch");
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("not a multiplexer"));
}

TEST_F(ServerTest, PubVirtualChannelWithoutMux) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  // Create a virtual channel (with mux) first.
  conn.CreatePublisher("vch_nomux", 64, 4, "", false, true, false, "/mux_nomux");
  // Try to create a non-virtual publisher on the same channel name.
  auto [resp, fds] = conn.CreatePublisher("vch_nomux", 64, 4);
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("virtual"));
}

TEST_F(ServerTest, PubNonVirtualChannelWithMux) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("nonvirt_ch", 64, 4);
  auto [resp, fds] = conn.CreatePublisher("nonvirt_ch", 64, 4, "", false,
                                           true, false, "/some_mux");
  // Hits the "not a multiplexer" check because /some_mux doesn't exist as a mux.
  EXPECT_FALSE(resp.create_publisher().error().empty());
}

TEST_F(ServerTest, PubVirtualRetirementNotSupported) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  auto [resp, fds] = conn.CreatePublisher(
      "vch_retire", 64, 4, "", false, true, false, "/mux_retire", 0, false,
      /*notify_retirement=*/true);
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("retirement"));
}

// ---------------------------------------------------------------------------
// CreateSubscriber error paths
// ---------------------------------------------------------------------------

TEST_F(ServerTest, SubTypeMismatch) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("subtype_ch", 64, 4, "typeX");
  auto [resp, fds] = conn.CreateSubscriber("subtype_ch", "typeY");
  EXPECT_THAT(resp.create_subscriber().error(),
              ::testing::HasSubstr("Inconsistent channel types"));
}

TEST_F(ServerTest, SubVirtualChannelWithoutMux) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("sub_vch_nm", 64, 4, "", false, true, false,
                        "/mux_sub_nm");
  auto [resp, fds] = conn.CreateSubscriber("sub_vch_nm");
  EXPECT_THAT(resp.create_subscriber().error(),
              ::testing::HasSubstr("virtual"));
}

TEST_F(ServerTest, SubNonVirtualChannelWithMux) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("sub_nonvirt", 64, 4);
  auto [resp, fds] = conn.CreateSubscriber("sub_nonvirt", "", false, 4,
                                            "/fake_mux");
  EXPECT_THAT(resp.create_subscriber().error(),
              ::testing::HasSubstr("not virtual"));
}

TEST_F(ServerTest, SubInvalidReclaim) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("sub_reclaim_ch", 64, 4);

  // Try to reclaim a subscriber with an invalid ID.
  subspace::Request req;
  auto *cmd = req.mutable_create_subscriber();
  cmd->set_channel_name("sub_reclaim_ch");
  cmd->set_subscriber_id(9999);
  cmd->set_max_active_messages(4);
  auto result = conn.Send(req);
  ASSERT_OK(result);
  auto &[resp, fds] = *result;
  EXPECT_FALSE(resp.create_subscriber().error().empty());
}

// ---------------------------------------------------------------------------
// GetTriggers error paths
// ---------------------------------------------------------------------------

TEST_F(ServerTest, GetTriggersNoSuchChannel) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  subspace::Request req;
  req.mutable_get_triggers()->set_channel_name("nonexistent_triggers");
  auto result = conn.Send(req);
  ASSERT_OK(result);
  EXPECT_THAT(result->first.get_triggers().error(),
              ::testing::HasSubstr("No such channel"));
}

TEST_F(ServerTest, GetTriggersSuccess) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("triggers_ch", 64, 4, "", /*reliable=*/true);
  conn.CreateSubscriber("triggers_ch");

  subspace::Request req;
  req.mutable_get_triggers()->set_channel_name("triggers_ch");
  auto result = conn.Send(req);
  ASSERT_OK(result);
  EXPECT_TRUE(result->first.get_triggers().error().empty());
}

// ---------------------------------------------------------------------------
// RemovePublisher / RemoveSubscriber
// ---------------------------------------------------------------------------

TEST_F(ServerTest, RemovePublisherNoSuchChannel) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  subspace::Request req;
  auto *cmd = req.mutable_remove_publisher();
  cmd->set_channel_name("ghost_channel");
  cmd->set_publisher_id(0);
  auto result = conn.Send(req);
  ASSERT_OK(result);
  EXPECT_THAT(result->first.remove_publisher().error(),
              ::testing::HasSubstr("No such channel"));
}

TEST_F(ServerTest, RemoveSubscriberNoSuchChannel) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  subspace::Request req;
  auto *cmd = req.mutable_remove_subscriber();
  cmd->set_channel_name("ghost_channel_sub");
  cmd->set_subscriber_id(0);
  auto result = conn.Send(req);
  ASSERT_OK(result);
  EXPECT_THAT(result->first.remove_subscriber().error(),
              ::testing::HasSubstr("No such channel"));
}

TEST_F(ServerTest, RemovePublisherSuccess) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  auto [create_resp, create_fds] = conn.CreatePublisher("rm_pub_ch", 64, 4);
  ASSERT_TRUE(create_resp.create_publisher().error().empty());
  int pub_id = create_resp.create_publisher().publisher_id();

  subspace::Request req;
  auto *cmd = req.mutable_remove_publisher();
  cmd->set_channel_name("rm_pub_ch");
  cmd->set_publisher_id(pub_id);
  auto result = conn.Send(req);
  ASSERT_OK(result);
  EXPECT_TRUE(result->first.remove_publisher().error().empty());
}

TEST_F(ServerTest, RemoveSubscriberSuccess) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  auto [create_resp, create_fds] = conn.CreateSubscriber("rm_sub_ch");
  ASSERT_TRUE(create_resp.create_subscriber().error().empty());
  int sub_id = create_resp.create_subscriber().subscriber_id();

  subspace::Request req;
  auto *cmd = req.mutable_remove_subscriber();
  cmd->set_channel_name("rm_sub_ch");
  cmd->set_subscriber_id(sub_id);
  auto result = conn.Send(req);
  ASSERT_OK(result);
  EXPECT_TRUE(result->first.remove_subscriber().error().empty());
}

TEST_F(ServerTest, RemovePublisherInvalidId) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("rm_inval_pub", 64, 4);
  subspace::Request req;
  auto *cmd = req.mutable_remove_publisher();
  cmd->set_channel_name("rm_inval_pub");
  cmd->set_publisher_id(9999);
  auto result = conn.Send(req);
  ASSERT_OK(result);
  // RemoveUser is a silent no-op for invalid IDs — no error expected.
  EXPECT_TRUE(result->first.remove_publisher().error().empty());
}

// ---------------------------------------------------------------------------
// GetChannelInfo
// ---------------------------------------------------------------------------

TEST_F(ServerTest, GetChannelInfoByName) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("info_ch", 128, 8, "info_type");

  subspace::Request req;
  req.mutable_get_channel_info()->set_channel_name("info_ch");
  auto result = conn.Send(req);
  ASSERT_OK(result);
  auto &resp = result->first.get_channel_info();
  EXPECT_TRUE(resp.error().empty());
  ASSERT_EQ(1, resp.channels_size());
  EXPECT_EQ("info_ch", resp.channels(0).name());
  EXPECT_EQ("info_type", resp.channels(0).type());
  EXPECT_EQ(1, resp.channels(0).num_pubs());
  EXPECT_EQ(128, resp.channels(0).slot_size());
  EXPECT_EQ(8, resp.channels(0).num_slots());
}

TEST_F(ServerTest, GetChannelInfoNoSuchChannel) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  subspace::Request req;
  req.mutable_get_channel_info()->set_channel_name("no_such_info");
  auto result = conn.Send(req);
  ASSERT_OK(result);
  EXPECT_THAT(result->first.get_channel_info().error(),
              ::testing::HasSubstr("No such channel"));
}

TEST_F(ServerTest, GetChannelInfoAll) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("allinfo1", 64, 4);
  conn.CreatePublisher("allinfo2", 64, 4);

  subspace::Request req;
  req.mutable_get_channel_info();
  auto result = conn.Send(req);
  ASSERT_OK(result);
  auto &resp = result->first.get_channel_info();
  EXPECT_TRUE(resp.error().empty());
  EXPECT_GE(resp.channels_size(), 2);
}

// ---------------------------------------------------------------------------
// GetChannelStats
// ---------------------------------------------------------------------------

TEST_F(ServerTest, GetChannelStatsByName) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("stats_ch", 256, 4);

  subspace::Request req;
  req.mutable_get_channel_stats()->set_channel_name("stats_ch");
  auto result = conn.Send(req);
  ASSERT_OK(result);
  auto &resp = result->first.get_channel_stats();
  EXPECT_TRUE(resp.error().empty());
  ASSERT_EQ(1, resp.channels_size());
  EXPECT_EQ("stats_ch", resp.channels(0).channel_name());
}

TEST_F(ServerTest, GetChannelStatsNoSuchChannel) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  subspace::Request req;
  req.mutable_get_channel_stats()->set_channel_name("no_such_stats");
  auto result = conn.Send(req);
  ASSERT_OK(result);
  EXPECT_THAT(result->first.get_channel_stats().error(),
              ::testing::HasSubstr("No such channel"));
}

TEST_F(ServerTest, GetChannelStatsAll) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("allstats_s1", 64, 4);
  conn.CreatePublisher("allstats_s2", 64, 4);

  subspace::Request req;
  req.mutable_get_channel_stats();
  auto result = conn.Send(req);
  ASSERT_OK(result);
  auto &resp = result->first.get_channel_stats();
  EXPECT_TRUE(resp.error().empty());
  EXPECT_GE(resp.channels_size(), 2);
}

// ---------------------------------------------------------------------------
// Pub/Sub with matching FDs verification
// ---------------------------------------------------------------------------

TEST_F(ServerTest, PubResponseFdIndexesValid) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  auto [resp, fds] = conn.CreatePublisher("fd_verify_ch", 64, 4);
  ASSERT_TRUE(resp.create_publisher().error().empty());
  auto &pub_resp = resp.create_publisher();

  ASSERT_GE(static_cast<int>(fds.size()),
            pub_resp.pub_trigger_fd_index() + 1);
  EXPECT_EQ(0, pub_resp.ccb_fd_index());
  EXPECT_EQ(1, pub_resp.bcb_fd_index());
  EXPECT_EQ(2, pub_resp.pub_poll_fd_index());
  EXPECT_EQ(3, pub_resp.pub_trigger_fd_index());

  for (size_t i = 0; i < fds.size(); i++) {
    EXPECT_TRUE(fds[i].Valid()) << "FD at index " << i << " is invalid";
  }
}

TEST_F(ServerTest, SubResponseFdIndexesValid) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("subfd_ch", 64, 16);
  auto [resp, fds] = conn.CreateSubscriber("subfd_ch");
  ASSERT_TRUE(resp.create_subscriber().error().empty())
      << "Error: " << resp.create_subscriber().error();
  auto &sub_resp = resp.create_subscriber();

  EXPECT_EQ(0, sub_resp.ccb_fd_index());
  EXPECT_EQ(1, sub_resp.bcb_fd_index());
  EXPECT_EQ(2, sub_resp.trigger_fd_index());
  EXPECT_EQ(3, sub_resp.poll_fd_index());

  for (size_t i = 0; i < fds.size(); i++) {
    EXPECT_TRUE(fds[i].Valid()) << "FD at index " << i << " is invalid";
  }
}

TEST_F(ServerTest, SubGetsChannelProperties) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("props_ch", 256, 8, "my_type", false, true, false, "",
                        0, false, false, /*checksum_size=*/8,
                        /*metadata_size=*/16);
  auto [resp, fds] = conn.CreateSubscriber("props_ch");
  ASSERT_TRUE(resp.create_subscriber().error().empty());
  auto &sub_resp = resp.create_subscriber();
  EXPECT_EQ(256, sub_resp.slot_size());
  EXPECT_EQ(8, sub_resp.num_slots());
  EXPECT_EQ("my_type", sub_resp.type());
  EXPECT_EQ(8, sub_resp.checksum_size());
  EXPECT_EQ(16, sub_resp.metadata_size());
}

TEST_F(ServerTest, ClientBufferRegisterRejectsForeignPublisherOverwrite) {
  RawConnection owner;
  ASSERT_OK(owner.Connect(Socket()));
  ASSERT_OK(owner.Init("buffer_owner"));
  auto [owner_resp, owner_fds] = owner.CreatePublisher("owned_buffer_ch");
  ASSERT_TRUE(owner_resp.create_publisher().error().empty());
  int owner_pub_id = owner_resp.create_publisher().publisher_id();

  RawConnection attacker;
  ASSERT_OK(attacker.Connect(Socket()));
  ASSERT_OK(attacker.Init("buffer_attacker"));
  auto [attacker_resp, attacker_fds] =
      attacker.CreatePublisher("owned_buffer_ch");
  ASSERT_TRUE(attacker_resp.create_publisher().error().empty());
  int attacker_pub_id = attacker_resp.create_publisher().publisher_id();
  ASSERT_NE(owner_pub_id, attacker_pub_id);

  subspace::Request register_owner_req;
  auto *register_owner = register_owner_req.mutable_register_client_buffer();
  auto *owner_metadata = register_owner->mutable_metadata();
  owner_metadata->set_channel_name("owned_buffer_ch");
  owner_metadata->set_session_id(owner.SessionId());
  owner_metadata->set_buffer_index(0);
  owner_metadata->set_slot_id(0);
  owner_metadata->set_full_size(64);
  owner_metadata->set_allocation_size(64);
  owner_metadata->set_object_name("owner-buffer");
  register_owner->set_publisher_id(owner_pub_id);
  auto owner_register_result = owner.Send(register_owner_req);
  ASSERT_OK(owner_register_result);
  ASSERT_TRUE(owner_register_result->first.register_client_buffer()
                  .error()
                  .empty());

  subspace::Request register_attacker_req;
  auto *register_attacker =
      register_attacker_req.mutable_register_client_buffer();
  auto *attacker_metadata = register_attacker->mutable_metadata();
  attacker_metadata->set_channel_name("owned_buffer_ch");
  attacker_metadata->set_session_id(owner.SessionId());
  attacker_metadata->set_buffer_index(0);
  attacker_metadata->set_slot_id(0);
  attacker_metadata->set_full_size(64);
  attacker_metadata->set_allocation_size(64);
  attacker_metadata->set_object_name("attacker-buffer");
  register_attacker->set_publisher_id(attacker_pub_id);
  auto attacker_register_result = attacker.Send(register_attacker_req);
  ASSERT_OK(attacker_register_result);
  EXPECT_THAT(attacker_register_result->first.register_client_buffer().error(),
              ::testing::HasSubstr("does not own client buffer group"));

  subspace::Request get_req;
  auto *get = get_req.mutable_get_client_buffers();
  get->set_channel_name("owned_buffer_ch");
  get->set_session_id(owner.SessionId());
  get->set_buffer_index(0);
  auto get_result = owner.Send(get_req);
  ASSERT_OK(get_result);
  const auto &get_resp = get_result->first.get_client_buffers();
  ASSERT_TRUE(get_resp.error().empty());
  ASSERT_EQ(1, get_resp.metadata_size());
  EXPECT_EQ("owner-buffer", get_resp.metadata(0).object_name());
}

TEST_F(ServerTest, ClientBufferUnregisterRejectsForeignPublisher) {
  RawConnection owner;
  ASSERT_OK(owner.Connect(Socket()));
  ASSERT_OK(owner.Init("unregister_owner"));
  auto [owner_resp, owner_fds] = owner.CreatePublisher("owned_unregister_ch");
  ASSERT_TRUE(owner_resp.create_publisher().error().empty());
  int owner_pub_id = owner_resp.create_publisher().publisher_id();

  RawConnection attacker;
  ASSERT_OK(attacker.Connect(Socket()));
  ASSERT_OK(attacker.Init("unregister_attacker"));
  auto [attacker_resp, attacker_fds] =
      attacker.CreatePublisher("owned_unregister_ch");
  ASSERT_TRUE(attacker_resp.create_publisher().error().empty());
  int attacker_pub_id = attacker_resp.create_publisher().publisher_id();
  ASSERT_NE(owner_pub_id, attacker_pub_id);

  subspace::Request register_req;
  auto *register_buffer = register_req.mutable_register_client_buffer();
  auto *metadata = register_buffer->mutable_metadata();
  metadata->set_channel_name("owned_unregister_ch");
  metadata->set_session_id(owner.SessionId());
  metadata->set_buffer_index(0);
  metadata->set_slot_id(0);
  metadata->set_full_size(64);
  metadata->set_allocation_size(64);
  metadata->set_object_name("still-owned");
  register_buffer->set_publisher_id(owner_pub_id);
  auto register_result = owner.Send(register_req);
  ASSERT_OK(register_result);
  ASSERT_TRUE(
      register_result->first.register_client_buffer().error().empty());

  subspace::Request unregister_req;
  auto *unregister = unregister_req.mutable_unregister_client_buffer();
  unregister->set_channel_name("owned_unregister_ch");
  unregister->set_session_id(owner.SessionId());
  unregister->set_buffer_index(0);
  unregister->set_publisher_id(attacker_pub_id);
  ASSERT_OK(attacker.SendOneWay(unregister_req));

  subspace::Request get_req;
  auto *get = get_req.mutable_get_client_buffers();
  get->set_channel_name("owned_unregister_ch");
  get->set_session_id(owner.SessionId());
  get->set_buffer_index(0);
  auto get_result = owner.Send(get_req);
  ASSERT_OK(get_result);
  const auto &get_resp = get_result->first.get_client_buffers();
  ASSERT_TRUE(get_resp.error().empty());
  ASSERT_EQ(1, get_resp.metadata_size());
  EXPECT_EQ("still-owned", get_resp.metadata(0).object_name());
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
