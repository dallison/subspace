// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// Tests that exercise the server's ClientHandler directly via the raw
// protobuf wire protocol.  Each test opens a Unix socket to the server,
// sends hand-crafted Request protos, and verifies Response fields and
// error strings — covering server-side validation that the client library
// would normally prevent.

#include "client/test_fixture.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "proto/subspace.pb.h"
#include "toolbelt/fd.h"
#include "toolbelt/sockets.h"
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#if defined(__linux__)
#include <dirent.h>
#include <limits.h>
#elif defined(__APPLE__)
#include <libproc.h>
#elif defined(__QNX__) || defined(__QNXNTO__)
#include <devctl.h>
#include <sys/procfs.h>
#endif

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

  uint64_t SessionId() const { return session_id_; }

  absl::StatusOr<
      std::pair<subspace::Response, std::vector<toolbelt::FileDescriptor>>>
  Send(const subspace::Request &req) {
    return SendWithFds(req, {});
  }

  absl::StatusOr<
      std::pair<subspace::Response, std::vector<toolbelt::FileDescriptor>>>
  SendWithFds(const subspace::Request &req,
              const std::vector<toolbelt::FileDescriptor> &send_fds) {
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

  // Build a RegisterClientBuffer request for the given (channel, session,
  // buffer_index, slot_id).  When send_fd is true a backing fd is attached and
  // fd_index is used as-is (allowing tests to inject an out-of-range index).
  subspace::Request
  MakeRegisterRequest(const std::string &channel, uint64_t session_id,
                      uint32_t buffer_index, uint32_t slot_id, bool has_fd,
                      int fd_index) {
    subspace::Request req;
    auto *r = req.mutable_register_client_buffer();
    auto *m = r->mutable_metadata();
    m->set_channel_name(channel);
    m->set_session_id(session_id);
    m->set_buffer_index(buffer_index);
    m->set_slot_id(slot_id);
    r->set_has_fd(has_fd);
    r->set_fd_index(fd_index);
    return req;
  }

  subspace::Request MakeUnregisterRequest(const std::string &channel,
                                          uint64_t session_id,
                                          uint32_t buffer_index) {
    subspace::Request req;
    auto *u = req.mutable_unregister_client_buffer();
    u->set_channel_name(channel);
    u->set_session_id(session_id);
    u->set_buffer_index(buffer_index);
    return req;
  }

  // Sends a one-way request that the server does not reply to (e.g.
  // UnregisterClientBuffer), then issues a round-trip GetClientBuffers on the
  // same connection so the caller can observe the resulting server state.  The
  // ordered processing on a single connection guarantees the one-way request
  // has been handled by the time the query returns.
  absl::StatusOr<subspace::GetClientBuffersResponse>
  SendOneWayThenGetBuffers(const subspace::Request &one_way,
                           const std::string &channel, uint64_t session_id,
                           uint32_t buffer_index) {
    size_t msg_len = one_way.ByteSizeLong();
    std::vector<char> buf(sizeof(int32_t) + msg_len);
    char *payload = buf.data() + sizeof(int32_t);
    if (!one_way.SerializeToArray(payload, msg_len)) {
      return absl::InternalError("Failed to serialize request");
    }
    if (auto n = socket_.SendMessage(payload, msg_len); !n.ok()) {
      return n.status();
    }
    return GetClientBuffers(channel, session_id, buffer_index);
  }

  absl::StatusOr<subspace::GetClientBuffersResponse>
  GetClientBuffers(const std::string &channel, uint64_t session_id,
                   uint32_t buffer_index) {
    subspace::Request req;
    auto *g = req.mutable_get_client_buffers();
    g->set_channel_name(channel);
    g->set_session_id(session_id);
    g->set_buffer_index(buffer_index);
    auto result = Send(req);
    if (!result.ok()) {
      return result.status();
    }
    return result->first.get_client_buffers();
  }

  // Convenience: create a publisher and return the response.
  std::pair<subspace::Response, std::vector<toolbelt::FileDescriptor>>
  CreatePublisher(const std::string &channel, int slot_size = 64,
                  int num_slots = 4, const std::string &type = "",
                  bool reliable = false, bool is_local = true,
                  bool fixed_size = false, const std::string &mux = "",
                  int vchan_id = 0, bool for_tunnel = false,
                  bool notify_retirement = false, int checksum_size = 0,
                  int metadata_size = 0, int max_publishers = 0,
                  uint64_t subscriber_queue_arena_size = 0) {
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
    cmd->set_subscriber_queue_arena_size(subscriber_queue_arena_size);
    cmd->set_publisher_id(-1);
    auto result = Send(req);
    return std::move(*result);
  }

  // Convenience: create a subscriber and return the response.
  std::pair<subspace::Response, std::vector<toolbelt::FileDescriptor>>
  CreateSubscriber(const std::string &channel,
                   const std::string &type = "", bool reliable = false,
                   int max_active_messages = 4, const std::string &mux = "",
                   int vchan_id = 0, bool for_tunnel = false,
                   int subscriber_queue_size = 0) {
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
    cmd->set_subscriber_queue_size(subscriber_queue_size);
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

TEST(ChannelAddressParsingTest, ParsesValidInetAddress) {
  subspace::ChannelAddress address;
  in_addr ip_addr;
  ip_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.set_address(&ip_addr, sizeof(ip_addr));
  address.set_port(12345);
  address.set_family(subspace::ChannelAddress::FAMILY_INET);

  absl::StatusOr<toolbelt::SocketAddress> parsed =
      subspace::ParseChannelAddress(address, "test address");
  ASSERT_OK(parsed);
  EXPECT_EQ(parsed->Type(), toolbelt::SocketAddress::kAddressInet);
  EXPECT_EQ(parsed->Port(), 12345);
}

TEST(ChannelAddressParsingTest, RejectsShortInetAddress) {
  subspace::ChannelAddress address;
  address.set_address("x");
  address.set_port(12345);
  address.set_family(subspace::ChannelAddress::FAMILY_INET);

  absl::StatusOr<toolbelt::SocketAddress> parsed =
      subspace::ParseChannelAddress(address, "test address");
  ASSERT_FALSE(parsed.ok());
  EXPECT_THAT(parsed.status().message(),
              ::testing::HasSubstr("invalid address length"));
}

TEST(ChannelAddressParsingTest, RejectsShortVsockAddress) {
  subspace::ChannelAddress address;
  address.set_address("x");
  address.set_port(12345);
  address.set_family(subspace::ChannelAddress::FAMILY_VSOCK);

  absl::StatusOr<toolbelt::SocketAddress> parsed =
      subspace::ParseChannelAddress(address, "test address");
  ASSERT_FALSE(parsed.ok());
  EXPECT_THAT(parsed.status().message(),
              ::testing::HasSubstr("invalid address length"));
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

TEST_F(ServerTest, PubSubscriberQueueArenaSizeMismatchFromDisabled) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("queue_size_disabled_ch", 64, 4);
  auto [resp, fds] = conn.CreatePublisher(
      "queue_size_disabled_ch", 64, 4, "", false, true, false, "", 0, false,
      false, 0, 0, 0, /*subscriber_queue_arena_size=*/8000);
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("subscriber queue arena size"));
}

TEST_F(ServerTest, PubSubscriberQueueArenaSizeTooLarge) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  auto [resp, fds] = conn.CreatePublisher(
      "queue_size_too_large", 64, 4, "", false, true, false, "", 0, false,
      false, 0, 0, 0,
      /*subscriber_queue_arena_size=*/
      subspace::kMaxChannelControlBlockSize + 1);
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("channel control block exceeds"));
}

TEST_F(ServerTest, PubCcbSizeLimitIsEnforced) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  auto [resp, fds] =
      conn.CreatePublisher("ccb_too_large", 64,
                           std::numeric_limits<int>::max());
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("channel control block limit"));
}

TEST_F(ServerTest, PubSubscriberQueueArenaSizeMismatchToDisabled) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("queue_size_enabled_ch", 64, 4, "", false, true, false,
                       "", 0, false, false, 0, 0, 0,
                       /*subscriber_queue_arena_size=*/8000);
  auto [resp, fds] = conn.CreatePublisher("queue_size_enabled_ch", 64, 4);
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("subscriber queue arena size"));
}

TEST_F(ServerTest, PubSubscriberQueueArenaSizeMismatchForMux) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("queue_size_vchan1", 64, 4, "", false, true, false,
                       "/queue_size_mux", 0, false, false, 0, 0, 0,
                       /*subscriber_queue_arena_size=*/8000);
  auto [resp, fds] = conn.CreatePublisher(
      "queue_size_vchan2", 64, 4, "", false, true, false, "/queue_size_mux",
      1, false, false, 0, 0, 0,
      /*subscriber_queue_arena_size=*/16000);
  EXPECT_THAT(resp.create_publisher().error(),
              ::testing::HasSubstr("subscriber queue arena size"));
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

TEST_F(ServerTest, SubNegativeSubscriberQueueSize) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("sub_negative_queue_size", 64, 4);
  auto [resp, fds] = conn.CreateSubscriber(
      "sub_negative_queue_size", "", false, 4, "", 0, false,
      /*subscriber_queue_size=*/-1);
  EXPECT_THAT(resp.create_subscriber().error(),
              ::testing::HasSubstr("subscriber_queue_size must be >= 0"));
}

TEST_F(ServerTest, SubQueueSizeTooLarge) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("sub_queue_size_too_large", 64, 4);
  auto [resp, fds] = conn.CreateSubscriber(
      "sub_queue_size_too_large", "", false, 4, "", 0, false,
      /*subscriber_queue_size=*/1025);
  EXPECT_THAT(resp.create_subscriber().error(),
              ::testing::HasSubstr("subscriber_queue_size must be <= 1024"));
}

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

// A channel is local if any of its publishers is, mirroring is_reliable.
TEST_F(ServerTest, GetChannelInfoReportsIsLocal) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("local_info_ch", 64, 4, "", /*reliable=*/false,
                       /*is_local=*/true);
  conn.CreatePublisher("public_info_ch", 64, 4, "", /*reliable=*/false,
                       /*is_local=*/false);

  subspace::Request local_req;
  local_req.mutable_get_channel_info()->set_channel_name("local_info_ch");
  auto local = conn.Send(local_req);
  ASSERT_OK(local);
  ASSERT_EQ(1, local->first.get_channel_info().channels_size());
  EXPECT_TRUE(local->first.get_channel_info().channels(0).is_local());

  subspace::Request public_req;
  public_req.mutable_get_channel_info()->set_channel_name("public_info_ch");
  auto pub = conn.Send(public_req);
  ASSERT_OK(pub);
  ASSERT_EQ(1, pub->first.get_channel_info().channels_size());
  EXPECT_FALSE(pub->first.get_channel_info().channels(0).is_local());
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

TEST_F(ServerTest, GetChannelStatsReportsIsLocal) {
  RawConnection conn;
  ASSERT_OK(conn.Connect(Socket()));
  ASSERT_OK(conn.Init());

  conn.CreatePublisher("local_stats_ch", 64, 4, "", /*reliable=*/false,
                       /*is_local=*/true);
  conn.CreatePublisher("public_stats_ch", 64, 4, "", /*reliable=*/false,
                       /*is_local=*/false);

  subspace::Request local_req;
  local_req.mutable_get_channel_stats()->set_channel_name("local_stats_ch");
  auto local = conn.Send(local_req);
  ASSERT_OK(local);
  ASSERT_EQ(1, local->first.get_channel_stats().channels_size());
  EXPECT_TRUE(local->first.get_channel_stats().channels(0).is_local());

  subspace::Request public_req;
  public_req.mutable_get_channel_stats()->set_channel_name("public_stats_ch");
  auto pub = conn.Send(public_req);
  ASSERT_OK(pub);
  ASSERT_EQ(1, pub->first.get_channel_stats().channels_size());
  EXPECT_FALSE(pub->first.get_channel_stats().channels(0).is_local());
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

// ---------------------------------------------------------------------------
// Client buffer registration ownership
// ---------------------------------------------------------------------------

// A publisher's own connection may register a client buffer for its channel.
TEST_F(ServerTest, RegisterClientBufferOwnerSucceeds) {
  RawConnection owner;
  ASSERT_OK(owner.Connect(Socket()));
  ASSERT_OK(owner.Init());
  auto [presp, pfds] = owner.CreatePublisher("cbreg_owner", 64, 4);
  ASSERT_TRUE(presp.create_publisher().error().empty());

  auto result = owner.Send(owner.MakeRegisterRequest(
      "cbreg_owner", owner.SessionId(), /*buffer_index=*/0, /*slot_id=*/0,
      /*has_fd=*/false, /*fd_index=*/-1));
  ASSERT_OK(result);
  EXPECT_TRUE(result->first.register_client_buffer().error().empty());

  auto buffers = owner.GetClientBuffers("cbreg_owner", owner.SessionId(), 0);
  ASSERT_OK(buffers);
  EXPECT_EQ(1, buffers->metadata_size());
}

// A client that owns no publisher on the channel cannot register a buffer for
// it, so it cannot inject a bogus (or fd-less) registration that other clients
// would then map.
TEST_F(ServerTest, RegisterClientBufferForeignRejected) {
  RawConnection owner;
  ASSERT_OK(owner.Connect(Socket()));
  ASSERT_OK(owner.Init());
  auto [presp, pfds] = owner.CreatePublisher("cbreg_foreign", 64, 4);
  ASSERT_TRUE(presp.create_publisher().error().empty());

  RawConnection foreign;
  ASSERT_OK(foreign.Connect(Socket()));
  ASSERT_OK(foreign.Init());

  auto result = foreign.Send(foreign.MakeRegisterRequest(
      "cbreg_foreign", foreign.SessionId(), 0, 0, /*has_fd=*/false, -1));
  ASSERT_OK(result);
  EXPECT_THAT(result->first.register_client_buffer().error(),
              ::testing::HasSubstr("does not own a publisher"));

  // Nothing was registered.
  auto buffers =
      foreign.GetClientBuffers("cbreg_foreign", foreign.SessionId(), 0);
  ASSERT_OK(buffers);
  EXPECT_EQ(0, buffers->metadata_size());
}

// A foreign client cannot erase another publisher's buffer registrations.
TEST_F(ServerTest, UnregisterClientBufferForeignRejected) {
  RawConnection owner;
  ASSERT_OK(owner.Connect(Socket()));
  ASSERT_OK(owner.Init());
  auto [presp, pfds] = owner.CreatePublisher("cbunreg_foreign", 64, 4);
  ASSERT_TRUE(presp.create_publisher().error().empty());

  auto reg = owner.Send(owner.MakeRegisterRequest(
      "cbunreg_foreign", owner.SessionId(), 0, 0, /*has_fd=*/false, -1));
  ASSERT_OK(reg);
  ASSERT_TRUE(reg->first.register_client_buffer().error().empty());

  RawConnection foreign;
  ASSERT_OK(foreign.Connect(Socket()));
  ASSERT_OK(foreign.Init());

  // UnregisterClientBuffer is a one-way request; observe the result via a
  // follow-up query on the same (foreign) connection.
  auto after = foreign.SendOneWayThenGetBuffers(
      foreign.MakeUnregisterRequest("cbunreg_foreign", foreign.SessionId(), 0),
      "cbunreg_foreign", foreign.SessionId(), 0);
  ASSERT_OK(after);
  EXPECT_EQ(1, after->metadata_size());

  // The owner can still see its buffer.
  auto owner_view =
      owner.GetClientBuffers("cbunreg_foreign", owner.SessionId(), 0);
  ASSERT_OK(owner_view);
  EXPECT_EQ(1, owner_view->metadata_size());
}

// An fd-backed registration that names an out-of-range fd index is rejected
// rather than silently registered without its backing fd.
TEST_F(ServerTest, RegisterClientBufferInvalidFdIndexRejected) {
  RawConnection owner;
  ASSERT_OK(owner.Connect(Socket()));
  ASSERT_OK(owner.Init());
  auto [presp, pfds] = owner.CreatePublisher("cbreg_badfd", 64, 4);
  ASSERT_TRUE(presp.create_publisher().error().empty());

  // Send one real fd but claim it is at index 5.
  int raw = ::open("/dev/null", O_RDONLY);
  ASSERT_GE(raw, 0);
  std::vector<toolbelt::FileDescriptor> send_fds;
  send_fds.emplace_back(raw);
  auto result = owner.SendWithFds(
      owner.MakeRegisterRequest("cbreg_badfd", owner.SessionId(), 0, 0,
                                /*has_fd=*/true, /*fd_index=*/5),
      send_fds);
  ASSERT_OK(result);
  EXPECT_THAT(result->first.register_client_buffer().error(),
              ::testing::HasSubstr("invalid fd index"));

  auto buffers = owner.GetClientBuffers("cbreg_badfd", owner.SessionId(), 0);
  ASSERT_OK(buffers);
  EXPECT_EQ(0, buffers->metadata_size());
}

#if !defined(__linux__) && !defined(__APPLE__) && !defined(__QNX__) &&         \
    !defined(__QNXNTO__)
TEST_F(ServerTest, ClientConnectionsDoNotLeakFileDescriptors) {
  GTEST_SKIP() << "open file descriptor counting is not implemented on this "
                  "platform";
}
#else

namespace {

struct OpenFileDescriptor {
  int fd = -1;
  std::string description;
};

#if defined(__linux__)
absl::StatusOr<std::vector<OpenFileDescriptor>> ListOpenFileDescriptors() {
  DIR *dir = ::opendir("/proc/self/fd");
  if (dir == nullptr) {
    return absl::InternalError(std::string("opendir /proc/self/fd: ") +
                               std::strerror(errno));
  }
  const int dir_fd = ::dirfd(dir);
  std::vector<OpenFileDescriptor> open_fds;
  while (const dirent *entry = ::readdir(dir)) {
    if (entry->d_name[0] == '.') {
      continue;
    }
    char *end = nullptr;
    const long fd = std::strtol(entry->d_name, &end, 10);
    if (end == entry->d_name || *end != '\0' || fd == dir_fd) {
      continue;
    }
    char link_path[64];
    std::snprintf(link_path, sizeof(link_path), "/proc/self/fd/%ld", fd);
    char target[PATH_MAX];
    const ssize_t n = ::readlink(link_path, target, sizeof(target) - 1);
    OpenFileDescriptor info;
    info.fd = static_cast<int>(fd);
    if (n >= 0) {
      target[n] = '\0';
      info.description = target;
    } else {
      info.description = "unknown";
    }
    open_fds.push_back(std::move(info));
  }
  ::closedir(dir);
  return open_fds;
}
#elif defined(__APPLE__)
const char *FileDescriptorTypeName(uint32_t type) {
  switch (type) {
  case PROX_FDTYPE_VNODE:
    return "vnode";
  case PROX_FDTYPE_SOCKET:
    return "socket";
  case PROX_FDTYPE_PSHM:
    return "posix_shm";
  case PROX_FDTYPE_PSEM:
    return "posix_sem";
  case PROX_FDTYPE_KQUEUE:
    return "kqueue";
  case PROX_FDTYPE_PIPE:
    return "pipe";
  case PROX_FDTYPE_FSEVENTS:
    return "fsevents";
  case PROX_FDTYPE_CHANNEL:
    return "channel";
  default:
    return "other";
  }
}

std::string DescribeFileDescriptor(pid_t pid, const proc_fdinfo &info) {
  const char *type = FileDescriptorTypeName(info.proc_fdtype);
  if (info.proc_fdtype == PROX_FDTYPE_VNODE) {
    vnode_fdinfowithpath vnode;
    const int n = ::proc_pidfdinfo(pid, info.proc_fd, PROC_PIDFDVNODEPATHINFO,
                                   &vnode, sizeof(vnode));
    if (n == static_cast<int>(sizeof(vnode)) &&
        vnode.pvip.vip_path[0] != '\0') {
      return std::string(type) + " " + vnode.pvip.vip_path;
    }
  }
  return type;
}

// macOS has no /proc. proc_pidinfo(PROC_PIDLISTFDS) lists this process's open
// descriptors. Grow the buffer until the kernel reports a short read, which
// means the list fit.
absl::StatusOr<std::vector<OpenFileDescriptor>> ListOpenFileDescriptors() {
  const pid_t pid = ::getpid();
  std::vector<char> buffer(32 * sizeof(proc_fdinfo));
  int result = 0;
  for (;;) {
    result = ::proc_pidinfo(pid, PROC_PIDLISTFDS, 0, buffer.data(),
                            static_cast<int>(buffer.size()));
    if (result <= 0) {
      return absl::InternalError(std::string("proc_pidinfo: ") +
                                 std::strerror(errno));
    }
    if (static_cast<size_t>(result) < buffer.size()) {
      break;
    }
    if (buffer.size() > (1u << 20)) {
      return absl::ResourceExhaustedError("too many open file descriptors");
    }
    buffer.resize(buffer.size() * 2);
  }

  const auto *info = reinterpret_cast<const proc_fdinfo *>(buffer.data());
  const int count = result / static_cast<int>(sizeof(proc_fdinfo));
  std::vector<OpenFileDescriptor> open_fds;
  open_fds.reserve(static_cast<size_t>(count));
  for (int i = 0; i < count; ++i) {
    OpenFileDescriptor fd;
    fd.fd = info[i].proc_fd;
    fd.description = DescribeFileDescriptor(pid, info[i]);
    open_fds.push_back(std::move(fd));
  }
  return open_fds;
}
#elif defined(__QNX__) || defined(__QNXNTO__)
// QNX has no /proc/self/fd. DCMD_PROC_INFO reports the process's open file
// descriptor count in procfs_info::num_fdcons. The descriptor opened to issue
// the devctl is included in that count and is subtracted here.
// https://www.qnx.com/developers/docs/7.1/com.qnx.doc.neutrino.prog/topic/process_DCMD_PROC_INFO.html
absl::StatusOr<std::vector<OpenFileDescriptor>> ListOpenFileDescriptors() {
  char path[64];
  std::snprintf(path, sizeof(path), "/proc/%d/as", ::getpid());
  const int ctl = ::open(path, O_RDONLY);
  if (ctl < 0) {
    return absl::InternalError(std::string("open ") + path + ": " +
                               std::strerror(errno));
  }
  procfs_info info;
  const int rc = ::devctl(ctl, DCMD_PROC_INFO, &info, sizeof(info), nullptr);
  ::close(ctl);
  if (rc != EOK) {
    return absl::InternalError(std::string("DCMD_PROC_INFO: ") +
                               std::strerror(rc));
  }
  if (info.num_fdcons == 0) {
    return absl::InternalError("DCMD_PROC_INFO reported no file descriptors");
  }
  const uint32_t open_count = info.num_fdcons - 1;
  std::vector<OpenFileDescriptor> open_fds;
  open_fds.reserve(open_count);
  for (uint32_t i = 0; i < open_count; ++i) {
    OpenFileDescriptor fd;
    // num_fdcons is a count, not a list of descriptor numbers. These entries
    // exist so the leak check can compare sizes; the numbers are not the
    // process's real fds.
    fd.fd = static_cast<int>(i);
    fd.description = "fd connection";
    open_fds.push_back(std::move(fd));
  }
  return open_fds;
}
#endif

absl::StatusOr<std::vector<OpenFileDescriptor>>
WaitForStableOpenFileDescriptors() {
  constexpr int kStableSamples = 5;
  constexpr auto kInterval = std::chrono::milliseconds(20);
  constexpr auto kTimeout = std::chrono::seconds(2);

  auto previous = ListOpenFileDescriptors();
  if (!previous.ok()) {
    return previous.status();
  }
  int stable_samples = 0;
  const auto deadline = std::chrono::steady_clock::now() + kTimeout;
  while (std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(kInterval);
    auto current = ListOpenFileDescriptors();
    if (!current.ok()) {
      return current.status();
    }
    if (current->size() == previous->size()) {
      if (++stable_samples >= kStableSamples) {
        return current;
      }
    } else {
      previous = std::move(current);
      stable_samples = 0;
    }
  }
  return previous;
}

// The server closes its side of a connection after it observes the client
// hangup, which happens on the server thread. Poll until the process fd count
// returns to `expected` or the timeout expires.
absl::StatusOr<std::vector<OpenFileDescriptor>>
WaitForOpenFileDescriptorCount(size_t expected) {
  constexpr auto kInterval = std::chrono::milliseconds(20);
  constexpr auto kTimeout = std::chrono::seconds(5);
  const auto deadline = std::chrono::steady_clock::now() + kTimeout;
  auto current = ListOpenFileDescriptors();
  while (current.ok() && current->size() != expected &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(kInterval);
    current = ListOpenFileDescriptors();
  }
  return current;
}

std::string FormatFileDescriptorsNotInBaseline(
    const std::vector<OpenFileDescriptor> &baseline,
    const std::vector<OpenFileDescriptor> &current) {
  std::set<int> baseline_fds;
  for (const OpenFileDescriptor &fd : baseline) {
    baseline_fds.insert(fd.fd);
  }
  std::string out;
  for (const OpenFileDescriptor &fd : current) {
    if (baseline_fds.count(fd.fd) != 0) {
      continue;
    }
    out += "  ";
    out += std::to_string(fd.fd);
    out += " ";
    out += fd.description;
    out += "\n";
  }
  if (out.empty()) {
    out = "  (every current fd number was also open at the baseline; "
          "descriptors may have been closed and their numbers reused)\n";
  }
  return out;
}

} // namespace

// The server runs in this process, so a descriptor it fails to close shows up
// in the process table once the client has closed its own. Linux reads
// /proc/self/fd. macOS has no /proc and uses proc_pidinfo(PROC_PIDLISTFDS).
// QNX reports the count through DCMD_PROC_INFO (num_fdcons). Trigger fds also
// differ by platform (one eventfd on Linux, a pipe on macOS); the test
// compares against a baseline taken on the same platform rather than a fixed
// count.
TEST_F(ServerTest, ClientConnectionsDoNotLeakFileDescriptors) {
  auto baseline_or = WaitForStableOpenFileDescriptors();
  ASSERT_OK(baseline_or);
  const std::vector<OpenFileDescriptor> baseline = std::move(*baseline_or);

  {
    constexpr int kConnections = 8;
    std::vector<RawConnection> connections;
    connections.reserve(kConnections);
    for (int i = 0; i < kConnections; ++i) {
      connections.emplace_back();
      ASSERT_OK(connections.back().Connect(Socket()));
      ASSERT_OK(connections.back().Init("fd_leak_idle"));
    }
    auto while_open = ListOpenFileDescriptors();
    ASSERT_OK(while_open);
    EXPECT_GT(while_open->size(), baseline.size())
        << "fd counter did not observe open client connections";
  }
  auto after_idle = WaitForOpenFileDescriptorCount(baseline.size());
  ASSERT_OK(after_idle);
  EXPECT_EQ(baseline.size(), after_idle->size())
      << "idle client connect/close leaked "
      << (static_cast<std::ptrdiff_t>(after_idle->size()) -
          static_cast<std::ptrdiff_t>(baseline.size()))
      << " file descriptors:\n"
      << FormatFileDescriptorsNotInBaseline(baseline, *after_idle);

  constexpr int kRounds = 8;
  for (int round = 0; round < kRounds; ++round) {
    constexpr int kConnections = 4;
    std::vector<RawConnection> connections;
    connections.reserve(kConnections);
    for (int i = 0; i < kConnections; ++i) {
      connections.emplace_back();
      ASSERT_OK(connections.back().Connect(Socket()));
      ASSERT_OK(connections.back().Init("fd_leak_raw"));
      const std::string channel =
          "fd_leak_raw_" + std::to_string(round) + "_" + std::to_string(i);
      // Drop the connection without an explicit remove so the server has to
      // reclaim the publisher, subscriber, trigger fds, and channel memory.
      auto [pub_resp, pub_fds] = connections.back().CreatePublisher(
          channel, 64, 4, "", /*reliable=*/true, /*is_local=*/true,
          /*fixed_size=*/false, /*mux=*/"", /*vchan_id=*/0,
          /*for_tunnel=*/false, /*notify_retirement=*/true);
      ASSERT_TRUE(pub_resp.has_create_publisher());
      ASSERT_TRUE(pub_resp.create_publisher().error().empty())
          << pub_resp.create_publisher().error();
      EXPECT_FALSE(pub_fds.empty());
      auto [sub_resp, sub_fds] =
          connections.back().CreateSubscriber(channel, "", /*reliable=*/true);
      ASSERT_TRUE(sub_resp.has_create_subscriber());
      ASSERT_TRUE(sub_resp.create_subscriber().error().empty())
          << sub_resp.create_subscriber().error();
      EXPECT_FALSE(sub_fds.empty());
    }
  }
  auto after_raw = WaitForOpenFileDescriptorCount(baseline.size());
  ASSERT_OK(after_raw);
  EXPECT_EQ(baseline.size(), after_raw->size())
      << "abrupt client disconnect leaked "
      << (static_cast<std::ptrdiff_t>(after_raw->size()) -
          static_cast<std::ptrdiff_t>(baseline.size()))
      << " file descriptors:\n"
      << FormatFileDescriptorsNotInBaseline(baseline, *after_raw);

  for (int round = 0; round < kRounds; ++round) {
    subspace::Client client;
    InitClient(client);
    const std::string channel = "fd_leak_client_" + std::to_string(round);
    absl::StatusOr<Publisher> pub = client.CreatePublisher(
        channel, 64, 4,
        subspace::PublisherOptions().SetReliable(true).SetNotifyRetirement(
            true));
    ASSERT_OK(pub);
    absl::StatusOr<Subscriber> sub = client.CreateSubscriber(
        channel, subspace::SubscriberOptions().SetReliable(true));
    ASSERT_OK(sub);
  }
  auto after_client = WaitForOpenFileDescriptorCount(baseline.size());
  ASSERT_OK(after_client);
  EXPECT_EQ(baseline.size(), after_client->size())
      << "client session shutdown leaked "
      << (static_cast<std::ptrdiff_t>(after_client->size()) -
          static_cast<std::ptrdiff_t>(baseline.size()))
      << " file descriptors:\n"
      << FormatFileDescriptorsNotInBaseline(baseline, *after_client);
}

#endif

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
