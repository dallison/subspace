// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "server/tunnel_config.h"

#include "absl/status/status_matchers.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <unistd.h>

#if defined(__ANDROID__)
#define TUNNEL_TEST_TMP "/data/local/tmp"
#else
#define TUNNEL_TEST_TMP "/tmp"
#endif

namespace subspace {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::testing::AllOf;
using ::testing::HasSubstr;

constexpr const char *kGoodConfig = R"pb(
  # This device and where peers reach it.
  device_name: "alpha"
  listen_address: "10.0.0.1"
  listen_port: 7000
  peers { name: "beta" tcp_addr: "10.0.0.2" tcp_port: 7000 }
  peers { name: "gamma" tcp_addr: "gamma.local" tcp_port: 7001 }
  discovery_denylist: "/too/chatty"
  discovery_denylist: "/also/chatty"
  discovery_refresh_secs: 2
)pb";

TEST(TunnelConfigTest, ParsesGoodConfig) {
  absl::StatusOr<TunnelConfig> config = ParseTunnelConfig(kGoodConfig, "good");
  ASSERT_THAT(config, IsOk());
  EXPECT_EQ(config->device_name(), "alpha");
  EXPECT_EQ(config->listen_address(), "10.0.0.1");
  EXPECT_EQ(config->listen_port(), 7000);
  ASSERT_EQ(config->peers_size(), 2);
  EXPECT_EQ(config->peers(1).name(), "gamma");
  EXPECT_EQ(config->peers(1).tcp_addr(), "gamma.local");
  EXPECT_EQ(config->peers(1).tcp_port(), 7001);
  ASSERT_EQ(config->discovery_denylist_size(), 2);
  EXPECT_EQ(config->discovery_denylist(0), "/too/chatty");
  EXPECT_EQ(TunnelDiscoveryRefreshSecs(*config), 2);
  EXPECT_THAT(ValidateTunnelConfig(*config, "alpha"), IsOk());
}

TEST(TunnelConfigTest, RefreshDefaultsAndZero) {
  absl::StatusOr<TunnelConfig> config =
      ParseTunnelConfig("listen_port: 7000", "minimal");
  ASSERT_THAT(config, IsOk());
  EXPECT_EQ(TunnelDiscoveryRefreshSecs(*config),
            kDefaultTunnelDiscoveryRefreshSecs);
  EXPECT_THAT(ValidateTunnelConfig(*config, "alpha"), IsOk());

  config = ParseTunnelConfig("listen_port: 7000 discovery_refresh_secs: 0",
                             "no-refresh");
  ASSERT_THAT(config, IsOk());
  EXPECT_EQ(TunnelDiscoveryRefreshSecs(*config), 0);
}

TEST(TunnelConfigTest, UnknownFieldIsAnErrorWithPosition) {
  absl::StatusOr<TunnelConfig> config =
      ParseTunnelConfig("listen_port: 7000\n  listen_prot: 7001\n", "typo.pb");
  EXPECT_THAT(config,
              StatusIs(absl::StatusCode::kInvalidArgument,
                       AllOf(HasSubstr("typo.pb:2:"), HasSubstr("listen_prot"))));
}

TEST(TunnelConfigTest, SyntaxErrorHasPosition) {
  absl::StatusOr<TunnelConfig> config = ParseTunnelConfig(
      "listen_port: 7000\npeers { name: \"beta\" \n", "unclosed.pb");
  EXPECT_THAT(config, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("unclosed.pb:3:")));

  config = ParseTunnelConfig("listen_port: \"seven\"", "type.pb");
  EXPECT_THAT(config, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("type.pb:1:")));
}

TEST(TunnelConfigTest, ValidationReportsEveryProblem) {
  absl::StatusOr<TunnelConfig> config = ParseTunnelConfig(R"pb(
    discovery_refresh_secs: -1
    peers { name: "alpha" tcp_addr: "10.0.0.1" tcp_port: 7000 }
    peers { name: "beta" tcp_port: 7000 }
    peers { name: "beta" tcp_addr: "10.0.0.3" tcp_port: 70000 }
    peers { tcp_addr: "10.0.0.4" tcp_port: 7000 }
  )pb",
                                                          "bad");
  ASSERT_THAT(config, IsOk());
  absl::Status status = ValidateTunnelConfig(*config, "alpha");
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(status.message(), HasSubstr("listen_port is required"));
  EXPECT_THAT(status.message(), HasSubstr("discovery_refresh_secs -1"));
  EXPECT_THAT(status.message(),
              HasSubstr("peer alpha has the same name as this device"));
  EXPECT_THAT(status.message(), HasSubstr("peer beta has no tcp_addr"));
  EXPECT_THAT(status.message(), HasSubstr("peer beta is listed more than once"));
  EXPECT_THAT(status.message(), HasSubstr("tcp_port 70000"));
  EXPECT_THAT(status.message(), HasSubstr("peers[3] has no name"));
}

TEST(TunnelConfigTest, ValidationChecksListenPortRange) {
  absl::StatusOr<TunnelConfig> config =
      ParseTunnelConfig("listen_port: 65536", "port");
  ASSERT_THAT(config, IsOk());
  EXPECT_THAT(ValidateTunnelConfig(*config, "alpha"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("listen_port 65536")));
  EXPECT_THAT(ValidateTunnelConfig(*config, ""),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("device name is empty")));
}

TEST(TunnelConfigTest, DeviceNamePrecedence) {
  TunnelConfig config;
  EXPECT_EQ(TunnelDeviceName(config, "", "host"), "host");
  EXPECT_EQ(TunnelDeviceName(config, "machine", "host"), "machine");
  config.set_device_name("configured");
  EXPECT_EQ(TunnelDeviceName(config, "machine", "host"), "configured");
}

TEST(TunnelConfigTest, LoadsFile) {
  char path[] = TUNNEL_TEST_TMP "/tunnel_config_test_XXXXXX";
  int fd = mkstemp(path);
  ASSERT_GE(fd, 0);
  close(fd);
  {
    std::ofstream out(path);
    out << kGoodConfig;
  }
  absl::StatusOr<TunnelConfig> config = LoadTunnelConfig(path);
  ASSERT_THAT(config, IsOk());
  EXPECT_EQ(config->device_name(), "alpha");

  {
    std::ofstream out(path);
    out << "bogus: 1\n";
  }
  EXPECT_THAT(LoadTunnelConfig(path),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr(std::string(path) + ":1:")));
  unlink(path);

  EXPECT_THAT(LoadTunnelConfig(path), StatusIs(absl::StatusCode::kNotFound));
}

} // namespace
} // namespace subspace
