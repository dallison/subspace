// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "google/protobuf/descriptor.h"
#include <iostream>
#include <map>
#include <memory>
#include <vector>

namespace subspace {

enum class RpcStyle { kCo, kAsio, kCoro, kCo20 };

class ServiceGenerator {
public:
  ServiceGenerator(const google::protobuf::ServiceDescriptor *service,
                   const std::string &added_namespace,
                   const std::string &package_name, RpcStyle rpc_style)
      : service_(service), added_namespace_(added_namespace),
        package_name_(package_name), rpc_style_(rpc_style) {}

  void GenerateClientHeader(std::ostream &os);
  void GenerateClientSource(std::ostream &os);

  void GenerateServerHeader(std::ostream &os);
  void GenerateServerSource(std::ostream &os);
  
private:
  void GenerateServerMethodRegistrations(std::ostream &os);
  void
  GenerateMethodClientHeader(const google::protobuf::MethodDescriptor *method,
                             std::ostream &os);
  void
  GenerateMethodClientSource(const google::protobuf::MethodDescriptor *method,
                             std::ostream &os);
  void
  GenerateMethodServerHeader(const google::protobuf::MethodDescriptor *method,
                             std::ostream &os);
  void
  GenerateMethodServerSource(const google::protobuf::MethodDescriptor *method,
                             std::ostream &os);

  std::string RpcNamespace() const;
  std::string CoroutineParam(bool with_default = false) const;
  std::string CoroutineArg() const;

  const google::protobuf::ServiceDescriptor *service_;
  std::string added_namespace_;
  std::string package_name_;
  RpcStyle rpc_style_;
};

} // namespace subspace
