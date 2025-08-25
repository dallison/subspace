// Copyright 2025 David Allison
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
class ServiceGenerator {
public:
  ServiceGenerator(const google::protobuf::ServiceDescriptor *service,
                   const std::string &added_namespace,
                   const std::string &package_name)
      : service_(service), added_namespace_(added_namespace),
        package_name_(package_name) {}

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
  const google::protobuf::ServiceDescriptor *service_;
  std::string added_namespace_;
  std::string package_name_;
};

} // namespace subspace
