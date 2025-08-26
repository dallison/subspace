// Copyright 2024 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "rpc/idl_compiler/service_gen.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_replace.h"
#include <algorithm>
#include <cassert>
#include <ctype.h>

namespace subspace {
void ServiceGenerator::GenerateClientHeader(std::ostream &os) {
  // Client side class.
  os << "class " << service_->name() << "Client {\n";
  os << "public:\n";
  os << "  " << service_->name()
     << "Client(uint64_t client_id, std::string "
        "subspace_socket) : client_(\""
     << service_->name()
     << "\", client_id, "
        "std::move(subspace_socket)) {\n";
  os << "  }\n";
  os << "  absl::Status Open(co::Coroutine* c = nullptr) {\n";
  os << "    return client_.Open(c);\n";
  os << "  }\n";
  os << "  absl::Status Close(co::Coroutine* c = nullptr) {\n";
  os << "    return client_.Close(c);\n";
  os << "  }\n";
  os << "  void SetLogLevel(const std::string& level) {\n";
  os << "    client_.SetLogLevel(level);\n";
  os << "  }\n";
  os << "\n";
  for (int i = 0; i < service_->method_count(); i++) {
    const auto *method = service_->method(i);
    GenerateMethodClientHeader(method, os);
  }
  os << "\n";
  os << "private:\n";
  os << "  friend class " << service_->name() << "Server;\n";
  os << "\n";
  for (int i = 0; i < service_->method_count(); i++) {
    const auto *method = service_->method(i);
    os << "  static constexpr int k" << absl::StrCat(method->name(), "Id")
       << " = " << i << ";\n";
  }
  os << "  subspace::RpcClient client_;\n";
  os << "};\n";

  os << "\n\n";
}

void ServiceGenerator::GenerateServerHeader(std::ostream &os) {
  // Server side class.
  os << "class " << service_->name() << "Server {\n";
  os << "public:\n";
  os << "  " << service_->name()
     << "Server(const std::string& "
        "subspace_socket) : server_(std::make_shared<subspace::RpcServer>(\""
     << service_->name() << "\", std::move(subspace_socket))) {}\n";
  os << "  virtual ~" << service_->name() << "Server() = default;\n";
  os << "\n";
  os << "  absl::Status RegisterMethods();\n";
  os << "\n";
  os << "  absl::Status Run(co::CoroutineScheduler* scheduler = nullptr) {\n";
  os << "    return server_->Run(scheduler);\n";
  os << "  }\n";
  os << "\n";
  os << "  void Stop() {\n";
  os << "    server_->Stop();\n";
  os << "  }\n";
  os << "\n";
  os << "  void SetLogLevel(const std::string& level) {\n";
  os << "    server_->SetLogLevel(level);\n";
  os << "  }\n";
  os << "\n";
  os << "protected:\n";
  for (int i = 0; i < service_->method_count(); i++) {
    const auto *method = service_->method(i);
    GenerateMethodServerHeader(method, os);
  }
  os << "\n";
  os << "private:\n";
  os << "  std::shared_ptr<subspace::RpcServer> server_;\n";
  os << "};\n";
}

void ServiceGenerator::GenerateClientSource(std::ostream &os) {
  // Client side method definitions.
  for (int i = 0; i < service_->method_count(); i++) {
    const auto *method = service_->method(i);
    GenerateMethodClientSource(method, os);
  }
}

void ServiceGenerator::GenerateServerSource(std::ostream &os) {
  // Method registrations.
  os << "\n";
  GenerateServerMethodRegistrations(os);
}

void ServiceGenerator::GenerateServerMethodRegistrations(std::ostream &os) {
  os << "absl::Status " << service_->name() << "Server::RegisterMethods() {\n";
  for (int i = 0; i < service_->method_count(); i++) {
    const auto *method = service_->method(i);
    os << "  {\n";
    os << "   auto status = server_->RegisterMethod<"
       << method->input_type()->name() << ", " << method->output_type()->name()
       << ">(\"" << method->name()
       << "\", [this](const auto &req, auto *res, co::Coroutine *c) -> "
          "absl::Status {\n";
    os << "      auto s = this->" << method->name() << "(req, c);\n";
    os << "      if (!s.ok()) {\n";
    os << "        return s.status();\n";
    os << "      }\n";
    os << "      *res = std::move(s.value());\n";
    os << "      return absl::OkStatus();\n";
    os << "    }, " << i << ");\n";
    os << "    if (!status.ok()) {\n";
    os << "      return status;\n";
    os << "    }\n";
    os << "  }\n";
  }
  os << "  return absl::OkStatus();\n";
  os << "}\n";
}

void ServiceGenerator::GenerateMethodClientHeader(
    const google::protobuf::MethodDescriptor *method, std::ostream &os) {
  std::string method_name = method->name();
  os << "  absl::StatusOr<" << method->output_type()->name() << "> "
     << method_name << "(";
  os << "const " << method->input_type()->name() << "& request";
  os << ", co::Coroutine* c = nullptr";
  os << ");\n";
}

void ServiceGenerator::GenerateMethodClientSource(
    const google::protobuf::MethodDescriptor *method, std::ostream &os) {
  std::string method_name = method->name();
  os << "absl::StatusOr<" << method->output_type()->name() << "> "
     << service_->name() << "Client::" << method_name << "(const "
     << method->input_type()->name() << "& request, co::Coroutine* c) {\n";
  os << "  return client_.Call<" << method->input_type()->name() << ", "
     << method->output_type()->name() << ">(k" << method_name
     << "Id, request, c);\n";
  os << "}\n";
}

void ServiceGenerator::GenerateMethodServerHeader(
    const google::protobuf::MethodDescriptor *method, std::ostream &os) {
  os << "  virtual absl::StatusOr<" << method->output_type()->name() << "> "
     << method->name() << "(const " << method->input_type()->name()
     << "& request, co::Coroutine* c = nullptr) = 0;\n";
}

void ServiceGenerator::GenerateMethodServerSource(
    const google::protobuf::MethodDescriptor *method, std::ostream &os) {}

} // namespace subspace
