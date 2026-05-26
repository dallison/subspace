// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "rpc/idl_compiler/service_gen.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_replace.h"
#include <algorithm>
#include <cassert>

namespace subspace {

std::string ServiceGenerator::ToSnakeCase(absl::string_view name) {
  std::string result;
  for (size_t i = 0; i < name.size(); i++) {
    char c = name[i];
    if (std::isupper(c)) {
      if (i > 0 && !std::isupper(name[i - 1])) {
        result += '_';
      } else if (i > 0 && i + 1 < name.size() && std::isupper(name[i - 1]) &&
                 std::islower(name[i + 1])) {
        result += '_';
      }
      result += static_cast<char>(std::tolower(c));
    } else {
      result += c;
    }
  }
  return result;
}

std::string ServiceGenerator::RpcNamespace() const {
  switch (rpc_style_) {
  case RpcStyle::kCo:
    return "subspace";
  case RpcStyle::kAsio:
    return "subspace::asio_rpc";
  case RpcStyle::kCoro:
    return "subspace::coro_rpc";
  case RpcStyle::kCo20:
    return "subspace::co20_rpc";
  case RpcStyle::kRust:
    return "subspace_rpc";
  }
  return "subspace";
}

std::string ServiceGenerator::CoroutineParam(bool with_default) const {
  switch (rpc_style_) {
  case RpcStyle::kCo:
    return with_default ? "co::Coroutine* c = nullptr" : "co::Coroutine* c";
  case RpcStyle::kAsio:
    return "boost::asio::yield_context yield";
  case RpcStyle::kCoro:
  case RpcStyle::kCo20:
  case RpcStyle::kRust:
    return "";
  }
  return "";
}

std::string ServiceGenerator::CoroutineArg() const {
  switch (rpc_style_) {
  case RpcStyle::kCo:
    return "c";
  case RpcStyle::kAsio:
    return "yield";
  case RpcStyle::kCoro:
  case RpcStyle::kCo20:
  case RpcStyle::kRust:
    return "";
  }
  return "";
}

void ServiceGenerator::GenerateClientHeader(std::ostream &os) {
  const auto ns = RpcNamespace();

  os << "class " << service_->name() << "Client {\n";
  os << "public:\n";
  os << "  " << service_->name()
     << "Client(uint64_t client_id, std::string "
        "subspace_socket) : client_(std::make_shared<"
     << ns << "::RpcClient>(\"" << service_->name()
     << "\", client_id, "
        "std::move(subspace_socket))) {\n";
  os << "  }\n";

  switch (rpc_style_) {
  case RpcStyle::kCo:
    os << "  static absl::StatusOr<std::shared_ptr<" << service_->name()
       << "Client>> Create(uint64_t client_id, const std::string& "
          "subspace_socket, co::Coroutine* c = nullptr) "
          "{\n";
    os << "    auto client = std::make_shared<" << service_->name()
       << "Client>(client_id, subspace_socket);\n";
    os << "    auto status = client->Open(c);\n";
    os << "    if (!status.ok()) {\n";
    os << "      return status;\n";
    os << "    }\n";
    os << "    return client;\n";
    os << "  }\n";
    os << "  absl::Status Open(co::Coroutine* c = nullptr) {\n";
    os << "    return client_->Open(c);\n";
    os << "  }\n";
    os << "  absl::Status Close(co::Coroutine* c = nullptr) {\n";
    os << "    return client_->Close(c);\n";
    os << "  }\n";
    os << "  absl::Status Close(std::chrono::nanoseconds timeout, "
          "co::Coroutine* c = nullptr) {\n";
    os << "    return client_->Close(timeout, c);\n";
    os << "  }\n";
    break;

  case RpcStyle::kAsio:
    os << "  static absl::StatusOr<std::shared_ptr<" << service_->name()
       << "Client>> Create(uint64_t client_id, const std::string& "
          "subspace_socket, boost::asio::yield_context yield) {\n";
    os << "    auto client = std::make_shared<" << service_->name()
       << "Client>(client_id, subspace_socket);\n";
    os << "    auto status = client->Open(yield);\n";
    os << "    if (!status.ok()) {\n";
    os << "      return status;\n";
    os << "    }\n";
    os << "    return client;\n";
    os << "  }\n";
    os << "  static absl::StatusOr<std::shared_ptr<" << service_->name()
       << "Client>> Create(uint64_t client_id, const std::string& "
          "subspace_socket) {\n";
    os << "    auto client = std::make_shared<" << service_->name()
       << "Client>(client_id, subspace_socket);\n";
    os << "    auto status = client->Open();\n";
    os << "    if (!status.ok()) {\n";
    os << "      return status;\n";
    os << "    }\n";
    os << "    return client;\n";
    os << "  }\n";
    os << "  absl::Status Open(boost::asio::yield_context yield) {\n";
    os << "    return client_->Open(yield);\n";
    os << "  }\n";
    os << "  absl::Status Open() {\n";
    os << "    return client_->Open();\n";
    os << "  }\n";
    os << "  absl::Status Close(boost::asio::yield_context yield) {\n";
    os << "    return client_->Close(yield);\n";
    os << "  }\n";
    os << "  absl::Status Close(std::chrono::nanoseconds timeout, "
          "boost::asio::yield_context yield) {\n";
    os << "    return client_->Close(timeout, yield);\n";
    os << "  }\n";
    os << "  absl::Status Close() {\n";
    os << "    return client_->Close();\n";
    os << "  }\n";
    os << "  absl::Status Close(std::chrono::nanoseconds timeout) {\n";
    os << "    return client_->Close(timeout);\n";
    os << "  }\n";
    break;

  case RpcStyle::kCoro:
    os << "  static boost::asio::awaitable<absl::StatusOr<std::shared_ptr<"
       << service_->name()
       << "Client>>> Create(uint64_t client_id, const std::string& "
          "subspace_socket) {\n";
    os << "    auto client = std::make_shared<" << service_->name()
       << "Client>(client_id, subspace_socket);\n";
    os << "    auto status = co_await client->Open();\n";
    os << "    if (!status.ok()) {\n";
    os << "      co_return status;\n";
    os << "    }\n";
    os << "    co_return client;\n";
    os << "  }\n";
    os << "  static absl::StatusOr<std::shared_ptr<" << service_->name()
       << "Client>> CreateBlocking(uint64_t client_id, const std::string& "
          "subspace_socket) {\n";
    os << "    auto client = std::make_shared<" << service_->name()
       << "Client>(client_id, subspace_socket);\n";
    os << "    auto status = client->OpenBlocking();\n";
    os << "    if (!status.ok()) {\n";
    os << "      return status;\n";
    os << "    }\n";
    os << "    return client;\n";
    os << "  }\n";
    os << "  boost::asio::awaitable<absl::Status> Open() {\n";
    os << "    return client_->Open();\n";
    os << "  }\n";
    os << "  absl::Status OpenBlocking() {\n";
    os << "    return client_->OpenBlocking();\n";
    os << "  }\n";
    os << "  boost::asio::awaitable<absl::Status> Close() {\n";
    os << "    return client_->Close();\n";
    os << "  }\n";
    os << "  boost::asio::awaitable<absl::Status> "
          "Close(std::chrono::nanoseconds timeout) {\n";
    os << "    return client_->Close(timeout);\n";
    os << "  }\n";
    os << "  absl::Status CloseBlocking() {\n";
    os << "    return client_->CloseBlocking();\n";
    os << "  }\n";
    os << "  absl::Status CloseBlocking(std::chrono::nanoseconds timeout) {\n";
    os << "    return client_->CloseBlocking(timeout);\n";
    os << "  }\n";
    break;

  case RpcStyle::kCo20:
    os << "  static co20::ValueTask<absl::StatusOr<std::shared_ptr<"
       << service_->name()
       << "Client>>> Create(uint64_t client_id, const std::string& "
          "subspace_socket) {\n";
    os << "    auto client = std::make_shared<" << service_->name()
       << "Client>(client_id, subspace_socket);\n";
    os << "    auto status = co_await client->Open();\n";
    os << "    if (!status.ok()) {\n";
    os << "      co_return status;\n";
    os << "    }\n";
    os << "    co_return client;\n";
    os << "  }\n";
    os << "  static absl::StatusOr<std::shared_ptr<" << service_->name()
       << "Client>> CreateBlocking(uint64_t client_id, const std::string& "
          "subspace_socket) {\n";
    os << "    auto client = std::make_shared<" << service_->name()
       << "Client>(client_id, subspace_socket);\n";
    os << "    auto status = client->OpenBlocking();\n";
    os << "    if (!status.ok()) {\n";
    os << "      return status;\n";
    os << "    }\n";
    os << "    return client;\n";
    os << "  }\n";
    os << "  co20::ValueTask<absl::Status> Open() {\n";
    os << "    return client_->Open();\n";
    os << "  }\n";
    os << "  absl::Status OpenBlocking() {\n";
    os << "    return client_->OpenBlocking();\n";
    os << "  }\n";
    os << "  co20::ValueTask<absl::Status> Close() {\n";
    os << "    return client_->Close();\n";
    os << "  }\n";
    os << "  co20::ValueTask<absl::Status> "
          "Close(std::chrono::nanoseconds timeout) {\n";
    os << "    return client_->Close(timeout);\n";
    os << "  }\n";
    os << "  absl::Status CloseBlocking() {\n";
    os << "    return client_->CloseBlocking();\n";
    os << "  }\n";
    os << "  absl::Status CloseBlocking(std::chrono::nanoseconds timeout) {\n";
    os << "    return client_->CloseBlocking(timeout);\n";
    os << "  }\n";
    break;
  case RpcStyle::kRust:
    break;
  }

  os << "  void SetLogLevel(const std::string& level) {\n";
  os << "    client_->SetLogLevel(level);\n";
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
  os << "  std::shared_ptr<" << ns << "::RpcClient> client_;\n";
  os << "};\n";
  os << "\n\n";
}

void ServiceGenerator::GenerateServerHeader(std::ostream &os) {
  const auto ns = RpcNamespace();

  os << "class " << service_->name() << "Server {\n";
  os << "public:\n";
  os << "  " << service_->name()
     << "Server(const std::string& "
        "subspace_socket) : server_(std::make_shared<"
     << ns << "::RpcServer>(\"" << service_->name()
     << "\", std::move(subspace_socket))) {}\n";
  os << "  virtual ~" << service_->name() << "Server() = default;\n";
  os << "\n";
  os << "  absl::Status RegisterMethods();\n";
  os << "\n";

  switch (rpc_style_) {
  case RpcStyle::kCo:
    os << "  absl::Status Run(co::CoroutineScheduler* scheduler = nullptr) {\n";
    os << "    return server_->Run(scheduler);\n";
    os << "  }\n";
    break;
  case RpcStyle::kAsio:
  case RpcStyle::kCoro:
    os << "  absl::Status Run(boost::asio::io_context* ioc = nullptr) {\n";
    os << "    return server_->Run(ioc);\n";
    os << "  }\n";
    break;
  case RpcStyle::kCo20:
    os << "  absl::Status Run(co20::Scheduler* scheduler = nullptr) {\n";
    os << "    return server_->Run(scheduler);\n";
    os << "  }\n";
    break;
  case RpcStyle::kRust:
    break;
  }

  os << "\n";
  os << "  void Stop() {\n";
  os << "    server_->Stop();\n";
  os << "  }\n";
  os << "\n";
  os << "  void SetLogLevel(const std::string& level) {\n";
  os << "    server_->SetLogLevel(level);\n";
  os << "  }\n";
  os << "\n";

  if (rpc_style_ == RpcStyle::kCo) {
    os << "  void DumpCoroutines() {\n";
    os << "    server_->Scheduler()->Show();\n";
    os << "  }\n";
    os << "\n";
  }

  os << "protected:\n";
  for (int i = 0; i < service_->method_count(); i++) {
    const auto *method = service_->method(i);
    GenerateMethodServerHeader(method, os);
  }
  os << "\n";
  os << "private:\n";
  os << "  std::shared_ptr<" << ns << "::RpcServer> server_;\n";
  os << "};\n";
}

void ServiceGenerator::GenerateClientSource(std::ostream &os) {
  for (int i = 0; i < service_->method_count(); i++) {
    const auto *method = service_->method(i);
    GenerateMethodClientSource(method, os);
  }
}

void ServiceGenerator::GenerateServerSource(std::ostream &os) {
  os << "\n";
  GenerateServerMethodRegistrations(os);
}

void ServiceGenerator::GenerateServerMethodRegistrations(std::ostream &os) {
  const auto ns = RpcNamespace();

  os << "absl::Status " << service_->name() << "Server::RegisterMethods() {\n";
  for (int i = 0; i < service_->method_count(); i++) {
    const auto *method = service_->method(i);

    if (method->server_streaming()) {
      os << "  {\n";
      os << "   auto status = server_->RegisterMethod<"
         << method->input_type()->name() << ", "
         << method->output_type()->name() << ">(\"" << method->name() << "\", ";

      switch (rpc_style_) {
      case RpcStyle::kCo:
        os << "[this](const auto &req, " << ns << "::StreamWriter<"
           << method->output_type()->name()
           << "> &writer, co::Coroutine *c) -> absl::Status {\n";
        os << "      return this->" << method->name() << "(req, writer, c);\n";
        break;
      case RpcStyle::kAsio:
        os << "[this](const auto &req, " << ns << "::StreamWriter<"
           << method->output_type()->name()
           << "> &writer, boost::asio::yield_context yield) -> absl::Status "
              "{\n";
        os << "      return this->" << method->name()
           << "(req, writer, yield);\n";
        break;
      case RpcStyle::kCoro:
        os << "[this](const auto &req, " << ns << "::StreamWriter<"
           << method->output_type()->name()
           << "> &writer) -> boost::asio::awaitable<absl::Status> {\n";
        os << "      co_return co_await this->" << method->name()
           << "(req, writer);\n";
        break;
      case RpcStyle::kCo20:
        os << "[this](const auto &req, " << ns << "::StreamWriter<"
           << method->output_type()->name()
           << "> &writer) -> co20::ValueTask<absl::Status> {\n";
        os << "      co_return co_await this->" << method->name()
           << "(req, writer);\n";
        break;
      case RpcStyle::kRust:
        break;
      }

      os << "    }, {.id=" << i << "});\n";
      os << "    if (!status.ok()) {\n";
      os << "      return status;\n";
      os << "    }\n";
      os << "  }\n";
      continue;
    }

    os << "  {\n";
    os << "   auto status = server_->RegisterMethod<"
       << method->input_type()->name() << ", " << method->output_type()->name()
       << ">(\"" << method->name() << "\", ";

    switch (rpc_style_) {
    case RpcStyle::kCo:
      os << "[this](const auto &req, auto *res, co::Coroutine *c) -> "
            "absl::Status {\n";
      os << "      auto s = this->" << method->name() << "(req, c);\n";
      os << "      if (!s.ok()) {\n";
      os << "        return s.status();\n";
      os << "      }\n";
      os << "      *res = std::move(s.value());\n";
      os << "      return absl::OkStatus();\n";
      break;
    case RpcStyle::kAsio:
      os << "[this](const auto &req, auto *res, boost::asio::yield_context "
            "yield) -> absl::Status {\n";
      os << "      auto s = this->" << method->name() << "(req, yield);\n";
      os << "      if (!s.ok()) {\n";
      os << "        return s.status();\n";
      os << "      }\n";
      os << "      *res = std::move(s.value());\n";
      os << "      return absl::OkStatus();\n";
      break;
    case RpcStyle::kCoro:
      os << "[this](const auto &req, auto *res) -> "
            "boost::asio::awaitable<absl::Status> {\n";
      os << "      auto s = co_await this->" << method->name() << "(req);\n";
      os << "      if (!s.ok()) {\n";
      os << "        co_return s.status();\n";
      os << "      }\n";
      os << "      *res = std::move(s.value());\n";
      os << "      co_return absl::OkStatus();\n";
      break;
    case RpcStyle::kCo20:
      os << "[this](const auto &req, auto *res) -> "
            "co20::ValueTask<absl::Status> {\n";
      os << "      auto s = co_await this->" << method->name()
         << "(req, res);\n";
      os << "      co_return s;\n";
      break;
    case RpcStyle::kRust:
      break;
    }

    os << "    }, {.id=" << i << "});\n";
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
  const auto method_name = method->name();
  const auto ns = RpcNamespace();

  if (method->server_streaming()) {
    switch (rpc_style_) {
    case RpcStyle::kCo:
      os << "  absl::Status " << method_name << "(";
      os << "const " << method->input_type()->name() << "& request";
      os << ", " << ns << "::ResponseReceiver<" << method->output_type()->name()
         << ">& receiver";
      os << ", std::chrono::nanoseconds timeout, co::Coroutine* c = nullptr";
      os << ");\n";
      os << "  absl::Status " << method_name << "(";
      os << "const " << method->input_type()->name() << "& request";
      os << ", " << ns << "::ResponseReceiver<" << method->output_type()->name()
         << ">& receiver";
      os << ", co::Coroutine* c = nullptr";
      os << ") {\n";
      os << "    return " << method_name
         << "(request, receiver, std::chrono::nanoseconds(0), c);\n";
      os << "  }\n";
      break;
    case RpcStyle::kAsio:
      os << "  absl::Status " << method_name << "(";
      os << "const " << method->input_type()->name() << "& request";
      os << ", " << ns << "::ResponseReceiver<" << method->output_type()->name()
         << ">& receiver";
      os << ", std::chrono::nanoseconds timeout, boost::asio::yield_context "
            "yield";
      os << ");\n";
      os << "  absl::Status " << method_name << "(";
      os << "const " << method->input_type()->name() << "& request";
      os << ", " << ns << "::ResponseReceiver<" << method->output_type()->name()
         << ">& receiver";
      os << ", boost::asio::yield_context yield";
      os << ") {\n";
      os << "    return " << method_name
         << "(request, receiver, std::chrono::nanoseconds(0), yield);\n";
      os << "  }\n";
      break;
    case RpcStyle::kCoro:
      os << "  boost::asio::awaitable<absl::Status> " << method_name << "(";
      os << "const " << method->input_type()->name() << "& request";
      os << ", " << ns << "::ResponseReceiver<" << method->output_type()->name()
         << ">& receiver";
      os << ", std::chrono::nanoseconds timeout";
      os << ");\n";
      os << "  boost::asio::awaitable<absl::Status> " << method_name << "(";
      os << "const " << method->input_type()->name() << "& request";
      os << ", " << ns << "::ResponseReceiver<" << method->output_type()->name()
         << ">& receiver";
      os << ") {\n";
      os << "    return " << method_name
         << "(request, receiver, std::chrono::nanoseconds(0));\n";
      os << "  }\n";
      break;
    case RpcStyle::kCo20:
      os << "  co20::ValueTask<absl::Status> " << method_name << "(";
      os << "const " << method->input_type()->name() << "& request";
      os << ", " << ns << "::ResponseReceiver<" << method->output_type()->name()
         << ">& receiver";
      os << ", std::chrono::nanoseconds timeout";
      os << ");\n";
      os << "  co20::ValueTask<absl::Status> " << method_name << "(";
      os << "const " << method->input_type()->name() << "& request";
      os << ", " << ns << "::ResponseReceiver<" << method->output_type()->name()
         << ">& receiver";
      os << ") {\n";
      os << "    return " << method_name
         << "(request, receiver, std::chrono::nanoseconds(0));\n";
      os << "  }\n";
      break;
    case RpcStyle::kRust:
      break;
    }
    return;
  }

  switch (rpc_style_) {
  case RpcStyle::kCo:
    os << "  absl::StatusOr<" << method->output_type()->name() << "> "
       << method_name << "(";
    os << "const " << method->input_type()->name() << "& request";
    os << ", std::chrono::nanoseconds timeout, co::Coroutine* c = nullptr";
    os << ");\n";
    os << "  absl::StatusOr<" << method->output_type()->name() << "> "
       << method_name << "(";
    os << "const " << method->input_type()->name() << "& request";
    os << ", co::Coroutine* c = nullptr";
    os << ") {\n";
    os << "    return " << method_name
       << "(request, std::chrono::nanoseconds(0), c);\n";
    os << "  }\n";
    break;
  case RpcStyle::kAsio:
    os << "  absl::StatusOr<" << method->output_type()->name() << "> "
       << method_name << "(";
    os << "const " << method->input_type()->name() << "& request";
    os << ", std::chrono::nanoseconds timeout, boost::asio::yield_context yield";
    os << ");\n";
    os << "  absl::StatusOr<" << method->output_type()->name() << "> "
       << method_name << "(";
    os << "const " << method->input_type()->name() << "& request";
    os << ", boost::asio::yield_context yield";
    os << ") {\n";
    os << "    return " << method_name
       << "(request, std::chrono::nanoseconds(0), yield);\n";
    os << "  }\n";
    break;
  case RpcStyle::kCoro:
    os << "  boost::asio::awaitable<absl::StatusOr<"
       << method->output_type()->name() << ">> " << method_name << "(";
    os << "const " << method->input_type()->name() << "& request";
    os << ", std::chrono::nanoseconds timeout";
    os << ");\n";
    os << "  boost::asio::awaitable<absl::StatusOr<"
       << method->output_type()->name() << ">> " << method_name << "(";
    os << "const " << method->input_type()->name() << "& request";
    os << ") {\n";
    os << "    return " << method_name
       << "(request, std::chrono::nanoseconds(0));\n";
    os << "  }\n";
    break;
  case RpcStyle::kCo20:
    os << "  co20::ValueTask<absl::Status> " << method_name << "(";
    os << "const " << method->input_type()->name() << "& request";
    os << ", absl::StatusOr<" << method->output_type()->name() << ">* out";
    os << ", std::chrono::nanoseconds timeout";
    os << ");\n";
    os << "  co20::ValueTask<absl::Status> " << method_name << "(";
    os << "const " << method->input_type()->name() << "& request";
    os << ", absl::StatusOr<" << method->output_type()->name() << ">* out";
    os << ") {\n";
    os << "    return " << method_name
       << "(request, out, std::chrono::nanoseconds(0));\n";
    os << "  }\n";
    break;
  case RpcStyle::kRust:
    break;
  }
}

void ServiceGenerator::GenerateMethodClientSource(
    const google::protobuf::MethodDescriptor *method, std::ostream &os) {
  const auto method_name = method->name();
  const auto ns = RpcNamespace();

  if (method->server_streaming()) {
    switch (rpc_style_) {
    case RpcStyle::kCo:
      os << "absl::Status " << service_->name() << "Client::" << method_name
         << "(const " << method->input_type()->name() << "& request, " << ns
         << "::ResponseReceiver<" << method->output_type()->name()
         << ">& receiver, std::chrono::nanoseconds timeout, co::Coroutine* c) "
            "{\n";
      os << "  return client_->Call<" << method->input_type()->name() << ", "
         << method->output_type()->name() << ">(k" << method_name
         << "Id, request, receiver, timeout, c);\n";
      break;
    case RpcStyle::kAsio:
      os << "absl::Status " << service_->name() << "Client::" << method_name
         << "(const " << method->input_type()->name() << "& request, " << ns
         << "::ResponseReceiver<" << method->output_type()->name()
         << ">& receiver, std::chrono::nanoseconds timeout, "
            "boost::asio::yield_context yield) {\n";
      os << "  return client_->Call<" << method->input_type()->name() << ", "
         << method->output_type()->name() << ">(k" << method_name
         << "Id, request, receiver, timeout, yield);\n";
      break;
    case RpcStyle::kCoro:
      os << "boost::asio::awaitable<absl::Status> " << service_->name()
         << "Client::" << method_name << "(const "
         << method->input_type()->name() << "& request, " << ns
         << "::ResponseReceiver<" << method->output_type()->name()
         << ">& receiver, std::chrono::nanoseconds timeout) {\n";
      os << "  return client_->Call<" << method->input_type()->name() << ", "
         << method->output_type()->name() << ">(k" << method_name
         << "Id, request, receiver, timeout);\n";
      break;
    case RpcStyle::kCo20:
      os << "co20::ValueTask<absl::Status> " << service_->name()
         << "Client::" << method_name << "(const "
         << method->input_type()->name() << "& request, " << ns
         << "::ResponseReceiver<" << method->output_type()->name()
         << ">& receiver, std::chrono::nanoseconds timeout) {\n";
      os << "  return client_->Call<" << method->input_type()->name() << ", "
         << method->output_type()->name() << ">(k" << method_name
         << "Id, request, receiver, timeout);\n";
      break;
    case RpcStyle::kRust:
      break;
    }
    os << "}\n";
    return;
  }

  switch (rpc_style_) {
  case RpcStyle::kCo:
    os << "absl::StatusOr<" << method->output_type()->name() << "> "
       << service_->name() << "Client::" << method_name << "(const "
       << method->input_type()->name()
       << "& request, std::chrono::nanoseconds timeout, co::Coroutine* c) {\n";
    os << "  return client_->Call<" << method->input_type()->name() << ", "
       << method->output_type()->name() << ">(k" << method_name
       << "Id, request, timeout, c);\n";
    break;
  case RpcStyle::kAsio:
    os << "absl::StatusOr<" << method->output_type()->name() << "> "
       << service_->name() << "Client::" << method_name << "(const "
       << method->input_type()->name()
       << "& request, std::chrono::nanoseconds timeout, "
          "boost::asio::yield_context yield) {\n";
    os << "  return client_->Call<" << method->input_type()->name() << ", "
       << method->output_type()->name() << ">(k" << method_name
       << "Id, request, timeout, yield);\n";
    break;
  case RpcStyle::kCoro:
    os << "boost::asio::awaitable<absl::StatusOr<"
       << method->output_type()->name() << ">> " << service_->name()
       << "Client::" << method_name << "(const "
       << method->input_type()->name()
       << "& request, std::chrono::nanoseconds timeout) {\n";
    os << "  return client_->Call<" << method->input_type()->name() << ", "
       << method->output_type()->name() << ">(k" << method_name
       << "Id, request, timeout);\n";
    break;
  case RpcStyle::kCo20:
    os << "co20::ValueTask<absl::Status> " << service_->name()
       << "Client::" << method_name << "(const "
       << method->input_type()->name()
       << "& request, absl::StatusOr<" << method->output_type()->name()
       << ">* out, std::chrono::nanoseconds timeout) {\n";
    os << "  return client_->Call<" << method->input_type()->name() << ", "
       << method->output_type()->name() << ">(k" << method_name
       << "Id, request, out, timeout);\n";
    break;
  case RpcStyle::kRust:
    break;
  }
  os << "}\n";
}

void ServiceGenerator::GenerateMethodServerHeader(
    const google::protobuf::MethodDescriptor *method, std::ostream &os) {
  const auto ns = RpcNamespace();

  if (method->server_streaming()) {
    switch (rpc_style_) {
    case RpcStyle::kCo:
      os << "  virtual absl::Status " << method->name() << "(const "
         << method->input_type()->name() << "& request, " << ns
         << "::StreamWriter<" << method->output_type()->name()
         << ">& writer, co::Coroutine* c = nullptr) = 0;\n";
      break;
    case RpcStyle::kAsio:
      os << "  virtual absl::Status " << method->name() << "(const "
         << method->input_type()->name() << "& request, " << ns
         << "::StreamWriter<" << method->output_type()->name()
         << ">& writer, boost::asio::yield_context yield) = 0;\n";
      break;
    case RpcStyle::kCoro:
      os << "  virtual boost::asio::awaitable<absl::Status> " << method->name()
         << "(const " << method->input_type()->name() << "& request, " << ns
         << "::StreamWriter<" << method->output_type()->name()
         << ">& writer) = 0;\n";
      break;
    case RpcStyle::kCo20:
      os << "  virtual co20::ValueTask<absl::Status> " << method->name()
         << "(const " << method->input_type()->name() << "& request, " << ns
         << "::StreamWriter<" << method->output_type()->name()
         << ">& writer) = 0;\n";
      break;
    case RpcStyle::kRust:
      break;
    }
    return;
  }

  switch (rpc_style_) {
  case RpcStyle::kCo:
    os << "  virtual absl::StatusOr<" << method->output_type()->name() << "> "
       << method->name() << "(const " << method->input_type()->name()
       << "& request, co::Coroutine* c = nullptr) = 0;\n";
    break;
  case RpcStyle::kAsio:
    os << "  virtual absl::StatusOr<" << method->output_type()->name() << "> "
       << method->name() << "(const " << method->input_type()->name()
       << "& request, boost::asio::yield_context yield) = 0;\n";
    break;
  case RpcStyle::kCoro:
    os << "  virtual boost::asio::awaitable<absl::StatusOr<"
       << method->output_type()->name() << ">> " << method->name() << "(const "
       << method->input_type()->name() << "& request) = 0;\n";
    break;
  case RpcStyle::kCo20:
    os << "  virtual co20::ValueTask<absl::Status> " << method->name()
       << "(const " << method->input_type()->name() << "& request, "
       << method->output_type()->name() << "* response) = 0;\n";
    break;
  case RpcStyle::kRust:
    break;
  }
}

void ServiceGenerator::GenerateMethodServerSource(
    [[maybe_unused]] const google::protobuf::MethodDescriptor *method,
    [[maybe_unused]] std::ostream &os) {}

// ---------------------------------------------------------------------------
// Rust code generation
// ---------------------------------------------------------------------------

void ServiceGenerator::GenerateRustClientFile(std::ostream &os) {
  const std::string service_name(service_->name());
  const std::string client_name = service_name + "Client";

  os << "pub struct " << client_name << " {\n";
  os << "    client: RpcClient,\n";
  os << "}\n\n";

  os << "impl " << client_name << " {\n";

  // Method ID constants.
  for (int i = 0; i < service_->method_count(); i++) {
    const auto *method = service_->method(i);
    os << "    const "
       << absl::AsciiStrToUpper(ToSnakeCase(method->name())) << "_ID"
       << ": i32 = " << i << ";\n";
  }
  os << "\n";

  // create()
  os << "    pub async fn create(client_id: u64, subspace_socket: &str) -> Result<Self> {\n";
  os << "        let mut client = RpcClient::new(client_id, subspace_socket, \""
     << service_name << "\");\n";
  os << "        client.open(None).await?;\n";
  os << "        Ok(Self { client })\n";
  os << "    }\n\n";

  // create_with_timeout()
  os << "    pub async fn create_with_timeout(client_id: u64, subspace_socket: &str, timeout: Duration) -> Result<Self> {\n";
  os << "        let mut client = RpcClient::new(client_id, subspace_socket, \""
     << service_name << "\");\n";
  os << "        client.open(Some(timeout)).await?;\n";
  os << "        Ok(Self { client })\n";
  os << "    }\n\n";

  // close()
  os << "    pub async fn close(&mut self) -> Result<()> {\n";
  os << "        self.client.close(None).await\n";
  os << "    }\n\n";

  // close_with_timeout()
  os << "    pub async fn close_with_timeout(&mut self, timeout: Duration) -> Result<()> {\n";
  os << "        self.client.close(Some(timeout)).await\n";
  os << "    }\n\n";

  // Per-method.
  for (int i = 0; i < service_->method_count(); i++) {
    const auto *method = service_->method(i);
    const std::string rust_method = ToSnakeCase(method->name());
    const std::string input_type(method->input_type()->name());
    const std::string output_type(method->output_type()->name());
    const std::string id_const =
        absl::AsciiStrToUpper(ToSnakeCase(method->name())) + "_ID";

    if (method->server_streaming()) {
      os << "    pub async fn " << rust_method
         << "(&self, request: &" << input_type
         << ", timeout: Option<Duration>) -> Result<ResponseStream<"
         << output_type << ">> {\n";
      os << "        self.client.call_server_streaming(Self::" << id_const
         << ", request, timeout).await\n";
      os << "    }\n\n";
    } else {
      os << "    pub async fn " << rust_method
         << "(&self, request: &" << input_type
         << ", timeout: Option<Duration>) -> Result<"
         << output_type << "> {\n";
      os << "        self.client.call(Self::" << id_const
         << ", request, timeout).await\n";
      os << "    }\n\n";
    }
  }

  os << "}\n\n";
}

void ServiceGenerator::GenerateRustServerFile(std::ostream &os) {
  const std::string service_name(service_->name());
  const std::string handler_name = service_name + "Handler";
  const std::string server_name = service_name + "Server";

  // Handler trait.
  os << "#[async_trait::async_trait]\n";
  os << "pub trait " << handler_name << ": Send + Sync + 'static {\n";
  for (int i = 0; i < service_->method_count(); i++) {
    const auto *method = service_->method(i);
    const std::string rust_method = ToSnakeCase(method->name());
    const std::string input_type(method->input_type()->name());
    const std::string output_type(method->output_type()->name());

    if (method->server_streaming()) {
      os << "    async fn " << rust_method << "(&self, request: "
         << input_type << ", writer: TypedStreamWriter<"
         << output_type << ">) -> Result<()>;\n";
    } else {
      os << "    async fn " << rust_method << "(&self, request: "
         << input_type << ") -> Result<" << output_type << ">;\n";
    }
  }
  os << "}\n\n";

  // Per-method handler adapter structs (bridges trait methods to MethodHandler).
  for (int i = 0; i < service_->method_count(); i++) {
    const auto *method = service_->method(i);
    const std::string method_name_pascal(method->name());
    const std::string input_type(method->input_type()->name());
    const std::string output_type(method->output_type()->name());
    const std::string adapter_name = method_name_pascal + "Adapter";

    if (method->server_streaming()) {
      os << "struct " << adapter_name << "<H: " << handler_name << "> {\n";
      os << "    handler: Arc<H>,\n";
      os << "}\n\n";

      os << "#[async_trait::async_trait]\n";
      os << "impl<H: " << handler_name << "> StreamingMethodHandler for "
         << adapter_name << "<H> {\n";
      os << "    async fn handle(&self, request: prost_types::Any, writer: StreamWriter) -> Result<()> {\n";
      os << "        let req: " << input_type
         << " = subspace_rpc::async_io::unpack_any(&request)?;\n";
      os << "        let typed_writer = TypedStreamWriter::new(writer, \""
         << "type.googleapis.com/" << package_name_ << "." << output_type
         << "\".to_string());\n";
      os << "        self.handler." << ToSnakeCase(method->name())
         << "(req, typed_writer).await\n";
      os << "    }\n";
      os << "}\n\n";
    } else {
      os << "struct " << adapter_name << "<H: " << handler_name << "> {\n";
      os << "    handler: Arc<H>,\n";
      os << "}\n\n";

      os << "#[async_trait::async_trait]\n";
      os << "impl<H: " << handler_name << "> MethodHandler for "
         << adapter_name << "<H> {\n";
      os << "    async fn handle(&self, request: prost_types::Any) -> Result<prost_types::Any> {\n";
      os << "        let req: " << input_type
         << " = subspace_rpc::async_io::unpack_any(&request)?;\n";
      os << "        let resp = self.handler." << ToSnakeCase(method->name())
         << "(req).await?;\n";
      os << "        Ok(subspace_rpc::async_io::pack_any(&resp, \""
         << "type.googleapis.com/" << package_name_ << "." << output_type
         << "\"))\n";
      os << "    }\n";
      os << "}\n\n";
    }
  }

  // Server struct.
  os << "pub struct " << server_name << " {\n";
  os << "    server: RpcServer,\n";
  os << "}\n\n";

  os << "impl " << server_name << " {\n";

  // new()
  os << "    pub fn new<H: " << handler_name << ">(subspace_socket: &str, handler: Arc<H>) -> Self {\n";
  os << "        let mut server = RpcServer::new(subspace_socket, \""
     << service_name << "\");\n";

  for (int i = 0; i < service_->method_count(); i++) {
    const auto *method = service_->method(i);
    const std::string adapter_name = std::string(method->name()) + "Adapter";
    if (method->server_streaming()) {
      os << "        server.register_streaming_method(\""
         << method->name() << "\", " << i
         << ", Arc::new(" << adapter_name
         << " { handler: handler.clone() }), MethodOptions { id: " << i
         << ", ..Default::default() });\n";
    } else {
      os << "        server.register_method(\""
         << method->name() << "\", " << i
         << ", Arc::new(" << adapter_name
         << " { handler: handler.clone() }), MethodOptions { id: " << i
         << ", ..Default::default() });\n";
    }
  }

  os << "        Self { server }\n";
  os << "    }\n\n";

  // run()
  os << "    pub async fn run(&self, shutdown: tokio::sync::watch::Receiver<bool>) -> Result<()> {\n";
  os << "        self.server.run(shutdown).await\n";
  os << "    }\n";

  os << "}\n\n";
}

} // namespace subspace
