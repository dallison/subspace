// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "rpc/idl_compiler/gen.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include <filesystem>
#include <fstream>
#include <string_view>

namespace subspace {

static void
WriteToZeroCopyStream(const std::string &data,
                      google::protobuf::io::ZeroCopyOutputStream *stream) {
  // Write to the stream that protobuf wants
  void *data_buffer;
  int size;
  size_t offset = 0;
  while (offset < data.size()) {
    stream->Next(&data_buffer, &size);
    int to_copy = std::min(size, static_cast<int>(data.size() - offset));
    std::memcpy(data_buffer, data.data() + offset, to_copy);
    offset += to_copy;
    stream->BackUp(size - to_copy);
  }
}

static std::string GeneratedFilename(const std::filesystem::path &package_name,
                                     const std::filesystem::path &target_name,
                                     std::string_view filename) {
  size_t virtual_imports = filename.find("_virtual_imports/");
  if (virtual_imports != std::string_view::npos) {
    // This is something like:
    // bazel-out/darwin_arm64-dbg/bin/external/com_google_protobuf/_virtual_imports/any_proto/google/protobuf/any.proto
    filename = filename.substr(virtual_imports + sizeof("_virtual_imports/"));
    // Remove the first directory.
    filename = filename.substr(filename.find('/') + 1);
  }
  return package_name / target_name / filename;
}

bool CodeGenerator::Generate(
    const google::protobuf::FileDescriptor *file, const std::string &parameter,
    google::protobuf::compiler::GeneratorContext *generator_context,
    std::string *error) const {

  // The options for the compiler are passed in the --subspace_rpc_out parameter
  // as a comma separated list of key=value pairs, followed by a colon
  // and then the output directory.
  std::vector<std::pair<std::string, std::string>> options;
  google::protobuf::compiler::ParseGeneratorParameter(parameter, &options);

  for (auto option : options) {
    std::cerr << "option " << option.first << "=" << option.second
              << "\n";
    if (option.first == "add_namespace") {
      added_namespace_ = option.second;
    } else if (option.first == "package_name") {
      package_name_ = option.second;
    } else if (option.first == "target_name") {
      target_name_ = option.second;
    }
  }

  Generator gen(file, added_namespace_, package_name_, target_name_);

  if (!GenerateClient(file, gen, generator_context, error)) {
    *error = "Failed to generate client code";
    return false;
  }

  if (!GenerateServer(file, gen, generator_context, error)) {
    *error = "Failed to generate server code";
    return false;
  }

  return true;
}

bool CodeGenerator::GenerateClient(
    const google::protobuf::FileDescriptor *file, Generator &gen,
    google::protobuf::compiler::GeneratorContext *generator_context,
    std::string *error) const {
  std::string filename =
      GeneratedFilename(package_name_, target_name_, file->name());

  std::filesystem::path hp(filename);
  hp.replace_extension(".subspace.rpc_client.h");
  std::cerr << "Generating " << hp << "\n";

  // There appears to be no way to get anything other than a
  // ZeorCopyOutputStream from the GeneratorContext.  We want to use
  // std::ofstream to write the file, so we'll write to a stringstream and then
  // copy the data to the file.
  std::unique_ptr<google::protobuf::io::ZeroCopyOutputStream> header_output(
      generator_context->Open(hp.string()));

  std::filesystem::create_directories(hp.parent_path());

  if (header_output == nullptr) {
    std::cerr << "Failed to open " << hp << " for writing\n";
    *error = absl::StrFormat("Failed to open %s for writing", hp.string());
    return false;
  }
  std::stringstream header_stream;
  gen.GenerateClientHeaders(header_stream);

  std::filesystem::path cp(filename);
  cp.replace_extension(".subspace.rpc_client.cc");
  std::cerr << "Generating " << cp << "\n";

  std::unique_ptr<google::protobuf::io::ZeroCopyOutputStream> source_output(
      generator_context->Open(cp.string()));
  if (source_output == nullptr) {
    *error = absl::StrFormat("Failed to open %s for writing", cp.string());
    return false;
  }
  std::stringstream source_stream;
  gen.GenerateClientSources(source_stream);

  // Write to the streams that protobuf wants
  WriteToZeroCopyStream(header_stream.str(), header_output.get());
  WriteToZeroCopyStream(source_stream.str(), source_output.get());

  return true;
}

bool CodeGenerator::GenerateServer(
    const google::protobuf::FileDescriptor *file, Generator &gen,
    google::protobuf::compiler::GeneratorContext *generator_context,
    std::string *error) const {
  std::string filename =
      GeneratedFilename(package_name_, target_name_, file->name());

  std::filesystem::path hp(filename);
  hp.replace_extension(".subspace.rpc_server.h");
  std::cerr << "Generating " << hp << "\n";

  // There appears to be no way to get anything other than a
  // ZeorCopyOutputStream from the GeneratorContext.  We want to use
  // std::ofstream to write the file, so we'll write to a stringstream and then
  // copy the data to the file.
  std::unique_ptr<google::protobuf::io::ZeroCopyOutputStream> header_output(
      generator_context->Open(hp.string()));

  std::filesystem::create_directories(hp.parent_path());

  if (header_output == nullptr) {
    std::cerr << "Failed to open " << hp << " for writing\n";
    *error = absl::StrFormat("Failed to open %s for writing", hp.string());
    return false;
  }
  std::stringstream header_stream;
  gen.GenerateServerHeaders(header_stream);

  std::filesystem::path cp(filename);
  cp.replace_extension(".subspace.rpc_server.cc");
  std::cerr << "Generating " << cp << "\n";

  std::unique_ptr<google::protobuf::io::ZeroCopyOutputStream> source_output(
      generator_context->Open(cp.string()));
  if (source_output == nullptr) {
    *error = absl::StrFormat("Failed to open %s for writing", cp.string());
    return false;
  }
  std::stringstream source_stream;
  gen.GenerateServerSources(source_stream);

  // Write to the streams that protobuf wants
  WriteToZeroCopyStream(header_stream.str(), header_output.get());
  WriteToZeroCopyStream(source_stream.str(), source_output.get());

  return true;
}

void Generator::OpenNamespace(std::ostream &os) {
  std::vector<std::string> parts = absl::StrSplit(file_->package(), '.');
  for (const auto &part : parts) {
    os << "namespace " << part << " {\n";
  }
  if (!added_namespace_.empty()) {
    os << "namespace " << added_namespace_ << " {\n";
  }
}

void Generator::CloseNamespace(std::ostream &os) {
  if (!added_namespace_.empty()) {
    os << "} // namespace " << added_namespace_ << "\n";
  }
  std::vector<std::string> parts = absl::StrSplit(file_->package(), '.');
  for (const auto &part : parts) {
    os << "} // namespace " << part << "\n";
  }
}

Generator::Generator(const google::protobuf::FileDescriptor *file,
                     const std::string &ns, const std::string &pn,
                     const std::string &tn)
    : file_(file), added_namespace_(ns), package_name_(pn), target_name_(tn) {
  for (int i = 0; i < file->service_count(); i++) {
    service_gens_.push_back(std::make_unique<ServiceGenerator>(
        file->service(i), added_namespace_, std::string{file->package()}));
  }
}

void Generator::GenerateClientHeaders(std::ostream &os) {
  os << "#pragma once\n";
  std::string main_base =
      GeneratedFilename("", "", file_->name());
  std::filesystem::path cpp_header(main_base);
  cpp_header.replace_extension(".pb.h");

  os << "#include \"rpc/client/rpc_client.h\"\n";
  os << "#include \"" << cpp_header.string() << "\"\n";
  for (int i = 0; i < file_->dependency_count(); i++) {
    std::string base = GeneratedFilename(package_name_, target_name_,
                                         file_->dependency(i)->name());
    std::filesystem::path p(base);
    p.replace_extension(".subspace.rpc_client.h");
    os << "#include \"" << p.string() << "\"\n";
  }

  OpenNamespace(os);

  for (auto &msg_gen : service_gens_) {
    msg_gen->GenerateClientHeader(os);
  }

  CloseNamespace(os);
}

void Generator::GenerateClientSources(std::ostream &os) {
  std::filesystem::path p(
      GeneratedFilename(package_name_, target_name_, file_->name()));
  p.replace_extension(".subspace.rpc_client.h");
  os << "#include \"" << p.string() << "\"\n";

  OpenNamespace(os);

  for (auto &svc_gen : service_gens_) {
    svc_gen->GenerateClientSource(os);
  }

  CloseNamespace(os);
}

void Generator::GenerateServerHeaders(std::ostream &os) {
  os << "#pragma once\n";
  std::string main_base =
      GeneratedFilename("", "", file_->name());
  std::filesystem::path cpp_header(main_base);
  cpp_header.replace_extension(".pb.h");

  os << "#include \"rpc/server/rpc_server.h\"\n";
  os << "#include \"" << cpp_header.string() << "\"\n";
  for (int i = 0; i < file_->dependency_count(); i++) {
    std::string base = GeneratedFilename(package_name_, target_name_,
                                         file_->dependency(i)->name());
    std::filesystem::path p(base);
    p.replace_extension(".subspace.rpc_client.h");
    os << "#include \"" << p.string() << "\"\n";
  }

  OpenNamespace(os);

  for (auto &msg_gen : service_gens_) {
    msg_gen->GenerateServerHeader(os);
  }

  CloseNamespace(os);
}

void Generator::GenerateServerSources(std::ostream &os) {
  std::filesystem::path p(
      GeneratedFilename(package_name_, target_name_, file_->name()));
  p.replace_extension(".subspace.rpc_server.h");
  os << "#include \"" << p.string() << "\"\n";

  OpenNamespace(os);

  for (auto &svc_gen : service_gens_) {
    svc_gen->GenerateServerSource(os);
  }

  CloseNamespace(os);
}

} // namespace subspace
