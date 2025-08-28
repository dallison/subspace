// Copyright 2025 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "google/protobuf/compiler/code_generator.h"
#include "google/protobuf/compiler/plugin.h"
#include "rpc/idl_compiler/gen.h"


int main(int argc, char *argv[]) {
  std::cerr << "subspace rpc compiler running\n";
  subspace::CodeGenerator generator;
  return google::protobuf::compiler::PluginMain(argc, argv, &generator);
}
