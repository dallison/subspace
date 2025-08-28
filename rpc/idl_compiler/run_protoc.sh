#!/bin/bash

../protobuf+/protoc \
    --plugin=protoc-gen-subspace_rpc=rpc/idl_compiler/subspace_rpc \
    rpc/proto/rpc_test.proto \
    --subspace_rpc_out=/tmp
