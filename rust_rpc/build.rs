// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

use std::path::{Path, PathBuf};

fn find_subspace_rpc_plugin(manifest_dir: &Path) -> Option<PathBuf> {
    if let Ok(path) = std::env::var("SUBSPACE_RPC_PLUGIN") {
        let p = PathBuf::from(&path);
        if p.exists() {
            return Some(p);
        }
    }

    let workspace_root = manifest_dir.parent()?;
    let bazel_plugin = workspace_root.join("bazel-bin/rpc/idl_compiler/subspace_rpc");
    if bazel_plugin.exists() {
        return Some(bazel_plugin);
    }

    None
}

fn generate_service_stubs(
    manifest_dir: &Path,
    out_dir: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let plugin = find_subspace_rpc_plugin(manifest_dir).ok_or_else(|| {
        "subspace_rpc protoc plugin not found. \
         Build it with `bazel build //rpc/idl_compiler:subspace_rpc`, \
         or set SUBSPACE_RPC_PLUGIN to the binary path."
    })?;

    let protoc = std::env::var("PROTOC").unwrap_or_else(|_| "protoc".to_string());

    let workspace_root = manifest_dir.parent().expect("manifest_dir has parent");
    let proto_dir = workspace_root.join("rpc/proto");
    let well_known_dir = workspace_root.join("proto");

    let status = std::process::Command::new(&protoc)
        .arg(format!(
            "--plugin=protoc-gen-subspace_rpc={}",
            plugin.display()
        ))
        .arg(format!("--subspace_rpc_out=rpc_style=rust:{}", out_dir))
        .arg(format!("-I{}", proto_dir.display()))
        .arg(format!("-I{}", well_known_dir.display()))
        .arg("rpc_test.proto")
        .status()?;

    if !status.success() {
        return Err(format!("protoc subspace_rpc plugin failed with status: {}", status).into());
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_file = "../rpc/proto/rpc_test.proto";
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let out_dir = std::env::var("OUT_DIR")?;

    println!("cargo:rerun-if-changed={}", proto_file);
    println!("cargo:rerun-if-env-changed=SUBSPACE_RPC_PLUGIN");

    prost_build::Config::new()
        .extern_path(".google.protobuf.Any", "::prost_types::Any")
        .compile_protos(&[proto_file], &["../proto", ".."])?;

    generate_service_stubs(&manifest_dir, &out_dir)?;

    Ok(())
}
