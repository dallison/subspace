// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// CMake generates the bindings with phaser_add_library and links their C++
// code, with everything it uses, into one shared library.  Bazel builds this
// crate from //proto:subspace_phaser_c_rs instead.

fn required(var: &str) -> String {
    println!("cargo:rerun-if-env-changed={var}");
    std::env::var(var)
        .unwrap_or_else(|_| panic!("{var} is not set: build the Rust client with CMake or Bazel"))
}

fn main() {
    let source = required("SUBSPACE_PHASER_C_RS");
    let lib_dir = required("SUBSPACE_PHASER_C_LIB_DIR");
    println!("cargo:rustc-env=SUBSPACE_PHASER_C_RS={source}");
    println!("cargo:rustc-link-search=native={lib_dir}");
    println!("cargo:rustc-link-lib=dylib=subspace_phaser_c_rust");
    // Dependents read this as DEP_SUBSPACE_PHASER_C_RUST_LIB_DIR.
    println!("cargo:lib_dir={lib_dir}");
}
