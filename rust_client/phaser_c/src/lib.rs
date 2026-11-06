// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// The attributes in phaser's rust_crate_attributes.rs, which the generated
// bindings need on their crate root.
#![allow(
    non_camel_case_types,
    non_snake_case,
    non_upper_case_globals,
    dead_code
)]

include!(env!("SUBSPACE_PHASER_C_RS"));
