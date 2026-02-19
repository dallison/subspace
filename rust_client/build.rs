fn find_well_known_types(start: &std::path::Path, depth: u32) -> Option<String> {
    if depth > 4 {
        return None;
    }
    if let Ok(entries) = std::fs::read_dir(start) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.join("google/protobuf/any.proto").exists() {
                    return Some(path.to_string_lossy().into_owned());
                }
                if let Some(found) = find_well_known_types(&path, depth + 1) {
                    return Some(found);
                }
            }
        }
    }
    None
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_file = std::env::var("SUBSPACE_PROTO_FILE")
        .unwrap_or_else(|_| "../proto/subspace.proto".into());

    let proto_include = std::path::Path::new(&proto_file)
        .parent()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|| "../proto".into());

    let mut includes = vec![proto_include];

    // Under Bazel the well-known types live under the exec root at
    // external/protobuf+/src/.  Derive the exec root from the absolute
    // proto file path (strip the trailing "proto/subspace.proto").
    let proto_path = std::path::Path::new(&proto_file);
    if proto_path.is_absolute() {
        if let Some(exec_root) = proto_path.parent().and_then(|p| p.parent()) {
            if let Some(wkt_dir) = find_well_known_types(exec_root, 0) {
                includes.push(wkt_dir);
            }
        }
    }

    let include_refs: Vec<&str> = includes.iter().map(|s| s.as_str()).collect();

    let mut config = prost_build::Config::new();
    config.extern_path(".google.protobuf.Any", "::prost_types::Any");
    config.compile_protos(&[&proto_file], &include_refs)?;
    Ok(())
}
