use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Protos are vendored into this repo at <workspace-root>/proto.
    let proto_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("proto");

    let proto_paths: Vec<_> = [
        "atoms/splitter/model.proto",
        "atoms/splitter/cluster.proto",
        "atoms/splitter/consumer.proto",
        "atoms/splitter/consumerapi.proto",
        "atoms/splitter/lib/service/session/session.proto",
        "atoms/splitter/lib/service/location/location.proto",
    ]
    .iter()
    .map(|p| proto_root.join(p))
    .collect();

    // Rebuild if any proto changes.
    for p in &proto_paths {
        println!("cargo:rerun-if-changed={}", p.display());
    }
    println!("cargo:rerun-if-changed=build.rs");

    tonic_prost_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(&proto_paths, &[proto_root])?;

    Ok(())
}
