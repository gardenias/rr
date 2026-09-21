fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use the vendored protoc so builds do not depend on a system-installed
    // protobuf-compiler (CI runner images no longer ship one; local machines
    // may not have it either).
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "proto/raft.proto",
                "proto/client.proto",
                "proto/admin.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
