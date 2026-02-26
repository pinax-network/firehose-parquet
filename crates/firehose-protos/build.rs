use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("proto");

    tonic_build::configure()
        .build_server(false)
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &[
                // Firehose core
                proto_root.join("firehose.proto"),
                // Solana
                proto_root.join("solana.proto"),
                // Ethereum (EVM)
                proto_root.join("ethereum.proto"),
                // Bitcoin
                proto_root.join("bitcoin.proto"),
                // NEAR
                proto_root.join("near.proto"),
                // Antelope
                proto_root.join("antelope.proto"),
                // Cosmos
                proto_root.join("cosmos.proto"),
                proto_root.join("cosmos_tx.proto"),
                // Tron
                proto_root.join("tron.proto"),
                // Beacon
                proto_root.join("beacon.proto"),
            ],
            &[&proto_root],
        )?;

    Ok(())
}
