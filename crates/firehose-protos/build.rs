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
                proto_root.join("sf/firehose/v2/firehose.proto"),
                // Solana
                proto_root.join("sf/solana/type/v1/type.proto"),
                // Ethereum (EVM)
                proto_root.join("sf/ethereum/type/v2/type.proto"),
                // Bitcoin
                proto_root.join("sf/bitcoin/type/v1/type.proto"),
                // NEAR
                proto_root.join("sf/near/type/v1/type.proto"),
                // Antelope
                proto_root.join("sf/antelope/type/v1/type.proto"),
                // Cosmos
                proto_root.join("sf/cosmos/type/v2/type.proto"),
                proto_root.join("cosmos/tx/v1beta1/tx.proto"),
                // Tron
                proto_root.join("sf/tron/type/v1/block.proto"),
                // Beacon
                proto_root.join("sf/beacon/type/v1/type.proto"),
            ],
            &[&proto_root],
        )?;

    Ok(())
}
