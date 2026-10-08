use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("proto");

    tonic_prost_build::configure()
        .build_server(false)
        // Owned Bytes input lets generated chain bytes fields share its allocation.
        // Externally supplied prost_types messages keep their own field types.
        .bytes(".")
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
                proto_root.join("tron_contract.proto"),
                // Beacon
                proto_root.join("beacon.proto"),
                // HyperCore (pinax.hypercore.v1, vendored verbatim with its
                // package path so the upstream imports resolve unchanged)
                proto_root.join("pinax/hypercore/v1/block.proto"),
                proto_root.join("pinax/hypercore/v1/event.proto"),
                proto_root.join("pinax/hypercore/v1/fill.proto"),
                // SEC (EDGAR): package pinax.sec.v1, imports by path under proto/
                proto_root.join("pinax/sec/v1/block.proto"),
                proto_root.join("pinax/sec/v1/ownership.proto"),
                proto_root.join("pinax/sec/v1/form13f.proto"),
                proto_root.join("pinax/sec/v1/beneficial.proto"),
                proto_root.join("pinax/sec/v1/form144.proto"),
                proto_root.join("pinax/sec/v1/nport.proto"),
                proto_root.join("pinax/sec/v1/formd.proto"),
                proto_root.join("pinax/sec/v1/npx.proto"),
                proto_root.join("pinax/sec/v1/ncen.proto"),
                proto_root.join("pinax/sec/v1/formc.proto"),
            ],
            &[proto_root],
        )?;

    Ok(())
}
