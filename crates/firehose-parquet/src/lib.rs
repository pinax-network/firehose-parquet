pub mod config;
pub mod grpc;
pub mod mapper;
pub mod schema;
pub mod writer;

/// Re-exported generated protobuf types.
pub mod proto {
    pub mod sf {
        pub mod firehose {
            pub mod v2 {
                tonic::include_proto!("sf.firehose.v2");
            }
        }
        pub mod solana {
            #[path = ""]
            pub mod r#type {
                pub mod v1 {
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/sf.solana.r#type.v1.rs"
                    ));
                }
            }
        }
    }
}

/// Convenience alias for the Solana block type.
pub use proto::sf::solana::r#type::v1 as solana;
/// Convenience alias for the Firehose v2 types.
pub use proto::sf::firehose::v2 as firehose;
