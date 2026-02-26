pub mod config;
pub mod encode;
pub mod grpc;
pub mod traits;
pub mod writer;

/// Re-exported generated protobuf types.
pub mod proto {
    pub mod sf {
        pub mod firehose {
            pub mod v2 {
                tonic::include_proto!("sf.firehose.v2");
            }
        }
    }
}

/// Convenience alias for the Firehose v2 types.
pub use proto::sf::firehose::v2 as firehose;
