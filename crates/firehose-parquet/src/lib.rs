pub mod config;
pub mod encode;
pub mod grpc;
pub mod traits;
pub mod writer;

/// Re-exported generated protobuf types from `firehose-protos`.
pub use firehose_protos as proto;

/// Convenience alias for the Firehose v2 types.
pub use proto::firehose;
