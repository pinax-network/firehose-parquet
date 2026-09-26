//! Authoritative ingestion transactions and accepted stream progress.

pub(crate) mod binding;
pub(crate) mod controller;
pub(crate) mod eligibility;
pub(crate) mod frontier;
pub(crate) mod maintenance;
pub(crate) mod mirror;
pub(crate) mod observe;
pub(crate) mod parts;
pub(crate) mod session;
pub(crate) mod state;
pub(crate) mod store;

pub use controller::CommittedFlush;
pub use session::{
    declare_inventory, load_authoritative_resume, IngestionSession, MapperSemantics,
};
pub use state::{BlockFamily, Digest};

/// Recover any protected dataset before publishing its standalone index.
pub async fn prepare_partitions_index_write(
    directory: &str,
    index: &str,
    aws: &crate::cli::AwsConfig,
) -> anyhow::Result<crate::dataset_lock::DatasetOwnership> {
    let prepared = maintenance::acquire(
        "partitions-build",
        vec![
            maintenance::MaintenanceTarget::directory(directory),
            maintenance::MaintenanceTarget::file(index),
        ],
        maintenance::MaintenancePolicy::Artifacts,
        Some(aws),
    )
    .await?;
    Ok(prepared.ownership)
}
