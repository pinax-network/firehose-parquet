//! `fireparq backfill`: fill a new dataset's history with parallel writers
//! (#725, [`crate::ingest::backfill`]). `open` runs through the `build`
//! setup, since it binds the dataset's stream; `seal` and `status` need only
//! the dataset root and its S3 settings.

use anyhow::Result;
use clap::{Args, Subcommand};

use crate::cli::{BuildArgs, ValidateOptions};
use crate::recovery::RecoveryStorageArgs;

#[derive(Subcommand, Debug)]
pub enum BackfillCommands {
    /// Open a parallel backfill of a new, empty S3 dataset root.
    ///
    /// Takes the options of `build`, with --start-block and --stop-block as
    /// the range to fill, and writes the plan and the empty tables: the dataset's settings
    /// and its lanes of --lane-blocks blocks. Then run one `build` per lane,
    /// with exactly the lane's --start-block and --stop-block and the same
    /// settings, as many at a time as wanted. Until `backfill seal`, every
    /// other `build` into the root is refused.
    Open(Box<BackfillOpenArgs>),
    /// Seal a complete backfill into an ordinary dataset.
    ///
    /// Refuses unless every lane is complete and released, and `blocks` is
    /// continuous across the lanes. Then `build` without --stop-block
    /// continues live from the backfill's stop block.
    Seal(BackfillSealArgs),
    /// Report a backfill's lanes without changing anything.
    Status(RecoveryStorageArgs),
}

#[derive(Args, Debug)]
pub struct BackfillOpenArgs {
    #[command(flatten)]
    pub build: BuildArgs,
    /// Blocks per lane. The last lane may be shorter.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..), help_heading = "Block Range")]
    pub lane_blocks: u64,
}

#[derive(Args, Debug)]
pub struct BackfillSealArgs {
    #[command(flatten)]
    pub storage: RecoveryStorageArgs,
    /// Skip the continuity check of `blocks` (gaps, duplicates and parent
    /// links across lanes), for example after `fireparq validate` passed.
    #[arg(long, default_value = "false")]
    pub no_validate: bool,
    /// Allow gaps in block numbers in that check (chains with skipped heights).
    #[arg(long, default_value = "false")]
    pub allow_gaps: bool,
}

/// `seal` and `status`. `open` is run by the binary, with the mapper.
pub async fn run_backfill(command: &BackfillCommands) -> Result<()> {
    match command {
        BackfillCommands::Open(_) => {
            anyhow::bail!("`backfill open` needs the block mapper; run it through fireparq")
        }
        BackfillCommands::Status(storage) => {
            let status = crate::ingest::backfill_status(&storage.path, &storage.aws()).await?;
            println!("{}", serde_json::to_string_pretty(&status)?);
        }
        BackfillCommands::Seal(args) => {
            let path = args.storage.path.trim_end_matches('/');
            let aws = args.storage.aws();
            crate::cli::validate_s3_output_credentials(
                path,
                args.storage.aws.aws_access_key_id.as_deref(),
                args.storage.aws.aws_secret_access_key.as_deref(),
            )?;
            // Cheap refusals first, then the read of `blocks`, then the seal
            // itself, which checks every lane again under the owner. A lane
            // that is complete stays complete, so nothing can change between.
            crate::ingest::seal_preflight(path, &aws).await?;
            if !args.no_validate {
                let blocks = format!("{path}/blocks");
                let result = crate::cli::validate_table(
                    &blocks,
                    Some(&aws),
                    &ValidateOptions {
                        cross_partition: true,
                        allow_gaps: args.allow_gaps,
                    },
                )?;
                if !result.is_valid() {
                    result.print(&blocks);
                    anyhow::bail!(
                        "`blocks` is not continuous across the backfill's lanes; the dataset \
                         was not sealed"
                    );
                }
            }
            let sealed = crate::ingest::seal_backfill(path, &aws).await?;
            println!("{}", serde_json::to_string_pretty(&sealed)?);
        }
    }
    Ok(())
}
