//! Ingestion orchestration. Dataset ownership outlives the session and runtime;
//! every exit after acquisition ends it through `DatasetOwnership::finish`.
use super::*;

mod runtime;
mod setup;

fn record_committed_flush_sizing(
    sizing: &mut FlushSizing,
    estimate: MapperBufferEstimate,
    committed: &firehose_parquet::ingest::CommittedFlush,
    trigger: &str,
    pace: StreamPace,
) {
    let largest_file_bytes = committed
        .tables
        .iter()
        .map(|table| table.bytes)
        .max()
        .unwrap_or(0);
    sizing.observe_committed(estimate.largest_table_bytes, largest_file_bytes);
    info!(
        trigger,
        pace = pace.as_str(),
        largest_file_bytes,
        largest_mapper_estimated_bytes = estimate.largest_table_bytes,
        total_mapper_estimated_bytes = estimate.total_bytes,
        compressed_to_mapper_ratio = sizing.ratio(),
        commit_ms = committed.elapsed.as_millis() as u64,
        files = committed.files,
        delta_commits = committed.delta.len(),
        delta_ms = committed.delta_elapsed.as_millis() as u64,
        delta_retries = committed
            .delta
            .iter()
            .map(|commit| commit.retries)
            .sum::<u64>(),
        peak_encoders = committed.work.peak_encoders,
        peak_publications = committed.work.peak_publications,
        peak_inflight_bytes = committed.work.peak_inflight_bytes,
        reencoded_parts = committed.work.reencoded_parts,
        "committed flush size observation"
    );
}

/// Blocks go to [`runtime::IngestionRuntime::observe`]; a quiet stream
/// flushes ([`runtime::IngestionRuntime::flush_when_quiet`]).
struct RuntimeHandler<'a, 'run, 'owner>(&'a mut runtime::IngestionRuntime<'run, 'owner>);

impl firehose_parquet::grpc::StreamHandler for RuntimeHandler<'_, '_, '_> {
    fn block(
        &mut self,
        payload: Vec<u8>,
        type_url: String,
        cursor: String,
        identity: BlockIdentity,
        step: i32,
    ) -> Result<()> {
        self.0.observe(payload, type_url, cursor, identity, step)
    }

    fn quiet(&mut self) -> Result<()> {
        self.0.flush_when_quiet()
    }
}

pub(super) async fn run_ingestion(args: &BuildArgs, global: &GlobalArgs) -> Result<()> {
    init_tracing(
        &args.common.log_level,
        args.common.verbose || global.verbose,
    );

    info!(version = env!("CARGO_PKG_VERSION"), "fireparq starting");

    // Install graceful shutdown handler for SIGINT (Ctrl-C) and SIGTERM.
    // The first signal cancels endpoint waits and lets current block work
    // finish. Unflushed buffers are discarded and previously completed flushes
    // are preserved. A second signal forces exit and may interrupt writes.
    let shutdown = CancellationToken::new();
    // Cursor retries retain their durable-write contract: finish in-flight I/O,
    // then interrupt retry backoff on the same shutdown signal.
    let cursor_shutdown = Arc::new(AtomicBool::new(false));
    spawn_ingestion_shutdown_handler(shutdown.clone(), Arc::clone(&cursor_shutdown));

    let Some(endpoint) = setup::ResolvedEndpoint::resolve(args, &shutdown, true).await? else {
        return Ok(());
    };
    // Keep ownership outside all session/runtime borrows and acquire before resume.
    let ownership = endpoint.acquire_ownership().await?;
    let result = run_owned(
        args,
        endpoint,
        ownership.as_ref(),
        &shutdown,
        &cursor_shutdown,
    )
    .await;
    // Every exit after acquisition ends here, and every borrow of the guard
    // (setup, session, runtime and their requests) ended with `run_owned`.
    // Success and graceful shutdown release. A failure releases each S3 owner
    // whose requests all had a definite outcome and keeps (and names) any other.
    // A panic unwinds past this point and keeps S3 ownership (logged on drop);
    // a second signal exits the process without running destructors.
    match ownership {
        Some(ownership) => ownership.finish(result).await,
        None => result,
    }
}

/// `fireparq backfill open`: the `build` setup up to the mapper's table
/// inventory, then the backfill plan and the empty tables instead of a
/// session. Nothing is streamed.
pub(super) async fn run_backfill_open(
    args: &firehose_parquet::backfill::BackfillOpenArgs,
    global: &GlobalArgs,
) -> Result<()> {
    let build = &args.build;
    init_tracing(
        &build.common.log_level,
        build.common.verbose || global.verbose,
    );
    anyhow::ensure!(
        !build.common.dry_run,
        "a backfill cannot be opened in a dry run"
    );
    let shutdown = CancellationToken::new();
    let Some(endpoint) = setup::ResolvedEndpoint::resolve(build, &shutdown, false).await? else {
        return Ok(());
    };
    let ownership = endpoint
        .acquire_ownership()
        .await?
        .context("a backfill needs ownership of its dataset root")?;
    let opened = async {
        let setup = setup::IngestionSetup::resolve(build, endpoint, Some(&ownership)).await?;
        let mut state = runtime::MapperState::new(build, &setup)?;
        let semantics = state.semantics(&setup)?;
        firehose_parquet::ingest::open_backfill(
            &setup.config,
            semantics,
            &ownership,
            args.lane_blocks,
        )
        .await
    }
    .await;
    let opened = ownership.finish(opened).await?;
    info!(
        created = opened.created,
        start_block = opened.start,
        stop_block = opened.stop,
        lane_blocks = opened.lane_blocks,
        lanes = opened.lanes,
        "parallel backfill is open; run one build per lane, then `fireparq backfill seal`"
    );
    Ok(())
}

async fn run_owned(
    args: &BuildArgs,
    endpoint: setup::ResolvedEndpoint,
    ownership: Option<&DatasetOwnership>,
    shutdown: &CancellationToken,
    cursor_shutdown: &AtomicBool,
) -> Result<()> {
    let setup = setup::IngestionSetup::resolve(args, endpoint, ownership).await?;

    let (mut metrics_registry, pipeline_metrics) = metrics::init();
    let _pipeline_activity = pipeline_metrics.begin_pipeline();
    setup.configure_metrics(&mut metrics_registry, &pipeline_metrics);
    // Spawn the metrics HTTP server if a port was provided.
    let metrics_registry = Arc::new(metrics_registry);
    if let Some(port) = setup.config.metrics_port {
        metrics::serve(
            Arc::clone(&metrics_registry),
            pipeline_metrics.clone(),
            port,
        );
    }

    // Pass metrics to the gRPC client for reconnect tracking.
    // Metadata/default resolution may have supplied an original start that was
    // absent when the startup Info client was built. Blocks uses the resolved request.
    let mut client = FirehoseClient::new(setup.config.clone())?;
    client.set_metrics(pipeline_metrics.clone());

    let mut state = runtime::MapperState::new(args, &setup)?;
    let mut session = if let Some(owner) = ownership {
        let semantics = state.semantics(&setup)?;
        Some(
            IngestionSession::open(
                &setup.config,
                semantics,
                owner,
                Some(&pipeline_metrics),
                Some(cursor_shutdown),
            )
            .await?,
        )
    } else {
        None
    };
    if let Some(session) = &session {
        state.restore_session_anchor(session);
    }
    let already_complete = match (&session, setup.config.stop_block) {
        (Some(session), Some(stop)) => session.request_already_complete(stop)?,
        _ => false,
    };
    if already_complete {
        info!(stop_block=?setup.config.stop_block, "requested range is already complete; recovered authority and cursor mirror without opening Blocks");
        return Ok(());
    }

    let mut runtime = runtime::IngestionRuntime::new(
        args,
        &setup,
        state,
        session.as_mut(),
        &pipeline_metrics,
        shutdown,
    )?;
    let resume_cursor = runtime.resume_cursor();
    let quiet_after = setup.config.flush_idle_secs.map(Duration::from_secs);
    let stream_result = client
        .stream_blocks_with_quiet(
            resume_cursor,
            shutdown,
            quiet_after,
            &mut RuntimeHandler(&mut runtime),
        )
        .await;
    // All synchronous writes have resolved when `finish` returns; the caller
    // decides what happens to ownership.
    let result = runtime.finish(stream_result).await;
    drop(runtime);
    drop(session);
    result
}
