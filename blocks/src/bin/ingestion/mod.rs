//! Ingestion orchestration. Dataset ownership outlives the session and runtime;
//! successful release remains explicit after all work has completed.
use super::*;

mod runtime;
mod setup;

/// Collect output and the fully resolved cursor before taking any ownership.
fn ingestion_mutation_scopes(
    config: &Config,
    cursor: Option<&CursorLocation>,
) -> Result<Vec<MutationScope>> {
    let output = config.output.to_string_lossy().into_owned();
    let mut scopes = vec![MutationScope::directory(output.clone())];
    match cursor {
        Some(CursorLocation::Local(path)) => {
            scopes.push(MutationScope::file(path.to_string_lossy()));
        }
        Some(CursorLocation::S3 { key, .. }) => {
            let explicit = config.cursor_path.as_deref().unwrap_or_default();
            let bucket_source = if explicit.starts_with("s3://") {
                explicit
            } else {
                &output
            };
            let (bucket, _) = firehose_parquet::writer::parse_s3_url(bucket_source)?;
            scopes.push(MutationScope::file(format!("s3://{bucket}/{key}")));
        }
        None => {}
    }
    Ok(scopes)
}

fn record_committed_flush_sizing(
    sizing: &mut FlushSizing,
    estimate: MapperBufferEstimate,
    committed: &firehose_parquet::ingest::CommittedFlush,
    trigger: &str,
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
        largest_file_bytes,
        largest_mapper_estimated_bytes = estimate.largest_table_bytes,
        total_mapper_estimated_bytes = estimate.total_bytes,
        compressed_to_mapper_ratio = sizing.ratio(),
        "committed flush size observation"
    );
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

    let Some(endpoint) = setup::ResolvedEndpoint::resolve(args, &shutdown).await? else {
        return Ok(());
    };
    // Keep ownership outside all session/runtime borrows and acquire before resume.
    let mut ownership = endpoint.acquire_ownership().await?;
    let setup = setup::IngestionSetup::resolve(args, endpoint, ownership.as_ref()).await?;

    let (mut metrics_registry, pipeline_metrics) = metrics::init();
    let _pipeline_activity = pipeline_metrics.begin_pipeline();
    setup.configure_metrics(args, &mut metrics_registry, &pipeline_metrics);
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
    let mut session = if let Some(owner) = &ownership {
        let semantics = state.semantics(&setup)?;
        Some(
            IngestionSession::open(
                &setup.config,
                semantics,
                owner,
                Some(&pipeline_metrics),
                Some(&cursor_shutdown),
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
        drop(session);
        if let Some(owner) = ownership.take() {
            owner.release().await?;
        }
        return Ok(());
    }

    let mut runtime = runtime::IngestionRuntime::new(
        args,
        &setup,
        state,
        session.as_mut(),
        &pipeline_metrics,
        &shutdown,
    )?;
    let resume_cursor = runtime.resume_cursor();
    let stream_result = client
        .stream_blocks(
            resume_cursor,
            &shutdown,
            |payload, type_url, cursor, identity, step| {
                runtime.observe(payload, type_url, cursor, identity, step)
            },
        )
        .await;
    runtime.finish(stream_result).await?;
    drop(runtime);

    drop(session);

    // All synchronous writes have resolved by this point. Any earlier error or
    // cancellation of this future drops the guard and retains remote ownership.
    if let Some(ownership) = ownership {
        ownership.release().await?;
    }
    Ok(())
}
