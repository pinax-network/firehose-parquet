//! Ingestion orchestration. Dataset ownership outlives the session and runtime;
//! successful release remains explicit after all work has completed.
use super::*;

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

fn commit_ingestion_flush(
    session: &mut IngestionSession<'_>,
    batches: HashMap<String, RecordBatch>,
    metadata: BlockMetadata,
    compression: Compression,
    file_metadata: ParquetFileMetadata,
    metrics: &metrics::PipelineMetrics,
    sizing: &mut FlushSizing,
    estimate: MapperBufferEstimate,
    trigger: &str,
) -> Result<WriterFlushOutcome> {
    let committed = session.flush_blocking(batches, metadata, compression, file_metadata)?;
    if let Some(flush) = &committed {
        record_committed_flush_sizing(sizing, estimate, flush, trigger);
        metrics
            .flushes_total
            .get_or_create(&metrics::FlushLabels {
                trigger: trigger.into(),
            })
            .inc();
    }
    Ok(WriterFlushOutcome {
        materialized: committed.is_some_and(|flush| flush.files > 0),
        buffered: WriterBufferStats::default(),
    })
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
    let setup::IngestionSetup {
        config,
        block_type,
        endpoint_info,
        existing_cursor_state,
        mut extended,
        with_votes,
        mut include_failed_transactions,
        initial_block_type,
        failed_transactions_block_type,
        initial_bytes_encoding,
        initial_bytes_encoding_label,
        solana_chain,
        antelope_chain,
        tron_style_evm_profile,
    } = setup;

    // Spawn the metrics HTTP server if a port was provided.
    let metrics_registry = Arc::new(metrics_registry);
    if let Some(port) = config.metrics_port {
        metrics::serve(
            Arc::clone(&metrics_registry),
            pipeline_metrics.clone(),
            port,
        );
    }

    // Pass metrics to the gRPC client for reconnect tracking.
    // Metadata/default resolution may have supplied an original start that was
    // absent when the startup Info client was built. Blocks uses the resolved request.
    let mut client = FirehoseClient::new(config.clone())?;
    client.set_metrics(pipeline_metrics.clone());

    let final_blocks_only = config.final_blocks_only;
    let include_fork_step = !final_blocks_only;
    let flush_rows = config.flush_rows.map(|r| r as usize);
    let flush_blocks = config.flush_blocks;
    let mut flush_sizing = FlushSizing::new(config.flush_bytes, config.flush_memory_bytes)?;
    let flush_interval_secs = config.flush_interval_secs;
    let dry_run = config.dry_run;
    let mut is_solana = block_type == "solana";
    let partition_config = config.partition.clone();
    let mut use_synthetic_partition_routing =
        use_last_known_timestamp_partition_routing(&block_type, &partition_config);
    let mut genesis_timestamp_bootstrap = GenesisTimestampBootstrap::new(config.start_block);
    let mut timestamp_routing = TimestampRouting::new(use_synthetic_partition_routing);
    if block_type != "auto" {
        restore_sparse_routing_cursor_anchor(
            &mut timestamp_routing,
            existing_cursor_state.as_ref(),
            args.cursor_override,
            &block_type,
            &partition_config,
        );
    }

    // If block type is known upfront, resolve encode_bytes and create mapper immediately.
    // If "auto", defer until first block arrives.
    let mut current_file_metadata = ParquetFileMetadata::new();
    let mut mapper: Option<Box<dyn BlockMapper>> = if block_type != "auto" {
        let encode_bytes = initial_bytes_encoding.clone();
        let meta = build_file_metadata(
            &block_type,
            &encode_bytes,
            &config.endpoint,
            config.compression,
            &endpoint_info,
        );
        let mut meta = meta;
        maybe_add_solana_with_votes_metadata(&mut meta, Some(&block_type), with_votes);
        maybe_add_synthetic_timestamp_metadata(
            &mut meta,
            &block_type,
            use_synthetic_partition_routing,
        );
        log_file_metadata(&meta);
        current_file_metadata = meta;
        Some(create_mapper(
            &block_type,
            extended,
            with_votes,
            include_fork_step,
            encode_bytes,
            use_synthetic_partition_routing,
            include_failed_transactions,
        )?)
    } else {
        None
    };

    let mut session = if let Some(owner) = &ownership {
        let mapper = mapper
            .as_mut()
            .context("protected ingestion requires a mapper before recovery")?;
        let empty_batches = mapper.flush()?;
        let tables = declare_inventory(&empty_batches, &mapper.table_names())?;
        let semantics = MapperSemantics {
            chain: endpoint_info
                .as_ref()
                .context("endpoint identity is required")?
                .chain_name
                .clone(),
            family: protected_block_family(&block_type)?,
            bytes_encoding: initial_bytes_encoding_label.clone(),
            extended,
            with_votes,
            include_failed_transactions,
            tables,
        };
        Some(
            IngestionSession::open(
                &config,
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
        if let Some((source_block, seconds)) = session.routing_anchor_source() {
            timestamp_routing.restore_anchor(source_block, seconds);
        }
    }
    let already_complete = match (&session, config.stop_block) {
        (Some(session), Some(stop)) => session.request_already_complete(stop)?,
        _ => false,
    };
    if already_complete {
        info!(stop_block=?config.stop_block, "requested range is already complete; recovered authority and cursor mirror without opening Blocks");
        drop(session);
        if let Some(owner) = ownership.take() {
            owner.release().await?;
        }
        return Ok(());
    }

    let mut blocks_processed: u64 = 0;
    let mut transactions_processed: u64 = 0;
    let mut min_block: Option<u64> = None;
    let mut max_block: Option<u64> = None;
    let mut min_timestamp: Option<i64> = None;
    let mut max_timestamp: Option<i64> = None;
    // Global accumulators (not reset on flush) for final summary.
    let mut global_min_block: Option<u64> = None;
    let mut global_max_block: Option<u64> = None;
    let mut blocks_since_flush: u64 = 0;
    let mut last_flush_time = Instant::now();
    let mut bytes_read: u64 = 0;
    let mut buffered_bootstrap_blocks: Vec<BufferedBootstrapBlock> = Vec::new();
    let progress_start = Instant::now();
    let mut current_partition_key: Option<String> = None;
    let mut start_block_filter = StartBlockFilter::new(config.start_block);
    // Chain resolved for this run (set once the mapper exists in auto mode).
    let mut resolved_block_type: Option<String> =
        (block_type != "auto").then(|| block_type.clone());

    // Build file-level metadata for the cursor (same `firehose-parquet.*`
    // namespace as table files). Includes version, endpoint, chain info, and
    // pipeline parameters.
    let initial_cursor_encoding = Some(initial_bytes_encoding.clone());
    let cursor_file_metadata = build_cursor_file_metadata(
        if block_type != "auto" {
            Some(block_type.as_str())
        } else {
            initial_block_type.as_deref()
        },
        initial_cursor_encoding.as_ref(),
        &config.endpoint,
        config.compression,
        &config.partition,
        &endpoint_info,
        extended,
        config.final_blocks_only,
        include_failed_transactions,
    );
    let mut cursor_file_metadata = cursor_file_metadata;
    if solana_chain {
        maybe_add_solana_with_votes_metadata(&mut cursor_file_metadata, Some("solana"), with_votes);
    }

    // Build a template CursorState with pipeline parameters that stay constant.
    let mut cursor_state_template = CursorState {
        start_block: config.start_block,
        stop_block: config.stop_block,
        extended,
        final_blocks_only: config.final_blocks_only,
        include_failed_transactions,
        file_metadata: cursor_file_metadata,
        ..CursorState::default()
    };

    // Validate cursor parameters against current CLI arguments.
    if let Some(loaded) = existing_cursor_state.as_ref().filter(|_| dry_run) {
        let mut mismatches = loaded.validate_params(&cursor_state_template);
        mismatches.retain(|mismatch| !mismatch.starts_with("stop_block:"));
        if solana_chain {
            apply_solana_cursor_feature_validation(&mut mismatches, loaded, with_votes);
        } else if antelope_chain {
            apply_antelope_cursor_feature_validation(&mut mismatches);
        }
        if !mismatches.is_empty() {
            if args.cursor_override {
                warn!(
                    "cursor parameter mismatch detected (overridden via --cursor-override):\n  {}",
                    mismatches.join("\n  ")
                );
            } else {
                return Err(anyhow!(
                    "cursor parameter mismatch detected:\n  {}\n\nUse --cursor-override to force resume with current parameters.",
                    mismatches.join("\n  ")
                ));
            }
        }
    }

    let resume_cursor = match &session {
        Some(session) => session.resume_cursor().map(str::to_owned),
        None => stream_resume_cursor(existing_cursor_state.as_ref(), args.cursor_override),
    };
    let stream_result = client
        .stream_blocks(resume_cursor, &shutdown, |block_bytes, type_url, cursor_str, identity: BlockIdentity, step: i32| {
            let received_ordinal = if let Some(session) = session.as_mut() {
                let family = protected_block_family(&detect_block_type(&type_url)?)?;
                session.receive(cursor_str.clone(), &identity, step, family)?
            } else { 0 };
            let fork_step_str = fork_step_name(step);
            if final_blocks_only && step == 2 {
                if let Some(session) = session.as_mut() { session.accept_filtered(received_ordinal)?; }
                return Ok(());
            }
            if !start_block_filter.admit(identity.block_num) {
                pipeline_metrics.blocks_skipped_below_start_total.inc();
                if let Some(session) = session.as_mut() { session.accept_filtered(received_ordinal)?; }
                return Ok(());
            }

            // Transfer the owned gRPC payload without copying. Buffered routing
            // and chain decoding share slices of this allocation.
            let block_bytes = prost::bytes::Bytes::from(block_bytes);

            // Lazy mapper creation for "auto" mode.
            if mapper.is_none() {
                let detected = detect_block_type(&type_url)?;
                info!(detected_type = %detected, type_url = %type_url, "auto-detected block type");
                for warning in unsupported_chain_feature_flag_warnings(
                    &detected,
                    &endpoint_info,
                    existing_cursor_state.as_ref(),
                    args.without_extended,
                    args.without_votes,
                ) {
                    warn!("{}", warning);
                }
                if detected == "solana" {
                    extended = false;
                    log_solana_vote_mode(with_votes);
                } else if detected == "antelope" {
                    extended = false;
                } else {
                    extended = resolve_extended_mode(extended, args.without_extended, &endpoint_info);
                }
                if failed_transactions_block_type.as_deref() != Some(detected.as_str()) {
                    let (resolved, warnings) = resolve_include_failed_transactions(
                        Some(&detected),
                        args.include_failed_transactions,
                        args.exclude_failed_transactions,
                        existing_cursor_state.as_ref(),
                        args.cursor_override,
                    );
                    for warning in warnings {
                        warn!("{}", warning);
                    }
                    include_failed_transactions = resolved;
                    cursor_state_template.include_failed_transactions = resolved;
                }
                let encode_bytes = resolve_auto_encode_bytes(
                    Some(&detected),
                    &endpoint_info,
                    tron_style_evm_profile,
                );
                let meta = build_file_metadata(
                    &detected,
                    &encode_bytes,
                    &config.endpoint,
                    config.compression,
                    &endpoint_info,
                );
                let mut meta = meta;
                maybe_add_solana_with_votes_metadata(&mut meta, Some(&detected), with_votes);
                let detected_uses_synthetic_partition_routing =
                    use_last_known_timestamp_partition_routing(&detected, &partition_config);
                maybe_add_synthetic_timestamp_metadata(
                    &mut meta,
                    &detected,
                    detected_uses_synthetic_partition_routing,
                );
                log_file_metadata(&meta);
                current_file_metadata = meta;
                let mut cursor_meta = build_cursor_file_metadata(
                    Some(&detected),
                    Some(&encode_bytes),
                    &config.endpoint,
                    config.compression,
                    &config.partition,
                    &endpoint_info,
                    extended,
                    config.final_blocks_only,
                    include_failed_transactions,
                );
                maybe_add_solana_with_votes_metadata(&mut cursor_meta, Some(&detected), with_votes);
                cursor_state_template.file_metadata = cursor_meta;
                cursor_state_template.extended = extended;
                is_solana = detected == "solana";
                resolved_block_type = Some(detected.clone());
                use_synthetic_partition_routing = detected_uses_synthetic_partition_routing;
                timestamp_routing = TimestampRouting::new(use_synthetic_partition_routing);
                restore_sparse_routing_cursor_anchor(
                    &mut timestamp_routing,
                    existing_cursor_state.as_ref(),
                    args.cursor_override,
                    &detected,
                    &partition_config,
                );
                mapper = Some(create_mapper(
                    &detected,
                    extended,
                    with_votes,
                    include_fork_step,
                    encode_bytes,
                    use_synthetic_partition_routing,
                    include_failed_transactions,
                )?);
            }

            let m = mapper.as_mut().unwrap();

            let block_number = identity.block_num;
            let ts = identity.timestamp;
            let fork_step_owned = fork_step_str.map(str::to_owned);
            let mut routed_block = if is_solana && use_synthetic_partition_routing {
                timestamp_routing.route_block(
                    block_bytes,
                    cursor_str,
                    fork_step_owned,
                    identity,
                )?
            } else {
                BufferedBootstrapBlock {
                    received_ordinal: 0,
                    block_bytes,
                    cursor: cursor_str,
                    fork_step: fork_step_owned,
                    identity,
                }
            };
            let resumed_bootstrap_timestamp = (!is_solana && ts == 0)
                .then(|| session.as_ref().and_then(IngestionSession::routing_timestamp_hint))
                .flatten();
            routed_block.received_ordinal = received_ordinal;
            if let Some(seconds) = resumed_bootstrap_timestamp {
                routed_block.identity.timestamp = seconds;
            }
            let current_anchor_timestamp = routed_block.identity.timestamp;
            // For Solana, blocks may legitimately lack timestamps — skip the
            // genesis bootstrap and timestamp validation entirely.
            if !is_solana && resumed_bootstrap_timestamp.is_none() {
                match genesis_timestamp_bootstrap.observe_block(blocks_processed, block_number, ts) {
                    GenesisTimestampBootstrapAction::Buffer => {
                        if genesis_timestamp_bootstrap.buffered_blocks == 1 {
                            warn!(
                                requested_start_block = ?config.start_block,
                                block_number,
                                "buffering first streamable block because it lacks timestamp metadata; its timestamp will be synthesized from the first later timestamped block automatically"
                            );
                        }
                        buffered_bootstrap_blocks.push(BufferedBootstrapBlock {
                            received_ordinal,
                            block_bytes: routed_block.block_bytes.clone(),
                            cursor: routed_block.cursor.clone(),
                            fork_step: routed_block.fork_step.clone(),
                            identity: routed_block.identity.clone(),
                        });
                        update_bootstrap_buffer_metrics(&pipeline_metrics, &buffered_bootstrap_blocks);
                        return Ok(());
                    }
                    GenesisTimestampBootstrapAction::Anchored {
                        anchor_block,
                        buffered_blocks,
                        first_buffered_block,
                    } => {
                        info!(
                            first_buffered_block,
                            anchor_block,
                            buffered_blocks,
                            "preserving buffered bootstrap block(s) with a synthesized timestamp from the first later timestamped block"
                        );
                    }
                    GenesisTimestampBootstrapAction::None => {}
                }
            } // end if !is_solana
            let mut process_block = |block_bytes: &prost::bytes::Bytes,
                                     identity: &BlockIdentity,
                                     fork_step: Option<&str>,
                                     _cursor: &str,
                                     received_ordinal: u64,
                                     lookahead_ordinal: Option<u64>|
             -> Result<()> {
                let block_number = identity.block_num;
                let ts = identity.timestamp;
                // Solana blocks may have no timestamp; skip validation for Solana.
                if !is_solana {
                    validate_block_timestamp(block_number, ts, &partition_config)?;
                }
                let has_timestamp = ts != 0;

                // Flush the mapper at partition boundaries to ensure each flush
                // produces batches belonging to exactly one partition.
                // See: https://github.com/pinax-network/firehose-parquet/issues/110
                let new_partition_key = partition_config.partition_key(block_number, ts)?;
                if let Some(ref new_key) = new_partition_key {
                    let partition_changed = current_partition_key
                        .as_ref()
                        .map_or(false, |cur| cur != new_key);
                    if partition_changed && (m.max_table_rows() > 0 || session.as_ref().map(IngestionSession::has_accepted).transpose()?.unwrap_or(false)) {
                        info!(
                            old_partition = %current_partition_key.as_deref().unwrap_or("?"),
                            new_partition = %new_key,
                            block_number,
                            "partition boundary detected, flushing mapper"
                        );
                        let preflush_estimate = update_mapper_buffer_metrics(&pipeline_metrics, m.as_mut());
                        let batches = m.flush()?;
                        update_mapper_buffer_metrics(&pipeline_metrics, m.as_mut());
                        let flushed_tables = batches.len();
                        let flushed_rows: usize = batches.values().map(|batch| batch.num_rows()).sum();
                        info!(
                            trigger = "partition_boundary",
                            old_partition = %current_partition_key.as_deref().unwrap_or("?"),
                            new_partition = %new_key,
                            block_number,
                            tables = flushed_tables,
                            rows = flushed_rows,
                            "mapper flush emitted record batches"
                        );
                        if !dry_run {
                            let metadata = BlockMetadata {
                                min_block_number: min_block.unwrap_or(0), max_block_number: max_block.unwrap_or(0), min_timestamp, max_timestamp,
                            };
                            let outcome = commit_ingestion_flush(session.as_mut().context("protected session is required")?, batches, metadata, config.compression, current_file_metadata.clone(), &pipeline_metrics, &mut flush_sizing, preflush_estimate, "partition_boundary")?;
                            log_writer_flush_outcome("partition_boundary", flushed_tables, flushed_rows, outcome);
                        } else {
                            info!(
                                trigger = "partition_boundary",
                                tables = flushed_tables,
                                rows = flushed_rows,
                                "dry run mapper flush skipped parquet writes"
                            );
                        }
                        min_block = None;
                        max_block = None;
                        min_timestamp = None;
                        max_timestamp = None;
                        blocks_since_flush = 0;
                        last_flush_time = Instant::now();
                    }
                }
                current_partition_key = new_partition_key;

                let mapped = m.map_block_bytes(block_bytes.clone(), identity, fork_step);
                let mapper_estimate = update_mapper_buffer_metrics(&pipeline_metrics, m.as_mut());
                transactions_processed += mapped?;
                if let Some(session) = session.as_mut() {
                    session.accept_mapped(received_ordinal, (ts != 0).then_some(ts), lookahead_ordinal)?;
                }

                // Only count the block in the file metadata once it is mapped.
                min_block = Some(min_block.map_or(block_number, |s: u64| s.min(block_number)));
                max_block = Some(max_block.map_or(block_number, |s: u64| s.max(block_number)));
                global_min_block = Some(global_min_block.map_or(block_number, |s: u64| s.min(block_number)));
                global_max_block = Some(global_max_block.map_or(block_number, |s: u64| s.max(block_number)));
                if has_timestamp {
                    min_timestamp = Some(min_timestamp.map_or(ts, |s: i64| s.min(ts)));
                    max_timestamp = Some(max_timestamp.map_or(ts, |s: i64| s.max(ts)));
                }

                blocks_processed += 1;
                blocks_since_flush += 1;
                bytes_read += block_bytes.len() as u64;

                // Update Prometheus metrics.
                pipeline_metrics.blocks_processed_total.inc();
                pipeline_metrics.bytes_read_total.inc_by(block_bytes.len() as u64);
                pipeline_metrics.current_block_number.set(block_number as i64);
                if global_min_block.map_or(true, |g| block_number <= g) {
                    pipeline_metrics.min_block_number.set(block_number as i64);
                }
                if global_max_block.map_or(true, |g| block_number >= g) {
                    pipeline_metrics.max_block_number.set(block_number as i64);
                }

                // Check for graceful shutdown after processing the current block.
                if shutdown.is_cancelled() {
                    info!(blocks_processed, block_number, "shutdown requested, breaking out of stream");
                    return Err(ShutdownRequested.into());
                }

                if should_emit_progress_log(blocks_processed) {
                    let elapsed_secs = progress_start.elapsed().as_secs_f64();
                    let blocks_per_sec = if elapsed_secs > 0.0 {
                        blocks_processed as f64 / elapsed_secs
                    } else {
                        0.0
                    };
                    let timestamp = format_optional_probe_timestamp(ts);
                    match (timestamp.as_deref(), current_partition_key.as_deref()) {
                        (Some(timestamp), Some(partition)) => info!(
                            blocks = blocks_processed,
                            transactions = transactions_processed,
                            block_num = block_number,
                            timestamp,
                            partition,
                            blocks_per_sec = format!("{:.0}", blocks_per_sec),
                            total_rows = m.total_rows(),
                            bytes_read = firehose_parquet::cli::format_bytes(bytes_read),
                            "progress"
                        ),
                        (Some(timestamp), None) => info!(
                            blocks = blocks_processed,
                            transactions = transactions_processed,
                            block_num = block_number,
                            timestamp,
                            blocks_per_sec = format!("{:.0}", blocks_per_sec),
                            total_rows = m.total_rows(),
                            bytes_read = firehose_parquet::cli::format_bytes(bytes_read),
                            "progress"
                        ),
                        (None, Some(partition)) => info!(
                            blocks = blocks_processed,
                            transactions = transactions_processed,
                            block_num = block_number,
                            partition,
                            blocks_per_sec = format!("{:.0}", blocks_per_sec),
                            total_rows = m.total_rows(),
                            bytes_read = firehose_parquet::cli::format_bytes(bytes_read),
                            "progress"
                        ),
                        (None, None) => info!(
                            blocks = blocks_processed,
                            transactions = transactions_processed,
                            block_num = block_number,
                            blocks_per_sec = format!("{:.0}", blocks_per_sec),
                            total_rows = m.total_rows(),
                            bytes_read = firehose_parquet::cli::format_bytes(bytes_read),
                            "progress"
                        ),
                    }

                }

                if let Some(flush_trigger) = next_mapper_flush_trigger(
                    flush_rows,
                    m.max_table_rows(),
                    flush_blocks,
                    blocks_since_flush,
                    flush_interval_secs,
                    last_flush_time,
                    &flush_sizing,
                    mapper_estimate,
                ) {
                    let flush_trigger = flush_trigger.as_str();
                    let batches = m.flush()?;
                    update_mapper_buffer_metrics(&pipeline_metrics, m.as_mut());
                    let flushed_tables = batches.len();
                    let flushed_rows: usize = batches.values().map(|batch| batch.num_rows()).sum();
                    info!(
                        trigger = flush_trigger,
                        tables = flushed_tables,
                        rows = flushed_rows,
                        "mapper flush emitted record batches"
                    );
                    if !dry_run {
                        let metadata = BlockMetadata {
                            min_block_number: min_block.unwrap_or(0), max_block_number: max_block.unwrap_or(0), min_timestamp, max_timestamp,
                        };
                        let outcome = commit_ingestion_flush(session.as_mut().context("protected session is required")?, batches, metadata, config.compression, current_file_metadata.clone(), &pipeline_metrics, &mut flush_sizing, mapper_estimate, flush_trigger)?;
                        log_writer_flush_outcome(flush_trigger, flushed_tables, flushed_rows, outcome);
                    } else {
                        info!(
                            trigger = flush_trigger,
                            tables = flushed_tables,
                            rows = flushed_rows,
                            "dry run mapper flush skipped parquet writes"
                        );
                    }
                    min_block = None;
                    max_block = None;
                    min_timestamp = None;
                    max_timestamp = None;
                    blocks_since_flush = 0;
                    last_flush_time = Instant::now();
                }

                Ok(())
            };

            let anchored_blocks = take_anchored_bootstrap_blocks(
                &mut buffered_bootstrap_blocks,
                current_anchor_timestamp,
            );
            update_bootstrap_buffer_metrics(&pipeline_metrics, &buffered_bootstrap_blocks);
            for buffered_block in anchored_blocks {
                process_block(
                    &buffered_block.block_bytes,
                    &buffered_block.identity,
                    buffered_block.fork_step.as_deref(),
                    &buffered_block.cursor,
                    buffered_block.received_ordinal,
                    Some(received_ordinal),
                )?;
            }

            process_block(
                &routed_block.block_bytes,
                &routed_block.identity,
                routed_block.fork_step.as_deref(),
                &routed_block.cursor,
                routed_block.received_ordinal,
                None,
            )?;

            Ok(())
        })
        .await;

    // Distinguish graceful shutdown from real errors.
    let exit = StreamExit::from_result(&stream_result);
    match &stream_result {
        Err(e) if exit == StreamExit::Failed => {
            warn!(error = %e, "stream ended with error, discarding buffered data without advancing the cursor");
        }
        Err(_) => info!("graceful shutdown initiated"),
        Ok(()) => {}
    }

    // Only a completed stream writes partial buffers. On graceful shutdown
    // they are discarded to avoid non-deterministic extra part files. On an
    // error they are discarded so the cursor is never saved past rows whose
    // write (or mapping) failed. Either way only complete partitions that were
    // already flushed during normal processing are preserved, and on restart
    // the stream resumes from the last saved cursor, which corresponds to the
    // last fully-written flush.
    if !exit.materializes_buffers() {
        info!(
            mapper_buffered_rows = mapper
                .as_ref()
                .map(|mapper| mapper.total_rows())
                .unwrap_or(0),
            "stream stopped; discarded uncommitted buffers and retained the authoritative prefix"
        );
    } else if let Some(mapper) = mapper.as_mut() {
        if mapper.max_table_rows() > 0
            || session
                .as_ref()
                .map(IngestionSession::has_accepted)
                .transpose()?
                .unwrap_or(false)
        {
            let preflush_estimate =
                update_mapper_buffer_metrics(&pipeline_metrics, mapper.as_mut());
            let batches = mapper.flush()?;
            update_mapper_buffer_metrics(&pipeline_metrics, mapper.as_mut());
            if let Some(session) = session.as_mut() {
                let metadata = BlockMetadata {
                    min_block_number: min_block.unwrap_or(0),
                    max_block_number: max_block.unwrap_or(0),
                    min_timestamp,
                    max_timestamp,
                };
                if let Some(committed) = session
                    .flush(
                        batches,
                        metadata,
                        config.compression,
                        current_file_metadata.clone(),
                    )
                    .await?
                {
                    record_committed_flush_sizing(
                        &mut flush_sizing,
                        preflush_estimate,
                        &committed,
                        "stream_end",
                    );
                    pipeline_metrics
                        .flushes_total
                        .get_or_create(&metrics::FlushLabels {
                            trigger: "stream_end".into(),
                        })
                        .inc();
                }
            }
        }
    }

    if exit == StreamExit::Completed && genesis_timestamp_bootstrap.enabled {
        if let Some(first_buffered_block) = genesis_timestamp_bootstrap.first_buffered_block {
            return Err(missing_genesis_timestamp_bootstrap_error(
                first_buffered_block,
                genesis_timestamp_bootstrap.buffered_blocks,
            ));
        }
    }

    if let (StreamExit::Completed, Some(stop)) = (exit, config.stop_block) {
        if let Some(session) = session.as_mut() {
            session.complete_request(stop, true).await?;
        } else {
            let resumed_block_num =
                stream_resume_cursor(existing_cursor_state.as_ref(), args.cursor_override)
                    .and(existing_cursor_state.as_ref())
                    .map(|state| state.last_block_num);
            let gaps_allowed = resolved_block_type
                .as_deref()
                .or(initial_block_type.as_deref())
                .is_some_and(block_type_allows_block_number_gaps);
            ensure_bounded_stream_reached_stop(
                stop,
                global_max_block.max(resumed_block_num),
                gaps_allowed,
            )?;
        }
    }

    if exit == StreamExit::Completed && !dry_run {
        if let Some(message) = firehose_parquet::cli::non_final_bounded_warning(
            config.final_blocks_only,
            config.stop_block,
        ) {
            warn!("{message}");
        }
    }

    // Final metrics.
    let elapsed = progress_start.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    let blocks_per_sec = if elapsed_secs > 0.0 {
        blocks_processed as f64 / elapsed_secs
    } else {
        0.0
    };
    let speed_per_sec = if elapsed_secs > 0.0 {
        bytes_read as f64 / elapsed_secs
    } else {
        0.0
    };

    // Format elapsed as human-readable duration.
    let elapsed_display = {
        let total_secs = elapsed.as_secs();
        let hours = total_secs / 3600;
        let minutes = (total_secs % 3600) / 60;
        let secs = total_secs % 60;
        if hours > 0 {
            format!("{}h{}m{}s", hours, minutes, secs)
        } else if minutes > 0 {
            format!("{}m{}s", minutes, secs)
        } else {
            format!("{}.{}s", secs, (elapsed.subsec_millis() / 100))
        }
    };

    // Format block range.
    let block_range = match (global_min_block, global_max_block) {
        (Some(min), Some(max)) => format!("{} — {}", min, max),
        _ => "N/A".to_string(),
    };

    info!(
        blocks_processed,
        blocks_skipped_below_start = start_block_filter.skipped,
        block_range = %block_range,
        elapsed = %elapsed_display,
        bytes_read = firehose_parquet::cli::format_bytes(bytes_read),
        speed = format!("{}/s | {:.0} blocks/s", firehose_parquet::cli::format_bytes(speed_per_sec as u64), blocks_per_sec),
        "pipeline finished",
    );

    // Propagate real (non-shutdown) errors so the process exits non-zero.
    if exit == StreamExit::Failed {
        return stream_result;
    }

    drop(session);

    // All synchronous writes have resolved by this point. Any earlier error or
    // cancellation of this future drops the guard and retains remote ownership.
    if let Some(ownership) = ownership {
        ownership.release().await?;
    }
    Ok(())
}
