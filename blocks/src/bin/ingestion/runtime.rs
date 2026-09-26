//! Ordered ingestion state. Only the borrowed Session commits authority/mirrors.
use super::*;
use setup::IngestionSetup;

/// Mapper and routing policy are prepared before recovery, so the Session binds
/// the same schemas that later receive blocks. No owner is stored here.
pub(super) struct MapperState {
    mapper: Option<Box<dyn BlockMapper>>,
    current_file_metadata: ParquetFileMetadata,
    sizing: FlushSizing,
    extended: bool,
    include_failed_transactions: bool,
    /// The resolved family may omit block timestamps (see `ChainProfile`).
    nullable_timestamps: bool,
    use_synthetic_partition_routing: bool,
    genesis_timestamp_bootstrap: GenesisTimestampBootstrap,
    timestamp_routing: TimestampRouting,
}

impl MapperState {
    pub(super) fn new(args: &BuildArgs, setup: &IngestionSetup) -> Result<Self> {
        let config = &setup.config;
        let sizing = FlushSizing::new(config.flush_bytes, config.flush_memory_bytes)?;
        let block_type = setup.block_type;
        let use_synthetic_partition_routing = block_type.is_some_and(|kind| {
            use_last_known_timestamp_partition_routing(kind, &config.partition)
        });
        let genesis_timestamp_bootstrap = GenesisTimestampBootstrap::new(config.start_block);
        let mut timestamp_routing = TimestampRouting::new(use_synthetic_partition_routing);
        if let Some(kind) = block_type {
            restore_sparse_routing_cursor_anchor(
                &mut timestamp_routing,
                setup.existing_cursor_state.as_ref(),
                args.cursor_override,
                kind,
                &config.partition,
            );
        }
        let mut current_file_metadata = ParquetFileMetadata::new();
        let mapper = if let Some(kind) = block_type {
            let encode_bytes = setup.initial_bytes_encoding.clone();
            let mut metadata = build_file_metadata(
                kind,
                &encode_bytes,
                &config.endpoint,
                config.compression,
                &setup.endpoint_info,
            );
            maybe_add_with_votes_metadata(&mut metadata, kind, setup.with_votes);
            maybe_add_synthetic_timestamp_metadata(
                &mut metadata,
                kind,
                use_synthetic_partition_routing,
            );
            log_file_metadata(&metadata);
            current_file_metadata = metadata;
            Some(kind.create_mapper(MapperOptions {
                extended: setup.extended,
                with_votes: setup.with_votes,
                include_fork_step: !config.final_blocks_only,
                encode_bytes,
                synthetic_partition_routing: use_synthetic_partition_routing,
                include_failed_transactions: setup.include_failed_transactions,
            }))
        } else {
            None
        };
        Ok(Self {
            mapper,
            current_file_metadata,
            sizing,
            extended: setup.extended,
            include_failed_transactions: setup.include_failed_transactions,
            nullable_timestamps: block_type.is_some_and(|kind| kind.profile().nullable_timestamps),
            use_synthetic_partition_routing,
            genesis_timestamp_bootstrap,
            timestamp_routing,
        })
    }

    pub(super) fn semantics(&mut self, setup: &IngestionSetup) -> Result<MapperSemantics> {
        let mapper = self
            .mapper
            .as_mut()
            .context("protected ingestion requires a mapper before recovery")?;
        let empty_batches = mapper.flush()?;
        let tables = declare_inventory(&empty_batches, &mapper.table_names())?;
        Ok(MapperSemantics {
            chain: setup
                .endpoint_info
                .as_ref()
                .context("endpoint identity is required")?
                .chain_name
                .clone(),
            family: setup
                .block_type
                .context("unsupported resolved mapper family")?
                .profile()
                .family,
            bytes_encoding: setup.initial_bytes_encoding_label.clone(),
            extended: self.extended,
            with_votes: setup.with_votes,
            include_failed_transactions: self.include_failed_transactions,
            tables,
        })
    }

    pub(super) fn restore_session_anchor(&mut self, session: &IngestionSession<'_>) {
        if let Some((source_block, seconds)) = session.routing_anchor_source() {
            self.timestamp_routing.restore_anchor(source_block, seconds);
        }
    }
}

struct FlushWindow {
    min_block: Option<u64>,
    max_block: Option<u64>,
    min_timestamp: Option<i64>,
    max_timestamp: Option<i64>,
    blocks: u64,
    last_flush: Instant,
    partition_key: Option<String>,
}

impl FlushWindow {
    fn new() -> Self {
        Self {
            min_block: None,
            max_block: None,
            min_timestamp: None,
            max_timestamp: None,
            blocks: 0,
            last_flush: Instant::now(),
            partition_key: None,
        }
    }

    fn metadata(&self) -> BlockMetadata {
        BlockMetadata {
            min_block_number: self.min_block.unwrap_or(0),
            max_block_number: self.max_block.unwrap_or(0),
            min_timestamp: self.min_timestamp,
            max_timestamp: self.max_timestamp,
        }
    }

    fn reset(&mut self) {
        self.min_block = None;
        self.max_block = None;
        self.min_timestamp = None;
        self.max_timestamp = None;
        self.blocks = 0;
        self.last_flush = Instant::now();
    }
}

struct RunStats {
    blocks: u64,
    transactions: u64,
    min_block: Option<u64>,
    max_block: Option<u64>,
    bytes_read: u64,
    started: Instant,
}

struct MapperFlush {
    batches: HashMap<String, RecordBatch>,
    metadata: BlockMetadata,
    estimate: MapperBufferEstimate,
    tables: usize,
    rows: usize,
}

pub(super) struct IngestionRuntime<'run, 'owner> {
    args: &'run BuildArgs,
    setup: &'run IngestionSetup,
    session: Option<&'run mut IngestionSession<'owner>>,
    pipeline_metrics: &'run metrics::PipelineMetrics,
    shutdown: &'run CancellationToken,
    state: MapperState,
    window: FlushWindow,
    stats: RunStats,
    buffered_bootstrap_blocks: Vec<BufferedBootstrapBlock>,
    start_block_filter: StartBlockFilter,
    cursor_state_template: CursorState,
}

impl<'run, 'owner> IngestionRuntime<'run, 'owner> {
    pub(super) fn new(
        args: &'run BuildArgs,
        setup: &'run IngestionSetup,
        state: MapperState,
        session: Option<&'run mut IngestionSession<'owner>>,
        pipeline_metrics: &'run metrics::PipelineMetrics,
        shutdown: &'run CancellationToken,
    ) -> Result<Self> {
        let mut runtime = Self {
            args,
            setup,
            session,
            pipeline_metrics,
            shutdown,
            state,
            window: FlushWindow::new(),
            stats: RunStats {
                blocks: 0,
                transactions: 0,
                min_block: None,
                max_block: None,
                bytes_read: 0,
                started: Instant::now(),
            },
            buffered_bootstrap_blocks: Vec::new(),
            start_block_filter: StartBlockFilter::new(setup.config.start_block),
            cursor_state_template: CursorState::default(),
        };
        runtime.cursor_state_template = runtime.dry_run_cursor_template()?;
        Ok(runtime)
    }

    pub(super) fn resume_cursor(&self) -> Option<String> {
        match self.session.as_deref() {
            Some(session) => session.resume_cursor().map(str::to_owned),
            None => stream_resume_cursor(
                self.setup.existing_cursor_state.as_ref(),
                self.args.cursor_override,
            ),
        }
    }

    /// Legacy cursor parameter validation remains reachable for dry runs.
    fn dry_run_cursor_template(&self) -> Result<CursorState> {
        // Build file-level metadata for the cursor (same `firehose-parquet.*`
        // namespace as table files). Includes version, endpoint, chain info, and
        // pipeline parameters.
        let initial_cursor_encoding = Some(self.setup.initial_bytes_encoding.clone());
        let cursor_file_metadata = build_cursor_file_metadata(
            self.setup.block_type.or(self.setup.initial_block_type),
            initial_cursor_encoding.as_ref(),
            &self.setup.config.endpoint,
            self.setup.config.compression,
            &self.setup.config.partition,
            &self.setup.endpoint_info,
            self.state.extended,
            self.setup.config.final_blocks_only,
            self.state.include_failed_transactions,
        );
        let mut cursor_file_metadata = cursor_file_metadata;
        if self.setup.chain_features.vote_transactions {
            add_with_votes_metadata(&mut cursor_file_metadata, self.setup.with_votes);
        }

        // Build a template CursorState with pipeline parameters that stay constant.
        let cursor_state_template = CursorState {
            start_block: self.setup.config.start_block,
            stop_block: self.setup.config.stop_block,
            extended: self.state.extended,
            final_blocks_only: self.setup.config.final_blocks_only,
            include_failed_transactions: self.state.include_failed_transactions,
            file_metadata: cursor_file_metadata,
            ..CursorState::default()
        };

        // Validate cursor parameters against current CLI arguments.
        if let Some(loaded) = self
            .setup
            .existing_cursor_state
            .as_ref()
            .filter(|_| self.setup.config.dry_run)
        {
            let mut mismatches = loaded.validate_params(&cursor_state_template);
            mismatches.retain(|mismatch| !mismatch.starts_with("stop_block:"));
            if self.setup.chain_features.vote_transactions {
                apply_solana_cursor_feature_validation(
                    &mut mismatches,
                    loaded,
                    self.setup.with_votes,
                );
            } else if self.setup.chain_features.extended_unsupported {
                apply_antelope_cursor_feature_validation(&mut mismatches);
            }
            if !mismatches.is_empty() {
                if self.args.cursor_override {
                    warn!(
                    "cursor parameter mismatch detected (overridden via --cursor-override):\n  {}",
                    mismatches.join("\n  ")
                );
                } else {
                    return Err(anyhow!(
                    "cursor parameter mismatch detected:\n  {}\n\nThis read-only dry run can preview the current parameters with --cursor-override. A real build cannot change protected output parameters; build into a new empty output root instead.",
                    mismatches.join("\n  ")
                ));
                }
            }
        }

        Ok(cursor_state_template)
    }

    pub(super) fn observe(
        &mut self,
        block_bytes: Vec<u8>,
        type_url: String,
        cursor_str: String,
        identity: BlockIdentity,
        step: i32,
    ) -> Result<()> {
        // Receipt must precede every filter and any buffered lookahead.
        let received_ordinal = if let Some(active_session) = self.session.as_mut() {
            let family = detect_block_type(&type_url)?.profile().family;
            active_session.receive(cursor_str.clone(), &identity, step, family)?
        } else {
            0
        };
        let fork_step_str = fork_step_name(step);
        if self.setup.config.final_blocks_only && step == 2 {
            if let Some(active_session) = self.session.as_mut() {
                active_session.accept_filtered(received_ordinal)?;
            }
            return Ok(());
        }
        if !self.start_block_filter.admit(identity.block_num) {
            self.pipeline_metrics.blocks_skipped_below_start_total.inc();
            if let Some(active_session) = self.session.as_mut() {
                active_session.accept_filtered(received_ordinal)?;
            }
            return Ok(());
        }

        // Transfer the owned gRPC payload without copying. Buffered routing
        // and chain decoding share slices of this allocation.
        let block_bytes = prost::bytes::Bytes::from(block_bytes);

        self.ensure_mapper(&type_url)?;

        let block_number = identity.block_num;
        let ts = identity.timestamp;
        let fork_step_owned = fork_step_str.map(str::to_owned);
        let mut routed_block =
            if self.state.nullable_timestamps && self.state.use_synthetic_partition_routing {
                self.state.timestamp_routing.route_block(
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
        let resumed_bootstrap_timestamp = (!self.state.nullable_timestamps && ts == 0)
            .then(|| {
                self.session
                    .as_deref()
                    .and_then(IngestionSession::routing_timestamp_hint)
            })
            .flatten();
        routed_block.received_ordinal = received_ordinal;
        if let Some(seconds) = resumed_bootstrap_timestamp {
            routed_block.identity.timestamp = seconds;
        }
        let current_anchor_timestamp = routed_block.identity.timestamp;
        // Nullable-timestamp chains (Solana) may legitimately lack timestamps;
        // skip the genesis bootstrap and timestamp validation entirely.
        if !self.state.nullable_timestamps && resumed_bootstrap_timestamp.is_none() {
            match self.state.genesis_timestamp_bootstrap.observe_block(
                self.stats.blocks,
                block_number,
                ts,
            ) {
                GenesisTimestampBootstrapAction::Buffer => {
                    if self.state.genesis_timestamp_bootstrap.buffered_blocks == 1 {
                        warn!(
                            requested_start_block = ?self.setup.config.start_block,
                            block_number,
                            "buffering first streamable block because it lacks timestamp metadata; its timestamp will be synthesized from the first later timestamped block automatically"
                        );
                    }
                    self.buffered_bootstrap_blocks.push(BufferedBootstrapBlock {
                        received_ordinal,
                        block_bytes: routed_block.block_bytes.clone(),
                        cursor: routed_block.cursor.clone(),
                        fork_step: routed_block.fork_step.clone(),
                        identity: routed_block.identity.clone(),
                    });
                    update_bootstrap_buffer_metrics(
                        &self.pipeline_metrics,
                        &self.buffered_bootstrap_blocks,
                    );
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
        } // end if !nullable_timestamps

        let anchored_blocks = take_anchored_bootstrap_blocks(
            &mut self.buffered_bootstrap_blocks,
            current_anchor_timestamp,
        );
        update_bootstrap_buffer_metrics(&self.pipeline_metrics, &self.buffered_bootstrap_blocks);
        for buffered_block in anchored_blocks {
            self.process_ready(
                &buffered_block.block_bytes,
                &buffered_block.identity,
                buffered_block.fork_step.as_deref(),
                buffered_block.received_ordinal,
                Some(received_ordinal),
            )?;
        }

        self.process_ready(
            &routed_block.block_bytes,
            &routed_block.identity,
            routed_block.fork_step.as_deref(),
            routed_block.received_ordinal,
            None,
        )?;

        Ok(())
    }

    // Protected runs bind a mapper before recovery. Dry-run auto can still
    // resolve its family and compatibility metadata from the first payload.
    fn ensure_mapper(&mut self, type_url: &str) -> Result<()> {
        if self.state.mapper.is_none() {
            let detected = detect_block_type(&type_url)?;
            info!(detected_type = %detected, type_url = %type_url, "auto-detected block type");
            for warning in unsupported_chain_feature_flag_warnings(
                Some(detected),
                &self.setup.endpoint_info,
                self.setup.existing_cursor_state.as_ref(),
                self.args.without_extended,
                self.args.without_votes,
            ) {
                warn!("{}", warning);
            }
            let profile = detected.profile();
            if profile.vote_transactions {
                log_solana_vote_mode(self.setup.with_votes);
            }
            self.state.extended = resolve_detected_extended(
                detected,
                self.state.extended,
                self.args.without_extended,
                &self.setup.endpoint_info,
            );
            if self.setup.failed_transactions_block_type != Some(detected) {
                let (resolved, warnings) = resolve_include_failed_transactions(
                    Some(detected),
                    self.args.include_failed_transactions,
                    self.args.exclude_failed_transactions,
                    self.setup.existing_cursor_state.as_ref(),
                    self.args.cursor_override,
                );
                for warning in warnings {
                    warn!("{}", warning);
                }
                self.state.include_failed_transactions = resolved;
                self.cursor_state_template.include_failed_transactions = resolved;
            }
            let encode_bytes = resolve_auto_encode_bytes(
                Some(detected),
                &self.setup.endpoint_info,
                self.setup.tron_style_evm_profile,
            );
            let meta = build_file_metadata(
                detected,
                &encode_bytes,
                &self.setup.config.endpoint,
                self.setup.config.compression,
                &self.setup.endpoint_info,
            );
            let mut meta = meta;
            maybe_add_with_votes_metadata(&mut meta, detected, self.setup.with_votes);
            let detected_uses_synthetic_partition_routing =
                use_last_known_timestamp_partition_routing(detected, &self.setup.config.partition);
            maybe_add_synthetic_timestamp_metadata(
                &mut meta,
                detected,
                detected_uses_synthetic_partition_routing,
            );
            log_file_metadata(&meta);
            self.state.current_file_metadata = meta;
            let mut cursor_meta = build_cursor_file_metadata(
                Some(detected),
                Some(&encode_bytes),
                &self.setup.config.endpoint,
                self.setup.config.compression,
                &self.setup.config.partition,
                &self.setup.endpoint_info,
                self.state.extended,
                self.setup.config.final_blocks_only,
                self.state.include_failed_transactions,
            );
            maybe_add_with_votes_metadata(&mut cursor_meta, detected, self.setup.with_votes);
            self.cursor_state_template.file_metadata = cursor_meta;
            self.cursor_state_template.extended = self.state.extended;
            self.state.nullable_timestamps = profile.nullable_timestamps;
            self.state.use_synthetic_partition_routing = detected_uses_synthetic_partition_routing;
            self.state.timestamp_routing =
                TimestampRouting::new(self.state.use_synthetic_partition_routing);
            restore_sparse_routing_cursor_anchor(
                &mut self.state.timestamp_routing,
                self.setup.existing_cursor_state.as_ref(),
                self.args.cursor_override,
                detected,
                &self.setup.config.partition,
            );
            self.state.mapper = Some(detected.create_mapper(MapperOptions {
                extended: self.state.extended,
                with_votes: self.setup.with_votes,
                include_fork_step: !self.setup.config.final_blocks_only,
                encode_bytes,
                synthetic_partition_routing: self.state.use_synthetic_partition_routing,
                include_failed_transactions: self.state.include_failed_transactions,
            }));
        }
        Ok(())
    }

    fn process_ready(
        &mut self,
        block_bytes: &prost::bytes::Bytes,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
        received_ordinal: u64,
        lookahead_ordinal: Option<u64>,
    ) -> Result<()> {
        let block_number = identity.block_num;
        let ts = identity.timestamp;
        // Nullable-timestamp (Solana) blocks may have no timestamp; skip validation.
        if !self.state.nullable_timestamps {
            validate_block_timestamp(block_number, ts, &self.setup.config.partition)?;
        }
        let has_timestamp = ts != 0;

        // Flush the mapper at partition boundaries to ensure each flush
        // produces batches belonging to exactly one partition.
        // See: https://github.com/pinax-network/firehose-parquet/issues/110
        let new_partition_key = self
            .setup
            .config
            .partition
            .partition_key(block_number, ts)?;
        if let Some(ref new_key) = new_partition_key {
            let partition_changed = self
                .window
                .partition_key
                .as_ref()
                .map_or(false, |cur| cur != new_key);
            if partition_changed && self.has_buffered()? {
                info!(
                    old_partition = %self.window.partition_key.as_deref().unwrap_or("?"),
                    new_partition = %new_key,
                    block_number,
                    "partition boundary detected, flushing mapper"
                );
                let flush = self.prepare_flush(None)?;
                info!(
                    trigger = "partition_boundary",
                    old_partition = %self.window.partition_key.as_deref().unwrap_or("?"),
                    new_partition = %new_key,
                    block_number,
                    tables = flush.tables,
                    rows = flush.rows,
                    "mapper flush emitted record batches"
                );
                self.commit_blocking(flush, "partition_boundary")?;
            }
        }
        // On a boundary, the previous accepted partition is durable before this map.
        self.window.partition_key = new_partition_key;

        let (mapped, mapper_estimate) = {
            let mapper = self.state.mapper.as_mut().unwrap();
            let mapped = mapper.map_block_bytes(block_bytes.clone(), identity, fork_step);
            let estimate = update_mapper_buffer_metrics(self.pipeline_metrics, mapper.as_mut());
            (mapped, estimate)
        };
        self.stats.transactions += mapped?;
        // Acceptance follows successful mapping; Session advances only on commit.
        if let Some(active_session) = self.session.as_mut() {
            active_session.accept_mapped(
                received_ordinal,
                (ts != 0).then_some(ts),
                lookahead_ordinal,
            )?;
        }

        // Only count the block in the file metadata once it is mapped.
        self.window.min_block = Some(
            self.window
                .min_block
                .map_or(block_number, |s: u64| s.min(block_number)),
        );
        self.window.max_block = Some(
            self.window
                .max_block
                .map_or(block_number, |s: u64| s.max(block_number)),
        );
        self.stats.min_block = Some(
            self.stats
                .min_block
                .map_or(block_number, |s: u64| s.min(block_number)),
        );
        self.stats.max_block = Some(
            self.stats
                .max_block
                .map_or(block_number, |s: u64| s.max(block_number)),
        );
        if has_timestamp {
            self.window.min_timestamp =
                Some(self.window.min_timestamp.map_or(ts, |s: i64| s.min(ts)));
            self.window.max_timestamp =
                Some(self.window.max_timestamp.map_or(ts, |s: i64| s.max(ts)));
        }

        self.stats.blocks += 1;
        self.window.blocks += 1;
        self.stats.bytes_read += block_bytes.len() as u64;

        // Update Prometheus metrics.
        self.pipeline_metrics.blocks_processed_total.inc();
        self.pipeline_metrics
            .bytes_read_total
            .inc_by(block_bytes.len() as u64);
        self.pipeline_metrics
            .current_block_number
            .set(block_number as i64);
        if self.stats.min_block.map_or(true, |g| block_number <= g) {
            self.pipeline_metrics
                .min_block_number
                .set(block_number as i64);
        }
        if self.stats.max_block.map_or(true, |g| block_number >= g) {
            self.pipeline_metrics
                .max_block_number
                .set(block_number as i64);
        }

        // Check for graceful shutdown after processing the current block.
        if self.shutdown.is_cancelled() {
            info!(
                blocks_processed = self.stats.blocks,
                block_number, "shutdown requested, breaking out of stream"
            );
            return Err(ShutdownRequested.into());
        }

        self.log_progress(block_number, ts);

        if let Some(flush_trigger) = next_mapper_flush_trigger(
            self.setup.config.flush_rows.map(|r| r as usize),
            self.state.mapper.as_ref().unwrap().max_table_rows(),
            self.setup.config.flush_blocks,
            self.window.blocks,
            self.setup.config.flush_interval_secs,
            self.window.last_flush,
            &self.state.sizing,
            mapper_estimate,
        ) {
            let flush_trigger = flush_trigger.as_str();
            let flush = self.prepare_flush(Some(mapper_estimate))?;
            info!(
                trigger = flush_trigger,
                tables = flush.tables,
                rows = flush.rows,
                "mapper flush emitted record batches"
            );
            self.commit_blocking(flush, flush_trigger)?;
        }

        Ok(())
    }

    fn log_progress(&self, block_number: u64, ts: i64) {
        if should_emit_progress_log(self.stats.blocks) {
            let elapsed_secs = self.stats.started.elapsed().as_secs_f64();
            let blocks_per_sec = if elapsed_secs > 0.0 {
                self.stats.blocks as f64 / elapsed_secs
            } else {
                0.0
            };
            let timestamp = format_optional_probe_timestamp(ts);
            match (timestamp.as_deref(), self.window.partition_key.as_deref()) {
                (Some(timestamp), Some(partition)) => info!(
                    blocks = self.stats.blocks,
                    transactions = self.stats.transactions,
                    block_num = block_number,
                    timestamp,
                    partition,
                    blocks_per_sec = format!("{:.0}", blocks_per_sec),
                    total_rows = self.state.mapper.as_ref().unwrap().total_rows(),
                    bytes_read = firehose_parquet::cli::format_bytes(self.stats.bytes_read),
                    "progress"
                ),
                (Some(timestamp), None) => info!(
                    blocks = self.stats.blocks,
                    transactions = self.stats.transactions,
                    block_num = block_number,
                    timestamp,
                    blocks_per_sec = format!("{:.0}", blocks_per_sec),
                    total_rows = self.state.mapper.as_ref().unwrap().total_rows(),
                    bytes_read = firehose_parquet::cli::format_bytes(self.stats.bytes_read),
                    "progress"
                ),
                (None, Some(partition)) => info!(
                    blocks = self.stats.blocks,
                    transactions = self.stats.transactions,
                    block_num = block_number,
                    partition,
                    blocks_per_sec = format!("{:.0}", blocks_per_sec),
                    total_rows = self.state.mapper.as_ref().unwrap().total_rows(),
                    bytes_read = firehose_parquet::cli::format_bytes(self.stats.bytes_read),
                    "progress"
                ),
                (None, None) => info!(
                    blocks = self.stats.blocks,
                    transactions = self.stats.transactions,
                    block_num = block_number,
                    blocks_per_sec = format!("{:.0}", blocks_per_sec),
                    total_rows = self.state.mapper.as_ref().unwrap().total_rows(),
                    bytes_read = firehose_parquet::cli::format_bytes(self.stats.bytes_read),
                    "progress"
                ),
            }
        }
    }

    fn has_buffered(&self) -> Result<bool> {
        Ok(self
            .state
            .mapper
            .as_ref()
            .is_some_and(|mapper| mapper.max_table_rows() > 0)
            || self
                .session
                .as_deref()
                .map(IngestionSession::has_accepted)
                .transpose()?
                .unwrap_or(false))
    }

    /// Capture the estimate before taking batches; reset mapper gauges at the
    /// same point even when a later commit fails. Session owns durable progress.
    fn prepare_flush(&mut self, estimate: Option<MapperBufferEstimate>) -> Result<MapperFlush> {
        let mapper = self
            .state
            .mapper
            .as_mut()
            .context("mapper is required for flush")?;
        let estimate = estimate.unwrap_or_else(|| {
            update_mapper_buffer_metrics(self.pipeline_metrics, mapper.as_mut())
        });
        let batches = mapper.flush()?;
        update_mapper_buffer_metrics(self.pipeline_metrics, mapper.as_mut());
        Ok(MapperFlush {
            tables: batches.len(),
            rows: batches.values().map(RecordBatch::num_rows).sum(),
            batches,
            metadata: self.window.metadata(),
            estimate,
        })
    }

    fn commit_blocking(&mut self, flush: MapperFlush, trigger: &str) -> Result<()> {
        if !self.setup.config.dry_run {
            let committed = self
                .session
                .as_mut()
                .context("protected session is required")?
                .flush_blocking(
                    flush.batches,
                    flush.metadata,
                    self.setup.config.compression,
                    self.state.current_file_metadata.clone(),
                )?;
            if let Some(committed) = &committed {
                self.record_commit(flush.estimate, committed, trigger);
            }
            log_writer_flush_outcome(
                trigger,
                flush.tables,
                flush.rows,
                WriterFlushOutcome {
                    materialized: committed.is_some_and(|flush| flush.files > 0),
                    buffered: WriterBufferStats::default(),
                },
            );
        } else {
            info!(
                trigger,
                tables = flush.tables,
                rows = flush.rows,
                "dry run mapper flush skipped parquet writes"
            );
        }
        self.window.reset();
        Ok(())
    }

    fn record_commit(
        &mut self,
        estimate: MapperBufferEstimate,
        committed: &firehose_parquet::ingest::CommittedFlush,
        trigger: &str,
    ) {
        record_committed_flush_sizing(&mut self.state.sizing, estimate, committed, trigger);
        self.pipeline_metrics
            .flushes_total
            .get_or_create(&metrics::FlushLabels {
                trigger: trigger.into(),
            })
            .inc();
    }

    pub(super) async fn finish(&mut self, stream_result: Result<()>) -> Result<()> {
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
            mapper_buffered_rows = self.state.mapper
                .as_ref()
                .map(|active_mapper| active_mapper.total_rows())
                .unwrap_or(0),
            "stream stopped; discarded uncommitted buffers and retained the authoritative prefix"
        );
        } else if self.state.mapper.is_some() && self.has_buffered()? {
            let flush = self.prepare_flush(None)?;
            if let Some(session) = self.session.as_mut() {
                if let Some(committed) = session
                    .flush(
                        flush.batches,
                        flush.metadata,
                        self.setup.config.compression,
                        self.state.current_file_metadata.clone(),
                    )
                    .await?
                {
                    self.record_commit(flush.estimate, &committed, "stream_end");
                }
            }
        }

        if exit == StreamExit::Completed && self.state.genesis_timestamp_bootstrap.enabled {
            if let Some(first_buffered_block) =
                self.state.genesis_timestamp_bootstrap.first_buffered_block
            {
                return Err(missing_genesis_timestamp_bootstrap_error(
                    first_buffered_block,
                    self.state.genesis_timestamp_bootstrap.buffered_blocks,
                ));
            }
        }

        if let (StreamExit::Completed, Some(stop)) = (exit, self.setup.config.stop_block) {
            if let Some(active_session) = self.session.as_mut() {
                active_session.complete_request(stop, true).await?;
            } else {
                let resumed_block_num = stream_resume_cursor(
                    self.setup.existing_cursor_state.as_ref(),
                    self.args.cursor_override,
                )
                .and(self.setup.existing_cursor_state.as_ref())
                .map(|state| state.last_block_num);
                // A dry run predicts the protected completion rule on every
                // chain, including Solana, NEAR and Beacon skipped heights.
                ensure_bounded_stream_reached_stop(
                    stop,
                    self.stats.max_block.max(resumed_block_num),
                )?;
            }
        }

        if exit == StreamExit::Completed && !self.setup.config.dry_run {
            if let Some(message) = firehose_parquet::cli::non_final_bounded_warning(
                self.setup.config.final_blocks_only,
                self.setup.config.stop_block,
            ) {
                warn!("{message}");
            }
        }

        // Final metrics.
        let elapsed = self.stats.started.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();
        let blocks_per_sec = if elapsed_secs > 0.0 {
            self.stats.blocks as f64 / elapsed_secs
        } else {
            0.0
        };
        let speed_per_sec = if elapsed_secs > 0.0 {
            self.stats.bytes_read as f64 / elapsed_secs
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
        let block_range = match (self.stats.min_block, self.stats.max_block) {
            (Some(min), Some(max)) => format!("{} — {}", min, max),
            _ => "N/A".to_string(),
        };

        info!(
        blocks_processed = self.stats.blocks,
                blocks_skipped_below_start = self.start_block_filter.skipped,
                block_range = %block_range,
                elapsed = %elapsed_display,
                bytes_read = firehose_parquet::cli::format_bytes(self.stats.bytes_read),
                speed = format!("{}/s | {:.0} blocks/s", firehose_parquet::cli::format_bytes(speed_per_sec as u64), blocks_per_sec),
                "pipeline finished",
            );

        // Propagate real (non-shutdown) errors so the process exits non-zero.
        if exit == StreamExit::Failed {
            return stream_result;
        }
        Ok(())
    }
}
