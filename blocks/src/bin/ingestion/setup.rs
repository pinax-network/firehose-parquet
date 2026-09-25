//! Endpoint preflight and resumed configuration. Ownership stays in the caller:
//! startup resolves its complete scopes before the resumed state is read.
use super::*;

pub(super) struct ResolvedEndpoint {
    config: Config,
    block_type: String,
    endpoint_info: Option<EndpointInfo>,
    cursor_location: Option<CursorLocation>,
}

impl ResolvedEndpoint {
    pub(super) async fn resolve(
        args: &BuildArgs,
        shutdown: &CancellationToken,
    ) -> Result<Option<Self>> {
        let mut block_type = args.block_type.to_lowercase();
        if block_type != "auto" && !BLOCK_TYPES.contains(&block_type.as_str()) {
            return Err(anyhow!(
                "unsupported block type: {block_type}. Supported: {}",
                BLOCK_TYPES.join(", ")
            ));
        }

        let mut common = args.common.clone();
        let mut resolved_network_name: Option<String> = None;
        if common.endpoint.is_none() {
            if let Some(network) = args.network.as_deref() {
                let resolved = resolve_network_endpoint(network)?;
                match &resolved.source {
                    EndpointSource::Builtin => info!(
                        network = %resolved.requested,
                        chain_name = resolved.chain_name,
                        endpoint = %resolved.endpoint,
                        "resolved built-in network endpoint"
                    ),
                    EndpointSource::EnvOverride { env_var } => info!(
                        network = %resolved.requested,
                        chain_name = resolved.chain_name,
                        endpoint = %resolved.endpoint,
                        env_var = %env_var,
                        "resolved network endpoint from environment override"
                    ),
                }
                resolved_network_name = Some(resolved.requested.clone());
                common.endpoint = Some(resolved.endpoint);
            }
        } else if let Some(network) = args.network.as_deref() {
            info!(
                endpoint = %common.endpoint.as_deref().unwrap_or_default(),
                network,
                "ignoring --network because --endpoint or ENDPOINT is already set"
            );
        }

        let mut config = build_config(&common)?;

        if let Some(template) = args.common.cursor_template.as_deref() {
            let context = CursorTemplateContext {
                chain: None,
                partition_type: None,
                partition_value: None,
                partition_from: None,
                partition_to: None,
            };
            let resolved_cursor_path = resolve_cursor_template(template, &context)?;
            config.cursor_path = Some(resolved_cursor_path.clone());
            info!(
                cursor_template = %template,
                cursor_path = %resolved_cursor_path,
                "resolved cursor path"
            );
        }

        validate_cursor_storage(&config)?;

        // Fetch endpoint info for auto-detection of encoding, chain_name-based
        // output directory, and feature capability logging.
        let client = FirehoseClient::new(config.clone())?;
        // A signal during endpoint startup stops before anything is written.
        // Preserve the required Info result; cancellation does not restore fallback
        // output identity when Info is unavailable.
        let startup = async {
            ensure_endpoint_available(&client, &config.endpoint, resolved_network_name.as_deref())
                .await?;
            client.info().await
        };
        let endpoint_info = match unless_shutdown(&shutdown, startup).await {
            Ok(endpoint_info) => Some(endpoint_info?),
            Err(error) if is_shutdown_error(&error) => {
                info!("shutdown requested during startup, exiting");
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        debug!(endpoint_info = ?endpoint_info, "fetched endpoint metadata");

        // Use chain_name as a subdirectory under the output path.
        config.output = resolve_output(&config.output, &endpoint_info)?;

        // Protected authority must bind the actual mapper before opening Blocks.
        // Unknown custom endpoint metadata therefore requires an explicit family.
        if !config.dry_run && block_type == "auto" {
            block_type = inferred_block_type_from_endpoint_info(&endpoint_info)
            .context("cannot resolve the mapper from EndpointInfo before protected recovery; provide --block-type explicitly")?.to_owned();
        }
        let cursor_location = resolve_cursor_location(&config)?;
        Ok(Some(Self {
            config,
            block_type,
            endpoint_info,
            cursor_location,
        }))
    }

    pub(super) async fn acquire_ownership(&self) -> Result<Option<DatasetOwnership>> {
        let Self {
            config,
            cursor_location,
            ..
        } = self;
        let ownership = if config.dry_run {
            None
        } else {
            let aws = AwsConfig::from(config);
            Some(
                DatasetOwnership::acquire(
                    "build",
                    ingestion_mutation_scopes(config, cursor_location.as_ref())?,
                    Some(&aws),
                )
                .await?,
            )
        };
        Ok(ownership)
    }
}

pub(super) struct IngestionSetup {
    pub config: Config,
    pub block_type: String,
    pub endpoint_info: Option<EndpointInfo>,
    pub existing_cursor_state: Option<CursorState>,
    pub extended: bool,
    pub with_votes: bool,
    pub include_failed_transactions: bool,
    pub initial_block_type: Option<String>,
    pub failed_transactions_block_type: Option<String>,
    pub initial_bytes_encoding: EncodeBytes,
    pub initial_bytes_encoding_label: String,
    pub solana_chain: bool,
    pub antelope_chain: bool,
    pub tron_style_evm_profile: bool,
}

impl IngestionSetup {
    pub(super) async fn resolve(
        args: &BuildArgs,
        endpoint: ResolvedEndpoint,
        ownership: Option<&DatasetOwnership>,
    ) -> Result<Self> {
        let ResolvedEndpoint {
            mut config,
            block_type,
            endpoint_info,
            cursor_location,
        } = endpoint;
        let mut extended = !args.without_extended;
        let with_votes = !args.without_votes;
        let existing_cursor_state = if let Some(ownership) = ownership {
            load_authoritative_resume(&config, ownership, args.cursor_override).await?
        } else {
            load_existing_cursor(cursor_location.as_ref(), args.cursor_override)?
        };
        let solana_chain =
            chain_is_solana(&block_type, &endpoint_info, existing_cursor_state.as_ref());
        let antelope_chain =
            chain_is_antelope(&block_type, &endpoint_info, existing_cursor_state.as_ref());
        let known_non_solana_chain =
            chain_is_known_non_solana(&block_type, &endpoint_info, existing_cursor_state.as_ref());

        for warning in unsupported_chain_feature_flag_warnings(
            &block_type,
            &endpoint_info,
            existing_cursor_state.as_ref(),
            args.without_extended,
            args.without_votes,
        ) {
            warn!("{}", warning);
        }

        let live = infer_ingestion_live_mode(config.stop_block);
        config.start_block = resolve_ingestion_start_block(
            config.start_block,
            existing_cursor_state.as_ref(),
            &endpoint_info,
            args.cursor_override,
        )?;
        firehose_parquet::cli::validate_stop_block_after_start(
            config.start_block,
            config.stop_block,
        )?;
        debug!(
            requested_start_block = ?args.common.start_block,
            resolved_start_block = ?config.start_block,
            resolved_stop_block = ?config.stop_block,
            cursor_override = args.cursor_override,
            has_cursor = existing_cursor_state.is_some(),
            "resolved ingestion bounds"
        );
        if let firehose_parquet::config::Partition::BlockRange { size, .. } = &mut config.partition
        {
            let explicit_start_block = args.common.start_block;
            let effective_start_block = config.start_block;
            validate_block_range_alignment(
                explicit_start_block,
                effective_start_block,
                config.stop_block,
                *size,
            )?;
            config
                .partition
                .set_block_range_start(effective_start_block);
        }

        if solana_chain {
            extended = false;
            log_solana_vote_mode(with_votes);
        } else if antelope_chain {
            extended = false;
        } else if known_non_solana_chain {
            extended = resolve_extended_mode(extended, args.without_extended, &endpoint_info);
        }

        if !config.dry_run && block_type != "evm" {
            extended = false;
        }

        let tron_style_evm_profile = endpoint_uses_tron_style_evm_profile(&endpoint_info);
        let initial_block_type = if block_type != "auto" {
            Some(block_type.as_str())
        } else {
            inferred_block_type_from_endpoint_info(&endpoint_info)
        };
        // Failed-transaction handling depends on the chain, so resolve it from the
        // best block type known before streaming; auto-detection re-resolves it.
        let failed_transactions_block_type = initial_block_type.map(str::to_string).or_else(|| {
            cursor_metadata_block_type(existing_cursor_state.as_ref()).map(str::to_string)
        });
        let (include_failed_transactions, failed_transactions_warnings) =
            resolve_include_failed_transactions(
                failed_transactions_block_type.as_deref(),
                args.include_failed_transactions,
                args.exclude_failed_transactions,
                existing_cursor_state.as_ref(),
                args.cursor_override,
            );
        for warning in failed_transactions_warnings {
            warn!("{}", warning);
        }
        let initial_bytes_encoding =
            resolve_auto_encode_bytes(initial_block_type, &endpoint_info, tron_style_evm_profile);
        let initial_bytes_encoding_label = encode_bytes_label(&initial_bytes_encoding).to_string();

        info!(
            block_type,
            extended,
            with_votes,
            bytes_encoding = %initial_bytes_encoding_label,
            include_failed_transactions,
            "starting pipeline\n{config}"
        );

        if let Some(cursor_state) = existing_cursor_state.as_ref() {
            if args.cursor_override {
                info!(
                    stored_cursor_last_block_num = cursor_state.last_block_num,
                    requested_start_block = ?config.start_block,
                    requested_stop_block = ?config.stop_block,
                    live,
                    "cursor override enabled, restarting from CLI-provided/default bounds"
                );
            } else {
                info!(
                    stored_cursor_last_block_num = cursor_state.last_block_num,
                    requested_start_block = ?config.start_block,
                    requested_stop_block = ?config.stop_block,
                    live,
                    "resuming from stored cursor"
                );
            }
        } else {
            info!(
                requested_start_block = ?config.start_block,
                requested_stop_block = ?config.stop_block,
                live,
                "starting without stored cursor"
            );
        }

        let initial_block_type = initial_block_type.map(str::to_owned);
        Ok(Self {
            config,
            block_type,
            endpoint_info,
            existing_cursor_state,
            extended,
            with_votes,
            include_failed_transactions,
            initial_block_type,
            failed_transactions_block_type,
            initial_bytes_encoding,
            initial_bytes_encoding_label,
            solana_chain,
            antelope_chain,
            tron_style_evm_profile,
        })
    }

    pub(super) fn configure_metrics(
        &self,
        args: &BuildArgs,
        metrics_registry: &mut prometheus_client::registry::Registry,
        pipeline_metrics: &metrics::PipelineMetrics,
    ) {
        let Self {
            config,
            existing_cursor_state,
            initial_bytes_encoding_label,
            extended,
            with_votes,
            endpoint_info,
            ..
        } = self;
        pipeline_metrics
            .set_readiness_timeout(Duration::from_secs(args.common.metrics_stale_after_secs));
        if let Some(cursor) = existing_cursor_state.as_ref() {
            pipeline_metrics
                .cursor_last_block_num
                .set(i64::try_from(cursor.last_block_num).unwrap_or(i64::MAX));
        }

        // Register the info metric with endpoint metadata labels.
        {
            let mut labels = vec![
                ("endpoint".to_string(), config.endpoint.clone()),
                ("partition".to_string(), config.partition.to_string()),
                ("compression".to_string(), config.compression.to_string()),
                (
                    "bytes_encoding".to_string(),
                    initial_bytes_encoding_label.clone(),
                ),
                ("extended".to_string(), extended.to_string()),
                ("with_votes".to_string(), with_votes.to_string()),
                (
                    "final_blocks_only".to_string(),
                    config.final_blocks_only.to_string(),
                ),
                ("version".to_string(), env!("CARGO_PKG_VERSION").to_string()),
                (
                    "block_start".to_string(),
                    config
                        .start_block
                        .map_or("N/A".to_string(), |n| n.to_string()),
                ),
                (
                    "block_end".to_string(),
                    config
                        .stop_block
                        .map_or("N/A".to_string(), |n| n.to_string()),
                ),
                (
                    "block_restarted_at".to_string(),
                    existing_cursor_state
                        .as_ref()
                        .map_or("N/A".to_string(), |cs| cs.last_block_num.to_string()),
                ),
            ];
            if let Some(ref ei) = endpoint_info {
                labels.push(("chain_name".to_string(), ei.chain_name.clone()));
                if !ei.chain_name_aliases.is_empty() {
                    labels.push((
                        "chain_name_aliases".to_string(),
                        ei.chain_name_aliases.join(","),
                    ));
                }
                labels.push((
                    "first_streamable_block_num".to_string(),
                    ei.first_streamable_block_num.to_string(),
                ));
                if !ei.first_streamable_block_id.is_empty() {
                    labels.push((
                        "first_streamable_block_id".to_string(),
                        ei.first_streamable_block_id.clone(),
                    ));
                }
                if ei.block_id_encoding > 0 {
                    labels.push((
                        "block_id_encoding".to_string(),
                        block_id_encoding_label(ei.block_id_encoding).to_string(),
                    ));
                }
                if !ei.block_features.is_empty() {
                    labels.push(("block_features".to_string(), ei.block_features.join(",")));
                }
            }
            metrics::register_info_metric(metrics_registry, labels);
        }
    }
}
