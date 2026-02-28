use super::proto::antelope;
use super::schema;
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::EncodeBytes;
use firehose_parquet::traits::{
    est_bin, est_i32, est_i64, est_opt_str, est_str, est_u32, est_u64,
    BlockIdentity, BlockMapper, CanonicalBuilder,
};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;

fn append_fork_step(builder: &mut Option<StringBuilder>, fork_step: Option<&str>) {
    if let Some(ref mut b) = builder {
        b.append_value(fork_step.unwrap_or("UNKNOWN"));
    }
}

fn finish_fork_step(builder: &mut Option<StringBuilder>, columns: &mut Vec<Arc<dyn Array>>) {
    if let Some(ref mut b) = builder {
        columns.push(Arc::new(b.finish()) as Arc<dyn Array>);
    }
}

fn format_authorization(auth: &[antelope::PermissionLevel]) -> String {
    let parts: Vec<String> = auth.iter().map(|a| format!("{}@{}", a.actor, a.permission)).collect();
    parts.join(",")
}

pub struct AntelopeBlockMapper {
    extended: bool,
    blocks: BlocksBuilder,
    transactions: TransactionsBuilder,
    actions: ActionsBuilder,
    db_ops: Option<DbOpsBuilder>,
    blocks_schema: Schema,
    transactions_schema: Schema,
    actions_schema: Schema,
    db_ops_schema: Schema,
}

impl AntelopeBlockMapper {
    pub fn new(extended: bool, include_fork_step: bool, encoding: EncodeBytes) -> Self {
        let enc = &encoding;
        Self {
            extended,
            blocks: BlocksBuilder::new(include_fork_step),
            transactions: TransactionsBuilder::new(include_fork_step),
            actions: ActionsBuilder::new(include_fork_step),
            db_ops: if extended { Some(DbOpsBuilder::new(include_fork_step)) } else { None },
            blocks_schema: schema::blocks_schema(include_fork_step, enc),
            transactions_schema: schema::transactions_schema(include_fork_step, enc),
            actions_schema: schema::actions_schema(include_fork_step, enc),
            db_ops_schema: schema::db_ops_schema(include_fork_step, enc),
        }
    }

    fn map_antelope_block(&mut self, block: &antelope::Block, identity: &BlockIdentity, fork_step: Option<&str>) {
        let header = block.header.as_ref();

        // blocks table
        self.blocks.canonical.append(identity);
        self.blocks.number.append_value(block.number);
        self.blocks.hash.append_value(&block.id);
        self.blocks.producer.append_value(header.map(|h| h.producer.as_str()).unwrap_or(""));
        self.blocks.confirmed.append_value(header.map(|h| h.confirmed).unwrap_or(0));
        self.blocks.schedule_version.append_value(header.map(|h| h.schedule_version).unwrap_or(0));
        append_fork_step(&mut self.blocks.fork_step, fork_step);

        // Use unfiltered_transaction_traces (or filtered if filtering was applied)
        let traces = if block.filtering_applied {
            &block.filtered_transaction_traces
        } else {
            &block.unfiltered_transaction_traces
        };

        for trace in traces {
            self.map_transaction(trace, identity, fork_step);
        }
    }

    fn map_transaction(&mut self, trace: &antelope::TransactionTrace, identity: &BlockIdentity, fork_step: Option<&str>) {
        let receipt = trace.receipt.as_ref();

        // transactions table
        self.transactions.canonical.append(identity);
        self.transactions.tx_hash.append_value(&trace.id);
        self.transactions.index.append_value(trace.index);
        self.transactions.status.append_value(receipt.map(|r| r.status).unwrap_or(0));
        self.transactions.cpu_usage_us.append_value(receipt.map(|r| r.cpu_usage_micro_seconds).unwrap_or(0));
        self.transactions.net_usage.append_value(trace.net_usage);
        self.transactions.elapsed.append_value(trace.elapsed);
        append_fork_step(&mut self.transactions.fork_step, fork_step);

        // actions table
        for action_trace in &trace.action_traces {
            self.map_action(action_trace, &trace.id, identity, fork_step);
        }

        // db_ops table (extended only)
        for db_op in &trace.db_ops {
            if let Some(ref mut db_ops) = self.db_ops {
                Self::map_db_op(db_ops, db_op, &trace.id, identity, fork_step);
            }
        }
    }

    fn map_action(&mut self, action_trace: &antelope::ActionTrace, tx_hash: &str, identity: &BlockIdentity, fork_step: Option<&str>) {
        let action = action_trace.action.as_ref();

        self.actions.canonical.append(identity);
        self.actions.tx_hash.append_value(tx_hash);
        self.actions.action_ordinal.append_value(action_trace.action_ordinal);
        self.actions.receiver.append_value(&action_trace.receiver);
        self.actions.account.append_value(action.map(|a| a.account.as_str()).unwrap_or(""));
        self.actions.name.append_value(action.map(|a| a.name.as_str()).unwrap_or(""));
        let auth_str = action.map(|a| format_authorization(&a.authorization)).unwrap_or_default();
        self.actions.authorization.append_value(&auth_str);
        self.actions.data.append_value(action.map(|a| a.raw_data.as_slice()).unwrap_or(&[]));
        self.actions.console.append_value(&action_trace.console);
        append_fork_step(&mut self.actions.fork_step, fork_step);
    }

    fn map_db_op(db_ops: &mut DbOpsBuilder, db_op: &antelope::DbOp, tx_hash: &str, identity: &BlockIdentity, fork_step: Option<&str>) {
        db_ops.canonical.append(identity);
        db_ops.tx_hash.append_value(tx_hash);
        db_ops.action_index.append_value(db_op.action_index);
        db_ops.operation.append_value(db_op.operation);
        db_ops.code.append_value(&db_op.code);
        db_ops.scope.append_value(&db_op.scope);
        db_ops.table_name.append_value(&db_op.table_name);
        db_ops.primary_key.append_value(&db_op.primary_key);
        if db_op.old_data.is_empty() {
            db_ops.old_data.append_null();
        } else {
            db_ops.old_data.append_value(&db_op.old_data);
        }
        if db_op.new_data.is_empty() {
            db_ops.new_data.append_null();
        } else {
            db_ops.new_data.append_value(&db_op.new_data);
        }
        append_fork_step(&mut db_ops.fork_step, fork_step);
    }
}

impl BlockMapper for AntelopeBlockMapper {
    fn map_block(&mut self, block_bytes: &[u8], identity: &BlockIdentity, fork_step: Option<&str>) -> anyhow::Result<()> {
        let block = antelope::Block::decode(block_bytes)?;
        self.map_antelope_block(&block, identity, fork_step);
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<HashMap<String, RecordBatch>> {
        let mut result = HashMap::new();
        result.insert("blocks".to_string(), self.blocks.finish(&self.blocks_schema)?);
        result.insert("transactions".to_string(), self.transactions.finish(&self.transactions_schema)?);
        result.insert("actions".to_string(), self.actions.finish(&self.actions_schema)?);
        if let Some(ref mut db_ops) = self.db_ops {
            result.insert("db_ops".to_string(), db_ops.finish(&self.db_ops_schema)?);
        }
        Ok(result)
    }

    fn max_table_rows(&self) -> usize {
        let mut max = self.blocks.canonical.len()
            .max(self.transactions.canonical.len())
            .max(self.actions.canonical.len());
        if let Some(ref db_ops) = self.db_ops { max = max.max(db_ops.canonical.len()); }
        max
    }

    fn estimated_bytes(&mut self) -> usize {
        let blocks = self.blocks.canonical.estimated_bytes()
            + est_u32(&self.blocks.number)
            + est_str(&self.blocks.hash)
            + est_str(&self.blocks.producer)
            + est_u32(&self.blocks.confirmed)
            + est_u32(&self.blocks.schedule_version)
            + est_opt_str(&self.blocks.fork_step);
        let transactions = self.transactions.canonical.estimated_bytes()
            + est_str(&self.transactions.tx_hash)
            + est_u64(&self.transactions.index)
            + est_i32(&self.transactions.status)
            + est_u32(&self.transactions.cpu_usage_us)
            + est_u64(&self.transactions.net_usage)
            + est_i64(&self.transactions.elapsed)
            + est_opt_str(&self.transactions.fork_step);
        let actions = self.actions.canonical.estimated_bytes()
            + est_str(&self.actions.tx_hash)
            + est_u32(&self.actions.action_ordinal)
            + est_str(&self.actions.receiver)
            + est_str(&self.actions.account)
            + est_str(&self.actions.name)
            + est_str(&self.actions.authorization)
            + est_bin(&self.actions.data)
            + est_str(&self.actions.console)
            + est_opt_str(&self.actions.fork_step);
        let mut tables = vec![blocks, transactions, actions];
        if let Some(ref db_ops) = self.db_ops {
            tables.push(
                db_ops.canonical.estimated_bytes()
                    + est_str(&db_ops.tx_hash)
                    + est_u32(&db_ops.action_index)
                    + est_i32(&db_ops.operation)
                    + est_str(&db_ops.code)
                    + est_str(&db_ops.scope)
                    + est_str(&db_ops.table_name)
                    + est_str(&db_ops.primary_key)
                    + est_bin(&db_ops.old_data)
                    + est_bin(&db_ops.new_data)
                    + est_opt_str(&db_ops.fork_step),
            );
        }
        tables.into_iter().max().unwrap_or(0)
    }

    fn table_names(&self) -> Vec<&str> {
        if self.extended {
            schema::EXTENDED_TABLE_NAMES.to_vec()
        } else {
            schema::BASE_TABLE_NAMES.to_vec()
        }
    }
}

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

struct BlocksBuilder {
    canonical: CanonicalBuilder,
    number: UInt32Builder,
    hash: StringBuilder,
    producer: StringBuilder,
    confirmed: UInt32Builder,
    schedule_version: UInt32Builder,
    fork_step: Option<StringBuilder>,
}

impl BlocksBuilder {
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            number: UInt32Builder::new(),
            hash: StringBuilder::new(),
            producer: StringBuilder::new(),
            confirmed: UInt32Builder::new(),
            schedule_version: UInt32Builder::new(),
            fork_step: if include_fork_step { Some(StringBuilder::new()) } else { None },
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.number.finish()) as Arc<dyn Array>,
            Arc::new(self.hash.finish()) as Arc<dyn Array>,
            Arc::new(self.producer.finish()) as Arc<dyn Array>,
            Arc::new(self.confirmed.finish()) as Arc<dyn Array>,
            Arc::new(self.schedule_version.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct TransactionsBuilder {
    canonical: CanonicalBuilder,
    tx_hash: StringBuilder,
    index: UInt64Builder,
    status: Int32Builder,
    cpu_usage_us: UInt32Builder,
    net_usage: UInt64Builder,
    elapsed: Int64Builder,
    fork_step: Option<StringBuilder>,
}

impl TransactionsBuilder {
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            tx_hash: StringBuilder::new(),
            index: UInt64Builder::new(),
            status: Int32Builder::new(),
            cpu_usage_us: UInt32Builder::new(),
            net_usage: UInt64Builder::new(),
            elapsed: Int64Builder::new(),
            fork_step: if include_fork_step { Some(StringBuilder::new()) } else { None },
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.tx_hash.finish()) as Arc<dyn Array>,
            Arc::new(self.index.finish()) as Arc<dyn Array>,
            Arc::new(self.status.finish()) as Arc<dyn Array>,
            Arc::new(self.cpu_usage_us.finish()) as Arc<dyn Array>,
            Arc::new(self.net_usage.finish()) as Arc<dyn Array>,
            Arc::new(self.elapsed.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct ActionsBuilder {
    canonical: CanonicalBuilder,
    tx_hash: StringBuilder,
    action_ordinal: UInt32Builder,
    receiver: StringBuilder,
    account: StringBuilder,
    name: StringBuilder,
    authorization: StringBuilder,
    data: BinaryBuilder,
    console: StringBuilder,
    fork_step: Option<StringBuilder>,
}

impl ActionsBuilder {
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            tx_hash: StringBuilder::new(),
            action_ordinal: UInt32Builder::new(),
            receiver: StringBuilder::new(),
            account: StringBuilder::new(),
            name: StringBuilder::new(),
            authorization: StringBuilder::new(),
            data: BinaryBuilder::new(),
            console: StringBuilder::new(),
            fork_step: if include_fork_step { Some(StringBuilder::new()) } else { None },
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.tx_hash.finish()) as Arc<dyn Array>,
            Arc::new(self.action_ordinal.finish()) as Arc<dyn Array>,
            Arc::new(self.receiver.finish()) as Arc<dyn Array>,
            Arc::new(self.account.finish()) as Arc<dyn Array>,
            Arc::new(self.name.finish()) as Arc<dyn Array>,
            Arc::new(self.authorization.finish()) as Arc<dyn Array>,
            Arc::new(self.data.finish()) as Arc<dyn Array>,
            Arc::new(self.console.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct DbOpsBuilder {
    canonical: CanonicalBuilder,
    tx_hash: StringBuilder,
    action_index: UInt32Builder,
    operation: Int32Builder,
    code: StringBuilder,
    scope: StringBuilder,
    table_name: StringBuilder,
    primary_key: StringBuilder,
    old_data: BinaryBuilder,
    new_data: BinaryBuilder,
    fork_step: Option<StringBuilder>,
}

impl DbOpsBuilder {
    fn new(include_fork_step: bool) -> Self {
        Self {
            canonical: CanonicalBuilder::new(),
            tx_hash: StringBuilder::new(),
            action_index: UInt32Builder::new(),
            operation: Int32Builder::new(),
            code: StringBuilder::new(),
            scope: StringBuilder::new(),
            table_name: StringBuilder::new(),
            primary_key: StringBuilder::new(),
            old_data: BinaryBuilder::new(),
            new_data: BinaryBuilder::new(),
            fork_step: if include_fork_step { Some(StringBuilder::new()) } else { None },
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.tx_hash.finish()) as Arc<dyn Array>,
            Arc::new(self.action_index.finish()) as Arc<dyn Array>,
            Arc::new(self.operation.finish()) as Arc<dyn Array>,
            Arc::new(self.code.finish()) as Arc<dyn Array>,
            Arc::new(self.scope.finish()) as Arc<dyn Array>,
            Arc::new(self.table_name.finish()) as Arc<dyn Array>,
            Arc::new(self.primary_key.finish()) as Arc<dyn Array>,
            Arc::new(self.old_data.finish()) as Arc<dyn Array>,
            Arc::new(self.new_data.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_block(number: u32) -> antelope::Block {
        antelope::Block {
            id: format!("block_hash_{number}"),
            number,
            version: 1,
            header: Some(antelope::BlockHeader {
                timestamp: Some(prost_types::Timestamp {
                    seconds: 1_700_000_000 + number as i64,
                    nanos: 0,
                }),
                producer: "eosproducer1".to_string(),
                confirmed: 0,
                previous: format!("block_hash_{}", number.saturating_sub(1)),
                transaction_mroot: vec![],
                action_mroot: vec![],
                schedule_version: 42,
                header_extensions: vec![],
                new_producers_v1: None,
                decoded_header_extensions: vec![],
            }),
            producer_signature: String::new(),
            block_extensions: vec![],
            dpos_proposed_irreversible_blocknum: 0,
            dpos_irreversible_blocknum: 0,
            blockroot_merkle: None,
            producer_to_last_produced: vec![],
            producer_to_last_implied_irb: vec![],
            confirm_count: vec![],
            pending_schedule: None,
            activated_protocol_features: None,
            validated: true,
            action_mroot_savanna: vec![],
            finality_lib: 0,
            finality_data: None,
            proposer_policy: None,
            finalizer_policy: None,
            rlimit_ops: vec![],
            unfiltered_transactions: vec![],
            filtered_transactions: vec![],
            unfiltered_transaction_count: 1,
            filtered_transaction_count: 0,
            unfiltered_implicit_transaction_ops: vec![],
            filtered_implicit_transaction_ops: vec![],
            unfiltered_transaction_traces: vec![antelope::TransactionTrace {
                id: "trx_hash_1".to_string(),
                block_num: number as u64,
                index: 0,
                block_time: None,
                producer_block_id: String::new(),
                receipt: Some(antelope::TransactionReceiptHeader {
                    status: 1, // EXECUTED
                    cpu_usage_micro_seconds: 500,
                    net_usage_words: 12,
                }),
                elapsed: 1234,
                net_usage: 96,
                scheduled: false,
                action_traces: vec![
                    antelope::ActionTrace {
                        receiver: "eosio.token".to_string(),
                        receipt: None,
                        action: Some(antelope::Action {
                            account: "eosio.token".to_string(),
                            name: "transfer".to_string(),
                            authorization: vec![antelope::PermissionLevel {
                                actor: "alice".to_string(),
                                permission: "active".to_string(),
                            }],
                            json_data: r#"{"from":"alice","to":"bob","quantity":"1.0000 EOS","memo":"test"}"#.to_string(),
                            raw_data: vec![1, 2, 3, 4],
                        }),
                        context_free: false,
                        elapsed: 100,
                        console: "".to_string(),
                        transaction_id: "trx_hash_1".to_string(),
                        block_num: number as u64,
                        producer_block_id: String::new(),
                        block_time: None,
                        account_ram_deltas: vec![],
                        raw_return_value: vec![],
                        json_return_value: String::new(),
                        exception: None,
                        error_code: 0,
                        action_ordinal: 1,
                        creator_action_ordinal: 0,
                        closest_unnotified_ancestor_action_ordinal: 0,
                        execution_index: 0,
                        filtering_matched: false,
                        filtering_matched_system_action_filter: false,
                    },
                    antelope::ActionTrace {
                        receiver: "bob".to_string(),
                        receipt: None,
                        action: Some(antelope::Action {
                            account: "eosio.token".to_string(),
                            name: "transfer".to_string(),
                            authorization: vec![antelope::PermissionLevel {
                                actor: "alice".to_string(),
                                permission: "active".to_string(),
                            }],
                            json_data: String::new(),
                            raw_data: vec![1, 2, 3, 4],
                        }),
                        context_free: false,
                        elapsed: 50,
                        console: "notify received".to_string(),
                        transaction_id: "trx_hash_1".to_string(),
                        block_num: number as u64,
                        producer_block_id: String::new(),
                        block_time: None,
                        account_ram_deltas: vec![],
                        raw_return_value: vec![],
                        json_return_value: String::new(),
                        exception: None,
                        error_code: 0,
                        action_ordinal: 2,
                        creator_action_ordinal: 1,
                        closest_unnotified_ancestor_action_ordinal: 1,
                        execution_index: 1,
                        filtering_matched: false,
                        filtering_matched_system_action_filter: false,
                    },
                ],
                failed_dtrx_trace: None,
                exception: None,
                error_code: 0,
                db_ops: vec![antelope::DbOp {
                    operation: 1, // INSERT
                    action_index: 0,
                    code: "eosio.token".to_string(),
                    scope: "alice".to_string(),
                    table_name: "accounts".to_string(),
                    primary_key: "EOS".to_string(),
                    old_payer: String::new(),
                    new_payer: "alice".to_string(),
                    old_data: vec![],
                    new_data: vec![10, 20, 30],
                    old_data_json: String::new(),
                    new_data_json: String::new(),
                }],
                dtrx_ops: vec![],
                feature_ops: vec![],
                perm_ops: vec![],
                ram_ops: vec![],
                ram_correction_ops: vec![],
                rlimit_ops: vec![],
                table_ops: vec![],
                creation_tree: vec![],
            }],
            filtered_transaction_traces: vec![],
            unfiltered_transaction_trace_count: 1,
            filtered_transaction_trace_count: 0,
            unfiltered_executed_input_action_count: 1,
            filtered_executed_input_action_count: 0,
            unfiltered_executed_total_action_count: 2,
            filtered_executed_total_action_count: 0,
            block_signing_key: String::new(),
            active_schedule_v1: None,
            valid_block_signing_authority_v2: None,
            active_schedule_v2: None,
            filtering_applied: false,
            filtering_include_filter_expr: String::new(),
            filtering_exclude_filter_expr: String::new(),
            filtering_system_actions_include_filter_expr: String::new(),
        }
    }

    #[test]
    fn test_map_and_flush_single_block() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = AntelopeBlockMapper::new(true, false, EncodeBytes::Hex);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();

        assert_eq!(mapper.max_table_rows(), 2); // 2 actions

        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 1);
        assert_eq!(batches["actions"].num_rows(), 2);
        assert_eq!(batches["db_ops"].num_rows(), 1);
    }

    #[test]
    fn test_flush_resets_builders() {
        let block = make_test_block(1);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = AntelopeBlockMapper::new(true, false, EncodeBytes::Hex);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();
        let _ = mapper.flush().unwrap();
        assert_eq!(mapper.max_table_rows(), 0);
    }

    #[test]
    fn test_empty_block() {
        let block = antelope::Block {
            id: "genesis".to_string(),
            number: 0,
            header: Some(antelope::BlockHeader {
                producer: "eosio".to_string(),
                confirmed: 0,
                schedule_version: 0,
                ..Default::default()
            }),
            ..Default::default()
        };
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = AntelopeBlockMapper::new(true, false, EncodeBytes::Hex);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();
        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 0);
        assert_eq!(batches["actions"].num_rows(), 0);
        assert_eq!(batches["db_ops"].num_rows(), 0);
    }

    #[test]
    fn test_fork_step_column_included() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = AntelopeBlockMapper::new(true, true, EncodeBytes::Hex);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), Some("NEW")).unwrap();

        let batches = mapper.flush().unwrap();
        let blocks_batch = &batches["blocks"];
        let last_col = blocks_batch.num_columns() - 1;
        assert_eq!(blocks_batch.schema().field(last_col).name(), "fork_step");
        let fork_col = blocks_batch.column(last_col).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(fork_col.value(0), "NEW");
    }

    #[test]
    fn test_table_names_base() {
        let mapper = AntelopeBlockMapper::new(false, false, EncodeBytes::Hex);
        let names = mapper.table_names();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"blocks"));
        assert!(names.contains(&"transactions"));
        assert!(names.contains(&"actions"));
        assert!(!names.contains(&"db_ops"));
    }

    #[test]
    fn test_table_names_extended() {
        let mapper = AntelopeBlockMapper::new(true, false, EncodeBytes::Hex);
        let names = mapper.table_names();
        assert_eq!(names.len(), 4);
        assert!(names.contains(&"blocks"));
        assert!(names.contains(&"transactions"));
        assert!(names.contains(&"actions"));
        assert!(names.contains(&"db_ops"));
    }

    #[test]
    fn test_base_excludes_db_ops() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = AntelopeBlockMapper::new(false, false, EncodeBytes::Hex);
        mapper.map_block(&block_bytes, &BlockIdentity::default(), None).unwrap();

        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 1);
        assert_eq!(batches["actions"].num_rows(), 2);
        assert!(!batches.contains_key("db_ops"));
    }
}
