use super::proto::antelope;
use super::schema;
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::{BytesColumn, EncodeBytes};
use firehose_parquet::traits::{
    est_bool, est_i64, est_opt_str, est_str, est_ts_ms, est_u32, est_u64, timestamp_millis,
    BlockIdentity, BlockMapper, CanonicalBuilder,
};
use prost::Message;
use serde_json::{json, Value};
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
    let parts: Vec<String> = auth
        .iter()
        .map(|a| format!("{}@{}", a.actor, a.permission))
        .collect();
    parts.join(",")
}

fn enum_text(name: &str, prefix: &str) -> String {
    name.strip_prefix(prefix).unwrap_or(name).to_string()
}

fn transaction_status_text(value: i32) -> String {
    antelope::TransactionStatus::try_from(value)
        .map(|status| enum_text(status.as_str_name(), "TRANSACTIONSTATUS_"))
        .unwrap_or_else(|_| "UNKNOWN".to_string())
}

fn db_op_operation_text(value: i32) -> String {
    antelope::db_op::Operation::try_from(value)
        .map(|operation| enum_text(operation.as_str_name(), "OPERATION_"))
        .unwrap_or_else(|_| "UNKNOWN".to_string())
}

fn optional_string_value(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

fn append_optional_string(builder: &mut StringBuilder, value: Option<&str>) {
    if let Some(value) = value {
        builder.append_value(value);
    } else {
        builder.append_null();
    }
}

fn append_optional_timestamp(
    builder: &mut TimestampMillisecondBuilder,
    timestamp: Option<&prost_types::Timestamp>,
) {
    if let Some(timestamp) = timestamp {
        builder.append_value(timestamp_millis(timestamp.seconds, timestamp.nanos));
    } else {
        builder.append_null();
    }
}

fn serialize_timestamp(timestamp: &prost_types::Timestamp) -> Value {
    json!({
        "seconds": timestamp.seconds,
        "nanos": timestamp.nanos,
    })
}

fn serialize_log_context(context: &antelope::exception::LogContext) -> Value {
    let mut result = json!({
        "level": context.level,
        "file": context.file,
        "line": context.line,
        "method": context.method,
        "hostname": context.hostname,
        "thread_name": context.thread_name,
    });

    if let Some(map) = result.as_object_mut() {
        if let Some(timestamp) = context.timestamp.as_ref() {
            map.insert("timestamp".to_string(), serialize_timestamp(timestamp));
        }
        if let Some(parent) = context.context.as_ref() {
            map.insert("context".to_string(), serialize_log_context(parent));
        }
    }

    result
}

fn serialize_exception(exception: &antelope::Exception) -> String {
    let stack = exception
        .stack
        .iter()
        .map(|message| {
            let mut result = json!({
                "format": message.format,
                "data": String::from_utf8_lossy(&message.data).into_owned(),
            });
            if let Some(map) = result.as_object_mut() {
                if let Some(context) = message.context.as_ref() {
                    map.insert("context".to_string(), serialize_log_context(context));
                }
            }
            result
        })
        .collect::<Vec<_>>();

    serde_json::to_string(&json!({
        "code": exception.code,
        "name": exception.name,
        "message": exception.message,
        "stack": stack,
    }))
    .expect("exception JSON serialization should be infallible")
}

fn serialize_auth_sequence(auth_sequence: &[antelope::AuthSequence]) -> Option<String> {
    if auth_sequence.is_empty() {
        return None;
    }

    Some(
        serde_json::to_string(
            &auth_sequence
                .iter()
                .map(|entry| {
                    json!({
                        "account_name": entry.account_name,
                        "sequence": entry.sequence,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .expect("auth sequence JSON serialization should be infallible"),
    )
}

fn antelope_canonical_identity(block: &antelope::Block, identity: &BlockIdentity) -> BlockIdentity {
    let mut canonical = identity.clone();
    canonical.block_id = block.id.clone();
    canonical.parent_id = block
        .header
        .as_ref()
        .map(|h| h.previous.clone())
        .unwrap_or_default();
    canonical
}

pub struct AntelopeBlockMapper {
    include_failed_transactions: bool,
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
    pub fn new(
        include_fork_step: bool,
        encoding: EncodeBytes,
        include_failed_transactions: bool,
    ) -> Self {
        let enc = &encoding;
        Self {
            include_failed_transactions,
            blocks: BlocksBuilder::new(include_fork_step, enc),
            transactions: TransactionsBuilder::new(include_fork_step, enc),
            actions: ActionsBuilder::new(include_fork_step, enc),
            db_ops: Some(DbOpsBuilder::new(include_fork_step, enc)),
            blocks_schema: schema::blocks_schema(include_fork_step, enc),
            transactions_schema: schema::transactions_schema(include_fork_step, enc),
            actions_schema: schema::actions_schema(include_fork_step, enc),
            db_ops_schema: schema::db_ops_schema(include_fork_step, enc),
        }
    }

    fn map_antelope_block(
        &mut self,
        block: &antelope::Block,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        let header = block.header.as_ref();
        let canonical_identity = antelope_canonical_identity(block, identity);

        // blocks table
        self.blocks.canonical.append(&canonical_identity);
        self.blocks.number.append_value(block.number);
        self.blocks.hash.append_value(&block.id);
        self.blocks
            .producer
            .append_value(header.map(|h| h.producer.as_str()).unwrap_or(""));
        self.blocks
            .confirmed
            .append_value(header.map(|h| h.confirmed).unwrap_or(0));
        self.blocks
            .schedule_version
            .append_value(header.map(|h| h.schedule_version).unwrap_or(0));
        append_fork_step(&mut self.blocks.fork_step, fork_step);

        // Use unfiltered_transaction_traces (or filtered if filtering was applied)
        let traces = if block.filtering_applied {
            &block.filtered_transaction_traces
        } else {
            &block.unfiltered_transaction_traces
        };

        for trace in traces {
            // Skip non-executed transactions (status != EXECUTED=1) unless flag is set.
            // Transactions without a receipt default to NONE=0 and are also skipped.
            if !self.include_failed_transactions {
                let status = trace.receipt.as_ref().map(|r| r.status).unwrap_or(0);
                if status != 1 {
                    continue;
                }
            }
            self.map_transaction(trace, &canonical_identity, fork_step);
        }
    }

    fn map_transaction(
        &mut self,
        trace: &antelope::TransactionTrace,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        let receipt = trace.receipt.as_ref();

        // transactions table
        self.transactions.canonical.append(identity);
        self.transactions.tx_hash.append_value(&trace.id);
        self.transactions.index.append_value(trace.index);
        self.transactions
            .status
            .append_value(transaction_status_text(
                receipt.map(|r| r.status).unwrap_or(0),
            ));
        self.transactions
            .cpu_usage_us
            .append_value(receipt.map(|r| r.cpu_usage_micro_seconds).unwrap_or(0));
        self.transactions.net_usage.append_value(trace.net_usage);
        self.transactions.elapsed.append_value(trace.elapsed);
        append_fork_step(&mut self.transactions.fork_step, fork_step);

        // actions table
        for action_trace in &trace.action_traces {
            self.map_action(action_trace, &trace.id, identity, fork_step);
        }

        // db_ops table (always included for Antelope output)
        for db_op in &trace.db_ops {
            if let Some(ref mut db_ops) = self.db_ops {
                Self::map_db_op(db_ops, db_op, identity, fork_step);
            }
        }
    }

    fn map_action(
        &mut self,
        action_trace: &antelope::ActionTrace,
        tx_hash: &str,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        let action = action_trace.action.as_ref();

        self.actions.canonical.append(identity);
        self.actions.tx_hash.append_value(tx_hash);
        self.actions
            .action_ordinal
            .append_value(action_trace.action_ordinal);
        self.actions
            .creator_action_ordinal
            .append_value(action_trace.creator_action_ordinal);
        self.actions
            .closest_unnotified_ancestor_action_ordinal
            .append_value(action_trace.closest_unnotified_ancestor_action_ordinal);
        self.actions
            .execution_index
            .append_value(action_trace.execution_index);
        self.actions.receiver.append_value(&action_trace.receiver);
        self.actions
            .account
            .append_value(action.map(|a| a.account.as_str()).unwrap_or(""));
        self.actions
            .name
            .append_value(action.map(|a| a.name.as_str()).unwrap_or(""));
        let auth_str = action
            .map(|a| format_authorization(&a.authorization))
            .unwrap_or_default();
        append_optional_string(
            &mut self.actions.authorization,
            (!auth_str.is_empty()).then_some(auth_str.as_str()),
        );
        append_optional_string(
            &mut self.actions.json_data,
            action.and_then(|a| optional_string_value(&a.json_data)),
        );
        if let Some(raw_data) = action
            .map(|a| a.raw_data.as_slice())
            .filter(|raw_data| !raw_data.is_empty())
        {
            self.actions.raw_data.append_value(raw_data);
        } else {
            self.actions.raw_data.append_null();
        }
        self.actions
            .context_free
            .append_value(action_trace.context_free);
        self.actions.elapsed.append_value(action_trace.elapsed);
        append_optional_string(
            &mut self.actions.console,
            optional_string_value(&action_trace.console),
        );
        self.actions
            .transaction_id
            .append_value(&action_trace.transaction_id);
        self.actions
            .trace_block_num
            .append_value(action_trace.block_num);
        self.actions
            .producer_block_id
            .append_value(&action_trace.producer_block_id);
        append_optional_timestamp(
            &mut self.actions.block_time,
            action_trace.block_time.as_ref(),
        );
        if action_trace.raw_return_value.is_empty() {
            self.actions.raw_return_value.append_null();
        } else {
            self.actions
                .raw_return_value
                .append_value(&action_trace.raw_return_value);
        }
        append_optional_string(
            &mut self.actions.json_return_value,
            optional_string_value(&action_trace.json_return_value),
        );
        if let Some(exception) = action_trace.exception.as_ref() {
            self.actions
                .exception
                .append_value(serialize_exception(exception));
        } else {
            self.actions.exception.append_null();
        }
        self.actions
            .error_code
            .append_value(action_trace.error_code);
        let receipt = action_trace.receipt.as_ref();
        self.actions
            .receipt_receiver
            .append_value(receipt.map(|r| r.receiver.as_str()).unwrap_or(""));
        self.actions
            .receipt_digest
            .append_value(receipt.map(|r| r.digest.as_str()).unwrap_or(""));
        self.actions
            .receipt_global_sequence
            .append_value(receipt.map(|r| r.global_sequence).unwrap_or(0));
        if let Some(auth_sequence) = receipt.and_then(|r| serialize_auth_sequence(&r.auth_sequence))
        {
            self.actions
                .receipt_auth_sequence
                .append_value(auth_sequence);
        } else {
            self.actions.receipt_auth_sequence.append_null();
        }
        self.actions
            .receipt_recv_sequence
            .append_value(receipt.map(|r| r.recv_sequence).unwrap_or(0));
        self.actions
            .receipt_code_sequence
            .append_value(receipt.map(|r| r.code_sequence).unwrap_or(0));
        self.actions
            .receipt_abi_sequence
            .append_value(receipt.map(|r| r.abi_sequence).unwrap_or(0));
        append_fork_step(&mut self.actions.fork_step, fork_step);
    }

    fn map_db_op(
        db_ops: &mut DbOpsBuilder,
        db_op: &antelope::DbOp,
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) {
        db_ops.canonical.append(identity);
        db_ops.action_index.append_value(db_op.action_index);
        db_ops
            .operation
            .append_value(db_op_operation_text(db_op.operation));
        db_ops.code.append_value(&db_op.code);
        db_ops.scope.append_value(&db_op.scope);
        db_ops.table_name.append_value(&db_op.table_name);
        db_ops.primary_key.append_value(&db_op.primary_key);
        db_ops.old_payer.append_value(&db_op.old_payer);
        db_ops.new_payer.append_value(&db_op.new_payer);
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
        append_optional_string(
            &mut db_ops.old_data_json,
            optional_string_value(&db_op.old_data_json),
        );
        append_optional_string(
            &mut db_ops.new_data_json,
            optional_string_value(&db_op.new_data_json),
        );
        append_fork_step(&mut db_ops.fork_step, fork_step);
    }
}

impl BlockMapper for AntelopeBlockMapper {
    fn map_block(
        &mut self,
        block_bytes: &[u8],
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) -> anyhow::Result<u64> {
        let block = antelope::Block::decode(block_bytes)?;
        let tx_count = if block.filtering_applied {
            block.filtered_transaction_traces.len()
        } else {
            block.unfiltered_transaction_traces.len()
        } as u64;
        self.map_antelope_block(&block, identity, fork_step);
        Ok(tx_count)
    }

    fn flush(&mut self) -> anyhow::Result<HashMap<String, RecordBatch>> {
        let mut result = HashMap::new();
        result.insert(
            "blocks".to_string(),
            self.blocks.finish(&self.blocks_schema)?,
        );
        result.insert(
            "transactions".to_string(),
            self.transactions.finish(&self.transactions_schema)?,
        );
        result.insert(
            "actions".to_string(),
            self.actions.finish(&self.actions_schema)?,
        );
        if let Some(ref mut db_ops) = self.db_ops {
            result.insert("db_ops".to_string(), db_ops.finish(&self.db_ops_schema)?);
        }
        Ok(result)
    }

    fn max_table_rows(&self) -> usize {
        let mut max = self
            .blocks
            .canonical
            .len()
            .max(self.transactions.canonical.len())
            .max(self.actions.canonical.len());
        if let Some(ref db_ops) = self.db_ops {
            max = max.max(db_ops.canonical.len());
        }
        max
    }

    fn total_rows(&self) -> usize {
        let mut total = self.blocks.canonical.len()
            + self.transactions.canonical.len()
            + self.actions.canonical.len();
        if let Some(ref db_ops) = self.db_ops {
            total += db_ops.canonical.len();
        }
        total
    }

    fn largest_table(&mut self) -> (&str, usize) {
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
            + est_str(&self.transactions.status)
            + est_u32(&self.transactions.cpu_usage_us)
            + est_u64(&self.transactions.net_usage)
            + est_i64(&self.transactions.elapsed)
            + est_opt_str(&self.transactions.fork_step);
        let actions = self.actions.canonical.estimated_bytes()
            + est_str(&self.actions.tx_hash)
            + est_u32(&self.actions.action_ordinal)
            + est_u32(&self.actions.creator_action_ordinal)
            + est_u32(&self.actions.closest_unnotified_ancestor_action_ordinal)
            + est_u32(&self.actions.execution_index)
            + est_str(&self.actions.receiver)
            + est_str(&self.actions.account)
            + est_str(&self.actions.name)
            + est_str(&self.actions.authorization)
            + est_str(&self.actions.json_data)
            + self.actions.raw_data.estimated_bytes()
            + est_bool(&self.actions.context_free)
            + est_i64(&self.actions.elapsed)
            + est_str(&self.actions.console)
            + est_str(&self.actions.transaction_id)
            + est_u64(&self.actions.trace_block_num)
            + est_str(&self.actions.producer_block_id)
            + est_ts_ms(&self.actions.block_time)
            + self.actions.raw_return_value.estimated_bytes()
            + est_str(&self.actions.json_return_value)
            + est_str(&self.actions.exception)
            + est_u64(&self.actions.error_code)
            + est_str(&self.actions.receipt_receiver)
            + est_str(&self.actions.receipt_digest)
            + est_u64(&self.actions.receipt_global_sequence)
            + est_str(&self.actions.receipt_auth_sequence)
            + est_u64(&self.actions.receipt_recv_sequence)
            + est_u64(&self.actions.receipt_code_sequence)
            + est_u64(&self.actions.receipt_abi_sequence)
            + est_opt_str(&self.actions.fork_step);
        let mut tables: Vec<(&str, usize)> = vec![
            ("blocks", blocks),
            ("transactions", transactions),
            ("actions", actions),
        ];
        if let Some(ref db_ops) = self.db_ops {
            tables.push((
                "db_ops",
                db_ops.canonical.estimated_bytes()
                    + est_u32(&db_ops.action_index)
                    + est_str(&db_ops.operation)
                    + est_str(&db_ops.code)
                    + est_str(&db_ops.scope)
                    + est_str(&db_ops.table_name)
                    + est_str(&db_ops.primary_key)
                    + est_str(&db_ops.old_payer)
                    + est_str(&db_ops.new_payer)
                    + db_ops.old_data.estimated_bytes()
                    + db_ops.new_data.estimated_bytes()
                    + est_str(&db_ops.old_data_json)
                    + est_str(&db_ops.new_data_json)
                    + est_opt_str(&db_ops.fork_step),
            ));
        }
        tables
            .into_iter()
            .max_by_key(|&(_, s)| s)
            .unwrap_or(("blocks", 0))
    }

    fn table_names(&self) -> Vec<&str> {
        schema::TABLE_NAMES.to_vec()
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
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            number: UInt32Builder::new(),
            hash: StringBuilder::new(),
            producer: StringBuilder::new(),
            confirmed: UInt32Builder::new(),
            schedule_version: UInt32Builder::new(),
            fork_step: if include_fork_step {
                Some(StringBuilder::new())
            } else {
                None
            },
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
    status: StringBuilder,
    cpu_usage_us: UInt32Builder,
    net_usage: UInt64Builder,
    elapsed: Int64Builder,
    fork_step: Option<StringBuilder>,
}

impl TransactionsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            tx_hash: StringBuilder::new(),
            index: UInt64Builder::new(),
            status: StringBuilder::new(),
            cpu_usage_us: UInt32Builder::new(),
            net_usage: UInt64Builder::new(),
            elapsed: Int64Builder::new(),
            fork_step: if include_fork_step {
                Some(StringBuilder::new())
            } else {
                None
            },
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
    creator_action_ordinal: UInt32Builder,
    closest_unnotified_ancestor_action_ordinal: UInt32Builder,
    execution_index: UInt32Builder,
    receiver: StringBuilder,
    account: StringBuilder,
    name: StringBuilder,
    authorization: StringBuilder,
    json_data: StringBuilder,
    raw_data: BytesColumn,
    context_free: BooleanBuilder,
    elapsed: Int64Builder,
    console: StringBuilder,
    transaction_id: StringBuilder,
    trace_block_num: UInt64Builder,
    producer_block_id: StringBuilder,
    block_time: TimestampMillisecondBuilder,
    raw_return_value: BytesColumn,
    json_return_value: StringBuilder,
    exception: StringBuilder,
    error_code: UInt64Builder,
    receipt_receiver: StringBuilder,
    receipt_digest: StringBuilder,
    receipt_global_sequence: UInt64Builder,
    receipt_auth_sequence: StringBuilder,
    receipt_recv_sequence: UInt64Builder,
    receipt_code_sequence: UInt64Builder,
    receipt_abi_sequence: UInt64Builder,
    fork_step: Option<StringBuilder>,
}

impl ActionsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            tx_hash: StringBuilder::new(),
            action_ordinal: UInt32Builder::new(),
            creator_action_ordinal: UInt32Builder::new(),
            closest_unnotified_ancestor_action_ordinal: UInt32Builder::new(),
            execution_index: UInt32Builder::new(),
            receiver: StringBuilder::new(),
            account: StringBuilder::new(),
            name: StringBuilder::new(),
            authorization: StringBuilder::new(),
            json_data: StringBuilder::new(),
            raw_data: BytesColumn::new(encoding),
            context_free: BooleanBuilder::new(),
            elapsed: Int64Builder::new(),
            console: StringBuilder::new(),
            transaction_id: StringBuilder::new(),
            trace_block_num: UInt64Builder::new(),
            producer_block_id: StringBuilder::new(),
            block_time: TimestampMillisecondBuilder::new().with_timezone("UTC"),
            raw_return_value: BytesColumn::new(encoding),
            json_return_value: StringBuilder::new(),
            exception: StringBuilder::new(),
            error_code: UInt64Builder::new(),
            receipt_receiver: StringBuilder::new(),
            receipt_digest: StringBuilder::new(),
            receipt_global_sequence: UInt64Builder::new(),
            receipt_auth_sequence: StringBuilder::new(),
            receipt_recv_sequence: UInt64Builder::new(),
            receipt_code_sequence: UInt64Builder::new(),
            receipt_abi_sequence: UInt64Builder::new(),
            fork_step: if include_fork_step {
                Some(StringBuilder::new())
            } else {
                None
            },
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.tx_hash.finish()) as Arc<dyn Array>,
            Arc::new(self.action_ordinal.finish()) as Arc<dyn Array>,
            Arc::new(self.creator_action_ordinal.finish()) as Arc<dyn Array>,
            Arc::new(self.closest_unnotified_ancestor_action_ordinal.finish()) as Arc<dyn Array>,
            Arc::new(self.execution_index.finish()) as Arc<dyn Array>,
            Arc::new(self.receiver.finish()) as Arc<dyn Array>,
            Arc::new(self.account.finish()) as Arc<dyn Array>,
            Arc::new(self.name.finish()) as Arc<dyn Array>,
            Arc::new(self.authorization.finish()) as Arc<dyn Array>,
            Arc::new(self.json_data.finish()) as Arc<dyn Array>,
            self.raw_data.finish(),
            Arc::new(self.context_free.finish()) as Arc<dyn Array>,
            Arc::new(self.elapsed.finish()) as Arc<dyn Array>,
            Arc::new(self.console.finish()) as Arc<dyn Array>,
            Arc::new(self.transaction_id.finish()) as Arc<dyn Array>,
            Arc::new(self.trace_block_num.finish()) as Arc<dyn Array>,
            Arc::new(self.producer_block_id.finish()) as Arc<dyn Array>,
            Arc::new(self.block_time.finish()) as Arc<dyn Array>,
            self.raw_return_value.finish(),
            Arc::new(self.json_return_value.finish()) as Arc<dyn Array>,
            Arc::new(self.exception.finish()) as Arc<dyn Array>,
            Arc::new(self.error_code.finish()) as Arc<dyn Array>,
            Arc::new(self.receipt_receiver.finish()) as Arc<dyn Array>,
            Arc::new(self.receipt_digest.finish()) as Arc<dyn Array>,
            Arc::new(self.receipt_global_sequence.finish()) as Arc<dyn Array>,
            Arc::new(self.receipt_auth_sequence.finish()) as Arc<dyn Array>,
            Arc::new(self.receipt_recv_sequence.finish()) as Arc<dyn Array>,
            Arc::new(self.receipt_code_sequence.finish()) as Arc<dyn Array>,
            Arc::new(self.receipt_abi_sequence.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

struct DbOpsBuilder {
    canonical: CanonicalBuilder,
    action_index: UInt32Builder,
    operation: StringBuilder,
    code: StringBuilder,
    scope: StringBuilder,
    table_name: StringBuilder,
    primary_key: StringBuilder,
    old_payer: StringBuilder,
    new_payer: StringBuilder,
    old_data: BytesColumn,
    new_data: BytesColumn,
    old_data_json: StringBuilder,
    new_data_json: StringBuilder,
    fork_step: Option<StringBuilder>,
}

impl DbOpsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            action_index: UInt32Builder::new(),
            operation: StringBuilder::new(),
            code: StringBuilder::new(),
            scope: StringBuilder::new(),
            table_name: StringBuilder::new(),
            primary_key: StringBuilder::new(),
            old_payer: StringBuilder::new(),
            new_payer: StringBuilder::new(),
            old_data: BytesColumn::new(encoding),
            new_data: BytesColumn::new(encoding),
            old_data_json: StringBuilder::new(),
            new_data_json: StringBuilder::new(),
            fork_step: if include_fork_step {
                Some(StringBuilder::new())
            } else {
                None
            },
        }
    }

    fn finish(&mut self, schema: &Schema) -> anyhow::Result<RecordBatch> {
        let mut columns = self.canonical.finish();
        columns.extend(vec![
            Arc::new(self.operation.finish()) as Arc<dyn Array>,
            Arc::new(self.action_index.finish()) as Arc<dyn Array>,
            Arc::new(self.code.finish()) as Arc<dyn Array>,
            Arc::new(self.scope.finish()) as Arc<dyn Array>,
            Arc::new(self.table_name.finish()) as Arc<dyn Array>,
            Arc::new(self.primary_key.finish()) as Arc<dyn Array>,
            Arc::new(self.old_payer.finish()) as Arc<dyn Array>,
            Arc::new(self.new_payer.finish()) as Arc<dyn Array>,
            self.old_data.finish(),
            self.new_data.finish(),
            Arc::new(self.old_data_json.finish()) as Arc<dyn Array>,
            Arc::new(self.new_data_json.finish()) as Arc<dyn Array>,
        ]);
        finish_fork_step(&mut self.fork_step, &mut columns);
        Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    fn make_test_hex_id(seed: u32) -> String {
        format!("{seed:064x}")
    }

    pub(crate) fn make_test_block(number: u32) -> antelope::Block {
        antelope::Block {
            id: make_test_hex_id(number),
            number,
            version: 1,
            header: Some(antelope::BlockHeader {
                timestamp: Some(prost_types::Timestamp {
                    seconds: 1_700_000_000 + number as i64,
                    nanos: 0,
                }),
                producer: "eosproducer1".to_string(),
                confirmed: 0,
                previous: make_test_hex_id(number.saturating_sub(1)),
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
                        receipt: Some(antelope::ActionReceipt {
                            receiver: "eosio.token".to_string(),
                            digest: "receipt-digest-1".to_string(),
                            global_sequence: 42,
                            auth_sequence: vec![antelope::AuthSequence {
                                account_name: "alice".to_string(),
                                sequence: 7,
                            }],
                            recv_sequence: 8,
                            code_sequence: 9,
                            abi_sequence: 10,
                        }),
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
                        console: "inline trace".to_string(),
                        transaction_id: "trx_hash_1".to_string(),
                        block_num: number as u64,
                        producer_block_id: make_test_hex_id(number),
                        block_time: Some(prost_types::Timestamp {
                            seconds: 1_700_000_000 + number as i64,
                            nanos: 500_000_000,
                        }),
                        account_ram_deltas: vec![],
                        raw_return_value: vec![9, 8, 7],
                        json_return_value: r#"{"ok":true}"#.to_string(),
                        exception: Some(antelope::Exception {
                            code: 13,
                            name: "test_exception".to_string(),
                            message: "boom".to_string(),
                            stack: vec![antelope::exception::LogMessage {
                                context: Some(antelope::exception::LogContext {
                                    level: "error".to_string(),
                                    file: "apply_context.cpp".to_string(),
                                    line: 99,
                                    method: "exec_one".to_string(),
                                    hostname: "node-1".to_string(),
                                    thread_name: "main".to_string(),
                                    timestamp: Some(prost_types::Timestamp {
                                        seconds: 1_700_000_000 + number as i64,
                                        nanos: 0,
                                    }),
                                    context: None,
                                }),
                                format: "assertion failure with message: {msg}".to_string(),
                                data: br#"{"msg":"boom"}"#.to_vec(),
                            }],
                        }),
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
                        producer_block_id: make_test_hex_id(number),
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
                    old_payer: "eosio".to_string(),
                    new_payer: "alice".to_string(),
                    old_data: vec![1, 2, 3],
                    new_data: vec![10, 20, 30],
                    old_data_json: r#"{"balance":"0.0000 EOS"}"#.to_string(),
                    new_data_json: r#"{"balance":"1.0000 EOS"}"#.to_string(),
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
        let mut mapper = AntelopeBlockMapper::new(true, EncodeBytes::HexNoPrefix, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();

        assert_eq!(mapper.max_table_rows(), 2); // 2 actions

        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 1);
        assert_eq!(batches["actions"].num_rows(), 2);
        assert_eq!(batches["db_ops"].num_rows(), 1);
    }

    #[test]
    fn test_antelope_canonical_ids_match_block_hash_fields() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = AntelopeBlockMapper::new(true, EncodeBytes::HexNoPrefix, false);
        let identity = BlockIdentity {
            block_num: 100,
            block_id: "firehose-envelope-id".to_string(),
            parent_num: 99,
            parent_id: "firehose-envelope-parent-id".to_string(),
            lib_num: 99,
            timestamp: 0,
            timestamp_nanos: 0,
            fork_step: None,
        };

        mapper.map_block(&block_bytes, &identity, None).unwrap();
        let batches = mapper.flush().unwrap();
        let blocks = &batches["blocks"];

        let block_id = blocks
            .column_by_name("block_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let parent_id = blocks
            .column_by_name("parent_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let hash = blocks
            .column_by_name("hash")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(block_id.value(0), hash.value(0));
        assert_eq!(parent_id.value(0), make_test_hex_id(99));
        assert!(!block_id.value(0).starts_with("0x"));
        assert!(!parent_id.value(0).starts_with("0x"));
    }

    #[test]
    fn test_antelope_hex_no_prefix_canonical_ids_match_block_hash_fields() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = AntelopeBlockMapper::new(false, EncodeBytes::HexNoPrefix, false);
        let identity = BlockIdentity {
            block_num: 100,
            block_id: "firehose-envelope-id".to_string(),
            parent_num: 99,
            parent_id: "firehose-envelope-parent-id".to_string(),
            lib_num: 99,
            timestamp: 0,
            timestamp_nanos: 0,
            fork_step: None,
        };

        mapper.map_block(&block_bytes, &identity, None).unwrap();
        let batches = mapper.flush().unwrap();
        let blocks = &batches["blocks"];

        let block_id = blocks
            .column_by_name("block_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let parent_id = blocks
            .column_by_name("parent_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let hash = blocks
            .column_by_name("hash")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(block_id.value(0), hash.value(0));
        assert_eq!(parent_id.value(0), make_test_hex_id(99));
        assert!(!block_id.value(0).starts_with("0x"));
        assert!(!parent_id.value(0).starts_with("0x"));
    }

    #[test]
    fn test_flush_resets_builders() {
        let block = make_test_block(1);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = AntelopeBlockMapper::new(true, EncodeBytes::HexNoPrefix, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
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
        let mut mapper = AntelopeBlockMapper::new(true, EncodeBytes::HexNoPrefix, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();
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
        let mut mapper = AntelopeBlockMapper::new(true, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), Some("NEW"))
            .unwrap();

        let batches = mapper.flush().unwrap();
        let blocks_batch = &batches["blocks"];
        let last_col = blocks_batch.num_columns() - 1;
        assert_eq!(blocks_batch.schema().field(last_col).name(), "fork_step");
        let fork_col = blocks_batch
            .column(last_col)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(fork_col.value(0), "NEW");
    }

    #[test]
    fn test_table_names_include_db_ops_without_extended() {
        let mapper = AntelopeBlockMapper::new(false, EncodeBytes::Hex, false);
        let names = mapper.table_names();
        assert_eq!(names.len(), 4);
        assert!(names.contains(&"blocks"));
        assert!(names.contains(&"transactions"));
        assert!(names.contains(&"actions"));
        assert!(names.contains(&"db_ops"));
    }

    #[test]
    fn test_table_names_extended_still_include_db_ops() {
        let mapper = AntelopeBlockMapper::new(false, EncodeBytes::Hex, false);
        let names = mapper.table_names();
        assert_eq!(names.len(), 4);
        assert!(names.contains(&"blocks"));
        assert!(names.contains(&"transactions"));
        assert!(names.contains(&"actions"));
        assert!(names.contains(&"db_ops"));
    }

    #[test]
    fn test_base_includes_db_ops() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = AntelopeBlockMapper::new(false, EncodeBytes::Hex, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();

        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["transactions"].num_rows(), 1);
        assert_eq!(batches["actions"].num_rows(), 2);
        assert_eq!(batches["db_ops"].num_rows(), 1);
    }

    #[test]
    fn test_antelope_schema_matches_current_contract() {
        let actions = schema::actions_schema(false, &EncodeBytes::HexNoPrefix);
        let action_names: Vec<&str> = actions
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        assert_eq!(
            action_names,
            vec![
                "block_num",
                "block_id",
                "parent_num",
                "parent_id",
                "lib_num",
                "timestamp",
                "date",
                "tx_hash",
                "action_ordinal",
                "creator_action_ordinal",
                "closest_unnotified_ancestor_action_ordinal",
                "execution_index",
                "receiver",
                "account",
                "name",
                "authorization",
                "json_data",
                "raw_data",
                "context_free",
                "elapsed",
                "console",
                "transaction_id",
                "trace_block_num",
                "producer_block_id",
                "block_time",
                "raw_return_value",
                "json_return_value",
                "exception",
                "error_code",
                "receipt_receiver",
                "receipt_digest",
                "receipt_global_sequence",
                "receipt_auth_sequence",
                "receipt_recv_sequence",
                "receipt_code_sequence",
                "receipt_abi_sequence",
            ]
        );
        assert_eq!(
            actions.field_with_name("raw_data").unwrap().data_type(),
            &arrow::datatypes::DataType::Utf8
        );
        assert!(actions
            .field_with_name("authorization")
            .unwrap()
            .is_nullable());
        assert!(actions.field_with_name("block_time").unwrap().is_nullable());

        let db_ops = schema::db_ops_schema(false, &EncodeBytes::HexNoPrefix);
        let db_op_names: Vec<&str> = db_ops
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        assert_eq!(
            db_op_names,
            vec![
                "block_num",
                "block_id",
                "parent_num",
                "parent_id",
                "lib_num",
                "timestamp",
                "date",
                "operation",
                "action_index",
                "code",
                "scope",
                "table_name",
                "primary_key",
                "old_payer",
                "new_payer",
                "old_data",
                "new_data",
                "old_data_json",
                "new_data_json",
            ]
        );
        assert_eq!(
            db_ops.field_with_name("operation").unwrap().data_type(),
            &arrow::datatypes::DataType::Utf8
        );
        assert_eq!(
            db_ops.field_with_name("old_data").unwrap().data_type(),
            &arrow::datatypes::DataType::Utf8
        );
    }

    #[test]
    fn test_antelope_mapping_emits_enum_text_and_expanded_action_db_op_fields() {
        let block = make_test_block(100);
        let block_bytes = prost::Message::encode_to_vec(&block);
        let mut mapper = AntelopeBlockMapper::new(false, EncodeBytes::HexNoPrefix, false);
        mapper
            .map_block(&block_bytes, &BlockIdentity::default(), None)
            .unwrap();

        let batches = mapper.flush().unwrap();

        let transactions = &batches["transactions"];
        let transaction_status = transactions
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(transaction_status.value(0), "EXECUTED");

        let actions = &batches["actions"];
        let authorization = actions
            .column_by_name("authorization")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(authorization.value(0), "alice@active");

        let json_data = actions
            .column_by_name("json_data")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            json_data.value(0),
            r#"{"from":"alice","to":"bob","quantity":"1.0000 EOS","memo":"test"}"#
        );

        let raw_data = actions
            .column_by_name("raw_data")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(raw_data.value(0), "01020304");

        let block_time = actions
            .column_by_name("block_time")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(block_time.value(0), 1_700_000_100_500);
        assert!(block_time.is_null(1));

        let raw_return_value = actions
            .column_by_name("raw_return_value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(raw_return_value.value(0), "090807");
        assert!(raw_return_value.is_null(1));

        let exception = actions
            .column_by_name("exception")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let exception_json: serde_json::Value =
            serde_json::from_str(exception.value(0)).expect("valid exception JSON");
        assert_eq!(exception_json["code"], 13);
        assert_eq!(exception_json["name"], "test_exception");

        let receipt_auth_sequence = actions
            .column_by_name("receipt_auth_sequence")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let auth_sequence_json: serde_json::Value =
            serde_json::from_str(receipt_auth_sequence.value(0)).expect("valid auth sequence JSON");
        assert_eq!(auth_sequence_json[0]["account_name"], "alice");
        assert_eq!(auth_sequence_json[0]["sequence"], 7);

        let db_ops = &batches["db_ops"];
        let operation = db_ops
            .column_by_name("operation")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(operation.value(0), "INSERT");

        let old_payer = db_ops
            .column_by_name("old_payer")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(old_payer.value(0), "eosio");

        let old_data = db_ops
            .column_by_name("old_data")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(old_data.value(0), "010203");

        let old_data_json = db_ops
            .column_by_name("old_data_json")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(old_data_json.value(0), r#"{"balance":"0.0000 EOS"}"#);
    }
}
