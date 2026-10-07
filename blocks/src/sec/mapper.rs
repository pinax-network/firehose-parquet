//! [`SecBlockMapper`]: `pinax.sec.v1.Block` → the 43 SEC tables.
//!
//! `map_block` runs the preflight (`super::prepare::prepare_block`), which
//! does every fallible step, and only then appends to the builders, so a
//! failing block leaves every table untouched (§4.7).

use std::collections::HashMap;

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::EncodeBytes;
use firehose_parquet::traits::{BlockIdentity, BlockMapper, StreamEvent};
use prost::Message;

use super::build::{self, AppendCtx, SecTable};
use super::prepare::{self, PreparedBody};
use super::proto::sec;
use super::schema;

/// The SEC mapper: one builder per table of [`schema::TABLE_NAMES`].
pub struct SecBlockMapper {
    encoding: EncodeBytes,
    hub: build::hub::HubTables,
    envelope: build::envelope::EnvelopeTables,
    ownership: build::ownership::OwnershipTables,
    form13f: build::form13f::Form13fTables,
    beneficial: build::beneficial::BeneficialTables,
    form144: build::form144::Form144Tables,
    nport: build::nport::NportTables,
    formd: build::formd::FormDTables,
    npx: build::npx::NpxTables,
    ncen: build::ncen::NcenTables,
    formc: build::formc::FormCTables,
    issues: build::issues::IssueTables,
}

impl SecBlockMapper {
    pub fn new(include_fork_step: bool, encoding: EncodeBytes) -> Self {
        let f = include_fork_step;
        let e = &encoding;
        Self {
            hub: build::hub::HubTables::new(f, e),
            envelope: build::envelope::EnvelopeTables::new(f, e),
            ownership: build::ownership::OwnershipTables::new(f, e),
            form13f: build::form13f::Form13fTables::new(f, e),
            beneficial: build::beneficial::BeneficialTables::new(f, e),
            form144: build::form144::Form144Tables::new(f, e),
            nport: build::nport::NportTables::new(f, e),
            formd: build::formd::FormDTables::new(f, e),
            npx: build::npx::NpxTables::new(f, e),
            ncen: build::ncen::NcenTables::new(f, e),
            formc: build::formc::FormCTables::new(f, e),
            issues: build::issues::IssueTables::new(f, e),
            encoding,
        }
    }

    fn map_decoded(
        &mut self,
        block: sec::Block,
        identity: &BlockIdentity,
        event: StreamEvent<'_>,
    ) -> Result<u64> {
        // 1. Preflight: every fallible step, no builder touched.
        let prepared = prepare::prepare_block(&block, identity, &self.encoding)?;

        // 2. Append: infallible.
        let ctx = AppendCtx {
            id: &prepared.identity,
            event,
        };
        self.hub.append_block(&ctx, &prepared.block);
        for (filing, p) in block.filings.iter().zip(&prepared.filings) {
            let fc = &p.fc;
            self.hub.append_filing(&ctx, fc, filing, &p.filing);
            self.envelope.append(&ctx, fc, filing, &p.envelope);
            match &p.body {
                PreparedBody::Unset | PreparedBody::Raw(_) => {}
                PreparedBody::Ownership(body, b) => self.ownership.append(&ctx, fc, body, b),
                PreparedBody::Form13f(body, b) => self.form13f.append(&ctx, fc, body, b),
                PreparedBody::Beneficial(body, b) => self.beneficial.append(&ctx, fc, body, b),
                PreparedBody::Form144(body, b) => self.form144.append(&ctx, fc, body, b),
                PreparedBody::Nport(body, b) => self.nport.append(&ctx, fc, body, b),
                PreparedBody::FormD(body, b) => self.formd.append(&ctx, fc, body, b),
                PreparedBody::Npx(body, b) => self.npx.append(&ctx, fc, body, b),
                PreparedBody::Ncen(body, b) => self.ncen.append(&ctx, fc, body, b),
                PreparedBody::FormC(body, b) => self.formc.append(&ctx, fc, body, b),
            }
        }
        self.issues.append(&ctx, &prepared);
        Ok(block.filings.len() as u64)
    }

    /// Every table, in [`schema::TABLE_NAMES`] order.
    fn tables(&self) -> impl Iterator<Item = &dyn SecTable> {
        self.hub
            .tables()
            .into_iter()
            .chain(self.envelope.tables())
            .chain(self.ownership.tables())
            .chain(self.form13f.tables())
            .chain(self.beneficial.tables())
            .chain(self.form144.tables())
            .chain(self.nport.tables())
            .chain(self.formd.tables())
            .chain(self.npx.tables())
            .chain(self.ncen.tables())
            .chain(self.formc.tables())
            .chain(self.issues.tables())
    }

    /// Every table, in [`schema::TABLE_NAMES`] order.
    fn tables_mut(&mut self) -> impl Iterator<Item = &mut dyn SecTable> {
        self.hub
            .tables_mut()
            .into_iter()
            .chain(self.envelope.tables_mut())
            .chain(self.ownership.tables_mut())
            .chain(self.form13f.tables_mut())
            .chain(self.beneficial.tables_mut())
            .chain(self.form144.tables_mut())
            .chain(self.nport.tables_mut())
            .chain(self.formd.tables_mut())
            .chain(self.npx.tables_mut())
            .chain(self.ncen.tables_mut())
            .chain(self.formc.tables_mut())
            .chain(self.issues.tables_mut())
    }
}

impl BlockMapper for SecBlockMapper {
    fn map_block(
        &mut self,
        block_bytes: &[u8],
        identity: &BlockIdentity,
        fork_step: StreamEvent<'_>,
    ) -> Result<u64> {
        self.map_decoded(sec::Block::decode(block_bytes)?, identity, fork_step)
    }

    /// Decodes from the owned buffer, so `Filing.raw_xml` shares its allocation.
    fn map_block_bytes(
        &mut self,
        block_bytes: prost::bytes::Bytes,
        identity: &BlockIdentity,
        fork_step: StreamEvent<'_>,
    ) -> Result<u64> {
        self.map_decoded(sec::Block::decode(block_bytes)?, identity, fork_step)
    }

    fn flush(&mut self) -> Result<HashMap<String, RecordBatch>> {
        let mut batches = HashMap::with_capacity(schema::TABLE_NAMES.len());
        for table in self.tables_mut() {
            batches.insert(table.name().to_string(), table.finish()?);
        }
        Ok(batches)
    }

    fn max_table_rows(&self) -> usize {
        self.tables().map(SecTable::len).max().unwrap_or(0)
    }

    fn total_rows(&self) -> usize {
        self.tables().map(SecTable::len).sum()
    }

    fn table_estimates(&mut self) -> Vec<(&str, usize)> {
        self.tables()
            .map(|table| (table.name(), table.estimated_bytes()))
            .collect()
    }

    fn table_names(&self) -> Vec<&str> {
        schema::TABLE_NAMES.to_vec()
    }
}

/// Fixture builders shared with `schema_contract_tests.rs`.
#[cfg(test)]
pub(crate) mod tests {
    pub(crate) use crate::sec::tests::{make_every_body_block, make_test_block};

    use super::*;
    use crate::sec::tests::{identity, Batches};

    #[test]
    fn tables_follow_table_names() {
        let mapper = SecBlockMapper::new(true, EncodeBytes::Hex);
        let names: Vec<&str> = mapper.tables().map(SecTable::name).collect();
        assert_eq!(names, schema::TABLE_NAMES);
        assert_eq!(mapper.table_names(), schema::TABLE_NAMES);
    }

    #[test]
    fn empty_flush_returns_every_table_with_its_schema() {
        for include_fork_step in [false, true] {
            for encoding in [EncodeBytes::Hex, EncodeBytes::Binary] {
                let mut mapper = SecBlockMapper::new(include_fork_step, encoding.clone());
                for _ in 0..2 {
                    let batches = mapper.flush().unwrap();
                    assert_eq!(batches.len(), 43);
                    for name in schema::TABLE_NAMES {
                        let batch = &batches[name];
                        assert_eq!(batch.num_rows(), 0);
                        assert_eq!(
                            batch.schema().as_ref(),
                            &schema::table_schema(name, include_fork_step, &encoding),
                            "{name}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn empty_window_writes_only_its_blocks_row() {
        let block = make_test_block(crate::sec::tests::BLOCK_NUM);
        let mut mapper = SecBlockMapper::new(false, EncodeBytes::Hex);
        let mapped = mapper
            .map_block(
                &block.encode_to_vec(),
                &identity(crate::sec::tests::BLOCK_NUM),
                StreamEvent::default(),
            )
            .unwrap();
        assert_eq!(mapped, 0);
        assert_eq!(mapper.max_table_rows(), 1);
        assert_eq!(mapper.total_rows(), 1);
        let batches = Batches::new(mapper.flush().unwrap());
        assert_eq!(batches.rows("blocks"), 1);
        for name in &schema::TABLE_NAMES[1..] {
            assert_eq!(batches.rows(name), 0, "{name}");
        }
        assert_eq!(batches.cell("blocks", "filing_count", 0), "0");
        assert_eq!(mapper.max_table_rows(), 0);
    }

    #[test]
    fn text_ids_are_written_verbatim_under_hex_and_binary() {
        let n = crate::sec::tests::BLOCK_NUM;
        for encoding in [EncodeBytes::Hex, EncodeBytes::Binary] {
            let batches =
                crate::sec::tests::map_with(&[make_test_block(n)], false, encoding.clone());
            let block_id = batches.cell("blocks", "block_id", 0);
            let parent_id = batches.cell("blocks", "parent_id", 0);
            match encoding {
                EncodeBytes::Binary => {
                    assert_eq!(
                        block_id,
                        crate::sec::tests::hex_of(n.to_string().as_bytes())
                    );
                    assert_eq!(
                        parent_id,
                        crate::sec::tests::hex_of((n - 1).to_string().as_bytes())
                    );
                }
                _ => {
                    assert_eq!(block_id, n.to_string());
                    assert_eq!(parent_id, (n - 1).to_string());
                }
            }
        }
    }

    #[test]
    fn fork_step_and_stream_ordinal_are_the_last_columns_on_every_table() {
        let n = crate::sec::tests::BLOCK_NUM;
        let mut mapper = SecBlockMapper::new(true, EncodeBytes::Hex);
        mapper
            .map_block(
                &make_every_body_block(n, crate::sec::tests::window_seconds(n)).encode_to_vec(),
                &identity(n),
                StreamEvent::new(Some("NEW"), 42),
            )
            .unwrap();
        let batches = Batches::new(mapper.flush().unwrap());
        for name in schema::TABLE_NAMES {
            let batch = batches.table(name);
            let fields = batch.schema().fields().clone();
            assert_eq!(fields[fields.len() - 2].name(), "fork_step", "{name}");
            assert_eq!(fields[fields.len() - 1].name(), "stream_ordinal", "{name}");
            for row in 0..batch.num_rows() {
                assert_eq!(batches.cell(name, "fork_step", row), "NEW");
                assert_eq!(batches.cell(name, "stream_ordinal", row), "42");
            }
        }
        assert!(batches.rows("filings") > 0);
    }

    #[test]
    fn structural_errors_leave_every_builder_untouched() {
        let n = crate::sec::tests::BLOCK_NUM;
        let good = make_every_body_block(n, crate::sec::tests::window_seconds(n));
        let cases: Vec<(&str, sec::Block, BlockIdentity)> = vec![
            ("header number", good.clone(), identity(n + 1)),
            (
                "header time",
                good.clone(),
                BlockIdentity {
                    timestamp: identity(n).timestamp + 1,
                    ..identity(n)
                },
            ),
            (
                "missing header",
                sec::Block {
                    header: None,
                    ..good.clone()
                },
                identity(n),
            ),
            (
                "ordinal",
                {
                    let mut block = good.clone();
                    block.filings[0].ordinal = 7;
                    block
                },
                identity(n),
            ),
            (
                "acceptance",
                {
                    let mut block = good.clone();
                    block.filings[0].acceptance_datetime = Some(prost_types::Timestamp {
                        seconds: 0,
                        nanos: -1,
                    });
                    block
                },
                identity(n),
            ),
        ];
        for (case, block, id) in cases {
            let mut mapper = SecBlockMapper::new(false, EncodeBytes::Hex);
            let error = mapper.map_block(&block.encode_to_vec(), &id, StreamEvent::default());
            assert!(error.is_err(), "{case}");
            assert_eq!(mapper.total_rows(), 0, "{case}");
        }
        assert!(SecBlockMapper::new(false, EncodeBytes::Hex)
            .map_block(&[255], &identity(n), StreamEvent::default())
            .is_err());
    }

    #[test]
    fn estimates_are_non_zero_for_tables_with_rows() {
        let n = crate::sec::tests::BLOCK_NUM;
        let mut mapper = SecBlockMapper::new(false, EncodeBytes::Hex);
        mapper
            .map_block(
                &make_every_body_block(n, crate::sec::tests::window_seconds(n)).encode_to_vec(),
                &identity(n),
                StreamEvent::default(),
            )
            .unwrap();
        let rows: HashMap<&str, usize> = mapper.tables().map(|t| (t.name(), t.len())).collect();
        for (name, bytes) in mapper.table_estimates() {
            if rows[name] > 0 {
                assert!(bytes > 0, "{name}");
            }
        }
    }

    /// Enable once every group has landed: the contract fixture must give every
    /// table at least one row (`schema_contract_tests.rs` requires it).
    #[test]
    #[ignore = "integration: passes once every group's tables are mapped"]
    fn every_body_block_fills_every_table() {
        let n = crate::sec::tests::BLOCK_NUM;
        let batches = crate::sec::tests::map(&[
            make_every_body_block(n, crate::sec::tests::window_seconds(n)),
            make_test_block(n + 1),
        ]);
        let empty: Vec<&str> = schema::TABLE_NAMES
            .into_iter()
            .filter(|name| batches.rows(name) == 0)
            .collect();
        assert!(empty.is_empty(), "tables without rows: {empty:?}");
    }
}
