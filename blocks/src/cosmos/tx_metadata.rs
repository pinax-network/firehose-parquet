//! Transaction-wide metadata, joined from messages by canonical block + tx index.
use super::proto::cosmos_tx;
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Fields};
use prost::Message;
use std::sync::Arc;

pub(super) fn decode_tx(raw: impl prost::bytes::Buf) -> Result<cosmos_tx::Tx, prost::DecodeError> {
    let tx = cosmos_tx::TxRaw::decode(raw)?;
    Ok(cosmos_tx::Tx {
        body: tx
            .body_bytes
            .map(|bytes| cosmos_tx::TxBody::decode(bytes))
            .transpose()?,
        auth_info: tx
            .auth_info_bytes
            .map(|bytes| cosmos_tx::AuthInfo::decode(bytes))
            .transpose()?,
        signatures: tx.signatures,
    })
}

fn fee_fields() -> Fields {
    vec![
        Field::new("denom", DataType::Utf8, false),
        Field::new("amount", DataType::Utf8, false),
    ]
    .into()
}
fn signer_fields() -> Fields {
    vec![
        Field::new("public_key_type_url", DataType::Utf8, true),
        Field::new("public_key_value", DataType::Binary, true),
        Field::new("mode_info", DataType::Binary, true),
        Field::new("sequence", DataType::UInt64, false),
    ]
    .into()
}
fn list_item(data_type: DataType) -> Arc<Field> {
    Arc::new(Field::new("item", data_type, false))
}
pub(super) fn fields() -> Vec<Field> {
    vec![
        Field::new("raw_tx", DataType::Binary, false),
        Field::new("decode_success", DataType::Boolean, false),
        Field::new("memo", DataType::Utf8, true),
        Field::new("timeout_height", DataType::UInt64, true),
        Field::new("fee_gas_limit", DataType::UInt64, true),
        Field::new("fee_payer", DataType::Utf8, true),
        Field::new("fee_granter", DataType::Utf8, true),
        Field::new(
            "fee_amount",
            DataType::List(list_item(DataType::Struct(fee_fields()))),
            true,
        ),
        Field::new(
            "signer_infos",
            DataType::List(list_item(DataType::Struct(signer_fields()))),
            true,
        ),
        Field::new(
            "signatures",
            DataType::List(list_item(DataType::Binary)),
            true,
        ),
    ]
}

pub(super) struct TxMetadataBuilder {
    raw: BinaryBuilder,
    decoded: BooleanBuilder,
    memo: StringBuilder,
    timeout: UInt64Builder,
    gas: UInt64Builder,
    payer: StringBuilder,
    granter: StringBuilder,
    fee: ListBuilder<StructBuilder>,
    signers: ListBuilder<StructBuilder>,
    signatures: ListBuilder<BinaryBuilder>,
    estimated_bytes: usize,
}
impl TxMetadataBuilder {
    pub(super) fn new() -> Self {
        Self {
            raw: BinaryBuilder::new(),
            decoded: BooleanBuilder::new(),
            memo: StringBuilder::new(),
            timeout: UInt64Builder::new(),
            gas: UInt64Builder::new(),
            payer: StringBuilder::new(),
            granter: StringBuilder::new(),
            fee: ListBuilder::new(StructBuilder::from_fields(fee_fields(), 0))
                .with_field(list_item(DataType::Struct(fee_fields()))),
            signers: ListBuilder::new(StructBuilder::from_fields(signer_fields(), 0))
                .with_field(list_item(DataType::Struct(signer_fields()))),
            signatures: ListBuilder::new(BinaryBuilder::new())
                .with_field(list_item(DataType::Binary)),
            estimated_bytes: 0,
        }
    }
    pub(super) fn append(&mut self, raw: &[u8], tx: Option<&cosmos_tx::Tx>) {
        self.raw.append_value(raw);
        self.decoded.append_value(tx.is_some());
        let body = tx.and_then(|tx| tx.body.as_ref());
        let auth = tx.and_then(|tx| tx.auth_info.as_ref());
        let fee = auth.and_then(|auth| auth.fee.as_ref());
        self.memo.append_option(body.map(|body| body.memo.as_str()));
        self.timeout
            .append_option(body.map(|body| body.timeout_height));
        self.gas.append_option(fee.map(|fee| fee.gas_limit));
        self.payer.append_option(fee.map(|fee| fee.payer.as_str()));
        self.granter
            .append_option(fee.map(|fee| fee.granter.as_str()));
        // Include offsets/validity plus nested values in the mapper's flush estimate.
        self.estimated_bytes += raw.len() + 64 + body.map_or(0, |b| b.memo.len());
        if let Some(fee) = fee {
            self.estimated_bytes += fee.payer.len() + fee.granter.len();
            for coin in &fee.amount {
                let values = self.fee.values();
                values
                    .field_builder::<StringBuilder>(0)
                    .unwrap()
                    .append_value(&coin.denom);
                values
                    .field_builder::<StringBuilder>(1)
                    .unwrap()
                    .append_value(&coin.amount);
                values.append(true);
                self.estimated_bytes += coin.denom.len() + coin.amount.len() + 16;
            }
        }
        self.fee.append(fee.is_some());
        if let Some(auth) = auth {
            for signer in &auth.signer_infos {
                let key = signer.public_key.as_ref();
                let values = self.signers.values();
                values
                    .field_builder::<StringBuilder>(0)
                    .unwrap()
                    .append_option(key.map(|k| k.type_url.as_str()));
                values
                    .field_builder::<BinaryBuilder>(1)
                    .unwrap()
                    .append_option(key.map(|k| k.value.as_slice()));
                let modes = values.field_builder::<BinaryBuilder>(2).unwrap();
                if signer.mode_info.is_empty() {
                    modes.append_null();
                } else {
                    // Concatenated embedded message payloads have the same merge
                    // semantics as repeated occurrences of that message field.
                    modes.append_value(signer.mode_info.concat());
                }
                values
                    .field_builder::<UInt64Builder>(3)
                    .unwrap()
                    .append_value(signer.sequence);
                values.append(true);
                self.estimated_bytes += 32
                    + key.map_or(0, |k| k.type_url.len() + k.value.len())
                    + signer
                        .mode_info
                        .iter()
                        .map(|mode| mode.len())
                        .sum::<usize>();
            }
        }
        self.signers.append(auth.is_some());
        if let Some(tx) = tx {
            for signature in &tx.signatures {
                self.signatures.values().append_value(signature);
                self.estimated_bytes += signature.len() + 4;
            }
        }
        self.signatures.append(tx.is_some());
    }
    pub(super) fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }
    pub(super) fn finish(&mut self) -> Vec<ArrayRef> {
        self.estimated_bytes = 0;
        vec![
            Arc::new(self.raw.finish()),
            Arc::new(self.decoded.finish()),
            Arc::new(self.memo.finish()),
            Arc::new(self.timeout.finish()),
            Arc::new(self.gas.finish()),
            Arc::new(self.payer.finish()),
            Arc::new(self.granter.finish()),
            Arc::new(self.fee.finish()),
            Arc::new(self.signers.finish()),
            Arc::new(self.signatures.finish()),
        ]
    }
}
