//! Decode selected contract payloads while retaining the full source Any separately.
use super::proto::protocol;
use anyhow::{ensure, Context, Result};
use prost::Message;

#[derive(Default)]
pub(super) struct DecodedContract {
    pub owner_address: Option<prost::bytes::Bytes>,
    pub to_address: Option<prost::bytes::Bytes>,
    pub amount: Option<i64>,
    pub asset_name: Option<prost::bytes::Bytes>,
    pub contract_address: Option<prost::bytes::Bytes>,
    pub data: Option<prost::bytes::Bytes>,
    pub call_value: Option<i64>,
    pub call_token_value: Option<i64>,
    pub token_id: Option<i64>,
}

pub(super) fn decode(contract: &protocol::transaction::Contract) -> Result<DecodedContract> {
    let mut out = DecodedContract::default();
    let Some(parameter) = &contract.parameter else {
        return Ok(out);
    };
    let expected = match contract.r#type {
        1 => "protocol.TransferContract",
        2 => "protocol.TransferAssetContract",
        31 => "protocol.TriggerSmartContract",
        _ => return Ok(out),
    };
    ensure!(
        parameter.type_url.rsplit('/').next() == Some(expected),
        "Tron contract parameter type does not match contract enum"
    );
    match contract.r#type {
        1 => {
            let value = protocol::TransferContract::decode(parameter.value.as_slice())
                .context("invalid Tron TransferContract parameter")?;
            out.owner_address = Some(value.owner_address);
            out.to_address = Some(value.to_address);
            out.amount = Some(value.amount);
        }
        2 => {
            let value = protocol::TransferAssetContract::decode(parameter.value.as_slice())
                .context("invalid Tron TransferAssetContract parameter")?;
            out.owner_address = Some(value.owner_address);
            out.to_address = Some(value.to_address);
            out.amount = Some(value.amount);
            out.asset_name = Some(value.asset_name);
        }
        31 => {
            let value = protocol::TriggerSmartContract::decode(parameter.value.as_slice())
                .context("invalid Tron TriggerSmartContract parameter")?;
            out.owner_address = Some(value.owner_address);
            out.contract_address = Some(value.contract_address);
            out.data = Some(value.data);
            out.call_value = Some(value.call_value);
            out.call_token_value = Some(value.call_token_value);
            out.token_id = Some(value.token_id);
        }
        _ => unreachable!(),
    }
    Ok(out)
}
