//! Received ordinals advance only through the contiguous successfully accepted prefix.

use anyhow::{bail, Context, Result};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;

use super::state::{
    canonical_json, AcceptedPrefix, AnchorProvenance, Checkpoint, Digest, EventIdentity,
    RoutingCheckpoint,
};

const MAX_BUFFERED_EVENTS: usize = 65_536;

struct ReceivedEvent {
    identity: EventIdentity,
    accepted_routing: Option<RoutingCheckpoint>,
}

pub struct AcceptedFrontier {
    base_ordinal: u64,
    assigned_ordinal: u64,
    accepted_ordinal: u64,
    last_event: Option<EventIdentity>,
    routing: RoutingCheckpoint,
    received: BTreeMap<u64, ReceivedEvent>,
    digest: Sha256,
}
impl AcceptedFrontier {
    pub fn resume(checkpoint: &Checkpoint) -> Self {
        Self {
            base_ordinal: checkpoint.ordinal,
            assigned_ordinal: checkpoint.ordinal,
            accepted_ordinal: checkpoint.ordinal,
            last_event: checkpoint.event.clone(),
            routing: checkpoint.routing.clone(),
            received: BTreeMap::new(),
            digest: fresh_digest(),
        }
    }
    /// Assign at receipt, before filtering, bootstrap buffering or mapper work.
    pub fn receive(&mut self, identity: EventIdentity) -> Result<u64> {
        identity.validate()?;
        if self.received.len() >= MAX_BUFFERED_EVENTS {
            bail!("accepted-event buffer limit reached before a contiguous prefix resolved");
        }
        let ordinal = self
            .assigned_ordinal
            .checked_add(1)
            .context("accepted event ordinal exhausted")?;
        if let Some(anchor) = self.routing.anchor.as_ref().filter(|anchor| {
            anchor.provenance == AnchorProvenance::Lookahead && anchor.source_ordinal == ordinal
        }) {
            if identity.block_num != anchor.source_block_num
                || identity.block_id != anchor.source_block_id
                || identity.source_timestamp != Some(anchor.seconds)
            {
                bail!("resumed input differs from the authoritative lookahead anchor source");
            }
        }
        self.received.insert(
            ordinal,
            ReceivedEvent {
                identity,
                accepted_routing: None,
            },
        );
        self.assigned_ordinal = ordinal;
        Ok(ordinal)
    }
    /// Mark only after mapping/filter handling succeeds. Zero emitted rows still accept.
    pub fn accept(&mut self, ordinal: u64, routing: RoutingCheckpoint) -> Result<()> {
        routing.validate(ordinal)?;
        if routing.policy != self.routing.policy {
            bail!("routing policy changed within an accepted stream");
        }
        if routing
            .anchor
            .as_ref()
            .is_some_and(|anchor| anchor.source_ordinal > self.assigned_ordinal)
        {
            bail!("routing anchor refers to an event that was never received");
        }
        if let Some(anchor) = &routing.anchor {
            if anchor.provenance != AnchorProvenance::SolanaGenesisFallback {
                let source = self
                    .received
                    .get(&anchor.source_ordinal)
                    .map(|event| &event.identity)
                    .or_else(|| {
                        (anchor.source_ordinal == self.accepted_ordinal)
                            .then_some(self.last_event.as_ref())
                            .flatten()
                    });
                let matches_source = source.is_some_and(|event| {
                    event.block_num == anchor.source_block_num
                        && event.block_id == anchor.source_block_id
                        && event.source_timestamp == Some(anchor.seconds)
                });
                let matches_restored = self.routing.anchor.as_ref().is_some_and(|previous| {
                    previous.source_ordinal == anchor.source_ordinal
                        && previous.source_block_num == anchor.source_block_num
                        && previous.source_block_id == anchor.source_block_id
                        && previous.seconds == anchor.seconds
                        && previous.provenance != AnchorProvenance::SolanaGenesisFallback
                });
                if !matches_source && !(source.is_none() && matches_restored) {
                    bail!("routing anchor differs from its received source or authoritative predecessor");
                }
            }
        }
        let event = self
            .received
            .get_mut(&ordinal)
            .context("event was not received or was already accepted")?;
        if event.accepted_routing.is_some() {
            bail!("event acceptance cannot be repeated or replaced");
        }
        event.accepted_routing = Some(routing);
        while let Some(next) = self.accepted_ordinal.checked_add(1) {
            if !self
                .received
                .get(&next)
                .is_some_and(|event| event.accepted_routing.is_some())
            {
                break;
            }
            let event = self.received.remove(&next).expect("accepted event present");
            let routing = event.accepted_routing.expect("accepted routing present");
            let bytes = canonical_json(&serde_json::to_value((next, &event.identity, &routing))?)?;
            self.digest.update((bytes.len() as u64).to_be_bytes());
            self.digest.update(bytes);
            self.accepted_ordinal = next;
            self.last_event = Some(event.identity);
            self.routing = routing;
        }
        Ok(())
    }
    pub fn snapshot(&self) -> Result<Option<AcceptedPrefix>> {
        if self.accepted_ordinal == self.base_ordinal {
            return Ok(None);
        }
        Ok(Some(AcceptedPrefix {
            first_ordinal: self
                .base_ordinal
                .checked_add(1)
                .context("accepted ordinal exhausted")?,
            last_ordinal: self.accepted_ordinal,
            events_sha256: Digest::parse(hex::encode(self.digest.clone().finalize()))?,
            last_event: self
                .last_event
                .clone()
                .context("accepted prefix is missing its last event")?,
            routing: self.routing.clone(),
        }))
    }
    /// A synchronous controller must acknowledge the exact frozen prefix before
    /// accepting later mapped events. Received look-ahead events remain queued.
    pub fn acknowledge(&mut self, prefix: &AcceptedPrefix) -> Result<()> {
        if self.snapshot()?.as_ref() != Some(prefix) {
            bail!("commit acknowledgement differs from the frozen accepted prefix");
        }
        self.base_ordinal = prefix.last_ordinal;
        self.digest = fresh_digest();
        Ok(())
    }
    pub fn unresolved_events(&self) -> usize {
        self.received.len()
    }
}
fn fresh_digest() -> Sha256 {
    let mut hash = Sha256::new();
    hash.update(b"fireparq-accepted-window-v1\0");
    hash
}

#[cfg(test)]
mod tests;
