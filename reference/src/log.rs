//! The job's output: one JSON object per line, each with an `event`.
//!
//! Keys are sorted as `fireparq maintenance` sorts them, whatever map
//! serde_json was built with (#698), and each line is flushed. A closed stdout
//! never stops the job. Every credential and the `/info` URL are replaced by
//! `***` in every string value: messages are built from the job's own words,
//! request subjects, table names and versions, and never carry a URL, a
//! response body or a credential, and this is the backstop.

use std::cmp::Reverse;
use std::io::Write;
use std::sync::{Arc, Mutex, PoisonError};

use fireparq_maintenance::sorted;
use serde_json::{Map, Value};

/// Writes the JSON lines; clones share the output.
#[derive(Clone)]
pub struct Logger {
    inner: Arc<Inner>,
}

struct Inner {
    out: Mutex<Box<dyn Write + Send>>,
    /// Non-empty, longest first: a secret inside another (the URL's query)
    /// never leaves part of the longer one.
    secrets: Vec<String>,
}

impl Logger {
    /// A logger writing to `out` that replaces each of `secrets` (empty ones
    /// are ignored) by `***`.
    pub fn new(out: Box<dyn Write + Send>, secrets: Vec<String>) -> Logger {
        let mut secrets: Vec<String> = secrets
            .into_iter()
            .filter(|secret| !secret.is_empty())
            .collect();
        secrets.sort_by_key(|secret| Reverse(secret.len()));
        secrets.dedup();
        Logger {
            inner: Arc::new(Inner {
                out: Mutex::new(out),
                secrets,
            }),
        }
    }

    /// Writes one line: `fields` (a JSON object) and `event`.
    pub fn emit(&self, event: &str, fields: Value) {
        let mut fields = match fields {
            Value::Object(fields) => fields,
            _ => Map::new(),
        };
        fields.insert("event".into(), Value::String(event.into()));
        let line = sorted(self.redact_value(Value::Object(fields)));
        let mut out = self
            .inner
            .out
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // A closed stdout must not stop the job.
        let _ = writeln!(out, "{line}");
        let _ = out.flush();
    }

    /// `text` with every secret replaced by `***`.
    pub fn redact(&self, text: &str) -> String {
        let mut text = text.to_owned();
        for secret in &self.inner.secrets {
            if text.contains(secret.as_str()) {
                text = text.replace(secret.as_str(), "***");
            }
        }
        text
    }

    fn redact_value(&self, value: Value) -> Value {
        match value {
            Value::String(text) => Value::String(self.redact(&text)),
            Value::Array(items) => Value::Array(
                items
                    .into_iter()
                    .map(|item| self.redact_value(item))
                    .collect(),
            ),
            Value::Object(entries) => Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, self.redact_value(value)))
                    .collect(),
            ),
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::MemoryLog;
    use serde_json::json;

    #[test]
    fn lines_are_sorted_objects_with_an_event() {
        let log = MemoryLog::new();
        log.logger()
            .emit("retry", json!({"request": "spotMeta", "attempt": 1}));
        log.logger()
            .emit("done", json!({"summary": {"b": 1, "a": 2}}));
        assert_eq!(
            log.text(),
            "{\"attempt\":1,\"event\":\"retry\",\"request\":\"spotMeta\"}\n\
             {\"event\":\"done\",\"summary\":{\"a\":2,\"b\":1}}\n"
        );
        assert_eq!(log.events()[1]["event"], "done");
    }

    #[test]
    fn credentials_are_redacted() {
        let url = "https://gw.example/v1/KEY/info?token=abc";
        let log = MemoryLog::with_secrets(vec![
            "".into(),
            "token=abc".into(),
            url.into(),
            "secret".into(),
        ]);
        log.logger().emit(
            "error",
            json!({"message": format!("{url} failed with secret"), "failed": ["x token=abc"], "n": 1}),
        );
        let text = log.text();
        for leaked in ["KEY", "token=abc", "secret"] {
            assert!(!text.contains(leaked), "{text}");
        }
        let event = &log.events()[0];
        assert_eq!(event["message"], "*** failed with ***");
        assert_eq!(event["failed"][0], "x ***");
        assert_eq!(event["n"], 1);
        // An empty secret replaces nothing.
        assert_eq!(log.logger().redact("abc"), "abc");
    }

    #[test]
    fn a_closed_output_never_stops_the_job() {
        struct Closed;
        impl Write for Closed {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::ErrorKind::BrokenPipe.into())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Err(std::io::ErrorKind::BrokenPipe.into())
            }
        }
        Logger::new(Box::new(Closed), vec![]).emit("start", json!({}));
    }
}
