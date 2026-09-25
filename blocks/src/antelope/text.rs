//! Stream the existing text representation into Arrow without an intermediate
//! JSON tree or row-sized String. Field order preserves the previous JSON bytes.
use super::proto::antelope;
use arrow::array::StringBuilder;
use serde::{ser::SerializeSeq, Serialize, Serializer};
use std::{borrow::Cow, fmt::Write as _, io};

pub(super) fn append_authorization(
    builder: &mut StringBuilder,
    auth: &[antelope::PermissionLevel],
) {
    if auth.is_empty() {
        builder.append_null();
        return;
    }
    for (index, entry) in auth.iter().enumerate() {
        if index != 0 {
            builder.write_char(',').expect("Arrow string write");
        }
        write!(builder, "{}@{}", entry.actor, entry.permission).expect("Arrow string write");
    }
    builder.append_value("");
}

// serde_json writes complete UTF-8 string fragments. Validate that invariant
// instead of exposing unchecked bytes through Arrow's UTF-8 builder.
struct JsonWriter<'a>(&'a mut StringBuilder);
impl io::Write for JsonWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let text = std::str::from_utf8(bytes).map_err(io::Error::other)?;
        self.0.write_str(text).map_err(io::Error::other)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn append_json(builder: &mut StringBuilder, value: &impl Serialize) {
    serde_json::to_writer(JsonWriter(builder), value)
        .expect("fixed Antelope JSON fields serialize as UTF-8");
    builder.append_value("");
}

#[derive(Serialize)]
struct Timestamp {
    nanos: i32,
    seconds: i64,
}

struct Context<'a>(&'a antelope::exception::LogContext);
impl Serialize for Context<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // A borrowed recursive wrapper avoids allocating a parallel context tree.
        #[derive(Serialize)]
        struct Fields<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            context: Option<Context<'a>>,
            file: &'a str,
            hostname: &'a str,
            level: &'a str,
            line: i32,
            method: &'a str,
            thread_name: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            timestamp: Option<Timestamp>,
        }
        let c = self.0;
        Fields {
            context: c.context.as_deref().map(Context),
            file: &c.file,
            hostname: &c.hostname,
            level: &c.level,
            line: c.line,
            method: &c.method,
            thread_name: &c.thread_name,
            timestamp: c.timestamp.as_ref().map(|t| Timestamp {
                nanos: t.nanos,
                seconds: t.seconds,
            }),
        }
        .serialize(serializer)
    }
}

struct Stack<'a>(&'a [antelope::exception::LogMessage]);
impl Serialize for Stack<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Message<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            context: Option<Context<'a>>,
            data: Cow<'a, str>,
            format: &'a str,
        }
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for m in self.0 {
            seq.serialize_element(&Message {
                context: m.context.as_ref().map(Context),
                data: String::from_utf8_lossy(&m.data),
                format: &m.format,
            })?;
        }
        seq.end()
    }
}

pub(super) fn append_exception(
    builder: &mut StringBuilder,
    exception: Option<&antelope::Exception>,
) {
    #[derive(Serialize)]
    struct Exception<'a> {
        code: i32,
        message: &'a str,
        name: &'a str,
        stack: Stack<'a>,
    }
    if let Some(e) = exception {
        append_json(
            builder,
            &Exception {
                code: e.code,
                message: &e.message,
                name: &e.name,
                stack: Stack(&e.stack),
            },
        );
    } else {
        builder.append_null();
    }
}

struct AuthSequence<'a>(&'a [antelope::AuthSequence]);
impl Serialize for AuthSequence<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Entry<'a> {
            account_name: &'a str,
            sequence: u64,
        }
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for entry in self.0 {
            seq.serialize_element(&Entry {
                account_name: &entry.account_name,
                sequence: entry.sequence,
            })?;
        }
        seq.end()
    }
}

pub(super) fn append_auth_sequence(
    builder: &mut StringBuilder,
    sequence: &[antelope::AuthSequence],
) {
    if sequence.is_empty() {
        builder.append_null();
    } else {
        append_json(builder, &AuthSequence(sequence));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use serde_json::{json, Value};

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

    #[test]
    fn streamed_json_preserves_previous_bytes_and_null_boundaries() {
        let odd = (0..=127).map(char::from).collect::<String>() + " 🐈雪\u{2028}é";
        let parent = antelope::exception::LogContext {
            level: odd.clone(),
            file: odd.clone(),
            line: i32::MIN,
            method: odd.clone(),
            hostname: odd.clone(),
            thread_name: odd.clone(),
            timestamp: None,
            context: None,
        };
        let child = antelope::exception::LogContext {
            context: Some(Box::new(parent.clone())),
            timestamp: Some(prost_types::Timestamp {
                seconds: i64::MIN,
                nanos: i32::MAX,
            }),
            ..parent
        };
        let exception = antelope::Exception {
            code: i32::MIN,
            name: odd.clone(),
            message: odd.clone(),
            stack: vec![
                antelope::exception::LogMessage {
                    context: Some(child),
                    format: odd.clone(),
                    data: vec![0xff, 0xc3, 0x28, 0, 34, 92].into(),
                },
                antelope::exception::LogMessage {
                    context: None,
                    format: String::new(),
                    data: odd.as_bytes().to_vec().into(),
                },
            ],
        };
        let empty = antelope::Exception::default();
        let mut builder = StringBuilder::new();
        append_exception(&mut builder, None);
        append_exception(&mut builder, Some(&exception));
        append_exception(&mut builder, Some(&empty));
        append_exception(&mut builder, None);
        let values = builder.finish();
        assert!(values.is_null(0) && values.is_null(3));
        assert_eq!(values.value(1), serialize_exception(&exception));
        assert_eq!(
            values.value(2),
            r#"{"code":0,"message":"","name":"","stack":[]}"#
        );
        let sequence = vec![
            antelope::AuthSequence {
                account_name: odd,
                sequence: u64::MAX,
            },
            antelope::AuthSequence::default(),
        ];
        append_auth_sequence(&mut builder, &sequence);
        append_auth_sequence(&mut builder, &[]);
        append_auth_sequence(&mut builder, &sequence);
        let values = builder.finish();
        assert_eq!(values.value(0), serialize_auth_sequence(&sequence).unwrap());
        assert!(values.is_null(1));
        assert_eq!(values.value(0), values.value(2));
    }

    #[test]
    fn authorization_preserves_delimiters_empty_entries_and_builder_reset() {
        let mut builder = StringBuilder::new();
        append_authorization(&mut builder, &[]);
        append_authorization(
            &mut builder,
            &[
                antelope::PermissionLevel::default(),
                antelope::PermissionLevel {
                    actor: "雪".into(),
                    permission: "active".into(),
                },
            ],
        );
        append_authorization(&mut builder, &[]);
        let values = builder.finish();
        assert!(values.is_null(0) && values.is_null(2));
        assert_eq!(values.value(1), "@,雪@active");
        append_authorization(
            &mut builder,
            &[antelope::PermissionLevel {
                actor: "alice".into(),
                permission: "owner".into(),
            }],
        );
        assert_eq!(builder.finish().value(0), "alice@owner");
    }
}
