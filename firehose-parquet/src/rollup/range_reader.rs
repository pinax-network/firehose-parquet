//! Page-oriented Parquet reads through the repository's existing object_store.
//! Unlike the async Parquet stream, the synchronous page reader does not need
//! to retain all selected column chunks of a row group at once.

use bytes::Bytes;
use object_store::{ObjectMeta, ObjectStore};
use parquet::errors::{ParquetError, Result};
use parquet::file::reader::{ChunkReader, Length};
use std::io::Read;
use std::ops::Range;
use std::sync::Arc;

const HEADER_BUFFER_BYTES: u64 = 64 * 1024;

#[derive(Clone)]
pub(super) struct RangeReader {
    store: Arc<dyn ObjectStore>,
    snapshot: ObjectMeta,
}

impl RangeReader {
    pub(super) fn new(store: Arc<dyn ObjectStore>, snapshot: ObjectMeta) -> Self {
        Self { store, snapshot }
    }

    /// Reads `range` of the pinned snapshot with merge's bounded read-only retries: transient
    /// request, body and timeout failures are retried against the same version; a changed
    /// object or a mismatching response fails immediately.
    fn fetch(&self, range: Range<u64>) -> Result<Bytes> {
        crate::cli::block_on_async(crate::merge::read::pinned_range(
            &self.store,
            &self.snapshot,
            range,
        ))
        .map_err(|error| ParquetError::General(format!("{error:#}")))
    }
}

impl Length for RangeReader {
    fn len(&self) -> u64 {
        self.snapshot.size
    }
}
impl ChunkReader for RangeReader {
    type T = RangeCursor;
    fn get_read(&self, start: u64) -> Result<Self::T> {
        if start > self.len() {
            return Err(ParquetError::General(
                "source reader starts beyond the listed object".into(),
            ));
        }
        Ok(RangeCursor {
            source: self.clone(),
            offset: start,
            buffer: Bytes::new(),
        })
    }
    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes> {
        let end = start
            .checked_add(u64::try_from(length).map_err(|_| {
                ParquetError::General("source range length is not representable".into())
            })?)
            .ok_or_else(|| ParquetError::General("source range overflow".into()))?;
        self.fetch(start..end)
    }
}

pub(super) struct RangeCursor {
    source: RangeReader,
    offset: u64,
    buffer: Bytes,
}
impl Read for RangeCursor {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if output.is_empty() || self.offset == self.source.len() {
            return Ok(0);
        }
        if self.buffer.is_empty() {
            let end = self
                .offset
                .saturating_add(HEADER_BUFFER_BYTES)
                .min(self.source.len());
            self.buffer = self
                .source
                .fetch(self.offset..end)
                .map_err(std::io::Error::other)?;
        }
        let count = output.len().min(self.buffer.len());
        output[..count].copy_from_slice(&self.buffer[..count]);
        self.buffer = self.buffer.slice(count..);
        self.offset += count as u64;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::stream::{self, BoxStream};
    use futures::StreamExt;
    use object_store::{
        memory::InMemory, path::Path, GetOptions, GetRange, GetResult, GetResultPayload,
        ListResult, MultipartUpload, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    struct TracedStore {
        inner: InMemory,
        requests: Mutex<Vec<GetOptions>>,
        fault: Mutex<u8>,
    }
    impl std::fmt::Display for TracedStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("range-fixture")
        }
    }
    #[async_trait]
    impl ObjectStore for TracedStore {
        async fn get_opts(&self, path: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
            self.requests.lock().unwrap().push(opts.clone());
            let fault = *self.fault.lock().unwrap();
            if fault == 3 {
                // One transient provider failure, then healthy responses.
                *self.fault.lock().unwrap() = 0;
                return Err(object_store::Error::Generic {
                    store: "range-fixture",
                    source: "transient".into(),
                });
            }
            let mut result = self
                .inner
                .get_opts(
                    path,
                    if fault == 1 {
                        GetOptions::default()
                    } else {
                        opts
                    },
                )
                .await?;
            if fault == 2 {
                result.payload = GetResultPayload::Stream(
                    stream::once(async { Ok(Bytes::from_static(b"body-larger-than-request")) })
                        .boxed(),
                );
            }
            Ok(result)
        }
        async fn put_opts(
            &self,
            p: &Path,
            b: PutPayload,
            o: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(p, b, o).await
        }
        async fn delete(&self, p: &Path) -> object_store::Result<()> {
            self.inner.delete(p).await
        }
        async fn put_multipart_opts(
            &self,
            p: &Path,
            o: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(p, o).await
        }
        fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(p)
        }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy(&self, a: &Path, b: &Path) -> object_store::Result<()> {
            self.inner.copy(a, b).await
        }
        async fn copy_if_not_exists(&self, a: &Path, b: &Path) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(a, b).await
        }
    }

    fn object(store: &TracedStore, bytes: Bytes) -> ObjectMeta {
        crate::cli::block_on_async(async {
            let path = Path::from("source.parquet");
            store.inner.put(&path, bytes.into()).await.unwrap();
            store.inner.head(&path).await.unwrap()
        })
    }
    #[test]
    fn parquet_decoding_fetches_pinned_pages_instead_of_a_whole_large_object() {
        use arrow::{
            array::{StringArray, UInt64Array},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use parquet::{
            arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
            file::properties::WriterProperties,
        };
        let rows = 20_000;
        let values: Vec<String> = (0..rows)
            .map(|n| format!("{n:08}-{}", "payload".repeat(20)))
            .collect();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("number", DataType::UInt64, false),
                Field::new("value", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(UInt64Array::from_iter_values(0..rows as u64)),
                Arc::new(StringArray::from(values)),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            Vec::new(),
            batch.schema(),
            Some(
                WriterProperties::builder()
                    .set_dictionary_enabled(false)
                    .set_max_row_group_row_count(Some(4096))
                    .set_data_page_size_limit(32 * 1024)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        let bytes = Bytes::from(writer.into_inner().unwrap());
        assert!(bytes.len() > 1_000_000);
        let store = Arc::new(TracedStore::default());
        let snapshot = object(&store, bytes);
        let reader = ParquetRecordBatchReaderBuilder::try_new(RangeReader::new(
            store.clone(),
            snapshot.clone(),
        ))
        .unwrap()
        .with_batch_size(1024)
        .build()
        .unwrap();
        let actual: Vec<_> = reader.map(|b| b.unwrap()).collect();
        assert!(actual.iter().all(|b| b.num_rows() <= 1024));
        assert_eq!(
            arrow::compute::concat_batches(&batch.schema(), &actual).unwrap(),
            batch
        );
        let requests = store.requests.lock().unwrap();
        assert!(requests.len() > 10);
        for request in requests.iter() {
            let Some(GetRange::Bounded(range)) = &request.range else {
                panic!("unbounded object request")
            };
            assert!(
                range.end - range.start < snapshot.size / 2,
                "request retained an entire source"
            );
            assert_eq!(request.if_match, snapshot.e_tag);
            assert_eq!(request.version, snapshot.version);
        }
    }
    #[test]
    fn unusable_snapshot_versions_are_rejected_before_any_get() {
        let store = Arc::new(TracedStore::default());
        let snapshot = object(&store, Bytes::from_static(b"original-object-bytes"));
        for (etag, version) in [
            (None, None),
            (None, Some("null")),
            (Some("*"), None),
            (Some("*"), Some("version-1")),
            (Some("one,two"), None),
            (Some(""), Some("version-1")),
            (Some("tag\n"), None),
            (None, Some("one,two")),
            (None, Some("*")),
            (Some("valid"), Some("version\r")),
        ] {
            let mut snapshot = snapshot.clone();
            snapshot.e_tag = etag.map(str::to_owned);
            snapshot.version = version.map(str::to_owned);
            assert!(RangeReader::new(store.clone(), snapshot)
                .get_bytes(0, 8)
                .is_err());
        }
        assert!(store.requests.lock().unwrap().is_empty());
    }

    #[test]
    fn stale_ignored_oversized_and_out_of_bounds_ranges_fail_closed() {
        let store = Arc::new(TracedStore::default());
        let snapshot = object(&store, Bytes::from_static(b"original-object-bytes"));
        let reader = RangeReader::new(store.clone(), snapshot);
        assert_eq!(
            reader.get_bytes(0, 8).unwrap(),
            Bytes::from_static(b"original")
        );
        assert!(reader.get_bytes(u64::MAX, 1).is_err());
        assert!(reader.get_bytes(1, usize::MAX).is_err());
        assert!(reader.get_read(reader.len() + 1).is_err());
        let mut no_version = reader.snapshot.clone();
        no_version.e_tag = None;
        no_version.version = None;
        assert!(RangeReader::new(store.clone(), no_version)
            .get_bytes(0, 8)
            .is_err());
        for fault in [1, 2] {
            *store.fault.lock().unwrap() = fault;
            assert!(reader.get_bytes(0, 8).is_err());
        }
        *store.fault.lock().unwrap() = 0;
        object(&store, Bytes::from_static(b"replaced-object-bytes"));
        assert!(reader.get_bytes(0, 8).is_err());
    }

    /// Rollup used a single attempt, so one transient S3 read error aborted the run; reads now
    /// retry the same pinned snapshot like merge.
    #[test]
    fn transient_failures_retry_the_same_pinned_snapshot() {
        let store = Arc::new(TracedStore::default());
        let snapshot = object(&store, Bytes::from_static(b"original-object-bytes"));
        let reader = RangeReader::new(store.clone(), snapshot.clone());
        *store.fault.lock().unwrap() = 3;
        assert_eq!(
            reader.get_bytes(0, 8).unwrap(),
            Bytes::from_static(b"original")
        );
        let requests = store.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(
            |request| request.if_match == snapshot.e_tag && request.version == snapshot.version
        ));
    }
}
