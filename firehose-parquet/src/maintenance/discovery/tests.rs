//! The whole-object read of `inspect`.
use super::*;

#[tokio::test]
async fn whole_object_reads_return_the_bytes_or_the_store_error() {
    use object_store::memory::InMemory;
    let store = InMemory::new();
    let key = ObjectPath::from("root/space %/part.parquet");
    store
        .put(&key, bytes::Bytes::from_static(b"part").into())
        .await
        .unwrap();
    assert_eq!(
        read_object_bytes(&store, &key).await.unwrap(),
        store.get(&key).await.unwrap().bytes().await.unwrap()
    );
    let missing = ObjectPath::from("root/missing");
    let error = read_object_bytes(&store, &missing).await.unwrap_err();
    assert!(matches!(error, object_store::Error::NotFound { .. }));
    assert_eq!(
        error.to_string(),
        store.get(&missing).await.unwrap_err().to_string()
    );
}
