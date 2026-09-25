//! Actual signed AmazonS3 reads against a loopback endpoint, without remote data.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn native_reads_pin_range_and_version_on_safe_retries_and_refuse_ignored_conditions() {
    // Success, ignored range, changed version, 503, lost headers, truncated body.
    for mode in 0..6 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            loop {
                let accepted = tokio::select! { _ = &mut stopped => break, socket = listener.accept() => socket.unwrap() };
                let (mut socket, _) = accepted;
                let mut bytes = Vec::new();
                loop {
                    let mut chunk = [0; 4096];
                    let count = socket.read(&mut chunk).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&chunk[..count]);
                    assert!(bytes.len() < 16384);
                    if bytes.windows(4).any(|value| value == b"\r\n\r\n") {
                        break;
                    }
                }
                let header = String::from_utf8(bytes).unwrap();
                let attempt = {
                    let mut requests = captured.lock().unwrap();
                    requests.push(header);
                    requests.len()
                };
                if mode == 4 && attempt == 1 {
                    socket.shutdown().await.unwrap();
                    continue;
                }
                let (status, body, content_range, version) = if mode == 3 && attempt == 1 {
                    (
                        503,
                        "<Error><Code>ServiceUnavailable</Code></Error>",
                        "",
                        "fixture-v1",
                    )
                } else if mode == 1 {
                    (200, "0123456789", "", "fixture-v1")
                } else {
                    (
                        206,
                        "234567",
                        "Content-Range: bytes 2-7/10\r\n",
                        if mode == 2 {
                            "changed-v2"
                        } else {
                            "fixture-v1"
                        },
                    )
                };
                let response = format!("HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\n{content_range}ETag: \"fixture-etag\"\r\nx-amz-version-id: {version}\r\nLast-Modified: Fri, 25 Sep 2026 00:00:00 GMT\r\nConnection: close\r\n\r\n{body}",body.len());
                let bytes = response.as_bytes();
                let bytes = if mode == 5 && attempt == 1 {
                    &bytes[..bytes.len() - 3]
                } else {
                    bytes
                };
                socket.write_all(bytes).await.unwrap();
                socket.shutdown().await.unwrap();
            }
        });
        let aws = crate::cli::AwsConfig {
            aws_access_key_id: Some("synthetic-key".into()),
            aws_secret_access_key: Some("synthetic-secret".into()),
            aws_session_token: None,
            aws_region: Some("us-east-1".into()),
            aws_endpoint_url: Some(endpoint),
        };
        let client: Arc<dyn ObjectStore> = Arc::new(
            aws.s3_client_builder("bucket", true)
                .unwrap()
                .with_allow_http(true)
                .build()
                .unwrap(),
        );
        let object = ObjectMeta {
            location: Path::from("blocks/source.parquet"),
            size: 10,
            e_tag: Some("\"fixture-etag\"".into()),
            version: Some("fixture-v1".into()),
            last_modified: std::time::SystemTime::UNIX_EPOCH.into(),
        };
        let result = pinned_range_with(
            &client,
            &object,
            2..8,
            2,
            Duration::from_secs(1),
            Duration::ZERO,
        )
        .await;
        if matches!(mode, 1 | 2) {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap().as_ref(), b"234567");
        }
        {
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), if mode == 1 || mode >= 3 { 2 } else { 1 });
            for header in requests.iter() {
                let lines = header.to_ascii_lowercase();
                assert!(
                    lines.starts_with("get /bucket/blocks/source.parquet?versionid=fixture-v1 ")
                );
                assert!(lines.contains("\r\nrange: bytes=2-7\r\n"));
                assert!(lines.contains("\r\nif-match: \"fixture-etag\"\r\n"));
                assert!(lines.contains("authorization: aws4-hmac-sha256 "));
            }
        }
        stop.send(()).unwrap();
        server.await.unwrap();
    }
}
