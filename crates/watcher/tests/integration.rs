//! Integration tests for the watcher and archiver.
//!
//! Requires a running Redis instance. Set `REDIS_URL` to enable these tests.
//! Run with: `REDIS_URL=redis://localhost:6379` cargo test --package gbe-watcher

use bytes::Bytes;
use flate2::read::GzDecoder;
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use gbe_nexus::Transport;
use gbe_nexus_redis::{RedisTransport, RedisTransportConfig};
use gbe_state_store::{StateStore, StateStoreConfig};
use gbe_state_store_redis::RedisStateStore;
use gbe_watcher::{
    ArchivalStream, Archiver, ArchiverConfig, DistributedLock, FsArchiveWriter, StreamRetention,
    Watcher, WatcherConfig,
};

fn redis_url() -> Option<String> {
    std::env::var("REDIS_URL").ok()
}

async fn connect_transport() -> RedisTransport {
    let url = redis_url().unwrap();
    RedisTransport::connect(RedisTransportConfig {
        url,
        ..Default::default()
    })
    .await
    .unwrap()
}

async fn connect_store() -> RedisStateStore {
    let url = redis_url().unwrap();
    RedisStateStore::connect(StateStoreConfig { url })
        .await
        .unwrap()
}

fn test_prefix(name: &str) -> String {
    format!(
        "gbe:test:sweep:{}:{}:",
        name,
        ulid::Ulid::new().to_string().to_lowercase()
    )
}

async fn cleanup_keys(keys: &[&str]) {
    let url = redis_url().unwrap();
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    for key in keys {
        let _: Result<(), _> = redis::cmd("DEL").arg(*key).query_async(&mut conn).await;
    }
}

#[allow(clippy::cast_possible_truncation)] // millis since epoch fits in u64 until year 584556
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn make_stuck_record(state: &str, updated_at: u64, retry_count: u32) -> HashMap<String, Bytes> {
    let mut fields = HashMap::new();
    fields.insert("state".to_string(), Bytes::from(state.to_string()));
    fields.insert(
        "updated_at".to_string(),
        Bytes::from(updated_at.to_string()),
    );
    fields.insert(
        "retry_count".to_string(),
        Bytes::from(retry_count.to_string()),
    );
    fields.insert("task_type".to_string(), Bytes::from("email-send"));
    fields
}

// --- Lock tests ---

#[tokio::test]
async fn test_lock_acquire_release() {
    if redis_url().is_none() {
        return;
    }
    let url = redis_url().unwrap();
    let lock_key = format!(
        "gbe:test:lock:{}",
        ulid::Ulid::new().to_string().to_lowercase()
    );

    let lock1 = DistributedLock::new(&url, lock_key.clone(), Duration::from_secs(10))
        .await
        .unwrap();
    let lock2 = DistributedLock::new(&url, lock_key.clone(), Duration::from_secs(10))
        .await
        .unwrap();

    // First acquire succeeds
    assert!(lock1.acquire().await.unwrap());

    // Second acquire fails (lock held)
    assert!(!lock2.acquire().await.unwrap());

    // Release and re-acquire
    lock1.release().await.unwrap();
    assert!(lock2.acquire().await.unwrap());

    lock2.release().await.unwrap();
    cleanup_keys(&[&lock_key]).await;
}

#[tokio::test]
async fn test_lock_expires() {
    if redis_url().is_none() {
        return;
    }
    let url = redis_url().unwrap();
    let lock_key = format!(
        "gbe:test:lock:exp:{}",
        ulid::Ulid::new().to_string().to_lowercase()
    );

    let lock1 = DistributedLock::new(&url, lock_key.clone(), Duration::from_secs(1))
        .await
        .unwrap();
    let lock2 = DistributedLock::new(&url, lock_key.clone(), Duration::from_secs(10))
        .await
        .unwrap();

    assert!(lock1.acquire().await.unwrap());
    assert!(!lock2.acquire().await.unwrap());

    // Wait for TTL expiry
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Now lock2 can acquire
    assert!(lock2.acquire().await.unwrap());

    lock2.release().await.unwrap();
    cleanup_keys(&[&lock_key]).await;
}

// --- Sweep tests ---

async fn make_watcher(
    prefix: &str,
    transport: Arc<dyn Transport>,
    store: Arc<dyn StateStore>,
) -> Watcher {
    let url = redis_url().unwrap();
    Watcher::new(
        WatcherConfig {
            redis_url: url,
            interval: Duration::from_secs(30),
            lock_ttl: Duration::from_secs(60),
            stuck_threshold: Duration::from_secs(60),
            max_retries: 3,
            state_prefix: prefix.to_string(),
            streams: Vec::new(),
        },
        transport,
        store,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn test_sweep_retries_stuck_job() {
    if redis_url().is_none() {
        return;
    }
    let store = connect_store().await;
    let transport = connect_transport().await;
    let prefix = test_prefix("retry");
    let key = format!("{prefix}email-send:job1");

    // Create a stuck job: updated 10 minutes ago, state=processing, retry_count=0
    let old_time = now_millis() - 600_000;
    store
        .set_fields(&key, make_stuck_record("processing", old_time, 0))
        .await
        .unwrap();

    // Ensure the queue stream exists for the retry publish
    let subject = "gbe.tasks.email-send.queue".to_string();
    transport
        .ensure_stream(gbe_nexus::StreamConfig {
            subject: subject.clone(),
            max_age: Duration::from_secs(3600),
            max_bytes: None,
            max_msgs: None,
        })
        .await
        .unwrap();

    let sweeper = make_watcher(&prefix, Arc::new(transport), Arc::new(store)).await;
    let report = sweeper.sweep_once().await.unwrap();

    assert_eq!(report.retried, 1);
    assert_eq!(report.failed, 0);

    // Verify state was updated
    let store2 = connect_store().await;
    let record = store2.get(&key).await.unwrap().unwrap();
    assert_eq!(record.fields.get("state").unwrap().as_ref(), b"pending");
    assert_eq!(record.fields.get("retry_count").unwrap().as_ref(), b"1");

    cleanup_keys(&[&key]).await;
    let stream_key = subject.replace('.', ":");
    cleanup_keys(&[stream_key.as_str()]).await;
}

#[tokio::test]
async fn test_sweep_fails_exhausted_job() {
    if redis_url().is_none() {
        return;
    }
    let store = connect_store().await;
    let transport = connect_transport().await;
    let prefix = test_prefix("exhaust");
    let key = format!("{prefix}email-send:job2");

    // retry_count=3 with max_retries=3 → should fail
    let old_time = now_millis() - 600_000;
    store
        .set_fields(&key, make_stuck_record("processing", old_time, 3))
        .await
        .unwrap();

    // Ensure error stream exists
    transport
        .ensure_stream(gbe_nexus::StreamConfig {
            subject: "gbe.events.system.error".to_string(),
            max_age: Duration::from_secs(3600),
            max_bytes: None,
            max_msgs: None,
        })
        .await
        .unwrap();

    let sweeper = make_watcher(&prefix, Arc::new(transport), Arc::new(store)).await;
    let report = sweeper.sweep_once().await.unwrap();

    assert_eq!(report.retried, 0);
    assert_eq!(report.failed, 1);

    // Verify state changed to failed
    let store2 = connect_store().await;
    let record = store2.get(&key).await.unwrap().unwrap();
    assert_eq!(record.fields.get("state").unwrap().as_ref(), b"failed");
    assert!(record.fields.contains_key("failed_at"));

    cleanup_keys(&[&key]).await;
    cleanup_keys(&["gbe:events:system:error"]).await;
}

#[tokio::test]
async fn test_sweep_skips_terminal_states() {
    if redis_url().is_none() {
        return;
    }
    let store = connect_store().await;
    let transport = connect_transport().await;
    let prefix = test_prefix("terminal");
    let key = format!("{prefix}email-send:job3");

    // completed job with old updated_at — should be skipped
    let old_time = now_millis() - 600_000;
    store
        .set_fields(&key, make_stuck_record("completed", old_time, 0))
        .await
        .unwrap();

    let sweeper = make_watcher(&prefix, Arc::new(transport), Arc::new(store)).await;
    let report = sweeper.sweep_once().await.unwrap();

    assert_eq!(report.retried, 0);
    assert_eq!(report.failed, 0);

    // Verify state unchanged
    let store2 = connect_store().await;
    let record = store2.get(&key).await.unwrap().unwrap();
    assert_eq!(record.fields.get("state").unwrap().as_ref(), b"completed");

    cleanup_keys(&[&key]).await;
}

#[tokio::test]
async fn test_stream_trimming() {
    if redis_url().is_none() {
        return;
    }
    let transport = connect_transport().await;
    let subject = format!(
        "gbe.test.trim.{}",
        ulid::Ulid::new().to_string().to_lowercase()
    );

    transport
        .ensure_stream(gbe_nexus::StreamConfig {
            subject: subject.clone(),
            max_age: Duration::from_secs(3600),
            max_bytes: None,
            max_msgs: None,
        })
        .await
        .unwrap();

    // Publish some messages
    for i in 0..5 {
        transport
            .publish(&subject, Bytes::from(format!("msg-{i}")), None)
            .await
            .unwrap();
    }

    // Trim with max_age=0 should remove everything (approximately)
    let trimmed = transport
        .trim_stream(&subject, Duration::from_secs(0))
        .await
        .unwrap();

    // XTRIM with ~ is approximate, but with only 5 messages it should trim most/all
    // We just verify it returns without error and the count is reasonable
    assert!(trimmed <= 5);

    let stream_key = subject.replace('.', ":");
    cleanup_keys(&[stream_key.as_str()]).await;
}

#[tokio::test]
async fn test_sweep_once_full_cycle() {
    if redis_url().is_none() {
        return;
    }
    let store = connect_store().await;
    let transport = connect_transport().await;
    let prefix = test_prefix("full");
    let key1 = format!("{prefix}email-send:stuck1");
    let key2 = format!("{prefix}email-send:exhausted1");
    let key3 = format!("{prefix}email-send:done1");

    let old_time = now_millis() - 600_000;

    // stuck job (will be retried)
    store
        .set_fields(&key1, make_stuck_record("processing", old_time, 1))
        .await
        .unwrap();

    // exhausted job (will be failed)
    store
        .set_fields(&key2, make_stuck_record("claimed", old_time, 3))
        .await
        .unwrap();

    // completed job (will be skipped)
    store
        .set_fields(&key3, make_stuck_record("completed", old_time, 0))
        .await
        .unwrap();

    // Ensure streams exist
    for subject in &["gbe.tasks.email-send.queue", "gbe.events.system.error"] {
        transport
            .ensure_stream(gbe_nexus::StreamConfig {
                subject: subject.to_string(),
                max_age: Duration::from_secs(3600),
                max_bytes: None,
                max_msgs: None,
            })
            .await
            .unwrap();
    }

    let url = redis_url().unwrap();
    let trim_subject = format!(
        "gbe.test.trimfull.{}",
        ulid::Ulid::new().to_string().to_lowercase()
    );
    transport
        .ensure_stream(gbe_nexus::StreamConfig {
            subject: trim_subject.clone(),
            max_age: Duration::from_secs(3600),
            max_bytes: None,
            max_msgs: None,
        })
        .await
        .unwrap();
    transport
        .publish(&trim_subject, Bytes::from("old"), None)
        .await
        .unwrap();

    let sweeper = Watcher::new(
        WatcherConfig {
            redis_url: url,
            interval: Duration::from_secs(30),
            lock_ttl: Duration::from_secs(60),
            stuck_threshold: Duration::from_secs(60),
            max_retries: 3,
            state_prefix: prefix.clone(),
            streams: vec![StreamRetention {
                subject: trim_subject.clone(),
                max_age: Duration::from_secs(0),
            }],
        },
        Arc::new(transport),
        Arc::new(store),
    )
    .await
    .unwrap();

    let report = sweeper.sweep_once().await.unwrap();

    assert_eq!(report.retried, 1, "expected 1 retried job");
    assert_eq!(report.failed, 1, "expected 1 failed job");
    assert_eq!(report.streams_trimmed, 1, "expected 1 stream trimmed");

    cleanup_keys(&[&key1, &key2, &key3]).await;
    let trim_key = trim_subject.replace('.', ":");
    cleanup_keys(&[
        trim_key.as_str(),
        "gbe:tasks:email-send:queue",
        "gbe:events:system:error",
    ])
    .await;
}

// --- Archiver tests ---

fn test_subject(name: &str) -> String {
    format!(
        "gbe.test.archive.{}.{}",
        name,
        ulid::Ulid::new().to_string().to_lowercase()
    )
}

async fn make_archiver(
    subject: &str,
    domain: &str,
    batch_size: u32,
    writer: Arc<dyn gbe_watcher::ArchiveWriter>,
    transport: Arc<dyn Transport>,
) -> Archiver {
    let url = redis_url().unwrap();
    Archiver::new(
        ArchiverConfig {
            redis_url: url,
            group: format!(
                "test-archiver-{}",
                ulid::Ulid::new().to_string().to_lowercase()
            ),
            streams: vec![ArchivalStream {
                subject: subject.to_string(),
                domain: domain.to_string(),
                batch_size,
                batch_timeout: Duration::from_secs(5),
            }],
        },
        writer,
        transport,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn test_archive_batch_writes_jsonl_gz() {
    if redis_url().is_none() {
        return;
    }
    let transport = connect_transport().await;
    let subject = test_subject("write");

    transport
        .ensure_stream(gbe_nexus::StreamConfig {
            subject: subject.clone(),
            max_age: Duration::from_secs(3600),
            max_bytes: None,
            max_msgs: None,
        })
        .await
        .unwrap();

    // Publish 3 messages
    for i in 0..3 {
        transport
            .publish(&subject, Bytes::from(format!("payload-{i}")), None)
            .await
            .unwrap();
    }

    let tmp_dir = tempfile::tempdir().unwrap();
    let writer = Arc::new(FsArchiveWriter::new(tmp_dir.path()));
    let transport = Arc::new(transport);

    let archiver = make_archiver(&subject, "audit", 3, writer, transport.clone()).await;
    let report = archiver
        .process_batch(&archiver.config().streams[0])
        .await
        .unwrap();

    assert_eq!(report.messages, 3);
    assert!(report.path.is_some());

    // Read and decompress the file
    let path = tmp_dir.path().join(report.path.as_ref().unwrap());
    let compressed = std::fs::read(&path).unwrap();
    let mut decoder = GzDecoder::new(&compressed[..]);
    let mut jsonl = String::new();
    decoder.read_to_string(&mut jsonl).unwrap();

    let lines: Vec<&str> = jsonl.lines().collect();
    assert_eq!(lines.len(), 3);

    // Each line should be valid JSON with envelope fields
    for line in &lines {
        let val: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(val.get("message_id").is_some());
        assert!(val.get("subject").is_some());
        assert!(val.get("timestamp").is_some());
        assert!(val.get("payload").is_some());
    }

    let stream_key = subject.replace('.', ":");
    cleanup_keys(&[stream_key.as_str()]).await;
}

#[tokio::test]
async fn test_archive_batch_acks_after_write() {
    if redis_url().is_none() {
        return;
    }
    let transport = connect_transport().await;
    let subject = test_subject("ack");

    transport
        .ensure_stream(gbe_nexus::StreamConfig {
            subject: subject.clone(),
            max_age: Duration::from_secs(3600),
            max_bytes: None,
            max_msgs: None,
        })
        .await
        .unwrap();

    for i in 0..3 {
        transport
            .publish(&subject, Bytes::from(format!("ack-{i}")), None)
            .await
            .unwrap();
    }

    let tmp_dir = tempfile::tempdir().unwrap();
    let writer = Arc::new(FsArchiveWriter::new(tmp_dir.path()));
    let transport = Arc::new(transport);

    let archiver = make_archiver(&subject, "audit", 3, writer, transport.clone()).await;
    let report = archiver
        .process_batch(&archiver.config().streams[0])
        .await
        .unwrap();
    assert_eq!(report.messages, 3);

    // Verify no pending messages in the consumer group
    let url = redis_url().unwrap();
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let stream_key = subject.replace('.', ":");

    let result: redis::Value = redis::cmd("XPENDING")
        .arg(&stream_key)
        .arg(archiver.config().group.as_str())
        .query_async(&mut conn)
        .await
        .unwrap();

    if let redis::Value::Array(arr) = &result
        && let Some(redis::Value::Int(pending)) = arr.first()
    {
        assert_eq!(*pending, 0, "expected 0 pending messages after ack");
    }

    cleanup_keys(&[stream_key.as_str()]).await;
}

#[tokio::test]
async fn test_archive_skips_empty_stream() {
    if redis_url().is_none() {
        return;
    }
    let transport = connect_transport().await;
    let subject = test_subject("empty");

    transport
        .ensure_stream(gbe_nexus::StreamConfig {
            subject: subject.clone(),
            max_age: Duration::from_secs(3600),
            max_bytes: None,
            max_msgs: None,
        })
        .await
        .unwrap();

    let tmp_dir = tempfile::tempdir().unwrap();
    let writer = Arc::new(FsArchiveWriter::new(tmp_dir.path()));
    let transport = Arc::new(transport);

    let archiver = make_archiver(&subject, "audit", 10, writer, transport.clone()).await;
    let report = archiver
        .process_batch(&archiver.config().streams[0])
        .await
        .unwrap();

    assert_eq!(report.messages, 0);
    assert!(report.path.is_none());

    let stream_key = subject.replace('.', ":");
    cleanup_keys(&[stream_key.as_str()]).await;
}

#[tokio::test]
async fn test_archive_path_format() {
    if redis_url().is_none() {
        return;
    }
    let transport = connect_transport().await;
    let subject = test_subject("path");

    transport
        .ensure_stream(gbe_nexus::StreamConfig {
            subject: subject.clone(),
            max_age: Duration::from_secs(3600),
            max_bytes: None,
            max_msgs: None,
        })
        .await
        .unwrap();

    transport
        .publish(&subject, Bytes::from("test"), None)
        .await
        .unwrap();

    let tmp_dir = tempfile::tempdir().unwrap();
    let writer = Arc::new(FsArchiveWriter::new(tmp_dir.path()));
    let transport = Arc::new(transport);

    let archiver = make_archiver(&subject, "audit", 1, writer, transport.clone()).await;
    let report = archiver
        .process_batch(&archiver.config().streams[0])
        .await
        .unwrap();

    let path = report.path.unwrap();
    // Should match: audit/YYYY/MM/DD/{ulid}.jsonl.gz
    assert!(
        path.starts_with("audit/"),
        "path should start with domain: {path}"
    );
    assert!(
        path.ends_with(".jsonl.gz"),
        "path should end with .jsonl.gz: {path}"
    );

    // Check date components are present (YYYY/MM/DD)
    let parts: Vec<&str> = path.split('/').collect();
    assert_eq!(parts.len(), 5, "expected 5 path segments: {path}");
    assert_eq!(parts[0], "audit");
    assert_eq!(parts[1].len(), 4, "year should be 4 digits");
    assert_eq!(parts[2].len(), 2, "month should be 2 digits");
    assert_eq!(parts[3].len(), 2, "day should be 2 digits");

    let stream_key = subject.replace('.', ":");
    cleanup_keys(&[stream_key.as_str()]).await;
}

#[tokio::test]
async fn test_archive_multiple_batches() {
    if redis_url().is_none() {
        return;
    }
    let transport = connect_transport().await;
    let subject = test_subject("multi");

    transport
        .ensure_stream(gbe_nexus::StreamConfig {
            subject: subject.clone(),
            max_age: Duration::from_secs(3600),
            max_bytes: None,
            max_msgs: None,
        })
        .await
        .unwrap();

    // Publish 6 messages, batch_size=3 → should produce 2 batches
    for i in 0..6 {
        transport
            .publish(&subject, Bytes::from(format!("multi-{i}")), None)
            .await
            .unwrap();
    }

    let tmp_dir = tempfile::tempdir().unwrap();
    let writer = Arc::new(FsArchiveWriter::new(tmp_dir.path()));
    let transport = Arc::new(transport);

    let archiver = make_archiver(&subject, "audit", 3, writer, transport.clone()).await;

    let stream_cfg = &archiver.config().streams[0];
    let report1 = archiver.process_batch(stream_cfg).await.unwrap();
    let report2 = archiver.process_batch(stream_cfg).await.unwrap();

    assert_eq!(report1.messages, 3);
    assert_eq!(report2.messages, 3);

    // Both should have different paths
    assert_ne!(report1.path, report2.path);

    let stream_key = subject.replace('.', ":");
    cleanup_keys(&[stream_key.as_str()]).await;
}
