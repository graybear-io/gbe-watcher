use bytes::Bytes;
use flate2::Compression;
use flate2::write::GzEncoder;
use redis::streams::StreamReadReply;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

use gbe_nexus::{dedup_id, EventEmitter, Transport};

use crate::archive_writer::ArchiveWriter;
use crate::error::WatcherError;

#[derive(Debug, Clone)]
pub struct ArchiverConfig {
    pub redis_url: String,
    pub group: String,
    pub streams: Vec<ArchivalStream>,
}

impl Default for ArchiverConfig {
    fn default() -> Self {
        Self {
            redis_url: "redis://127.0.0.1:6379".to_string(),
            group: "archiver".to_string(),
            streams: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArchivalStream {
    pub subject: String,
    pub domain: String,
    pub batch_size: u32,
    pub batch_timeout: Duration,
}

impl Default for ArchivalStream {
    fn default() -> Self {
        Self {
            subject: String::new(),
            domain: String::new(),
            batch_size: 1000,
            batch_timeout: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Default)]
pub struct BatchReport {
    pub messages: u32,
    pub bytes_written: u64,
    pub path: Option<String>,
}

pub struct Archiver {
    config: ArchiverConfig,
    writer: Arc<dyn ArchiveWriter>,
    conn: redis::aio::ConnectionManager,
    consumer_id: String,
    emitter: EventEmitter,
}

impl Archiver {
    /// # Errors
    /// Returns `WatcherError::Redis` if the Redis connection fails.
    pub async fn new(
        config: ArchiverConfig,
        writer: Arc<dyn ArchiveWriter>,
        transport: Arc<dyn Transport>,
    ) -> Result<Self, WatcherError> {
        let client = redis::Client::open(config.redis_url.as_str())
            .map_err(|e| WatcherError::Redis(e.to_string()))?;
        let conn = redis::aio::ConnectionManager::new(client)
            .await
            .map_err(|e| WatcherError::Redis(e.to_string()))?;

        let host = hostname::get().map_or_else(
            |_| "unknown".to_string(),
            |h| h.to_string_lossy().to_string(),
        );
        let consumer_id = format!("{host}-{}", ulid::Ulid::new());
        let emitter = EventEmitter::new(transport.clone(), "archiver", &consumer_id);

        Ok(Self {
            config,
            writer,
            conn,
            consumer_id,
            emitter,
        })
    }

    #[must_use]
    pub fn config(&self) -> &ArchiverConfig {
        &self.config
    }

    /// # Errors
    /// Returns `WatcherError` if a batch processing or stream operation fails.
    pub async fn run(&self, token: CancellationToken) -> Result<(), WatcherError> {
        for stream in &self.config.streams {
            self.ensure_group(&subject_to_key(&stream.subject)).await?;
        }

        loop {
            if token.is_cancelled() {
                // Flush partial batches before exit
                for stream in &self.config.streams {
                    let _ = self.process_batch(stream).await;
                }
                tracing::info!("archiver shutting down");
                return Ok(());
            }

            for stream in &self.config.streams {
                match self.process_batch(stream).await {
                    Ok(report) if report.messages > 0 => {
                        tracing::info!(
                            domain = %stream.domain,
                            messages = report.messages,
                            bytes = report.bytes_written,
                            path = ?report.path,
                            "archived batch"
                        );
                    }
                    Err(e) => {
                        tracing::error!(domain = %stream.domain, "archive batch failed: {e}");
                    }
                    _ => {}
                }
            }

            tokio::select! {
                () = token.cancelled() => {}
                () = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }

    /// # Errors
    /// Returns `WatcherError` if reading, compressing, writing, or acking fails.
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::cast_possible_truncation)] // batch sizes are small, usize->u32 is safe
    pub async fn process_batch(
        &self,
        stream: &ArchivalStream,
    ) -> Result<BatchReport, WatcherError> {
        let stream_key = subject_to_key(&stream.subject);
        let mut batch_lines: Vec<String> = Vec::new();
        let mut entry_ids: Vec<String> = Vec::new();
        let batch_start = Instant::now();

        // Read messages until batch is full or timeout
        while (batch_lines.len() as u32) < stream.batch_size {
            let remaining = stream
                .batch_timeout
                .checked_sub(batch_start.elapsed())
                .unwrap_or(Duration::ZERO);

            if remaining.is_zero() && !batch_lines.is_empty() {
                break;
            }

            let block_ms = if batch_lines.is_empty() {
                // First read: short block so process_batch returns quickly when empty
                500_u64
            } else {
                remaining.as_millis().min(2000) as u64
            };

            let count = stream.batch_size - batch_lines.len() as u32;

            let result: Result<StreamReadReply, _> = redis::cmd("XREADGROUP")
                .arg("GROUP")
                .arg(&self.config.group)
                .arg(&self.consumer_id)
                .arg("COUNT")
                .arg(count)
                .arg("BLOCK")
                .arg(block_ms)
                .arg("STREAMS")
                .arg(&stream_key)
                .arg(">")
                .query_async(&mut self.conn.clone())
                .await;

            match result {
                Ok(reply) => {
                    for key in &reply.keys {
                        for entry in &key.ids {
                            let envelope_json: Option<String> = entry.get("envelope");
                            if let Some(json) = envelope_json {
                                batch_lines.push(json);
                                entry_ids.push(entry.id.clone());
                            }
                        }
                    }
                }
                Err(e) => {
                    // Timeout (nil reply) is normal
                    if !is_timeout_nil(&e) {
                        return Err(WatcherError::Redis(e.to_string()));
                    }
                    // On timeout with no messages, return empty
                    if batch_lines.is_empty() {
                        return Ok(BatchReport::default());
                    }
                    // On timeout with partial batch, flush what we have
                    break;
                }
            }
        }

        if batch_lines.is_empty() {
            return Ok(BatchReport::default());
        }

        // Compress to gzipped JSONL
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        for line in &batch_lines {
            encoder
                .write_all(line.as_bytes())
                .map_err(|e| WatcherError::Writer(e.to_string()))?;
            encoder
                .write_all(b"\n")
                .map_err(|e| WatcherError::Writer(e.to_string()))?;
        }
        let compressed = encoder
            .finish()
            .map_err(|e| WatcherError::Writer(e.to_string()))?;

        let path = make_archive_path(&stream.domain);
        let bytes_written = compressed.len() as u64;

        // Write to storage
        self.writer.write(&path, Bytes::from(compressed)).await?;

        // ACK all messages after successful write
        if !entry_ids.is_empty() {
            let mut cmd = redis::cmd("XACK");
            cmd.arg(&stream_key).arg(&self.config.group);
            for id in &entry_ids {
                cmd.arg(id);
            }
            cmd.query_async::<()>(&mut self.conn.clone())
                .await
                .map_err(|e| WatcherError::Redis(e.to_string()))?;
        }

        // Publish archival event
        let event = serde_json::json!({
            "domain": stream.domain,
            "count": batch_lines.len(),
            "path": path,
            "source": "archiver",
        });
        let dedup = dedup_id(
            self.emitter.component(),
            self.emitter.instance_id(),
            "archive-batch",
        );
        let _ = self
            .emitter
            .emit("gbe.events.system.archive", 1, dedup, event)
            .await;

        Ok(BatchReport {
            messages: batch_lines.len() as u32,
            bytes_written,
            path: Some(path),
        })
    }

    async fn ensure_group(&self, stream_key: &str) -> Result<(), WatcherError> {
        let result: Result<String, redis::RedisError> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(stream_key)
            .arg(&self.config.group)
            .arg("0")
            .arg("MKSTREAM")
            .query_async(&mut self.conn.clone())
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) if e.to_string().contains("BUSYGROUP") => Ok(()),
            Err(e) => Err(WatcherError::Redis(e.to_string())),
        }
    }
}

fn subject_to_key(subject: &str) -> String {
    subject.replace('.', ":")
}

fn make_archive_path(domain: &str) -> String {
    let now = chrono_free_date();
    let batch_id = ulid::Ulid::new().to_string().to_lowercase();
    format!("{domain}/{now}/{batch_id}.jsonl.gz")
}

/// Date path component without chrono dependency.
fn chrono_free_date() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    // Days since epoch
    let days = secs / 86400;
    // Gregorian calendar calculation
    let (y, m, d) = days_to_ymd(days);
    format!("{y:04}/{m:02}/{d:02}")
}

fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    // Algorithm from https://howardhinnant.github.io/date_algorithms.html
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

fn is_timeout_nil(e: &redis::RedisError) -> bool {
    matches!(e.kind(), redis::ErrorKind::TypeError)
}
