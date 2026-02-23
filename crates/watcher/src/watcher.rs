use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

use gbe_nexus::{EventEmitter, Transport, dedup_id};
use gbe_state_store::{ScanFilter, ScanOp, StateStore};

use crate::config::WatcherConfig;
use crate::error::WatcherError;
use crate::lock::DistributedLock;

const TERMINAL_STATES: &[&str] = &["completed", "failed", "cancelled"];

pub struct Watcher {
    config: WatcherConfig,
    transport: Arc<dyn Transport>,
    store: Arc<dyn StateStore>,
    lock: DistributedLock,
    emitter: EventEmitter,
}

#[derive(Debug, Default)]
pub struct SweepReport {
    pub retried: u32,
    pub failed: u32,
    pub streams_trimmed: u32,
    pub entries_trimmed: u64,
}

impl Watcher {
    /// # Errors
    /// Returns `WatcherError` if the distributed lock connection fails.
    pub async fn new(
        config: WatcherConfig,
        transport: Arc<dyn Transport>,
        store: Arc<dyn StateStore>,
    ) -> Result<Self, WatcherError> {
        let lock = DistributedLock::new(
            &config.redis_url,
            "gbe:lock:watcher".to_string(),
            config.lock_ttl,
        )
        .await?;

        let instance_id = format!("watcher-{}", ulid::Ulid::new().to_string().to_lowercase());
        let emitter = EventEmitter::new(transport.clone(), "watcher", &instance_id);

        Ok(Self {
            config,
            transport,
            store,
            lock,
            emitter,
        })
    }

    /// # Errors
    /// Returns `WatcherError` if a sweep or lock operation fails fatally.
    pub async fn run(&self, token: CancellationToken) -> Result<(), WatcherError> {
        loop {
            tokio::select! {
                () = token.cancelled() => {
                    tracing::info!("watcher shutting down");
                    return Ok(());
                }
                () = tokio::time::sleep(self.config.interval) => {
                    match self.lock.acquire().await {
                        Ok(true) => {
                            let report = match self.sweep_once().await {
                                Ok(r) => r,
                                Err(e) => {
                                    tracing::error!("sweep failed: {e}");
                                    let _ = self.lock.release().await;
                                    continue;
                                }
                            };
                            tracing::info!(
                                retried = report.retried,
                                failed = report.failed,
                                streams = report.streams_trimmed,
                                entries = report.entries_trimmed,
                                "sweep complete"
                            );
                            let sweep_event = serde_json::json!({
                                "retried": report.retried,
                                "failed": report.failed,
                                "streams_trimmed": report.streams_trimmed,
                                "entries_trimmed": report.entries_trimmed,
                                "source": "watcher",
                            });
                            let dedup = dedup_id(
                                self.emitter.component(),
                                self.emitter.instance_id(),
                                "sweep",
                            );
                            if let Err(e) = self.emitter.emit(
                                "gbe.events.system.sweep",
                                1,
                                dedup,
                                sweep_event,
                            ).await {
                                tracing::warn!("failed to emit sweep event: {e}");
                            }
                            let _ = self.lock.release().await;
                        }
                        Ok(false) => {
                            tracing::debug!("lock held by another instance, skipping");
                        }
                        Err(e) => {
                            tracing::warn!("lock acquire failed: {e}");
                        }
                    }
                }
            }
        }
    }

    /// # Errors
    /// Returns `WatcherError` if state store or transport operations fail.
    pub async fn sweep_once(&self) -> Result<SweepReport, WatcherError> {
        let mut report = SweepReport::default();

        self.detect_stuck_jobs(&mut report).await?;
        self.trim_streams(&mut report).await?;

        Ok(report)
    }

    async fn detect_stuck_jobs(&self, report: &mut SweepReport) -> Result<(), WatcherError> {
        // Safety: stuck_threshold is a Duration with millis that fit in u64
        #[allow(clippy::cast_possible_truncation)]
        let threshold = now_millis().saturating_sub(self.config.stuck_threshold.as_millis() as u64);
        let threshold_str = threshold.to_string();

        let records = self
            .store
            .scan(
                &self.config.state_prefix,
                Some(ScanFilter {
                    field: "updated_at".to_string(),
                    op: ScanOp::Lt,
                    value: Bytes::from(threshold_str),
                    max_results: Some(100),
                }),
            )
            .await?;

        for (key, record) in records {
            let state = record
                .fields
                .get("state")
                .map(|v| String::from_utf8_lossy(v).to_string())
                .unwrap_or_default();

            if TERMINAL_STATES.contains(&state.as_str()) {
                continue;
            }

            let retry_count: u32 = record
                .fields
                .get("retry_count")
                .and_then(|v| String::from_utf8_lossy(v).parse().ok())
                .unwrap_or(0);

            if retry_count >= self.config.max_retries {
                self.fail_job(&key).await?;
                report.failed += 1;
            } else {
                self.retry_job(&key, &record.fields, retry_count).await?;
                report.retried += 1;
            }
        }

        Ok(())
    }

    async fn retry_job(
        &self,
        key: &str,
        fields: &HashMap<String, Bytes>,
        retry_count: u32,
    ) -> Result<(), WatcherError> {
        let now = now_millis();
        // Safety: stuck_threshold is a Duration with millis that fit in u64
        #[allow(clippy::cast_possible_truncation)]
        let timeout_at = now + self.config.stuck_threshold.as_millis() as u64;

        let mut updates = HashMap::new();
        updates.insert("state".to_string(), Bytes::from("pending"));
        updates.insert(
            "retry_count".to_string(),
            Bytes::from((retry_count + 1).to_string()),
        );
        updates.insert("updated_at".to_string(), Bytes::from(now.to_string()));
        updates.insert(
            "timeout_at".to_string(),
            Bytes::from(timeout_at.to_string()),
        );

        self.store.set_fields(key, updates).await?;

        if let Some(task_type) = extract_task_type(key) {
            let subject = format!("gbe.tasks.{task_type}.queue");
            let payload = serde_json::json!({
                "key": key,
                "retry_count": retry_count + 1,
                "source": "watcher",
            });
            let dedup = dedup_id(
                self.emitter.component(),
                self.emitter.instance_id(),
                "task-retry",
            );
            let _ = self.emitter.emit(&subject, 1, dedup, payload).await;
        }

        // Re-read original_payload from state if present
        let _original_payload = fields.get("params_ref");

        Ok(())
    }

    async fn fail_job(&self, key: &str) -> Result<(), WatcherError> {
        let now = now_millis();

        let mut updates = HashMap::new();
        updates.insert("state".to_string(), Bytes::from("failed"));
        updates.insert("updated_at".to_string(), Bytes::from(now.to_string()));
        updates.insert("failed_at".to_string(), Bytes::from(now.to_string()));

        self.store.set_fields(key, updates).await?;

        let payload = serde_json::json!({
            "key": key,
            "reason": "retry budget exhausted",
            "source": "watcher",
        });
        let dedup = dedup_id(
            self.emitter.component(),
            self.emitter.instance_id(),
            "task-fail",
        );
        let _ = self
            .emitter
            .emit("gbe.events.system.error", 1, dedup, payload)
            .await;

        Ok(())
    }

    async fn trim_streams(&self, report: &mut SweepReport) -> Result<(), WatcherError> {
        for stream in &self.config.streams {
            match self
                .transport
                .trim_stream(&stream.subject, stream.max_age)
                .await
            {
                Ok(trimmed) => {
                    report.streams_trimmed += 1;
                    report.entries_trimmed += trimmed;
                }
                Err(e) => {
                    tracing::warn!(subject = %stream.subject, "trim failed: {e}");
                }
            }
        }
        Ok(())
    }
}

/// Extract task type from a state key like `gbe:state:tasks:email-send:job_abc`.
fn extract_task_type(key: &str) -> Option<&str> {
    let parts: Vec<&str> = key.split(':').collect();
    // gbe:state:tasks:{task_type}:{task_id}
    if parts.len() >= 5 && parts[0] == "gbe" && parts[1] == "state" && parts[2] == "tasks" {
        Some(parts[3])
    } else {
        None
    }
}

#[allow(clippy::cast_possible_truncation)] // millis since epoch fits in u64 until year 584556
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
