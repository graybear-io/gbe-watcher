use std::time::Duration;

#[derive(Debug, Clone)]
pub struct WatcherConfig {
    pub redis_url: String,
    pub interval: Duration,
    pub lock_ttl: Duration,
    pub stuck_threshold: Duration,
    pub max_retries: u32,
    pub state_prefix: String,
    pub streams: Vec<StreamRetention>,
}

impl Default for WatcherConfig {
    fn default() -> Self {
        Self {
            redis_url: "redis://127.0.0.1:6379".to_string(),
            interval: Duration::from_secs(30),
            lock_ttl: Duration::from_secs(60),
            stuck_threshold: Duration::from_secs(300),
            max_retries: 3,
            state_prefix: "gbe:state:tasks:".to_string(),
            streams: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StreamRetention {
    pub subject: String,
    pub max_age: Duration,
}
