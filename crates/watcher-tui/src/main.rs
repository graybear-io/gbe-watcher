use std::io;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph};
use tokio::sync::mpsc;

use gbe_nexus::{
    DomainPayload, Message, MessageHandler, StartPosition, SubscribeOpts, Transport, TransportError,
};

const COMPONENTS: &[&str] = &["operative", "oracle", "watcher", "sentinel", "envoy"];
const LIFECYCLE_EVENTS: &[&str] = &["started", "stopped", "heartbeat", "degraded"];
const SYSTEM_EVENTS: &[&str] = &["health", "error", "deploy", "sweep", "archive"];

/// An event received from the transport, ready for display.
#[derive(Debug, Clone)]
struct DisplayEvent {
    subject: String,
    timestamp: u64,
    source: String,
    summary: String,
}

/// Source categories shown in the left panel.
#[derive(Debug, Clone)]
struct Source {
    name: String,
    subject_prefix: String,
    count: usize,
}

struct App {
    sources: Vec<Source>,
    source_state: ListState,
    events: Vec<DisplayEvent>,
    total_events: usize,
    transport_label: String,
}

impl App {
    fn new(transport_label: &str) -> Self {
        let sources = vec![
            Source {
                name: "lifecycle".into(),
                subject_prefix: "gbe.events.lifecycle.".into(),
                count: 0,
            },
            Source {
                name: "system".into(),
                subject_prefix: "gbe.events.system.".into(),
                count: 0,
            },
            Source {
                name: "jobs".into(),
                subject_prefix: "gbe.jobs.".into(),
                count: 0,
            },
            Source {
                name: "tasks".into(),
                subject_prefix: "gbe.tasks.".into(),
                count: 0,
            },
            Source {
                name: "sentinel".into(),
                subject_prefix: "gbe.events.sentinel.".into(),
                count: 0,
            },
            Source {
                name: "all".into(),
                subject_prefix: String::new(),
                count: 0,
            },
        ];
        let mut source_state = ListState::default();
        source_state.select(Some(0));

        Self {
            sources,
            source_state,
            events: Vec::new(),
            total_events: 0,
            transport_label: transport_label.to_string(),
        }
    }

    fn selected_prefix(&self) -> &str {
        self.source_state
            .selected()
            .and_then(|i| self.sources.get(i))
            .map(|s| s.subject_prefix.as_str())
            .unwrap_or("")
    }

    fn filtered_events(&self) -> Vec<&DisplayEvent> {
        let prefix = self.selected_prefix();
        if prefix.is_empty() {
            self.events.iter().collect()
        } else {
            self.events
                .iter()
                .filter(|e| e.subject.starts_with(prefix))
                .collect()
        }
    }

    fn ingest(&mut self, evt: DisplayEvent) {
        for source in &mut self.sources {
            if !source.subject_prefix.is_empty() && evt.subject.starts_with(&source.subject_prefix)
            {
                source.count += 1;
            }
        }
        // "all" category
        if let Some(all) = self.sources.last_mut() {
            all.count += 1;
        }
        self.total_events += 1;
        self.events.push(evt);
    }

    fn next_source(&mut self) {
        let i = self.source_state.selected().unwrap_or(0);
        let next = if i >= self.sources.len() - 1 {
            0
        } else {
            i + 1
        };
        self.source_state.select(Some(next));
    }

    fn prev_source(&mut self) {
        let i = self.source_state.selected().unwrap_or(0);
        let prev = if i == 0 {
            self.sources.len() - 1
        } else {
            i - 1
        };
        self.source_state.select(Some(prev));
    }
}

/// MessageHandler that forwards events into an mpsc channel.
struct TuiHandler {
    tx: mpsc::UnboundedSender<DisplayEvent>,
}

#[async_trait]
impl MessageHandler for TuiHandler {
    async fn handle(&self, msg: &dyn Message) -> Result<(), TransportError> {
        let envelope = msg.envelope();
        let summary = parse_summary(&envelope.subject, msg.payload());

        let evt = DisplayEvent {
            subject: envelope.subject.clone(),
            timestamp: envelope.timestamp,
            source: extract_source(&envelope.subject),
            summary,
        };

        let _ = self.tx.send(evt);
        msg.ack().await?;
        Ok(())
    }
}

fn extract_source(subject: &str) -> String {
    // gbe.events.lifecycle.operative.started -> operative
    // gbe.jobs.report.created -> report
    // gbe.tasks.email-send.queue -> email-send
    let parts: Vec<&str> = subject.split('.').collect();
    match parts.as_slice() {
        ["gbe", "events", "lifecycle", component, ..] => (*component).to_string(),
        ["gbe", "events", "system", ..] => "system".to_string(),
        ["gbe", "events", "sentinel", host, ..] => format!("sentinel/{host}"),
        ["gbe", "jobs", job_type, ..] => format!("job:{job_type}"),
        ["gbe", "tasks", task_type, ..] => format!("task:{task_type}"),
        _ => subject.to_string(),
    }
}

fn parse_summary(subject: &str, payload: &Bytes) -> String {
    // Try to parse as DomainPayload<serde_json::Value>
    if let Ok(dp) = DomainPayload::<serde_json::Value>::from_bytes(payload) {
        // Extract useful fields from data
        let data = &dp.data;
        let mut parts = Vec::new();

        if let Some(comp) = data.get("component").and_then(|v| v.as_str()) {
            parts.push(comp.to_string());
        }
        if let Some(inst) = data.get("instance_id").and_then(|v| v.as_str()) {
            parts.push(inst.to_string());
        }
        if let Some(reason) = data.get("reason").and_then(|v| v.as_str()) {
            parts.push(format!("reason={reason}"));
        }
        if let Some(msg) = data.get("message").and_then(|v| v.as_str()) {
            parts.push(msg.to_string());
        }
        if let Some(error) = data.get("error").and_then(|v| v.as_str()) {
            parts.push(format!("err={error}"));
        }
        if let Some(uptime) = data.get("uptime_secs").and_then(|v| v.as_u64()) {
            parts.push(format!("up={uptime}s"));
        }

        if parts.is_empty() {
            // Fallback: show the event type from subject
            event_type_from_subject(subject)
        } else {
            parts.join(" ")
        }
    } else {
        // Not a DomainPayload — show raw
        let raw = String::from_utf8_lossy(payload);
        if raw.len() > 80 {
            format!("{}...", &raw[..77])
        } else {
            raw.to_string()
        }
    }
}

fn event_type_from_subject(subject: &str) -> String {
    subject
        .split('.')
        .next_back()
        .unwrap_or("unknown")
        .to_string()
}

fn format_ts(ts: u64) -> String {
    let secs = ts / 1000;
    let h = (secs / 3600) % 24;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

async fn subscribe_all(
    transport: &Arc<dyn Transport>,
    tx: &mpsc::UnboundedSender<DisplayEvent>,
) -> Result<Vec<Box<dyn gbe_nexus::Subscription>>, TransportError> {
    let mut subs = Vec::new();
    let mut subjects = Vec::new();

    // Lifecycle events for each known component
    for comp in COMPONENTS {
        for evt in LIFECYCLE_EVENTS {
            subjects.push(format!("gbe.events.lifecycle.{comp}.{evt}"));
        }
    }

    // System events
    for evt in SYSTEM_EVENTS {
        subjects.push(format!("gbe.events.system.{evt}"));
    }

    for subject in &subjects {
        let sub = transport
            .subscribe(
                subject,
                "watcher-tui",
                Box::new(TuiHandler { tx: tx.clone() }),
                Some(SubscribeOpts {
                    start_from: StartPosition::Earliest,
                    ..Default::default()
                }),
            )
            .await?;
        subs.push(sub);
    }

    Ok(subs)
}

async fn run_demo_publisher(transport: Arc<dyn Transport>) {
    use gbe_jobs_domain::{ComponentDegraded, ComponentStarted, ComponentStopped, Heartbeat};
    use gbe_nexus::{EventEmitter, dedup_id};

    let emitter = EventEmitter::new(transport, "demo", "demo-001");

    // Emit a startup sequence
    let components = ["operative", "oracle", "sentinel"];
    for comp in components {
        let started = ComponentStarted {
            component: comp.to_string(),
            instance_id: format!("{comp}-001"),
            started_at: now_millis(),
            version: Some("0.1.0".to_string()),
        };
        let subject = gbe_jobs_domain::subjects::lifecycle::started(comp);
        let _ = emitter
            .emit(
                &subject,
                1,
                dedup_id(comp, &format!("{comp}-001"), "started"),
                started,
            )
            .await;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Heartbeat loop
    let mut tick: u64 = 0;
    loop {
        tokio::time::sleep(Duration::from_secs(3)).await;
        tick += 3;

        for comp in &components[..2] {
            let hb = Heartbeat {
                component: comp.to_string(),
                instance_id: format!("{comp}-001"),
                timestamp: now_millis(),
                uptime_secs: tick,
            };
            let subject = gbe_jobs_domain::subjects::lifecycle::heartbeat(comp);
            let _ = emitter
                .emit(
                    &subject,
                    1,
                    dedup_id(comp, &format!("{comp}-001"), "heartbeat"),
                    hb,
                )
                .await;
        }

        // Occasionally emit degraded/stopped for variety
        if tick == 12 {
            let degraded = ComponentDegraded {
                component: "sentinel".to_string(),
                instance_id: "sentinel-001".to_string(),
                degraded_at: now_millis(),
                reason: "redis connection pool exhausted".to_string(),
            };
            let subject = gbe_jobs_domain::subjects::lifecycle::degraded("sentinel");
            let _ = emitter
                .emit(
                    &subject,
                    1,
                    dedup_id("sentinel", "sentinel-001", "degraded"),
                    degraded,
                )
                .await;
        }

        if tick == 18 {
            let stopped = ComponentStopped {
                component: "sentinel".to_string(),
                instance_id: "sentinel-001".to_string(),
                stopped_at: now_millis(),
                reason: "SIGTERM".to_string(),
            };
            let subject = gbe_jobs_domain::subjects::lifecycle::stopped("sentinel");
            let _ = emitter
                .emit(
                    &subject,
                    1,
                    dedup_id("sentinel", "sentinel-001", "stopped"),
                    stopped,
                )
                .await;
        }
    }
}

#[allow(clippy::cast_possible_truncation)]
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis() as u64
}

fn draw(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>, app: &mut App) -> io::Result<()> {
    terminal.draw(|f| {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(1), Constraint::Length(1)])
            .split(f.area());

        let main_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(22), Constraint::Min(1)])
            .split(chunks[0]);

        // Left panel: sources
        let items: Vec<ListItem> = app
            .sources
            .iter()
            .map(|s| {
                let label = format!("{} ({})", s.name, s.count);
                ListItem::new(label)
            })
            .collect();

        let sources = List::new(items)
            .block(Block::default().borders(Borders::ALL).title(" Sources "))
            .highlight_style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("> ");

        f.render_stateful_widget(sources, main_chunks[0], &mut app.source_state);

        // Right panel: events
        let filtered = app.filtered_events();
        let visible_height = main_chunks[1].height.saturating_sub(2) as usize;
        let skip = filtered.len().saturating_sub(visible_height);

        let event_lines: Vec<Line> = filtered
            .iter()
            .skip(skip)
            .map(|e| {
                let ts = format_ts(e.timestamp);
                let event_type = event_type_from_subject(&e.subject);
                let color = match event_type.as_str() {
                    "started" => Color::Green,
                    "stopped" => Color::Red,
                    "degraded" => Color::Yellow,
                    "heartbeat" => Color::DarkGray,
                    "error" => Color::Red,
                    _ => Color::White,
                };

                Line::from(vec![
                    Span::styled(format!("{ts} "), Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!("{:<12} ", e.source),
                        Style::default().fg(Color::Cyan),
                    ),
                    Span::styled(format!("{:<10} ", event_type), Style::default().fg(color)),
                    Span::raw(&e.summary),
                ])
            })
            .collect();

        let selected_name = app
            .source_state
            .selected()
            .and_then(|i| app.sources.get(i))
            .map(|s| s.name.as_str())
            .unwrap_or("all");

        let events_panel = Paragraph::new(event_lines).block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" Events [{selected_name}] ")),
        );
        f.render_widget(events_panel, main_chunks[1]);

        // Status bar
        let status = Line::from(vec![
            Span::styled(" ", Style::default()),
            Span::styled(
                app.transport_label.to_string(),
                Style::default().fg(Color::DarkGray),
            ),
            Span::raw(" | "),
            Span::styled(
                format!("events: {}", app.total_events),
                Style::default().fg(Color::White),
            ),
            Span::raw(" | "),
            Span::styled(
                "j/k: navigate  q: quit",
                Style::default().fg(Color::DarkGray),
            ),
        ]);
        let status_bar = Paragraph::new(status);
        f.render_widget(status_bar, chunks[1]);
    })?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let demo = args.iter().any(|a| a == "--demo");
    let redis_url = args
        .iter()
        .position(|a| a == "--redis-url")
        .and_then(|i| args.get(i + 1))
        .cloned();

    let is_redis = redis_url.is_some();
    let (transport, label): (Arc<dyn Transport>, String) = if let Some(url) = redis_url {
        let config = gbe_nexus_redis::RedisTransportConfig {
            url: url.clone(),
            ..Default::default()
        };
        let t = gbe_nexus_redis::RedisTransport::connect(config).await?;
        (Arc::new(t), format!("redis: {url}"))
    } else {
        let t = gbe_nexus_memory::MemoryTransport::new(
            gbe_nexus_memory::MemoryTransportConfig::default(),
        );
        (Arc::new(t), "memory (demo)".to_string())
    };

    let (tx, mut rx) = mpsc::unbounded_channel::<DisplayEvent>();

    // Subscribe to all known subjects
    let _subs = subscribe_all(&transport, &tx).await?;

    // In demo mode, spawn a publisher that emits fake events
    if demo || !is_redis {
        tokio::spawn(run_demo_publisher(transport.clone()));
    }

    // Terminal setup
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(&label);

    loop {
        draw(&mut terminal, &mut app)?;

        // Drain any pending events from the channel
        while let Ok(evt) = rx.try_recv() {
            app.ingest(evt);
        }

        // Poll for terminal events with a short timeout (for event refresh)
        if event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            match key.code {
                KeyCode::Char('q') => break,
                KeyCode::Char('j') | KeyCode::Down => app.next_source(),
                KeyCode::Char('k') | KeyCode::Up => app.prev_source(),
                _ => {}
            }
        }
    }

    // Cleanup
    disable_raw_mode()?;
    crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}
