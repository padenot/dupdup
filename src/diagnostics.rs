use anyhow::{Context, Result};
use std::collections::VecDeque;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use tracing::warn;

const ERROR_BUFFER_LIMIT: usize = 8;

pub(crate) struct ErrorLog {
    sink: Mutex<File>,
    recent: Arc<Mutex<VecDeque<String>>>,
    count: Arc<AtomicU64>,
}

impl ErrorLog {
    pub(crate) fn new(path: &Path) -> Result<Self> {
        let sink = File::create(path)
            .with_context(|| format!("cannot create error log {}", path.display()))?;
        Ok(Self {
            sink: Mutex::new(sink),
            recent: Arc::new(Mutex::new(VecDeque::with_capacity(ERROR_BUFFER_LIMIT))),
            count: Arc::new(AtomicU64::new(0)),
        })
    }

    pub(crate) fn log(&self, kind: &str, msg: impl AsRef<str>) {
        let msg = msg.as_ref().to_string();
        warn!(error = %msg, kind, "operation error");
        if let Ok(mut sink) = self.sink.lock() {
            let _ = writeln!(sink, "{}", msg);
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut recent) = self.recent.lock() {
            if recent.len() >= ERROR_BUFFER_LIMIT {
                recent.pop_front();
            }
            recent.push_back(msg);
        }
    }

    pub(crate) fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    pub(crate) fn recent_messages(&self) -> Arc<Mutex<VecDeque<String>>> {
        self.recent.clone()
    }

    pub(crate) fn count_handle(&self) -> Arc<AtomicU64> {
        self.count.clone()
    }
}
