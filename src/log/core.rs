//! Copyright © 2025 Wenze Wei. All Rights Reserved.
//!
//! This file is part of Zi.
//! The Zi project belongs to the Dunimd project team.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! You may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//!     http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{mpsc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Map, Value};

use crate::log::analytics::ZiCLogAnalyzer;
use crate::log::config::ZiCLogConfig;
use crate::log::context::ZiCLogContext;
use crate::log::handlers::{ZiCFileHandler, ZiCLogHandler, ZiCStdoutHandler};
use crate::log::security::ZiCSecurityAuditor;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZiCLogLevel {
    Debug,
    Info,
    Warning,
    Error,
    Success,
}

impl ZiCLogLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            ZiCLogLevel::Debug => "DEBUG",
            ZiCLogLevel::Info => "INFO",
            ZiCLogLevel::Warning => "WARNING",
            ZiCLogLevel::Error => "ERROR",
            ZiCLogLevel::Success => "SUCCESS",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ZiCLogRecord {
    pub level: ZiCLogLevel,
    pub event: String,
    pub message: String,
    pub fields: Map<String, Value>,
    pub timestamp: SystemTime,
    pub context: Map<String, Value>,
}

impl ZiCLogRecord {
    #[allow(non_snake_case)]
    pub fn ZiFToJson(&self) -> Value {
        let ts = self
            .timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();

        let mut data = Map::new();
        data.insert("level".into(), json!(self.level.as_str()));
        data.insert("event".into(), json!(self.event));
        data.insert("message".into(), json!(self.message));
        data.insert("timestamp_ms".into(), json!(ts));

        let mut merged = self.context.clone();
        for (k, v) in &self.fields {
            merged.insert(k.clone(), v.clone());
        }
        data.insert("fields".into(), Value::Object(merged));

        Value::Object(data)
    }
}

struct ZiCLoggerInner {
    config: ZiCLogConfig,
    handlers: Vec<Box<dyn ZiCLogHandler + Send + Sync>>,
    analyzer: Option<Mutex<ZiCLogAnalyzer>>,
    security: Option<Mutex<ZiCSecurityAuditor>>,
}

impl ZiCLoggerInner {
    fn emit(&self, record: ZiCLogRecord) {
        // Basic level filtering based on config.
        if !self.config.ZiFShouldLog(&record) {
            return;
        }

        // Per-event sampling: use a pseudo-random hash of (event, timestamp)
        // to decide whether to keep this log record.
        let rate = self.config.ZiFGetSamplingRate(&record.event);
        if rate <= 0.0 {
            return;
        }
        if rate < 1.0 {
            let mut hasher = DefaultHasher::new();
            record.event.hash(&mut hasher);
            if let Ok(dur) = record.timestamp.duration_since(UNIX_EPOCH) {
                dur.as_nanos().hash(&mut hasher);
            }
            let v = (hasher.finish() as f64) / (u64::MAX as f64);
            if v > rate {
                return;
            }
        }

        // Security auditing: track records whose message appears to contain
        // sensitive patterns.
        if let Some(sec) = &self.security {
            if let Ok(mut auditor) = sec.lock() {
                let _ = auditor.ZiFAudit(&record);
            }
        }

        // Analytics: feed the record into the analyzer window.
        if let Some(an) = &self.analyzer {
            if let Ok(mut analyzer) = an.lock() {
                analyzer.ZiFAdd(record.clone());
            }
        }

        for h in &self.handlers {
            h.handle(&record);
        }
    }
}

static ASYNC_TX: OnceLock<Mutex<mpsc::Sender<ZiCLogRecord>>> = OnceLock::new();
static LOGGER: OnceLock<ZiCLoggerInner> = OnceLock::new();

#[derive(Debug, Default)]
pub struct ZiCLogger;

impl ZiCLogger {
    /// Initialize the global logger with a configuration. Safe to call multiple
    /// times; the first call wins.
    #[allow(non_snake_case)]
    pub fn ZiFInit(config: ZiCLogConfig) {
        let _ = LOGGER.get_or_init(|| {
            let mut handlers: Vec<Box<dyn ZiCLogHandler + Send + Sync>> = Vec::new();
            if config.console_enabled {
                handlers.push(Box::new(ZiCStdoutHandler::ZiFNew(
                    config.json_format_console,
                )));
            }
            if config.file_enabled {
                if let Some(path) = &config.file_path {
                    handlers.push(Box::new(ZiCFileHandler::ZiFNew(
                        path.clone(),
                        config.json_format_console,
                        config.rotate_when.clone(),
                        config.max_bytes,
                        config.backup_count,
                    )));
                }
            }

            let analyzer = Some(Mutex::new(ZiCLogAnalyzer::ZiFNew(10_000)));
            let security = Some(Mutex::new(ZiCSecurityAuditor::ZiFNew()));
            let inner = ZiCLoggerInner {
                config: config.clone(),
                handlers,
                analyzer,
                security,
            };

            if inner.config.async_logging {
                let (tx, rx) = mpsc::channel::<ZiCLogRecord>();
                let _ = ASYNC_TX.set(Mutex::new(tx));

                std::thread::spawn(move || {
                    while let Ok(record) = rx.recv() {
                        if let Some(inner_ref) = LOGGER.get() {
                            inner_ref.emit(record);
                        }
                    }
                });
            }

            inner
        });
    }

    /// Emit a structured log event.
    #[allow(non_snake_case)]
    pub fn ZiFEvent<L, S>(level: ZiCLogLevel, event: S, message: S, fields: L)
    where
        L: IntoIterator<Item = (String, Value)>,
        S: Into<String> + Clone,
    {
        if let Some(inner) = LOGGER.get() {
            let mut field_map = Map::new();
            for (k, v) in fields {
                field_map.insert(k, v);
            }
            let context_map = ZiCLogContext::ZiFGet();
            let mut ctx_obj = Map::new();
            for (k, v) in context_map {
                ctx_obj.insert(k, v);
            }

            let record = ZiCLogRecord {
                level,
                event: event.clone().into(),
                message: message.clone().into(),
                fields: field_map,
                timestamp: SystemTime::now(),
                context: ctx_obj,
            };
            // If async logging is enabled, try to send through the channel;
            // otherwise, fall back to synchronous emission.
            if let Some(tx_mutex) = ASYNC_TX.get() {
                if let Ok(tx) = tx_mutex.lock() {
                    let _ = tx.send(record);
                    return;
                }
            }
            inner.emit(record);
        }
    }
}
