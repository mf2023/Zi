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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::core::{ZiCLogLevel, ZiCLogRecord};

/// Configuration for the ZiCLogger. This mirrors the main concepts of the
/// Python logging configuration used in the LLM stack (console/file enablement,
/// default level, JSON formatting, sampling rates, async logging, and
/// anomaly detector configuration).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCLogConfig {
    pub default_level: String,
    pub console_enabled: bool,
    pub json_format_console: bool,
    /// Whether file logging is enabled.
    pub file_enabled: bool,
    /// Optional log file path when file logging is enabled.
    pub file_path: Option<String>,
    /// Rolling/rotation strategy (e.g., "time" or "size").
    pub rotate_when: Option<String>,
    /// Maximum file size in bytes when size-based rotation is used.
    pub max_bytes: Option<u64>,
    /// Number of backup files to keep when rotating.
    pub backup_count: Option<u32>,
    /// Per-event sampling rates in [0.0, 1.0].
    pub sampling_rates: HashMap<String, f64>,
    /// Whether logging should be performed asynchronously.
    pub async_logging: bool,
    /// Configuration blob for anomaly detectors.
    pub anomaly_detectors: HashMap<String, Value>,
}

impl Default for ZiCLogConfig {
    fn default() -> Self {
        ZiCLogConfig {
            default_level: "INFO".to_string(),
            console_enabled: true,
            json_format_console: true,
            file_enabled: false,
            file_path: None,
            rotate_when: Some("time".to_string()),
            max_bytes: Some(10 * 1024 * 1024),
            backup_count: Some(7),
            sampling_rates: HashMap::new(),
            async_logging: false,
            anomaly_detectors: HashMap::new(),
        }
    }
}

impl ZiCLogConfig {
    #[allow(non_snake_case)]
    pub fn ZiFShouldLog(&self, record: &ZiCLogRecord) -> bool {
        let threshold = self.ZiFParseLevel(&self.default_level);
        let level = record.level;
        self.ZiFLevelValue(level) >= self.ZiFLevelValue(threshold)
    }

    fn ZiFParseLevel(&self, s: &str) -> ZiCLogLevel {
        match s.to_ascii_uppercase().as_str() {
            "DEBUG" => ZiCLogLevel::Debug,
            "WARNING" => ZiCLogLevel::Warning,
            "ERROR" => ZiCLogLevel::Error,
            "SUCCESS" => ZiCLogLevel::Success,
            _ => ZiCLogLevel::Info,
        }
    }

    fn ZiFLevelValue(&self, level: ZiCLogLevel) -> i32 {
        match level {
            ZiCLogLevel::Debug => 10,
            ZiCLogLevel::Info => 20,
            ZiCLogLevel::Warning => 30,
            ZiCLogLevel::Error => 40,
            ZiCLogLevel::Success => 25,
        }
    }

    /// Get the sampling rate for a specific event. Defaults to 1.0 when not
    /// explicitly configured.
    #[allow(non_snake_case)]
    pub fn ZiFGetSamplingRate(&self, event: &str) -> f64 {
        self.sampling_rates
            .get(event)
            .copied()
            .unwrap_or(1.0)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZiCLogConfigBuilder {
    pub default_level: Option<String>,
    pub console_enabled: Option<bool>,
    pub json_format_console: Option<bool>,
    pub file_enabled: Option<bool>,
    pub file_path: Option<String>,
    pub rotate_when: Option<String>,
    pub max_bytes: Option<u64>,
    pub backup_count: Option<u32>,
    pub sampling_rates: Option<HashMap<String, f64>>,
    pub async_logging: Option<bool>,
    pub anomaly_detectors: Option<HashMap<String, Value>>,
}

impl ZiCLogConfigBuilder {
    #[allow(non_snake_case)]
    pub fn ZiFBuild(self) -> ZiCLogConfig {
        let base = ZiCLogConfig::default();
        ZiCLogConfig {
            default_level: self
                .default_level
                .unwrap_or_else(|| base.default_level.clone()),
            console_enabled: self.console_enabled.unwrap_or(base.console_enabled),
            json_format_console: self
                .json_format_console
                .unwrap_or(base.json_format_console),
            file_enabled: self.file_enabled.unwrap_or(base.file_enabled),
            file_path: self.file_path.or_else(|| base.file_path.clone()),
            rotate_when: self.rotate_when.or_else(|| base.rotate_when.clone()),
            max_bytes: self.max_bytes.or(base.max_bytes),
            backup_count: self.backup_count.or(base.backup_count),
            sampling_rates: self
                .sampling_rates
                .unwrap_or_else(|| base.sampling_rates.clone()),
            async_logging: self.async_logging.unwrap_or(base.async_logging),
            anomaly_detectors: self
                .anomaly_detectors
                .unwrap_or_else(|| base.anomaly_detectors.clone()),
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFFromJson(value: &Value) -> ZiCLogConfig {
        let builder: ZiCLogConfigBuilder = serde_json::from_value(value.clone())
            .unwrap_or_else(|_| ZiCLogConfigBuilder::default());
        builder.ZiFBuild()
    }
}
