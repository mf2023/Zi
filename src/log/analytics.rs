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

use std::collections::{HashMap, VecDeque};
use std::time::SystemTime;

use crate::log::core::{ZiCLogLevel, ZiCLogRecord};

#[derive(Clone, Debug)]
pub struct ZiCLogPattern {
    pub pattern: String,
    pub count: u64,
    pub first_timestamp: SystemTime,
    pub last_timestamp: SystemTime,
    pub severity: ZiCLogLevel,
}

#[derive(Clone, Debug, Default)]
pub struct ZiCLogAnalysisResult {
    pub total_logs: u64,
    pub error_count: u64,
    pub warning_count: u64,
    pub info_count: u64,
    pub debug_count: u64,
    pub patterns: HashMap<String, ZiCLogPattern>,
    pub top_errors: Vec<String>,
}

#[allow(dead_code)]
pub struct ZiCLogAnalyzer {
    window_size: usize,
    buffer: VecDeque<ZiCLogRecord>,
    patterns: HashMap<String, ZiCLogPattern>,
}

impl ZiCLogAnalyzer {
    #[allow(non_snake_case)]
    pub fn ZiFNew(window_size: usize) -> Self {
        ZiCLogAnalyzer {
            window_size,
            buffer: VecDeque::with_capacity(window_size),
            patterns: HashMap::new(),
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFAdd(&mut self, record: ZiCLogRecord) {
        if self.buffer.len() == self.window_size {
            self.buffer.pop_front();
        }
        let key = record.message.chars().take(50).collect::<String>();
        let now = record.timestamp;
        let entry = self.patterns.entry(key.clone()).or_insert(ZiCLogPattern {
            pattern: record.message.clone(),
            count: 0,
            first_timestamp: now,
            last_timestamp: now,
            severity: record.level,
        });
        entry.count += 1;
        entry.last_timestamp = now;
        self.buffer.push_back(record);
    }

    #[allow(non_snake_case)]
    pub fn ZiFAnalyze(&self) -> ZiCLogAnalysisResult {
        let mut result = ZiCLogAnalysisResult::default();
        result.total_logs = self.buffer.len() as u64;

        for rec in &self.buffer {
            match rec.level {
                ZiCLogLevel::Error => result.error_count += 1,
                ZiCLogLevel::Warning => result.warning_count += 1,
                ZiCLogLevel::Info | ZiCLogLevel::Success => result.info_count += 1,
                ZiCLogLevel::Debug => result.debug_count += 1,
            }
        }

        // Top patterns by count
        let mut items: Vec<_> = self.patterns.values().cloned().collect();
        items.sort_by_key(|p| std::cmp::Reverse(p.count));
        for p in items.iter().take(20) {
            result.patterns.insert(p.pattern.clone(), p.clone());
        }

        // Top error messages
        let mut errors: Vec<_> = self
            .patterns
            .values()
            .filter(|p| matches!(p.severity, ZiCLogLevel::Error))
            .cloned()
            .collect();
        errors.sort_by_key(|p| std::cmp::Reverse(p.count));
        result.top_errors = errors.into_iter().take(10).map(|p| p.pattern).collect();

        result
    }
}
