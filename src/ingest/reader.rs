//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
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

use std::path::Path;
use std::io::{BufRead, BufReader};

use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::record::{ZiCRecord, ZiCRecordBatch};
use crate::ingest::format::{ZiCDataFormat, ZiCFormatDetector};

#[derive(Clone, Debug)]
pub struct ZiCReaderConfig {
    pub batch_size: usize,
    pub skip_errors: bool,
}

impl Default for ZiCReaderConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            skip_errors: true,
        }
    }
}

#[derive(Debug)]
pub struct ZiCStreamReader {
    config: ZiCReaderConfig,
    detector: ZiCFormatDetector,
}

impl ZiCStreamReader {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        Self {
            config: ZiCReaderConfig::default(),
            detector: ZiCFormatDetector::ZiFNew(),
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFWithConfig(mut self, config: ZiCReaderConfig) -> Self {
        self.config = config;
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFReadPath(&self, path: &Path) -> Result<ZiCRecordBatch> {
        let format = self.detector.ZiFDetectFromPath(path)?;
        
        match format {
            ZiCDataFormat::Jsonl => self.read_jsonl(path),
            ZiCDataFormat::Json => self.read_json(path),
            ZiCDataFormat::Csv => self.read_csv(path),
            ZiCDataFormat::Parquet => self.read_parquet(path),
            ZiCDataFormat::Unknown => Err(ZiError::validation(format!(
                "Unknown file format: {}",
                path.display()
            ))),
        }
    }

    fn read_jsonl(&self, path: &Path) -> Result<ZiCRecordBatch> {
        let file = std::fs::File::open(path)?;
        let reader = BufReader::new(file);
        let mut batch = Vec::with_capacity(self.config.batch_size);

        for (idx, line) in reader.lines().enumerate() {
            match line {
                Ok(text) => {
                    if text.trim().is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<Value>(&text) {
                        Ok(value) => {
                            let record = ZiCRecord::ZiFNew(Some(format!("{}", idx)), value);
                            batch.push(record);
                        }
                        Err(e) => {
                            if !self.config.skip_errors {
                                return Err(ZiError::validation(format!(
                                    "Failed to parse line {}: {}",
                                    idx, e
                                )));
                            }
                            log::warn!("Skipping invalid JSON line {}: {}", idx, e);
                        }
                    }
                }
                Err(e) => {
                    if !self.config.skip_errors {
                        return Err(ZiError::io(format!("Failed to read line: {}", e)));
                    }
                    log::warn!("Skipping unreadable line: {}", e);
                }
            }
        }

        Ok(batch)
    }

    fn read_json(&self, path: &Path) -> Result<ZiCRecordBatch> {
        let content = std::fs::read_to_string(path)?;
        let value: Value = serde_json::from_str(&content)?;

        match value {
            Value::Array(arr) => {
                let batch: ZiCRecordBatch = arr
                    .into_iter()
                    .enumerate()
                    .map(|(idx, v)| ZiCRecord::ZiFNew(Some(format!("{}", idx)), v))
                    .collect();
                Ok(batch)
            }
            Value::Object(_) => {
                Ok(vec![ZiCRecord::ZiFNew(Some("0".to_string()), value)])
            }
            _ => Err(ZiError::validation("JSON must be array or object")),
        }
    }

    fn read_csv(&self, path: &Path) -> Result<ZiCRecordBatch> {
        let content = std::fs::read_to_string(path)?;
        let mut reader = csv::Reader::from_reader(content.as_bytes());
        let headers: Vec<String> = reader.headers()?.iter().map(|s| s.to_string()).collect();
        
        let mut batch = Vec::with_capacity(self.config.batch_size);

        for (idx, result) in reader.records().enumerate() {
            match result {
                Ok(record) => {
                    let mut obj = serde_json::Map::new();
                    for (i, field) in record.iter().enumerate() {
                        if i < headers.len() {
                            obj.insert(headers[i].clone(), Value::String(field.to_string()));
                        }
                    }
                    let value = Value::Object(obj);
                    batch.push(ZiCRecord::ZiFNew(Some(format!("{}", idx)), value));
                }
                Err(e) => {
                    if !self.config.skip_errors {
                        return Err(ZiError::validation(format!(
                            "Failed to parse CSV row {}: {}",
                            idx, e
                        )));
                    }
                    log::warn!("Skipping invalid CSV row {}: {}", idx, e);
                }
            }
        }

        Ok(batch)
    }

    fn read_parquet(&self, _path: &Path) -> Result<ZiCRecordBatch> {
        Err(ZiError::validation("Parquet reading requires 'parquet' feature"))
    }
}
