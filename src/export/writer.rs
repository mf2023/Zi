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
use std::io::{BufWriter, Write};

use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::record::{ZiCRecord, ZiCRecordBatch};

#[derive(Clone, Debug)]
pub enum ZiCOutputFormat {
    Jsonl,
    Json,
    Csv,
}

#[derive(Clone, Debug)]
pub struct ZiCWriterConfig {
    pub format: ZiCOutputFormat,
    pub pretty: bool,
    pub batch_size: usize,
}

impl Default for ZiCWriterConfig {
    fn default() -> Self {
        Self {
            format: ZiCOutputFormat::Jsonl,
            pretty: false,
            batch_size: 1000,
        }
    }
}

#[derive(Debug)]
pub struct ZiCStreamWriter {
    config: ZiCWriterConfig,
}

impl ZiCStreamWriter {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        Self {
            config: ZiCWriterConfig::default(),
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFWithConfig(mut self, config: ZiCWriterConfig) -> Self {
        self.config = config;
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFWrite(&self, batch: &ZiCRecordBatch, path: &Path) -> Result<()> {
        let file = std::fs::File::create(path)?;
        let mut writer = BufWriter::new(file);

        match self.config.format {
            ZiCOutputFormat::Jsonl => self.write_jsonl(batch, &mut writer),
            ZiCOutputFormat::Json => self.write_json(batch, &mut writer),
            ZiCOutputFormat::Csv => self.write_csv(batch, &mut writer),
        }
    }

    fn write_jsonl(&self, batch: &ZiCRecordBatch, writer: &mut BufWriter<std::fs::File>) -> Result<()> {
        for record in batch {
            let output = self.record_to_output(record);
            let line = serde_json::to_string(&output)?;
            writeln!(writer, "{}", line)?;
        }
        writer.flush()?;
        Ok(())
    }

    fn write_json(&self, batch: &ZiCRecordBatch, writer: &mut BufWriter<std::fs::File>) -> Result<()> {
        let outputs: Vec<Value> = batch.iter().map(|r| self.record_to_output(r)).collect();
        
        let json = if self.config.pretty {
            serde_json::to_string_pretty(&outputs)?
        } else {
            serde_json::to_string(&outputs)?
        };
        
        write!(writer, "{}", json)?;
        writer.flush()?;
        Ok(())
    }

    fn write_csv(&self, batch: &ZiCRecordBatch, writer: &mut BufWriter<std::fs::File>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let first_record = &batch[0];
        let headers = self.extract_headers(first_record);
        
        let mut csv_writer = csv::Writer::from_writer(writer);
        
        csv_writer.write_record(&headers)?;
        
        for record in batch {
            let row = self.record_to_row(record, &headers);
            csv_writer.write_record(&row)?;
        }
        
        csv_writer.flush()?;
        Ok(())
    }

    fn record_to_output(&self, record: &ZiCRecord) -> Value {
        let mut obj = serde_json::Map::new();
        
        if let Some(id) = &record.id {
            obj.insert("id".to_string(), Value::String(id.clone()));
        }
        
        obj.insert("payload".to_string(), record.payload.clone());
        
        if let Some(meta) = &record.metadata {
            obj.insert("metadata".to_string(), Value::Object(meta.clone()));
        }
        
        Value::Object(obj)
    }

    fn extract_headers(&self, record: &ZiCRecord) -> Vec<String> {
        let mut headers = vec!["id".to_string()];
        
        if let Value::Object(map) = &record.payload {
            for key in map.keys() {
                headers.push(format!("payload.{}", key));
            }
        }
        
        if let Some(meta) = &record.metadata {
            for key in meta.keys() {
                headers.push(format!("metadata.{}", key));
            }
        }
        
        headers
    }

    fn record_to_row(&self, record: &ZiCRecord, headers: &[String]) -> Vec<String> {
        headers
            .iter()
            .map(|header| {
                match header.as_str() {
                    "id" => record.id.clone().unwrap_or_default(),
                    h if h.starts_with("payload.") => {
                        let key = &h[8..];
                        if let Value::Object(map) = &record.payload {
                            map.get(key)
                                .and_then(|v| match v {
                                    Value::String(s) => Some(s.clone()),
                                    Value::Number(n) => Some(n.to_string()),
                                    Value::Bool(b) => Some(b.to_string()),
                                    _ => None,
                                })
                                .unwrap_or_default()
                        } else {
                            String::new()
                        }
                    }
                    h if h.starts_with("metadata.") => {
                        let key = &h[9..];
                        record.metadata
                            .as_ref()
                            .and_then(|m| m.get(key))
                            .and_then(|v| match v {
                                Value::String(s) => Some(s.clone()),
                                Value::Number(n) => Some(n.to_string()),
                                Value::Bool(b) => Some(b.to_string()),
                                _ => None,
                            })
                            .unwrap_or_default()
                    }
                    _ => String::new(),
                }
            })
            .collect()
    }
}
