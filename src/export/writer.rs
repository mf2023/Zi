//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
//!
//! This file is part of Zi.
//! The Zi project belongs to the Dunimd project team.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//!     http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::path::{Path, PathBuf};
use std::io::{BufWriter, Write};
use std::fs::File;

use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::record::{ZiCRecord, ZiCRecordBatch};
use crate::ingest::format::ZiCCompression;

#[derive(Clone, Debug)]
pub enum ZiCOutputFormat {
    Jsonl,
    Json,
    Csv,
    Parquet,
}

#[derive(Clone, Debug)]
pub struct ZiCWriterConfig {
    pub format: ZiCOutputFormat,
    pub pretty: bool,
    pub batch_size: usize,
    pub compression: ZiCCompression,
    pub split_by_size: Option<usize>,
    pub split_by_count: Option<usize>,
    pub atomic_write: bool,
}

impl Default for ZiCWriterConfig {
    fn default() -> Self {
        Self {
            format: ZiCOutputFormat::Jsonl,
            pretty: false,
            batch_size: 1000,
            compression: ZiCCompression::None,
            split_by_size: None,
            split_by_count: None,
            atomic_write: true,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ZiCWriteStats {
    pub records_written: usize,
    pub bytes_written: usize,
    pub files_created: usize,
}

#[derive(Debug)]
pub struct ZiCStreamWriter {
    config: ZiCWriterConfig,
    stats: ZiCWriteStats,
}

impl ZiCStreamWriter {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        Self {
            config: ZiCWriterConfig::default(),
            stats: ZiCWriteStats {
                records_written: 0,
                bytes_written: 0,
                files_created: 0,
            },
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFWithConfig(mut self, config: ZiCWriterConfig) -> Self {
        self.config = config;
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFWrite(&mut self, batch: &ZiCRecordBatch, path: &Path) -> Result<ZiCWriteStats> {
        self.stats = ZiCWriteStats::default();

        if let Some(split_count) = self.config.split_by_count {
            self.write_split_by_count(batch, path, split_count)?;
        } else if let Some(split_size) = self.config.split_by_size {
            self.write_split_by_size(batch, path, split_size)?;
        } else {
            self.write_single(batch, path)?;
        }

        Ok(self.stats.clone())
    }

    fn write_single(&mut self, batch: &ZiCRecordBatch, path: &Path) -> Result<()> {
        let final_path = if self.config.atomic_write {
            let temp_path = self.temp_path(path);
            self.write_to_path(batch, &temp_path)?;
            std::fs::rename(&temp_path, path)?;
            path.to_path_buf()
        } else {
            self.write_to_path(batch, path)?;
            path.to_path_buf()
        };

        self.stats.files_created += 1;
        if let Ok(metadata) = std::fs::metadata(&final_path) {
            self.stats.bytes_written += metadata.len() as usize;
        }

        Ok(())
    }

    fn write_split_by_count(&mut self, batch: &ZiCRecordBatch, base_path: &Path, count: usize) -> Result<()> {
        let total_records = batch.len();
        let num_files = (total_records + count - 1) / count;

        for (i, chunk) in batch.chunks(count).enumerate() {
            let path = self.split_path(base_path, i, num_files);
            let chunk_vec: ZiCRecordBatch = chunk.to_vec();
            self.write_single(&chunk_vec, &path)?;
        }

        self.stats.records_written = total_records;
        Ok(())
    }

    fn write_split_by_size(&mut self, batch: &ZiCRecordBatch, base_path: &Path, max_size: usize) -> Result<()> {
        let mut current_batch = Vec::new();
        let mut current_size = 0;
        let mut file_index = 0;
        let estimated_files = (self.estimate_batch_size(batch) / max_size) + 1;

        for record in batch {
            let record_size = self.estimate_record_size(record);
            
            if current_size + record_size > max_size && !current_batch.is_empty() {
                let path = self.split_path(base_path, file_index, estimated_files);
                self.write_single(&current_batch, &path)?;
                file_index += 1;
                current_batch.clear();
                current_size = 0;
            }

            current_batch.push(record.clone());
            current_size += record_size;
        }

        if !current_batch.is_empty() {
            let path = self.split_path(base_path, file_index, estimated_files);
            self.write_single(&current_batch, &path)?;
        }

        self.stats.records_written = batch.len();
        Ok(())
    }

    fn write_to_path(&self, batch: &ZiCRecordBatch, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        match self.config.compression {
            ZiCCompression::Gzip => self.write_compressed(batch, path, CompressionType::Gzip),
            ZiCCompression::Zstd => self.write_compressed(batch, path, CompressionType::Zstd),
            ZiCCompression::None => self.write_uncompressed(batch, path),
            _ => self.write_uncompressed(batch, path),
        }
    }

    fn write_uncompressed(&self, batch: &ZiCRecordBatch, path: &Path) -> Result<()> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        match self.config.format {
            ZiCOutputFormat::Jsonl => self.write_jsonl(batch, &mut writer),
            ZiCOutputFormat::Json => self.write_json(batch, &mut writer),
            ZiCOutputFormat::Csv => self.write_csv(batch, &mut writer),
            ZiCOutputFormat::Parquet => self.write_parquet(batch, path),
        }
    }

    fn write_compressed(&self, batch: &ZiCRecordBatch, path: &Path, compression: CompressionType) -> Result<()> {
        match compression {
            CompressionType::Gzip => {
                let file = File::create(path)?;
                let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
                let mut writer = BufWriter::new(encoder);

                match self.config.format {
                    ZiCOutputFormat::Jsonl => self.write_jsonl(batch, &mut writer),
                    ZiCOutputFormat::Json => self.write_json(batch, &mut writer),
                    ZiCOutputFormat::Csv => self.write_csv(batch, &mut writer),
                    ZiCOutputFormat::Parquet => self.write_parquet(batch, path),
                }
            }
            CompressionType::Zstd => {
                let file = File::create(path)?;
                let encoder = zstd::Encoder::new(file, 0)
                    .map_err(|e| ZiError::validation(format!("Zstd encoder error: {}", e)))?;
                let mut writer = BufWriter::new(encoder);

                match self.config.format {
                    ZiCOutputFormat::Jsonl => self.write_jsonl(batch, &mut writer),
                    ZiCOutputFormat::Json => self.write_json(batch, &mut writer),
                    ZiCOutputFormat::Csv => self.write_csv(batch, &mut writer),
                    ZiCOutputFormat::Parquet => self.write_parquet(batch, path),
                }
            }
        }
    }

    fn write_jsonl<W: Write>(&self, batch: &ZiCRecordBatch, writer: &mut BufWriter<W>) -> Result<()> {
        for record in batch {
            let output = self.record_to_output(record);
            let line = serde_json::to_string(&output)?;
            writeln!(writer, "{}", line)?;
        }
        writer.flush()?;
        Ok(())
    }

    fn write_json<W: Write>(&self, batch: &ZiCRecordBatch, writer: &mut BufWriter<W>) -> Result<()> {
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

    fn write_csv<W: Write>(&self, batch: &ZiCRecordBatch, writer: &mut BufWriter<W>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let first_record = &batch[0];
        let headers = self.extract_headers(first_record);
        
        let mut csv_writer = csv::Writer::from_writer(writer);
        
        csv_writer.write_record(&headers).map_err(|e| ZiError::validation(format!("CSV write error: {}", e)))?;
        
        for record in batch {
            let row = self.record_to_row(record, &headers);
            csv_writer.write_record(&row).map_err(|e| ZiError::validation(format!("CSV write error: {}", e)))?;
        }
        
        csv_writer.flush().map_err(|e| ZiError::validation(format!("CSV flush error: {}", e)))?;
        Ok(())
    }

    fn write_parquet(&self, _batch: &ZiCRecordBatch, _path: &Path) -> Result<()> {
        #[cfg(feature = "parquet")]
        {
            return self.write_parquet_impl(_batch, _path);
        }
        #[cfg(not(feature = "parquet"))]
        {
            Err(ZiError::validation("Parquet writing requires 'parquet' feature"))
        }
    }

    #[cfg(feature = "parquet")]
    fn write_parquet_impl(&self, batch: &ZiCRecordBatch, path: &Path) -> Result<()> {
        use arrow::array::{ArrayRef, RecordBatch, StringArray, Int64Array};
        use arrow::datatypes::{Schema, Field, DataType};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        if batch.is_empty() {
            return Ok(());
        }

        let mut fields = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();

        let ids: Vec<Option<String>> = batch.iter().map(|r| r.id.clone()).collect();
        fields.push(Field::new("id", DataType::Utf8, true));
        columns.push(Arc::new(StringArray::from(ids)) as ArrayRef);

        let payloads: Vec<String> = batch.iter().map(|r| r.payload.to_string()).collect();
        fields.push(Field::new("payload", DataType::Utf8, false));
        columns.push(Arc::new(StringArray::from(payloads)) as ArrayRef);

        let schema = Arc::new(Schema::new(fields));
        let record_batch = RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| ZiError::validation(format!("Arrow error: {}", e)))?;

        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|e| ZiError::validation(format!("Parquet writer error: {}", e)))?;

        writer.write(&record_batch)
            .map_err(|e| ZiError::validation(format!("Parquet write error: {}", e)))?;

        writer.close()
            .map_err(|e| ZiError::validation(format!("Parquet close error: {}", e)))?;

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

    fn temp_path(&self, path: &Path) -> PathBuf {
        let filename = path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("temp");
        
        if let Some(parent) = path.parent() {
            parent.join(format!(".{}.tmp", filename))
        } else {
            PathBuf::from(format!(".{}.tmp", filename))
        }
    }

    fn split_path(&self, base_path: &Path, index: usize, total: usize) -> PathBuf {
        let stem = base_path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("output");
        
        let extension = base_path.extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");

        let padding = total.to_string().len().max(2);
        let new_name = if extension.is_empty() {
            format!("{}_{:0width$}", stem, index, width = padding)
        } else {
            format!("{}_{:0width$}.{}", stem, index, extension, width = padding)
        };

        if let Some(parent) = base_path.parent() {
            parent.join(new_name)
        } else {
            PathBuf::from(new_name)
        }
    }

    fn estimate_record_size(&self, record: &ZiCRecord) -> usize {
        let id_size = record.id.as_ref().map(|s| s.len()).unwrap_or(0);
        let payload_size = record.payload.to_string().len();
        let meta_size = record.metadata.as_ref()
            .and_then(|m| serde_json::to_string(m).ok())
            .map(|s| s.len())
            .unwrap_or(0);
        id_size + payload_size + meta_size + 64
    }

    fn estimate_batch_size(&self, batch: &ZiCRecordBatch) -> usize {
        batch.iter().map(|r| self.estimate_record_size(r)).sum()
    }

    pub fn stats(&self) -> &ZiCWriteStats {
        &self.stats
    }
}

enum CompressionType {
    Gzip,
    Zstd,
}
