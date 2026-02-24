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

//! # Data Writer Module
//!
//! This module provides stream-based data writing capabilities with support for
//! multiple output formats, compression, and file splitting.

use std::path::{Path, PathBuf};
use std::io::{BufWriter, Write};
use std::fs::File;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::record::{ZiRecord, ZiRecordBatch};
use crate::ingest::format::ZiCompression;

/// Supported output data formats.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiOutputFormat {
    /// Line-delimited JSON format.
    Jsonl,
    /// JSON array format.
    Json,
    /// Comma-separated values format.
    Csv,
    /// Apache Parquet columnar format.
    Parquet,
}

/// Configuration for stream writer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiWriterConfig {
    /// Output data format.
    pub format: ZiOutputFormat,
    /// Pretty-print JSON output.
    pub pretty: bool,
    /// Number of records per batch.
    pub batch_size: usize,
    /// Compression type to apply.
    pub compression: ZiCompression,
    /// Split output by maximum file size in bytes.
    pub split_by_size: Option<usize>,
    /// Split output by maximum record count per file.
    pub split_by_count: Option<usize>,
    /// Use atomic write (write to temp then rename).
    pub atomic_write: bool,
}

impl Default for ZiWriterConfig {
    fn default() -> Self {
        Self {
            format: ZiOutputFormat::Jsonl,
            pretty: false,
            batch_size: 1000,
            compression: ZiCompression::None,
            split_by_size: None,
            split_by_count: None,
            atomic_write: true,
        }
    }
}

/// Statistics about write operations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ZiWriteStats {
    /// Total number of records written.
    pub records_written: usize,
    /// Total number of bytes written.
    pub bytes_written: usize,
    /// Number of files created.
    pub files_created: usize,
}

/// Internal compression type enumeration.
enum CompressionType {
    Gzip,
    Zstd,
}

/// Stream writer for exporting records to various formats.
///
/// Supports JSONL, JSON, CSV, and Parquet formats with optional compression
/// and automatic file splitting.
#[derive(Debug)]
pub struct ZiStreamWriter {
    config: ZiWriterConfig,
    stats: ZiWriteStats,
}

impl ZiStreamWriter {
    /// Creates a new stream writer with default configuration.
    #[allow(non_snake_case)]
    pub fn new() -> Self {
        Self {
            config: ZiWriterConfig::default(),
            stats: ZiWriteStats {
                records_written: 0,
                bytes_written: 0,
                files_created: 0,
            },
        }
    }

    /// Creates a new stream writer with custom configuration.
    #[allow(non_snake_case)]
    pub fn with_config(mut self, config: ZiWriterConfig) -> Self {
        self.config = config;
        self
    }

    /// Writes a batch of records to the specified path.
    ///
    /// Supports splitting by count or size based on configuration.
    #[allow(non_snake_case)]
    pub fn write(&mut self, batch: &ZiRecordBatch, path: &Path) -> Result<ZiWriteStats> {
        self.stats = ZiWriteStats::default();

        if let Some(split_count) = self.config.split_by_count {
            self.write_split_by_count(batch, path, split_count)?;
        } else if let Some(split_size) = self.config.split_by_size {
            self.write_split_by_size(batch, path, split_size)?;
        } else {
            self.write_single(batch, path)?;
        }

        Ok(self.stats.clone())
    }

    /// Writes batch to a single file.
    fn write_single(&mut self, batch: &ZiRecordBatch, path: &Path) -> Result<()> {
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

    /// Writes batch split across multiple files by record count.
    fn write_split_by_count(&mut self, batch: &ZiRecordBatch, base_path: &Path, count: usize) -> Result<()> {
        let total_records = batch.len();
        let num_files = (total_records + count - 1) / count;

        for (i, chunk) in batch.chunks(count).enumerate() {
            let path = self.split_path(base_path, i, num_files);
            let chunk_vec: ZiRecordBatch = chunk.to_vec();
            self.write_single(&chunk_vec, &path)?;
        }

        self.stats.records_written = total_records;
        Ok(())
    }

    /// Writes batch split across multiple files by size.
    fn write_split_by_size(&mut self, batch: &ZiRecordBatch, base_path: &Path, max_size: usize) -> Result<()> {
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

    /// Writes batch to a specific path with compression handling.
    fn write_to_path(&self, batch: &ZiRecordBatch, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        match self.config.compression {
            ZiCompression::Gzip => self.write_compressed(batch, path, CompressionType::Gzip),
            ZiCompression::Zstd => self.write_compressed(batch, path, CompressionType::Zstd),
            ZiCompression::None => self.write_uncompressed(batch, path),
            _ => self.write_uncompressed(batch, path),
        }
    }

    /// Writes uncompressed data.
    fn write_uncompressed(&self, batch: &ZiRecordBatch, path: &Path) -> Result<()> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        match self.config.format {
            ZiOutputFormat::Jsonl => self.write_jsonl(batch, &mut writer),
            ZiOutputFormat::Json => self.write_json(batch, &mut writer),
            ZiOutputFormat::Csv => self.write_csv(batch, &mut writer),
            ZiOutputFormat::Parquet => self.write_parquet(batch, path),
        }
    }

    /// Writes compressed data using specified compression type.
    fn write_compressed(&self, batch: &ZiRecordBatch, path: &Path, compression: CompressionType) -> Result<()> {
        match compression {
            CompressionType::Gzip => {
                let file = File::create(path)?;
                let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
                let mut writer = BufWriter::new(encoder);

                match self.config.format {
                    ZiOutputFormat::Jsonl => self.write_jsonl(batch, &mut writer),
                    ZiOutputFormat::Json => self.write_json(batch, &mut writer),
                    ZiOutputFormat::Csv => self.write_csv(batch, &mut writer),
                    ZiOutputFormat::Parquet => self.write_parquet(batch, path),
                }
            }
            CompressionType::Zstd => {
                let file = File::create(path)?;
                let encoder = zstd::Encoder::new(file, 0)
                    .map_err(|e| ZiError::validation(format!("Zstd encoder error: {}", e)))?;
                let mut writer = BufWriter::new(encoder);

                match self.config.format {
                    ZiOutputFormat::Jsonl => self.write_jsonl(batch, &mut writer),
                    ZiOutputFormat::Json => self.write_json(batch, &mut writer),
                    ZiOutputFormat::Csv => self.write_csv(batch, &mut writer),
                    ZiOutputFormat::Parquet => self.write_parquet(batch, path),
                }
            }
        }
    }

    /// Writes records in JSONL format (one JSON object per line).
    fn write_jsonl<W: Write>(&self, batch: &ZiRecordBatch, writer: &mut BufWriter<W>) -> Result<()> {
        for record in batch {
            let output = self.record_to_output(record);
            let line = serde_json::to_string(&output)?;
            writeln!(writer, "{}", line)?;
        }
        writer.flush()?;
        Ok(())
    }

    /// Writes records in JSON array format.
    fn write_json<W: Write>(&self, batch: &ZiRecordBatch, writer: &mut BufWriter<W>) -> Result<()> {
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

    /// Writes records in CSV format.
    fn write_csv<W: Write>(&self, batch: &ZiRecordBatch, writer: &mut BufWriter<W>) -> Result<()> {
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

    /// Writes records in Parquet format.
    fn write_parquet(&self, _batch: &ZiRecordBatch, _path: &Path) -> Result<()> {
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
    fn write_parquet_impl(&self, batch: &ZiRecordBatch, path: &Path) -> Result<()> {
        use arrow::array::{ArrayRef, RecordBatch, StringArray};
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

    /// Converts a record to output format (JSON object).
    fn record_to_output(&self, record: &ZiRecord) -> Value {
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

    /// Extracts CSV headers from a record.
    fn extract_headers(&self, record: &ZiRecord) -> Vec<String> {
        let mut headers = vec!["id".to_string()];
        
        if let Value::Object(map) = &record.payload {
            for key in map.keys() {
                headers.push(format!("payload.{}", key));
            }
        }
        
        headers
    }

    /// Converts a record to CSV row.
    fn record_to_row(&self, record: &ZiRecord, headers: &[String]) -> Vec<String> {
        let mut row = Vec::new();
        
        row.push(record.id.clone().unwrap_or_default());
        
        if let Value::Object(map) = &record.payload {
            for header in headers.iter().skip(1) {
                let key = header.strip_prefix("payload.").unwrap_or(header);
                let value = map.get(key)
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                row.push(value);
            }
        }
        
        row
    }

    /// Generates temporary path for atomic writes.
    fn temp_path(&self, path: &Path) -> PathBuf {
        let _ext = path.extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");
        let stem = path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("output");
        let parent = path.parent().unwrap_or(Path::new("."));
        
        parent.join(format!(".{}.tmp", stem))
    }

    /// Generates split file path with index.
    fn split_path(&self, base_path: &Path, index: usize, total: usize) -> PathBuf {
        let ext = base_path.extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");
        let stem = base_path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("output");
        let parent = base_path.parent().unwrap_or(Path::new("."));
        
        let width = total.to_string().len();
        parent.join(format!("{}_{:0>width$}.{}", stem, index, ext))
    }

    /// Estimates total batch size in bytes.
    fn estimate_batch_size(&self, batch: &ZiRecordBatch) -> usize {
        batch.iter().map(|r| self.estimate_record_size(r)).sum()
    }

    /// Estimates single record size in bytes.
    fn estimate_record_size(&self, record: &ZiRecord) -> usize {
        let id_size = record.id.as_ref().map(|s| s.len()).unwrap_or(0);
        let payload_size = record.payload.to_string().len();
        id_size + payload_size
    }
}
