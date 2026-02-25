//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
//!
//! This file is part of Zi.
//! The Zi project belongs to the Dunimd Team.
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

//! # Data Reader Module
//!
//! This module provides stream-based data reading capabilities with support for
//! multiple input formats, error handling, and progress tracking.

use std::path::Path;
use std::io::{BufRead, BufReader, Read};
use std::fs::File;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::record::{ZiRecord, ZiRecordBatch};
use crate::ingest::format::{ZiDataFormat, ZiFormatDetector};

use arrow2::array::Array;

type Utf8ArrayI32 = arrow2::array::Utf8Array<i32>;

fn _as_utf8_array(array: &dyn Array) -> Result<&Utf8ArrayI32> {
    array
        .as_any()
        .downcast_ref::<Utf8ArrayI32>()
        .ok_or_else(|| ZiError::validation("parquet columns must be utf8"))
}

/// Callback function type for progress updates.
pub type ProgressCallback = Box<dyn Fn(ProgressInfo) + Send + Sync>;

/// Progress information for reading operations.
#[derive(Clone, Debug)]
pub struct ProgressInfo {
    /// Number of records read so far.
    pub records_read: usize,
    /// Number of bytes read so far.
    pub bytes_read: usize,
    /// Total bytes if known, None otherwise.
    pub total_bytes: Option<usize>,
    /// Current file being read.
    pub current_file: String,
}

/// Configuration for stream reader.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiReaderConfig {
    /// Number of records to read per batch.
    pub batch_size: usize,
    /// Whether to skip records with parse errors.
    pub skip_errors: bool,
    /// Maximum number of errors before failing.
    pub max_errors: usize,
    /// Character encoding for text files.
    pub encoding: String,
    /// Number of records between progress callbacks.
    pub progress_interval: usize,
}

impl Default for ZiReaderConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            skip_errors: true,
            max_errors: 100,
            encoding: "utf-8".to_string(),
            progress_interval: 10000,
        }
    }
}

/// Iterator for streaming record reads.
#[allow(dead_code)]
pub struct ZiRecordIterator<'a> {
    reader: &'a ZiStreamReader,
    format: ZiDataFormat,
    file: File,
    file_size: Option<usize>,
    file_path: String,
    records_read: usize,
    bytes_read: usize,
    error_count: usize,
    buffer: Vec<u8>,
    exhausted: bool,
}

impl<'a> Iterator for ZiRecordIterator<'a> {
    type Item = Result<ZiRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        loop {
            self.buffer.clear();
            
            let bytes_read = match self.file.read(&mut self.buffer) {
                Ok(0) => {
                    self.exhausted = true;
                    return None;
                }
                Ok(n) => n,
                Err(e) => {
                    self.error_count += 1;
                    if !self.reader.config.skip_errors 
                        || self.error_count > self.reader.config.max_errors {
                        self.exhausted = true;
                        return Some(Err(ZiError::validation(format!("Read error: {}", e))));
                    }
                    continue;
                }
            };

            self.bytes_read += bytes_read;
            
            let line = match String::from_utf8(self.buffer.clone()) {
                Ok(l) => l,
                Err(_) => continue,
            };

            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            match serde_json::from_str::<Value>(trimmed) {
                Ok(value) => {
                    self.records_read += 1;
                    let record = ZiRecord::new(
                        Some(format!("{}_{}", self.file_path, self.records_read)), 
                        value
                    );
                    
                    if self.records_read % self.reader.config.progress_interval == 0 {
                        self.reader.report_progress(
                            self.records_read,
                            self.bytes_read,
                            self.file_size,
                            Path::new(&self.file_path),
                        );
                    }
                    
                    return Some(Ok(record));
                }
                Err(e) => {
                    self.error_count += 1;
                    if !self.reader.config.skip_errors 
                        || self.error_count > self.reader.config.max_errors {
                        self.exhausted = true;
                        return Some(Err(ZiError::validation(format!(
                            "Parse error at record {}: {}",
                            self.records_read + 1, e
                        ))));
                    }
                    log::warn!("Skipping invalid record: {}", e);
                }
            }
        }
    }
}

/// Stream reader for importing records from various formats.
///
/// Supports JSONL, JSON, CSV, and Parquet formats with automatic format detection.
pub struct ZiStreamReader {
    config: ZiReaderConfig,
    detector: ZiFormatDetector,
    progress_callback: Option<ProgressCallback>,
}

impl ZiStreamReader {
    /// Creates a new stream reader with default configuration.
    #[allow(non_snake_case)]
    pub fn new() -> Self {
        Self {
            config: ZiReaderConfig::default(),
            detector: ZiFormatDetector::new(),
            progress_callback: None,
        }
    }

    /// Sets custom reader configuration.
    #[allow(non_snake_case)]
    pub fn with_config(mut self, config: ZiReaderConfig) -> Self {
        self.config = config;
        self
    }

    /// Sets progress callback function.
    #[allow(non_snake_case)]
    pub fn with_progress(mut self, callback: ProgressCallback) -> Self {
        self.progress_callback = Some(callback);
        self
    }

    /// Reads all records from a file into a batch.
    ///
    /// Automatically detects format based on file extension and content.
    #[allow(non_snake_case)]
    pub fn read_path(&self, path: &Path) -> Result<ZiRecordBatch> {
        let format = self.detector.detect_from_path(path)?;
        
        match format {
            ZiDataFormat::Jsonl => self.read_jsonl(path),
            ZiDataFormat::Json => self.read_json(path),
            ZiDataFormat::Csv => self.read_csv(path),
            ZiDataFormat::Parquet => self.read_parquet(path),
            ZiDataFormat::Unknown => Err(ZiError::validation(format!(
                "Unknown file format: {}",
                path.display()
            ))),
        }
    }

    /// Returns an iterator for streaming reads.
    ///
    /// Useful for processing large files without loading all records into memory.
    #[allow(non_snake_case)]
    pub fn read_path_batch<'a>(
        &'a self,
        path: &'a Path,
    ) -> Result<ZiRecordIterator<'a>> {
        let format = self.detector.detect_from_path(path)?;
        let file = File::open(path)?;
        let file_size = file.metadata().ok().map(|m| m.len() as usize);
        
        Ok(ZiRecordIterator {
            reader: self,
            format,
            file,
            file_size,
            file_path: path.to_string_lossy().to_string(),
            records_read: 0,
            bytes_read: 0,
            error_count: 0,
            buffer: Vec::new(),
            exhausted: false,
        })
    }

    /// Creates a buffered reader from a file handle.
    fn create_reader(file: File) -> BufReader<File> {
        BufReader::with_capacity(1024 * 1024, file)
    }

    /// Reads records from JSONL format.
    fn read_jsonl(&self, path: &Path) -> Result<ZiRecordBatch> {
        let file = File::open(path)?;
        let file_size = file.metadata().ok().map(|m| m.len() as usize);
        let reader = Self::create_reader(file);
        let mut batch = Vec::with_capacity(self.config.batch_size);
        let mut error_count = 0;
        let mut records_read = 0;
        let mut bytes_read = 0;

        for (idx, line) in reader.lines().enumerate() {
            match line {
                Ok(text) => {
                    bytes_read += text.len() + 1;
                    if text.trim().is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<Value>(&text) {
                        Ok(value) => {
                            let record = ZiRecord::new(Some(format!("{}", idx)), value);
                            batch.push(record);
                            records_read += 1;
                            
                            if records_read % self.config.progress_interval == 0 {
                                self.report_progress(records_read, bytes_read, file_size, path);
                            }
                        }
                        Err(e) => {
                            error_count += 1;
                            if !self.config.skip_errors || error_count > self.config.max_errors {
                                return Err(ZiError::validation(format!(
                                    "Too many errors ({}): last error at line {}: {}",
                                    error_count, idx, e
                                )));
                            }
                            log::warn!("Skipping invalid JSON line {}: {}", idx, e);
                        }
                    }
                }
                Err(e) => {
                    error_count += 1;
                    if !self.config.skip_errors || error_count > self.config.max_errors {
                        return Err(ZiError::validation(format!("Failed to read line: {}", e)));
                    }
                    log::warn!("Skipping unreadable line: {}", e);
                }
            }
        }

        Ok(batch)
    }

    /// Reads records from JSON format (array or object).
    fn read_json(&self, path: &Path) -> Result<ZiRecordBatch> {
        let content = std::fs::read_to_string(path)?;
        let value: Value = serde_json::from_str(&content)?;

        match value {
            Value::Array(arr) => {
                let total = arr.len();
                let batch: ZiRecordBatch = arr
                    .into_iter()
                    .enumerate()
                    .map(|(idx, v)| {
                        if idx % self.config.progress_interval == 0 {
                            self.report_progress(idx, content.len(), Some(content.len()), path);
                        }
                        ZiRecord::new(Some(format!("{}", idx)), v)
                    })
                    .collect();
                self.report_progress(total, content.len(), Some(content.len()), path);
                Ok(batch)
            }
            Value::Object(_) => {
                Ok(vec![ZiRecord::new(Some("0".to_string()), value)])
            }
            _ => Err(ZiError::validation("JSON must be array or object")),
        }
    }

    /// Reads records from CSV format.
    fn read_csv(&self, path: &Path) -> Result<ZiRecordBatch> {
        let file = File::open(path)?;
        let file_size = file.metadata().ok().map(|m| m.len() as usize);
        let reader = Self::create_reader(file);
        let mut csv_reader = csv::Reader::from_reader(reader);
        let headers: Vec<String> = csv_reader.headers()
            .map_err(|e| ZiError::validation(format!("CSV headers error: {}", e)))?
            .iter()
            .map(|s| s.to_string())
            .collect();
        
        let mut batch = Vec::with_capacity(self.config.batch_size);
        let mut error_count = 0;
        let mut records_read = 0;
        let bytes_read = 0;

        for (idx, result) in csv_reader.records().enumerate() {
            match result {
                Ok(record) => {
                    let mut obj = serde_json::Map::new();
                    for (i, field) in record.iter().enumerate() {
                        if i < headers.len() {
                            obj.insert(headers[i].clone(), Value::String(field.to_string()));
                        }
                    }
                    let value = Value::Object(obj);
                    batch.push(ZiRecord::new(Some(format!("{}", idx)), value));
                    records_read += 1;
                    
                    if records_read % self.config.progress_interval == 0 {
                        self.report_progress(records_read, bytes_read, file_size, path);
                    }
                }
                Err(e) => {
                    error_count += 1;
                    if !self.config.skip_errors || error_count > self.config.max_errors {
                        return Err(ZiError::validation(format!(
                            "Too many errors ({}): last error at row {}: {}",
                            error_count, idx, e
                        )));
                    }
                    log::warn!("Skipping invalid CSV row {}: {}", idx, e);
                }
            }
        }

        Ok(batch)
    }

    /// Reads records from Parquet format.
    fn read_parquet(&self, path: &Path) -> Result<ZiRecordBatch> {
        #[cfg(feature = "parquet")]
        {
            use arrow2::io::parquet::read::{infer_schema, read_metadata, FileReader as ParquetFileReader};

            let mut file = File::open(path)?;
            let metadata = read_metadata(&mut file).map_err(|e| ZiError::validation(format!("Read metadata: {}", e)))?;
            let schema = infer_schema(&metadata).map_err(|e| ZiError::validation(format!("Infer schema: {}", e)))?;

            if schema.fields.len() < 2 {
                return Err(ZiError::validation("parquet file must contain id, payload columns"));
            }

            let row_groups = metadata.row_groups.clone();
            let file = File::open(path)?;
            let reader = ParquetFileReader::new(file, row_groups, schema, None, None, None);

            let mut batch = Vec::new();

            for maybe_chunk in reader {
                let chunk = maybe_chunk.map_err(|e| ZiError::validation(format!("Read chunk: {}", e)))?;
                let columns = chunk.columns();

                let id_array = _as_utf8_array(columns[0].as_ref())?;
                let payload_array = _as_utf8_array(columns[1].as_ref())?;

                for i in 0..chunk.len() {
                    let id = if id_array.is_null(i) {
                        None
                    } else {
                        Some(id_array.value(i).to_string())
                    };
                    let payload_str = if payload_array.is_null(i) {
                        "{}"
                    } else {
                        payload_array.value(i)
                    };
                    let payload: Value = serde_json::from_str(payload_str)
                        .unwrap_or(Value::String(payload_str.to_string()));
                    
                    batch.push(ZiRecord::new(id, payload));
                }
            }

            Ok(batch)
        }

        #[cfg(not(feature = "parquet"))]
        {
            Err(ZiError::validation("Parquet reading requires 'parquet' feature"))
        }
    }

    /// Reports progress to callback if configured.
    fn report_progress(&self, records: usize, bytes: usize, total: Option<usize>, path: &Path) {
        if let Some(ref callback) = self.progress_callback {
            callback(ProgressInfo {
                records_read: records,
                bytes_read: bytes,
                total_bytes: total,
                current_file: path.to_string_lossy().to_string(),
            });
        }
    }
}
