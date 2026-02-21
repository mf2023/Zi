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

use std::path::Path;
use std::io::{BufRead, BufReader, Read};
use std::fs::File;

use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::record::{ZiCRecord, ZiCRecordBatch};
use crate::ingest::format::{ZiCDataFormat, ZiCFormatDetector};

pub type ProgressCallback = Box<dyn Fn(ProgressInfo) + Send + Sync>;

#[derive(Clone, Debug)]
pub struct ProgressInfo {
    pub records_read: usize,
    pub bytes_read: usize,
    pub total_bytes: Option<usize>,
    pub current_file: String,
}

#[derive(Clone, Debug)]
pub struct ZiCReaderConfig {
    pub batch_size: usize,
    pub skip_errors: bool,
    pub max_errors: usize,
    pub encoding: String,
    pub progress_interval: usize,
}

impl Default for ZiCReaderConfig {
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

pub struct ZiCStreamReader {
    config: ZiCReaderConfig,
    detector: ZiCFormatDetector,
    progress_callback: Option<ProgressCallback>,
}

impl ZiCStreamReader {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        Self {
            config: ZiCReaderConfig::default(),
            detector: ZiCFormatDetector::ZiFNew(),
            progress_callback: None,
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFWithConfig(mut self, config: ZiCReaderConfig) -> Self {
        self.config = config;
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFWithProgress(mut self, callback: ProgressCallback) -> Self {
        self.progress_callback = Some(callback);
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

    #[allow(non_snake_case)]
    pub fn ZiFReadPathBatch<'a>(
        &'a self,
        path: &'a Path,
    ) -> Result<ZiCRecordIterator<'a>> {
        let format = self.detector.ZiFDetectFromPath(path)?;
        let file = File::open(path)?;
        let file_size = file.metadata().ok().map(|m| m.len() as usize);
        
        Ok(ZiCRecordIterator {
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

    fn read_jsonl(&self, path: &Path) -> Result<ZiCRecordBatch> {
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
                            let record = ZiCRecord::ZiFNew(Some(format!("{}", idx)), value);
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

    fn read_json(&self, path: &Path) -> Result<ZiCRecordBatch> {
        let content = std::fs::read_to_string(path)?;
        let value: Value = serde_json::from_str(&content)?;

        match value {
            Value::Array(arr) => {
                let total = arr.len();
                let batch: ZiCRecordBatch = arr
                    .into_iter()
                    .enumerate()
                    .map(|(idx, v)| {
                        if idx % self.config.progress_interval == 0 {
                            self.report_progress(idx, content.len(), Some(content.len()), path);
                        }
                        ZiCRecord::ZiFNew(Some(format!("{}", idx)), v)
                    })
                    .collect();
                self.report_progress(total, content.len(), Some(content.len()), path);
                Ok(batch)
            }
            Value::Object(_) => {
                Ok(vec![ZiCRecord::ZiFNew(Some("0".to_string()), value)])
            }
            _ => Err(ZiError::validation("JSON must be array or object")),
        }
    }

    fn read_csv(&self, path: &Path) -> Result<ZiCRecordBatch> {
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
                    batch.push(ZiCRecord::ZiFNew(Some(format!("{}", idx)), value));
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

    fn read_parquet(&self, _path: &Path) -> Result<ZiCRecordBatch> {
        #[cfg(feature = "parquet")]
        {
            return self.read_parquet_impl(_path);
        }
        #[cfg(not(feature = "parquet"))]
        {
            Err(ZiError::validation("Parquet reading requires 'parquet' feature"))
        }
    }

    #[cfg(feature = "parquet")]
    fn read_parquet_impl(&self, path: &Path) -> Result<ZiCRecordBatch> {
        use parquet::file::reader::{SerializedFileReader, FileReader};
        
        let file = File::open(path)?;
        let reader = SerializedFileReader::new(file)
            .map_err(|e| ZiError::validation(format!("Failed to open parquet: {}", e)))?;
        
        let mut batch = Vec::with_capacity(self.config.batch_size);
        let mut records_read = 0;
        
        let iter = reader.get_row_iter(None)
            .map_err(|e| ZiError::validation(format!("Failed to create row iterator: {}", e)))?;
        
        for row in iter {
            match row {
                Ok(row) => {
                    let value = self.parquet_row_to_value(&row);
                    batch.push(ZiCRecord::ZiFNew(Some(format!("{}", records_read)), value));
                    records_read += 1;
                    
                    if records_read % self.config.progress_interval == 0 {
                        self.report_progress(records_read, 0, None, path);
                    }
                }
                Err(e) => {
                    if !self.config.skip_errors {
                        return Err(ZiError::validation(format!("Parquet row error: {}", e)));
                    }
                    log::warn!("Skipping invalid parquet row: {}", e);
                }
            }
        }
        
        Ok(batch)
    }

    #[cfg(feature = "parquet")]
    fn parquet_row_to_value(&self, row: &parquet::record::Row) -> Value {
        let mut obj = serde_json::Map::new();
        for field in row.get_column_iter() {
            let key = field.0.clone();
            let value = self.parquet_field_to_value(field.1);
            obj.insert(key, value);
        }
        Value::Object(obj)
    }

    #[cfg(feature = "parquet")]
    fn parquet_field_to_value(&self, field: &parquet::record::Field) -> Value {
        use parquet::record::Field;
        match field {
            Field::Null => Value::Null,
            Field::Bool(b) => Value::Bool(*b),
            Field::Byte(b) => Value::Number((*b).into()),
            Field::Short(s) => Value::Number((*s).into()),
            Field::Int(i) => Value::Number((*i).into()),
            Field::Long(l) => Value::Number((*l).into()),
            Field::Float(f) => {
                serde_json::Number::from_f64(*f as f64)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            }
            Field::Double(d) => {
                serde_json::Number::from_f64(*d)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            }
            Field::Str(s) => Value::String(s.to_string()),
            Field::Bytes(b) => Value::String(base64_encode(b.data())),
            Field::Group(group) => {
                let mut obj = serde_json::Map::new();
                for (k, v) in group.get_column_iter() {
                    obj.insert(k.clone(), self.parquet_field_to_value(v));
                }
                Value::Object(obj)
            }
            _ => Value::Null,
        }
    }

    fn create_reader<R: Read + 'static>(reader: R) -> Box<dyn BufRead> {
        Box::new(BufReader::new(reader))
    }

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

#[cfg(feature = "parquet")]
fn base64_encode(data: &[u8]) -> String {
    use base64::{Engine, engine::general_purpose::STANDARD};
    STANDARD.encode(data)
}

#[cfg(not(feature = "parquet"))]
fn base64_encode(data: &[u8]) -> String {
    format!("<{} bytes>", data.len())
}

pub struct ZiCRecordIterator<'a> {
    reader: &'a ZiCStreamReader,
    format: ZiCDataFormat,
    file: File,
    file_size: Option<usize>,
    file_path: String,
    records_read: usize,
    bytes_read: usize,
    error_count: usize,
    #[allow(dead_code)]
    buffer: ZiCRecordBatch,
    exhausted: bool,
}

impl<'a> ZiCRecordIterator<'a> {
    pub fn next_batch(&mut self) -> Result<Option<ZiCRecordBatch>> {
        if self.exhausted {
            return Ok(None);
        }

        match self.format {
            ZiCDataFormat::Jsonl => self.next_jsonl_batch(),
            ZiCDataFormat::Csv => self.next_csv_batch(),
            _ => {
                self.exhausted = true;
                Ok(None)
            }
        }
    }

    fn next_jsonl_batch(&mut self) -> Result<Option<ZiCRecordBatch>> {
        let reader = BufReader::new(&self.file);
        let mut batch = Vec::with_capacity(self.reader.config.batch_size);

        for line in reader.lines().take(self.reader.config.batch_size) {
            match line {
                Ok(text) => {
                    self.bytes_read += text.len() + 1;
                    let trimmed = text.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<Value>(trimmed) {
                        Ok(value) => {
                            let record = ZiCRecord::ZiFNew(
                                Some(format!("{}", self.records_read)),
                                value,
                            );
                            batch.push(record);
                            self.records_read += 1;
                        }
                        Err(e) => {
                            self.error_count += 1;
                            if !self.reader.config.skip_errors 
                                || self.error_count > self.reader.config.max_errors 
                            {
                                return Err(ZiError::validation(format!(
                                    "Too many errors at line {}: {}",
                                    self.records_read, e
                                )));
                            }
                        }
                    }
                }
                Err(e) => {
                    self.error_count += 1;
                    if !self.reader.config.skip_errors {
                        return Err(ZiError::validation(format!("Read error: {}", e)));
                    }
                }
            }
        }

        if batch.is_empty() && self.exhausted {
            Ok(None)
        } else {
            self.reader.report_progress(
                self.records_read,
                self.bytes_read,
                self.file_size,
                Path::new(&self.file_path),
            );
            Ok(Some(batch))
        }
    }

    fn next_csv_batch(&mut self) -> Result<Option<ZiCRecordBatch>> {
        let reader = BufReader::new(&self.file);
        let mut csv_reader = csv::Reader::from_reader(reader);
        
        let headers: Vec<String> = csv_reader.headers()
            .map(|h| h.iter().map(|s| s.to_string()).collect())
            .unwrap_or_default();

        let mut batch = Vec::with_capacity(self.reader.config.batch_size);

        for _ in 0..self.reader.config.batch_size {
            match csv_reader.records().next() {
                Some(Ok(record)) => {
                    let mut obj = serde_json::Map::new();
                    for (i, field) in record.iter().enumerate() {
                        if i < headers.len() {
                            obj.insert(headers[i].clone(), Value::String(field.to_string()));
                        }
                    }
                    batch.push(ZiCRecord::ZiFNew(
                        Some(format!("{}", self.records_read)),
                        Value::Object(obj),
                    ));
                    self.records_read += 1;
                }
                Some(Err(e)) => {
                    self.error_count += 1;
                    if !self.reader.config.skip_errors {
                        return Err(ZiError::validation(format!("CSV error: {}", e)));
                    }
                }
                None => {
                    self.exhausted = true;
                    break;
                }
            }
        }

        if batch.is_empty() && self.exhausted {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }

    pub fn records_read(&self) -> usize {
        self.records_read
    }

    pub fn bytes_read(&self) -> usize {
        self.bytes_read
    }

    pub fn error_count(&self) -> usize {
        self.error_count
    }
}
