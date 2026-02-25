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

//! # Data I/O Module
//!
//! This module provides comprehensive data import and export capabilities for Zi records,
//! supporting multiple file formats including JSONL, CSV, and Apache Parquet.
//!
//! ## Supported Formats
//!
//! - **JSONL** (JSON Lines): Each line is a complete JSON object containing `id`,
//!   `payload`, and optional `metadata` fields
//! - **CSV**: Tabular format with columns for id, payload (JSON), and metadata (JSON)
//! - **Parquet**: Columnar storage format optimized for analytical workloads
//!
//! ## Usage Example
//!
//! ```rust,no_run
//! use zi::io::ZiIO;
//! use zi::ZiIOFormat;
//!
//! // Auto-detect format from extension
//! let records = ZiIO::load_auto("data.jsonl").unwrap();
//!
//! // Explicit format
//! let records = ZiIO::load("data.csv", ZiIOFormat::Csv(Default::default())).unwrap();
//!
//! // Write records
//! ZiIO::write_auto("output.jsonl", &records).unwrap();
//! ```
//!
//! ## Format Requirements
//!
//! ### JSONL Format
//! Each line must be a valid JSON object with the following structure:
//! ```json
//! {"id": "record_1", "payload": "content", "metadata": {"key": "value"}}
//! ```
//!
//! ### CSV Format
//! Requires a `payload` column containing valid JSON. Optional `id` and `metadata` columns.
//!
//! ### Parquet Format
//! Must contain exactly three UTF-8 columns: `id`, `payload`, and `metadata`.

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use arrow2::array::{Array, MutableArray, MutableUtf8Array, Utf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::error::Error as ArrowError;
use serde_json::{Map, Value};

#[cfg(feature = "parquet")]
use arrow2::io::parquet::read::{infer_schema, read_metadata, FileReader as ParquetFileReader};
#[cfg(feature = "parquet")]
use arrow2::io::parquet::write::{
    CompressionOptions, Encoding, FileWriter as ParquetFileWriter, RowGroupIterator, Version,
    WriteOptions,
};
#[cfg(feature = "csv")]
use csv::{ReaderBuilder, WriterBuilder};

use crate::errors::{Result, ZiError};
use crate::record::{ZiMetadata, ZiRecord, ZiRecordBatch};

/// Supported Zi Core IO formats.
///
/// This enum defines all file formats that Zi can read and write.
/// Each variant may carry format-specific configuration options.
#[derive(Clone, Debug)]
pub enum ZiIOFormat {
    /// JSON Lines format: one JSON object per line
    Jsonl,
    /// Comma-Separated Values with optional custom configuration
    Csv(ZiCsvOptions),
    /// Apache Parquet columnar format
    Parquet,
}

/// Configuration for CSV ingestion.
///
/// Allows customization of delimiter character, header handling,
/// and other CSV-specific parsing options.
#[derive(Clone, Debug)]
pub struct ZiCsvOptions {
    /// Character used to separate fields (default: comma)
    pub delimiter: u8,
    /// Whether first row contains column headers (default: true)
    pub has_headers: bool,
}

impl Default for ZiCsvOptions {
    fn default() -> Self {
        ZiCsvOptions {
            delimiter: b',',
            has_headers: true,
        }
    }
}

/// Configuration flags for JSONL ingestion.
///
/// Controls comment handling, error tolerance, and record limits
/// when parsing JSON Lines format.
#[derive(Clone, Debug)]
pub struct ZiJsonlOptions {
    /// Allow lines starting with #, //, or -- as comments (default: false)
    pub allow_comments: bool,
    /// Skip malformed lines instead of failing (default: false)
    pub skip_invalid_lines: bool,
    /// Maximum number of records to read (default: None = unlimited)
    pub max_records: Option<usize>,
}

impl Default for ZiJsonlOptions {
    fn default() -> Self {
        ZiJsonlOptions {
            allow_comments: false,
            skip_invalid_lines: false,
            max_records: None,
        }
    }
}

/// Data ingestion façade exposing convenience helpers for Zi Core IO.
///
/// This struct provides a unified interface for reading and writing Zi records
/// across multiple file formats. All operations are synchronous and blocking.
pub struct ZiIO;

impl ZiIO {
    /// Infers the IO format from file extension.
    ///
    /// Supported extensions:
    /// - `jsonl`, `ndjson` -> Jsonl
    /// - `csv` -> Csv
    /// - `parquet`, `pq`, `parq` -> Parquet
    ///
    /// Returns `None` if the extension is not recognized.
    #[allow(non_snake_case)]
    pub fn detect_format(path: impl AsRef<Path>) -> Option<ZiIOFormat> {
        let ext = path
            .as_ref()
            .extension()?
            .to_string_lossy()
            .to_ascii_lowercase();
        match ext.as_str() {
            "jsonl" | "ndjson" => Some(ZiIOFormat::Jsonl),
            "csv" => Some(ZiIOFormat::Csv(ZiCsvOptions::default())),
            "parquet" | "pq" | "parq" => Some(ZiIOFormat::Parquet),
            _ => None,
        }
    }

    /// Loads records from the specified path using the given format.
    ///
    /// # Arguments
    /// * `path` - File path to read from
    /// * `format` - Explicit format specification
    ///
    /// # Errors
    /// Returns an error if the file cannot be read or parsing fails.
    #[allow(non_snake_case)]
    pub fn load(path: impl AsRef<Path>, format: ZiIOFormat) -> Result<ZiRecordBatch> {
        match format {
            ZiIOFormat::Jsonl => Self::load_jsonl(path),
            ZiIOFormat::Csv(opts) => Self::_load_csv(path, &opts),
            ZiIOFormat::Parquet => Self::_load_parquet(path),
        }
    }

    /// Loads records by inferring format from file extension.
    #[allow(non_snake_case)]
    pub fn load_auto(path: impl AsRef<Path>) -> Result<ZiRecordBatch> {
        let format = Self::detect_format(&path)
            .ok_or_else(|| ZiError::validation("unable to detect format from extension"))?;
        Self::load(path, format)
    }

    /// Writes a batch in the specified format.
    #[allow(non_snake_case)]
    pub fn write(
        path: impl AsRef<Path>,
        format: ZiIOFormat,
        batch: &[ZiRecord],
    ) -> Result<()> {
        match format {
            ZiIOFormat::Jsonl => Self::write_jsonl(path, batch),
            ZiIOFormat::Csv(opts) => Self::_write_csv(path, &opts, batch),
            ZiIOFormat::Parquet => Self::_write_parquet(path, batch),
        }
    }

    /// Writes a batch inferring format from extension.
    #[allow(non_snake_case)]
    pub fn write_auto(path: impl AsRef<Path>, batch: &[ZiRecord]) -> Result<()> {
        let format = Self::detect_format(&path)
            .ok_or_else(|| ZiError::validation("unable to detect format from extension"))?;
        Self::write(path, format, batch)
    }

    /// Loads records from a JSONL file where each line is either a payload value
    /// or an object containing `id`, `payload`, and optional `metadata` fields.
    #[allow(non_snake_case)]
    pub fn load_jsonl(path: impl AsRef<Path>) -> Result<ZiRecordBatch> {
        Self::load_jsonl_with_options(path, ZiJsonlOptions::default())
    }

    #[allow(non_snake_case)]
    pub fn load_jsonl_with_options(
        path: impl AsRef<Path>,
        options: ZiJsonlOptions,
    ) -> Result<ZiRecordBatch> {
        let file = File::open(path)?;
        Self::load_jsonl_reader_with_options(BufReader::new(file), options)
    }

    /// Loads records from any buffered reader that yields JSONL content.
    #[allow(non_snake_case)]
    pub fn load_jsonl_reader<R: BufRead>(reader: R) -> Result<ZiRecordBatch> {
        Self::load_jsonl_reader_with_options(reader, ZiJsonlOptions::default())
    }

    #[allow(non_snake_case)]
    pub fn load_jsonl_reader_with_options<R: BufRead>(
        reader: R,
        options: ZiJsonlOptions,
    ) -> Result<ZiRecordBatch> {
        let mut records = Vec::new();
        for (idx, line) in reader.lines().enumerate() {
            let line = line?;
            let trimmed = line.trim();

            if trimmed.is_empty() {
                continue;
            }

            if options.allow_comments
                && (trimmed.starts_with('#')
                    || trimmed.starts_with("//")
                    || trimmed.starts_with("--"))
            {
                continue;
            }

            match _parse_record(line, idx + 1) {
                Ok(record) => {
                    records.push(record);
                    if let Some(limit) = options.max_records {
                        if records.len() >= limit {
                            break;
                        }
                    }
                }
                Err(err) => {
                    if options.skip_invalid_lines {
                        log::warn!(
                            "skipping invalid jsonl line {} due to error: {}",
                            idx + 1,
                            err
                        );
                        continue;
                    } else {
                        return Err(err);
                    }
                }
            }
        }

        Ok(records)
    }

    /// Writes records to a JSONL file, storing payload, id, and metadata fields.
    #[allow(non_snake_case)]
    pub fn write_jsonl(path: impl AsRef<Path>, batch: &[ZiRecord]) -> Result<()> {
        let file = File::create(path)?;
        Self::write_jsonl_writer(BufWriter::new(file), batch)
    }

    /// Writes records to any output supporting [`Write`].
    #[allow(non_snake_case)]
    pub fn write_jsonl_writer<W: Write>(mut writer: W, batch: &[ZiRecord]) -> Result<()> {
        for record in batch {
            let mut root = Map::new();
            if let Some(id) = &record.id {
                root.insert("id".to_string(), Value::String(id.clone()));
            }
            root.insert("payload".to_string(), record.payload.clone());
            if let Some(metadata) = &record.metadata {
                root.insert("metadata".to_string(), Value::Object(metadata.clone()));
            }
            serde_json::to_writer(&mut writer, &Value::Object(root))?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
        Ok(())
    }

    fn _load_csv(path: impl AsRef<Path>, options: &ZiCsvOptions) -> Result<ZiRecordBatch> {
        let mut reader = ReaderBuilder::new()
            .delimiter(options.delimiter)
            .has_headers(options.has_headers)
            .from_path(path.as_ref())
            .map_err(|err| ZiError::Internal(format!("csv error: {err}")))?;

        let headers = if options.has_headers {
            Some(
                reader
                    .headers()
                    .map_err(|err| ZiError::Internal(format!("csv error: {err}")))?
                    .clone(),
            )
        } else {
            None
        };

        let id_idx = headers
            .as_ref()
            .and_then(|h| h.iter().position(|name| name == "id"));
        let payload_idx = headers
            .as_ref()
            .and_then(|h| h.iter().position(|name| name == "payload"));
        let metadata_idx = headers
            .as_ref()
            .and_then(|h| h.iter().position(|name| name == "metadata"));

        let mut batch = Vec::new();

        for (row_idx, result) in reader.records().enumerate() {
            let row_number = row_idx + 1;
            let record = result.map_err(|err| ZiError::Internal(format!("csv error: {err}")))?;

            let payload_value = if let Some(idx) = payload_idx {
                record
                    .get(idx)
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "{}".to_string())
            } else {
                return Err(ZiError::schema(
                    "csv requires a 'payload' column when headers are enabled",
                ));
            };

            let payload: Value = serde_json::from_str(&payload_value).map_err(|err| {
                ZiError::schema(format!(
                    "csv row {row_number}: payload column is not valid json ({err})"
                ))
            })?;

            let metadata = if let Some(idx) = metadata_idx {
                let raw = record.get(idx).map(ToString::to_string).unwrap_or_default();
                if raw.trim().is_empty() {
                    None
                } else {
                    let value: Value = serde_json::from_str(&raw).map_err(|err| {
                        ZiError::schema(format!(
                            "csv row {row_number}: metadata column is not valid json ({err})"
                        ))
                    })?;
                    match value {
                        Value::Object(map) => Some(map),
                        Value::Null => None,
                        other => {
                            return Err(ZiError::schema(format!(
                                "csv row {row_number}: metadata must be object or null, got {other}"
                            )))
                        }
                    }
                }
            } else {
                None
            };

            let id = id_idx
                .and_then(|idx| record.get(idx))
                .map(|id| id.trim())
                .filter(|id| !id.is_empty())
                .map(|id| id.to_string());

            batch.push(ZiRecord {
                id,
                payload,
                metadata,
            });
        }

        Ok(batch)
    }

    fn _write_csv(
        path: impl AsRef<Path>,
        options: &ZiCsvOptions,
        batch: &[ZiRecord],
    ) -> Result<()> {
        let mut writer = WriterBuilder::new()
            .delimiter(options.delimiter)
            .has_headers(options.has_headers)
            .from_path(path.as_ref())
            .map_err(|err| ZiError::Internal(format!("csv error: {err}")))?;

        if options.has_headers {
            writer
                .write_record(["id", "payload", "metadata"])
                .map_err(|err| ZiError::Internal(format!("csv error: {err}")))?;
        }

        for record in batch {
            let id = record.id.clone().unwrap_or_default();
            let payload_json = serde_json::to_string(&record.payload)?;
            let metadata_json = if let Some(metadata) = &record.metadata {
                serde_json::to_string(&Value::Object(metadata.clone()))?
            } else {
                String::new()
            };

            writer
                .write_record(&[id.as_str(), payload_json.as_str(), metadata_json.as_str()])
                .map_err(|err| ZiError::Internal(format!("csv error: {err}")))?;
        }

        writer
            .flush()
            .map_err(|err| ZiError::Internal(format!("csv error: {err}")))?;
        Ok(())
    }

    fn _load_parquet(path: impl AsRef<Path>) -> Result<ZiRecordBatch> {
        let mut file = File::open(path.as_ref())?;
        let metadata = read_metadata(&mut file).map_err(_parquet_err)?;
        let schema = infer_schema(&metadata).map_err(_parquet_err)?;

        if schema.fields.len() != 3
            || schema.fields[0].name != "id"
            || schema.fields[1].name != "payload"
            || schema.fields[2].name != "metadata"
        {
            return Err(ZiError::schema(
                "parquet file must contain id, payload, metadata utf8 columns",
            ));
        }

        let row_groups = metadata.row_groups.clone();
        let reader = ParquetFileReader::new(file, row_groups, schema, None, None, None);

        let mut batch = Vec::new();
        for chunk in reader {
            let chunk = chunk.map_err(_parquet_err)?;
            let columns = chunk.columns();
            let id_array = _as_utf8_array(columns[0].as_ref())?;
            let payload_array = _as_utf8_array(columns[1].as_ref())?;
            let metadata_array = _as_utf8_array(columns[2].as_ref())?;

            for row in 0..chunk.len() {
                let id = if id_array.is_null(row) {
                    None
                } else {
                    Some(id_array.value(row).to_string())
                };

                let payload_json = payload_array.value(row);
                let payload: Value = serde_json::from_str(payload_json).map_err(|err| {
                    ZiError::schema(format!(
                        "parquet row {row}: payload column is not valid json ({err})"
                    ))
                })?;

                let metadata = if metadata_array.is_null(row) {
                    None
                } else {
                    let raw = metadata_array.value(row);
                    if raw.trim().is_empty() {
                        None
                    } else {
                        let value: Value = serde_json::from_str(raw).map_err(|err| {
                            ZiError::schema(format!(
                                "parquet row {row}: metadata column is not valid json ({err})"
                            ))
                        })?;
                        match value {
                            Value::Object(map) => Some(map),
                            Value::Null => None,
                            other => {
                                return Err(ZiError::schema(format!(
                                "parquet row {row}: metadata must be object or null, got {other}"
                            )))
                            }
                        }
                    }
                };

                batch.push(ZiRecord {
                    id,
                    payload,
                    metadata,
                });
            }
        }

        Ok(batch)
    }

    fn _write_parquet(path: impl AsRef<Path>, batch: &[ZiRecord]) -> Result<()> {
        let mut id_col = MutableUtf8Array::<i32>::new();
        let mut payload_col = MutableUtf8Array::<i32>::new();
        let mut metadata_col = MutableUtf8Array::<i32>::new();

        for record in batch {
            match &record.id {
                Some(id) => id_col.push(Some(id.as_str())),
                None => id_col.push_null(),
            }

            let payload_json = serde_json::to_string(&record.payload)?;
            payload_col.push(Some(payload_json.as_str()));

            if let Some(metadata) = &record.metadata {
                let metadata_json = serde_json::to_string(&Value::Object(metadata.clone()))?;
                metadata_col.push(Some(metadata_json.as_str()));
            } else {
                metadata_col.push_null();
            }
        }

        let arrays: Vec<Arc<dyn Array>> = vec![
            id_col.into_arc(),
            payload_col.into_arc(),
            metadata_col.into_arc(),
        ];

        let chunk = Chunk::try_new(arrays).map_err(_parquet_err)?;

        let schema = Schema::from(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("payload", DataType::Utf8, false),
            Field::new("metadata", DataType::Utf8, true),
        ]);

        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Uncompressed,
            version: Version::V2,
            data_pagesize_limit: Some(1024 * 1024),
        };

        let encodings: Vec<Vec<Encoding>> = schema
            .fields
            .iter()
            .map(|_| vec![Encoding::Plain])
            .collect();

        let row_groups = RowGroupIterator::try_new(
            vec![arrow2::error::Result::Ok(chunk)].into_iter(),
            &schema,
            options,
            encodings,
        )
        .map_err(_parquet_err)?;

        let mut file = File::create(path.as_ref())?;
        let mut writer =
            ParquetFileWriter::try_new(&mut file, schema, options).map_err(_parquet_err)?;

        for group in row_groups {
            let group = group.map_err(_parquet_err)?;
            writer.write(group).map_err(_parquet_err)?;
        }

        writer.end(None).map_err(_parquet_err)?;
        Ok(())
    }
}

fn _as_utf8_array(array: &dyn Array) -> Result<&Utf8Array<i32>> {
    array
        .as_any()
        .downcast_ref::<Utf8Array<i32>>()
        .ok_or_else(|| ZiError::schema("parquet columns must be utf8"))
}

#[allow(non_snake_case)]
fn _parse_record(line: String, line_number: usize) -> Result<ZiRecord> {
    let value: Value = serde_json::from_str(&line)?;
    match value {
        Value::Object(mut object) => {
            let id = match object.remove("id") {
                Some(Value::String(s)) => Some(s),
                Some(_) => {
                    return Err(ZiError::schema(format!(
                        "line {line_number}: id must be string"
                    )))
                }
                None => None,
            };

            let metadata = match object.remove("metadata") {
                Some(value) => Some(_value_to_metadata(value).map_err(|_| {
                    ZiError::schema(format!("line {line_number}: metadata must be object"))
                })?),
                None => None,
            };

            let payload = object
                .remove("payload")
                .unwrap_or_else(|| Value::Object(object));

            Ok(ZiRecord {
                id,
                payload,
                metadata,
            })
        }
        payload => Ok(ZiRecord {
            id: None,
            payload,
            metadata: None,
        }),
    }
}

#[allow(non_snake_case)]
fn _value_to_metadata(value: Value) -> std::result::Result<ZiMetadata, Value> {
    match value {
        Value::Object(map) => Ok(map),
        other => Err(other),
    }
}

fn _parquet_err(err: ArrowError) -> ZiError {
    ZiError::Internal(format!("parquet error: {err}"))
}
