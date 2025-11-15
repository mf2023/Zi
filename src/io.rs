//! Copyright © 2025 Dunimd Team. All Rights Reserved.
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

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
#[cfg(test)]
use std::io::{Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use arrow2::array::{Array, MutableArray, MutableUtf8Array, Utf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::error::Error as ArrowError;
use arrow2::io::parquet::read::{infer_schema, read_metadata, FileReader as ParquetFileReader};
use arrow2::io::parquet::write::{
    CompressionOptions, Encoding, FileWriter as ParquetFileWriter, RowGroupIterator, Version,
    WriteOptions,
};
use csv::{ReaderBuilder, WriterBuilder};
use serde_json::{Map, Value};

use crate::errors::{Result, ZiError};
use crate::record::{ZiCMetadata, ZiCRecord, ZiCRecordBatch};

/// Supported Zi Core IO formats.
#[derive(Clone, Debug)]
pub enum ZiCIOFormat {
    Jsonl,
    Csv(ZiCCsvOptions),
    Parquet,
}

/// Configuration for CSV ingestion.
#[derive(Clone, Debug)]
pub struct ZiCCsvOptions {
    pub delimiter: u8,
    pub has_headers: bool,
}

impl Default for ZiCCsvOptions {
    fn default() -> Self {
        ZiCCsvOptions {
            delimiter: b',',
            has_headers: true,
        }
    }
}

/// Data ingestion façade exposing convenience helpers for Zi Core IO.
pub struct ZiCIO;

impl ZiCIO {
    /// Attempts to infer an IO format from the file extension.
    #[allow(non_snake_case)]
    pub fn ZiFDetectFormat(path: impl AsRef<Path>) -> Option<ZiCIOFormat> {
        let ext = path
            .as_ref()
            .extension()?
            .to_string_lossy()
            .to_ascii_lowercase();
        match ext.as_str() {
            "jsonl" | "ndjson" => Some(ZiCIOFormat::Jsonl),
            "csv" => Some(ZiCIOFormat::Csv(ZiCCsvOptions::default())),
            "parquet" | "pq" | "parq" => Some(ZiCIOFormat::Parquet),
            _ => None,
        }
    }

    /// Loads records according to the provided format.
    #[allow(non_snake_case)]
    pub fn ZiFLoad(path: impl AsRef<Path>, format: ZiCIOFormat) -> Result<ZiCRecordBatch> {
        match format {
            ZiCIOFormat::Jsonl => Self::ZiFLoadJsonl(path),
            ZiCIOFormat::Csv(opts) => Self::_load_csv(path, &opts),
            ZiCIOFormat::Parquet => Self::_load_parquet(path),
        }
    }

    /// Loads records by inferring format from file extension.
    #[allow(non_snake_case)]
    pub fn ZiFLoadAuto(path: impl AsRef<Path>) -> Result<ZiCRecordBatch> {
        let format = Self::ZiFDetectFormat(&path)
            .ok_or_else(|| ZiError::validation("unable to detect format from extension"))?;
        Self::ZiFLoad(path, format)
    }

    /// Writes a batch in the specified format.
    #[allow(non_snake_case)]
    pub fn ZiFWrite(
        path: impl AsRef<Path>,
        format: ZiCIOFormat,
        batch: &[ZiCRecord],
    ) -> Result<()> {
        match format {
            ZiCIOFormat::Jsonl => Self::ZiFWriteJsonl(path, batch),
            ZiCIOFormat::Csv(opts) => Self::_write_csv(path, &opts, batch),
            ZiCIOFormat::Parquet => Self::_write_parquet(path, batch),
        }
    }

    /// Writes a batch inferring format from extension.
    #[allow(non_snake_case)]
    pub fn ZiFWriteAuto(path: impl AsRef<Path>, batch: &[ZiCRecord]) -> Result<()> {
        let format = Self::ZiFDetectFormat(&path)
            .ok_or_else(|| ZiError::validation("unable to detect format from extension"))?;
        Self::ZiFWrite(path, format, batch)
    }

    /// Loads records from a JSONL file where each line is either a payload value
    /// or an object containing `id`, `payload`, and optional `metadata` fields.
    #[allow(non_snake_case)]
    pub fn ZiFLoadJsonl(path: impl AsRef<Path>) -> Result<ZiCRecordBatch> {
        let file = File::open(path)?;
        Self::ZiFLoadJsonlReader(BufReader::new(file))
    }

    /// Loads records from any buffered reader that yields JSONL content.
    #[allow(non_snake_case)]
    pub fn ZiFLoadJsonlReader<R: BufRead>(reader: R) -> Result<ZiCRecordBatch> {
        reader
            .lines()
            .enumerate()
            .filter_map(|(idx, line)| match line {
                Ok(content) if content.trim().is_empty() => None,
                Ok(content) => Some(_parse_record(content, idx + 1)),
                Err(err) => Some(Err(err.into())),
            })
            .collect()
    }

    /// Writes records to a JSONL file, storing payload, id, and metadata fields.
    #[allow(non_snake_case)]
    pub fn ZiFWriteJsonl(path: impl AsRef<Path>, batch: &[ZiCRecord]) -> Result<()> {
        let file = File::create(path)?;
        Self::ZiFWriteJsonlWriter(BufWriter::new(file), batch)
    }

    /// Writes records to any output supporting [`Write`].
    #[allow(non_snake_case)]
    pub fn ZiFWriteJsonlWriter<W: Write>(mut writer: W, batch: &[ZiCRecord]) -> Result<()> {
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

    fn _load_csv(path: impl AsRef<Path>, options: &ZiCCsvOptions) -> Result<ZiCRecordBatch> {
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

            batch.push(ZiCRecord {
                id,
                payload,
                metadata,
            });
        }

        Ok(batch)
    }

    fn _write_csv(
        path: impl AsRef<Path>,
        options: &ZiCCsvOptions,
        batch: &[ZiCRecord],
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

    fn _load_parquet(path: impl AsRef<Path>) -> Result<ZiCRecordBatch> {
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

                batch.push(ZiCRecord {
                    id,
                    payload,
                    metadata,
                });
            }
        }

        Ok(batch)
    }

    fn _write_parquet(path: impl AsRef<Path>, batch: &[ZiCRecord]) -> Result<()> {
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
fn _parse_record(line: String, line_number: usize) -> Result<ZiCRecord> {
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

            Ok(ZiCRecord {
                id,
                payload,
                metadata,
            })
        }
        payload => Ok(ZiCRecord {
            id: None,
            payload,
            metadata: None,
        }),
    }
}

#[allow(non_snake_case)]
fn _value_to_metadata(value: Value) -> std::result::Result<ZiCMetadata, Value> {
    match value {
        Value::Object(map) => Ok(map),
        other => Err(other),
    }
}

fn _parquet_err(err: ArrowError) -> ZiError {
    ZiError::Internal(format!("parquet error: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::NamedTempFile;

    #[test]
    fn load_jsonl_parses_records_with_defaults() {
        let content = r#"{"payload": {"text": "hello"}}
{"id": "a", "payload": "value", "metadata": {"score": 0.5}}
42
"#;
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();

        let records = ZiCIO::ZiFLoadJsonl(file.path()).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].payload["text"], json!("hello"));
        assert_eq!(records[1].id.as_deref(), Some("a"));
        assert_eq!(records[1].metadata.as_ref().unwrap()["score"], json!(0.5));
        assert_eq!(records[2].payload, json!(42));
    }

    #[test]
    fn write_and_reload_roundtrip_jsonl() {
        let records = vec![
            ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "hi"})),
            ZiCRecord::ZiFNew(None, json!("raw")),
        ];

        let mut tmp = NamedTempFile::new().unwrap();
        ZiCIO::ZiFWriteJsonlWriter(&mut tmp, &records).unwrap();

        tmp.flush().unwrap();
        tmp.seek(SeekFrom::Start(0)).unwrap();

        let reloaded = ZiCIO::ZiFLoadJsonlReader(BufReader::new(tmp.reopen().unwrap())).unwrap();
        assert_eq!(reloaded.len(), 2);
        assert_eq!(reloaded[0].payload["text"], json!("hi"));
        assert_eq!(reloaded[1].payload, json!("raw"));
    }

    #[test]
    fn csv_roundtrip() {
        let records = vec![
            ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "hi"})).ZiFWithMetadata({
                let mut meta = Map::new();
                meta.insert("score".into(), json!(0.9));
                meta
            }),
            ZiCRecord::ZiFNew(None, json!({"text": "bye"})),
        ];

        let tmp = NamedTempFile::new().unwrap();
        ZiCIO::ZiFWrite(
            tmp.path(),
            ZiCIOFormat::Csv(ZiCCsvOptions::default()),
            &records,
        )
        .unwrap();

        let loaded = ZiCIO::ZiFLoad(tmp.path(), ZiCIOFormat::Csv(ZiCCsvOptions::default())).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].id.as_deref(), Some("1"));
        assert_eq!(loaded[0].metadata.as_ref().unwrap()["score"], json!(0.9));
        assert_eq!(loaded[1].payload["text"], json!("bye"));
    }

    #[test]
    fn parquet_roundtrip() {
        let records = vec![
            ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "hi"})).ZiFWithMetadata({
                let mut meta = Map::new();
                meta.insert("score".into(), json!(0.95));
                meta
            }),
            ZiCRecord::ZiFNew(None, json!({"text": "bye"})),
        ];

        let tmp = NamedTempFile::new().unwrap();
        ZiCIO::ZiFWrite(tmp.path(), ZiCIOFormat::Parquet, &records).unwrap();

        let loaded = ZiCIO::ZiFLoad(tmp.path(), ZiCIOFormat::Parquet).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].id.as_deref(), Some("1"));
        assert_eq!(loaded[0].metadata.as_ref().unwrap()["score"], json!(0.95));
        assert_eq!(loaded[1].payload["text"], json!("bye"));
    }
}
