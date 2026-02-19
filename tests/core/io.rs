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

use std::io::{BufReader, Cursor, Seek, SeekFrom, Write};

use serde_json::{json, Map};
use tempfile::NamedTempFile;
use Zi::io::{
    ZiCCsvOptions,
    ZiCIO,
    ZiCIOFormat,
    ZiCJsonlOptions,
};
use Zi::record::{ZiCMetadata, ZiCRecord};

fn ZiFTWriteJsonl(temp: &mut NamedTempFile, records: &[ZiCRecord]) {
    ZiCIO::ZiFWriteJsonlWriter(temp, records).expect("write jsonl");
    temp.flush().expect("flush");
    temp.seek(SeekFrom::Start(0)).expect("seek start");
}

#[test]
fn ZiFTIoLoadJsonlParsesRecordsWithDefaults() {
    let content = "{\"payload\": {\"text\": \"hello\"}}\n{\"id\": \"a\", \"payload\": \"value\", \"metadata\": {\"score\": 0.5}}\n42\n";
    let mut file = NamedTempFile::new().expect("tmp");
    file.write_all(content.as_bytes()).expect("write");

    let records = ZiCIO::ZiFLoadJsonl(file.path()).expect("load");
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].payload["text"], json!("hello"));
    assert_eq!(records[1].id.as_deref(), Some("a"));
    assert_eq!(records[1].metadata.as_ref().unwrap()["score"], json!(0.5));
    assert_eq!(records[2].payload, json!(42));
}

#[test]
fn ZiFTIoJsonlOptionsEnableCommentAndErrorSkipping() {
    let data = "# heading\n// comment\n{\"payload\": {\"text\": \"keep\"}}\n{\"id\": 1, \"payload\": {}}\ninvalid json\n{\"payload\": {\"text\": \"second\"}}\n";
    let cursor = Cursor::new(data.as_bytes());
    let options = ZiCJsonlOptions {
        allow_comments: true,
        skip_invalid_lines: true,
        max_records: Some(1),
    };

    let records = ZiCIO::ZiFLoadJsonlReaderWithOptions(BufReader::new(cursor), options)
        .expect("load with options");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload["text"], json!("keep"));
}

#[test]
fn ZiFTIoWriteAndReloadJsonlRoundtrip() {
    let records = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "hi"})),
        ZiCRecord::ZiFNew(None, json!("raw")),
    ];

    let mut tmp = NamedTempFile::new().expect("tmp");
    ZiFTWriteJsonl(&mut tmp, &records);

    let reloaded =
        ZiCIO::ZiFLoadJsonlReader(BufReader::new(tmp.reopen().expect("reopen"))).expect("load");
    assert_eq!(reloaded.len(), 2);
    assert_eq!(reloaded[0].payload["text"], json!("hi"));
    assert_eq!(reloaded[1].payload, json!("raw"));
}

#[test]
fn ZiFTIoCsvRoundtrip() {
    let records = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "hi"})).ZiFWithMetadata({
            let mut meta = ZiCMetadata::new();
            meta.insert("score".into(), json!(0.9));
            meta
        }),
        ZiCRecord::ZiFNew(None, json!({"text": "bye"})),
    ];

    let tmp = NamedTempFile::new().expect("tmp");
    ZiCIO::ZiFWrite(
        tmp.path(),
        ZiCIOFormat::Csv(ZiCCsvOptions::default()),
        &records,
    )
    .expect("write csv");

    let loaded = ZiCIO::ZiFLoad(tmp.path(), ZiCIOFormat::Csv(ZiCCsvOptions::default()))
        .expect("load csv");
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].id.as_deref(), Some("1"));
    assert_eq!(loaded[0].metadata.as_ref().unwrap()["score"], json!(0.9));
    assert_eq!(loaded[1].payload["text"], json!("bye"));
}

#[test]
fn ZiFTIoParquetRoundtrip() {
    let records = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "hi"})).ZiFWithMetadata({
            let mut meta = Map::new();
            meta.insert("score".into(), json!(0.95));
            meta
        }),
        ZiCRecord::ZiFNew(None, json!({"text": "bye"})),
    ];

    let tmp = NamedTempFile::new().expect("tmp");
    ZiCIO::ZiFWrite(tmp.path(), ZiCIOFormat::Parquet, &records).expect("write parquet");

    let loaded = ZiCIO::ZiFLoad(tmp.path(), ZiCIOFormat::Parquet).expect("load parquet");
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].id.as_deref(), Some("1"));
    assert_eq!(loaded[0].metadata.as_ref().unwrap()["score"], json!(0.95));
    assert_eq!(loaded[1].payload["text"], json!("bye"));
}
