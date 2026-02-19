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

use Zi::record::{ZiCMetadata, ZiCRecord, ZiCRecordBatch};
use serde_json::json;

#[test]
fn ZiFTRecordNewSetsIdAndPayloadWithoutMetadata() {
    let record = ZiCRecord::ZiFNew(Some("sample-id".to_string()), json!({"text": "hello"}));

    assert_eq!(record.id.as_deref(), Some("sample-id"));
    assert_eq!(record.payload, json!({"text": "hello"}));
    assert!(record.metadata.is_none());
}

#[test]
fn ZiFTRecordWithMetadataAttachesMetadataMap() {
    let mut metadata = ZiCMetadata::new();
    metadata.insert("quality".to_string(), json!(0.95));

    let record = ZiCRecord::ZiFNew(None, json!("payload")).ZiFWithMetadata(metadata.clone());

    assert!(record.id.is_none());
    assert_eq!(record.metadata.as_ref(), Some(&metadata));
}

#[test]
fn ZiFTRecordMetadataMutCreatesMapOnDemand() {
    let mut record = ZiCRecord::ZiFNew(None, json!(42));
    record
        .ZiFMetadataMut()
        .insert("score".to_string(), json!(0.8));

    assert_eq!(record.metadata.unwrap().get("score"), Some(&json!(0.8)));
}

#[test]
fn ZiFTRecordBatchAliasHandlesMultipleRecords() {
    let batch: ZiCRecordBatch = vec![
        ZiCRecord::ZiFNew(Some("a".into()), json!(1)),
        ZiCRecord::ZiFNew(Some("b".into()), json!(2)),
    ];

    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].payload, json!(1));
    assert_eq!(batch[1].payload, json!(2));
}
