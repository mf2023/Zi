//! Copyright © 2025 Wenze Wei. All Rights Reserved.
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

use serde_json::json;
use Zi::operators::dedup::*;
use Zi::record::ZiCRecord;

#[test]
fn ZiFTDedupSimHashRemovesNearDuplicates() {
    let op = _DedupSimHash::new(ZiCFieldPath::ZiFParse("payload.text").unwrap(), 0.9);
    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "Hello world!"})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "hello   world"})),
        ZiCRecord::ZiFNew(Some("3".into()), json!({"text": "Different text"})),
    ];
    let out = op.apply(batch).unwrap();
    assert_eq!(out.len(), 2);
}

#[test]
fn ZiFTDedupMinHashRemovesSimilar() {
    let op = _DedupMinHash::new(ZiCFieldPath::ZiFParse("payload.text").unwrap(), 0.8, 32, 8);
    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "Alpha beta gamma"})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "alpha  beta   gamma"})),
        ZiCRecord::ZiFNew(Some("3".into()), json!({"text": "delta epsilon"})),
    ];
    let out = op.apply(batch).unwrap();
    assert_eq!(out.len(), 2);
}

#[test]
fn ZiFTDedupSemanticRemovesNearDuplicates() {
    let op = _DedupSemantic::new(ZiCFieldPath::ZiFParse("payload.text").unwrap(), 0.5);
    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "Large language model"})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "Large language models"})),
        ZiCRecord::ZiFNew(Some("3".into()), json!({"text": "Small cats"})),
    ];
    let out = op.apply(batch).unwrap();
    assert_eq!(out.len(), 2);
}

#[test]
fn ZiFTDedupSemanticRecordsMetadataMatches() {
    let op = _DedupSemantic::new(ZiCFieldPath::ZiFParse("payload.text").unwrap(), 0.6)
        .with_details(Some("semantic_dup".into()), 5);
    let batch = vec![
        ZiCRecord::ZiFNew(
            Some("keep".into()),
            json!({"text": "A quick brown fox jumps over"}),
        ),
        ZiCRecord::ZiFNew(
            Some("dup1".into()),
            json!({"text": "A quick brown fox jumps"}),
        ),
        ZiCRecord::ZiFNew(
            Some("unique".into()),
            json!({"text": "Completely different"}),
        ),
    ];

    let out = op.apply(batch).unwrap();
    assert_eq!(out.len(), 2);

    let kept = out
        .iter()
        .find(|rec| rec.id.as_deref() == Some("keep"))
        .expect("kept record should remain");
    let metadata = kept.metadata.as_ref().expect("metadata should exist");
    let details = metadata
        .get("semantic_dup")
        .and_then(Value::as_object)
        .expect("details should be present");
    assert_eq!(details.get("duplicate"), Some(&Value::Bool(true)));
    let matches = details
        .get("matches")
        .and_then(Value::as_array)
        .expect("matches array present");
    assert_eq!(matches.len(), 1);
    let entry = matches[0].as_object().expect("match entry");
    assert_eq!(entry.get("id"), Some(&Value::String("dup1".into())));
    let similarity = entry
        .get("similarity")
        .and_then(Value::as_f64)
        .expect("similarity value");
    assert!(similarity >= 0.6 && similarity <= 1.0);
}
