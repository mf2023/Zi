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
use Zi::errors::ZiError;
use Zi::operators::metadata::*;
use Zi::record::ZiCRecord;

#[test]
fn ZiFTMetadataEnrichAddsMetadataEntries() {
    let operator = ZiCMetadataEnrich::ZiFNew(vec![("quality".into(), json!(0.9))]);
    let batch = vec![ZiCRecord::ZiFNew(None, json!("text"))];
    let output = operator.apply(batch).unwrap();

    let metadata = output[0].metadata.as_ref().unwrap();
    assert_eq!(metadata.get("quality"), Some(&json!(0.9)));
}

#[test]
fn ZiFTMetadataEnrichFactoryBuildsOperator() {
    let config = json!({"entries": {"source": "crawler"}});
    let operator = ZiFMetadataEnrichFactory(&config).unwrap();

    let batch = vec![ZiCRecord::ZiFNew(None, json!("payload"))];
    let output = operator.apply(batch).unwrap();
    assert_eq!(
        output[0].metadata.as_ref().unwrap()["source"],
        json!("crawler")
    );
}

#[test]
fn ZiFTMetadataRemoveDropsMetadataKeys() {
    let mut record = ZiCRecord::ZiFNew(None, json!(null));
    let metadata = record.ZiFMetadataMut();
    metadata.insert("keep".into(), json!(1));
    metadata.insert("drop".into(), json!(2));

    let operator = ZiCMetadataRemove::ZiFNew(vec!["drop".into()]);
    let output = operator.apply(vec![record]).unwrap();

    let metadata = output[0].metadata.as_ref().unwrap();
    assert!(metadata.get("drop").is_none());
    assert_eq!(metadata.get("keep"), Some(&json!(1)));
}

#[test]
fn ZiFTMetadataRemoveClearsMetadataWhenEmpty() {
    let mut record = ZiCRecord::ZiFNew(None, json!(null));
    record.ZiFMetadataMut().insert("drop".into(), json!(1));

    let output = ZiCMetadataRemove::ZiFNew(vec!["drop".into()])
        .apply(vec![record])
        .unwrap();

    assert!(output[0].metadata.is_none());
}

#[test]
fn ZiFTMetadataRemoveFactoryBuildsOperator() {
    let operator = ZiFMetadataRemoveFactory(&json!({"keys": ["foo", "bar"]})).unwrap();

    let mut record = ZiCRecord::ZiFNew(None, json!(null));
    let metadata = record.ZiFMetadataMut();
    metadata.insert("foo".into(), json!(1));
    metadata.insert("bar".into(), json!(2));

    let output = operator.apply(vec![record]).unwrap();
    assert!(output[0].metadata.is_none());
}

#[test]
fn ZiFTMetadataKeepRetainsOnlySpecifiedKeys() {
    let mut record = ZiCRecord::ZiFNew(None, json!(null));
    let metadata = record.ZiFMetadataMut();
    metadata.insert("keep".into(), json!(1));
    metadata.insert("drop".into(), json!(2));

    let output = ZiCMetadataKeep::ZiFNew(vec!["keep".into()])
        .apply(vec![record])
        .unwrap();

    let metadata = output[0].metadata.as_ref().unwrap();
    assert_eq!(metadata.get("keep"), Some(&json!(1)));
    assert!(metadata.get("drop").is_none());
}

#[test]
fn ZiFTMetadataKeepFactoryBuildsOperator() {
    let operator = ZiFMetadataKeepFactory(&json!({"keys": ["a"]})).unwrap();

    let mut record = ZiCRecord::ZiFNew(None, json!(null));
    record.ZiFMetadataMut().insert("a".into(), json!(3));
    record.ZiFMetadataMut().insert("b".into(), json!(4));

    let output = operator.apply(vec![record]).unwrap();
    let metadata = output[0].metadata.as_ref().unwrap();
    assert_eq!(metadata.get("a"), Some(&json!(3)));
    assert!(metadata.get("b").is_none());

    let err = ZiFMetadataKeepFactory(&json!({"keys": []})).unwrap_err();
    match err {
        ZiError::Validation { message } => {
            assert!(message.contains("may not be empty"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn ZiFTMetadataRenameMovesKeysToNewNames() {
    let mut record = ZiCRecord::ZiFNew(None, json!(null));
    let metadata = record.ZiFMetadataMut();
    metadata.insert("old".into(), json!(1));
    metadata.insert("keep".into(), json!(2));

    let output = ZiCMetadataRename::ZiFNew(vec![("old".into(), "new".into())])
        .apply(vec![record])
        .unwrap();

    let metadata = output[0].metadata.as_ref().unwrap();
    assert!(metadata.get("old").is_none());
    assert_eq!(metadata.get("new"), Some(&json!(1)));
    assert_eq!(metadata.get("keep"), Some(&json!(2)));
}

#[test]
fn ZiFTMetadataRenameFactoryBuildsOperator() {
    let operator = ZiFMetadataRenameFactory(&json!({"keys": {"a": "b"}})).unwrap();

    let mut record = ZiCRecord::ZiFNew(None, json!(null));
    record.ZiFMetadataMut().insert("a".into(), json!(3));

    let output = operator.apply(vec![record]).unwrap();
    let metadata = output[0].metadata.as_ref().unwrap();
    assert!(metadata.get("a").is_none());
    assert_eq!(metadata.get("b"), Some(&json!(3)));
}

#[test]
fn ZiFTMetadataCopyDuplicatesValuesWithoutRemoval() {
    let mut record = ZiCRecord::ZiFNew(None, json!(null));
    let metadata = record.ZiFMetadataMut();
    metadata.insert("from".into(), json!(42));

    let output = ZiCMetadataCopy::ZiFNew(vec![("from".into(), "to".into())])
        .apply(vec![record])
        .unwrap();

    let metadata = output[0].metadata.as_ref().unwrap();
    assert_eq!(metadata.get("from"), Some(&json!(42)));
    assert_eq!(metadata.get("to"), Some(&json!(42)));
}

#[test]
fn ZiFTMetadataCopyFactoryBuildsOperator() {
    let operator = ZiFMetadataCopyFactory(&json!({"keys": {"foo": "bar"}})).unwrap();

    let mut record = ZiCRecord::ZiFNew(None, json!(null));
    record.ZiFMetadataMut().insert("foo".into(), json!(7));

    let output = operator.apply(vec![record]).unwrap();
    let metadata = output[0].metadata.as_ref().unwrap();
    assert_eq!(metadata.get("foo"), Some(&json!(7)));
    assert_eq!(metadata.get("bar"), Some(&json!(7)));
}

#[test]
fn ZiFTMetadataRequireEnsuresKeysPresent() {
    let mut record = ZiCRecord::ZiFNew(None, json!(null));
    record.ZiFMetadataMut().insert("needed".into(), json!(1));

    let output = ZiCMetadataRequire::ZiFNew(vec!["needed".into()])
        .apply(vec![record.clone()])
        .unwrap();

    assert_eq!(output.len(), 1);

    let err = ZiCMetadataRequire::ZiFNew(vec!["missing".into()])
        .apply(vec![record])
        .unwrap_err();

    match err {
        ZiError::Validation { message } => {
            assert!(message.contains("missing metadata key"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn ZiFTMetadataRequireFactoryBuildsOperator() {
    let operator = ZiFMetadataRequireFactory(&json!({"keys": ["a", "b"]})).unwrap();

    let mut record = ZiCRecord::ZiFNew(None, json!(null));
    let metadata = record.ZiFMetadataMut();
    metadata.insert("a".into(), json!(1));
    metadata.insert("b".into(), json!(2));

    let output = operator.apply(vec![record]).unwrap();
    assert_eq!(output.len(), 1);

    let err = ZiFMetadataRequireFactory(&json!({"keys": []})).unwrap_err();
    match err {
        ZiError::Validation { message } => {
            assert!(message.contains("may not be empty"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn ZiFTMetadataExtractCopiesPayloadValues() {
    let record = ZiCRecord::ZiFNew(None, json!({"value": 1}));

    let batch = ZiCMetadataExtract::ZiFNew(vec![ZiCExtractionRule {
        path_segments: vec!["value".into()],
        target_key: "payload_value".into(),
        default_value: None,
        optional: false,
        pattern: None,
        capture_index: None,
    }])
    .apply(vec![record])
    .unwrap();

    let metadata = batch[0].metadata.as_ref().unwrap();
    assert_eq!(metadata.get("payload_value"), Some(&json!(1)));
}

#[test]
fn ZiFTMetadataExtractErrorsWhenPathMissing() {
    let record = ZiCRecord::ZiFNew(None, json!({"present": true}));
    let err = ZiCMetadataExtract::ZiFNew(vec![ZiCExtractionRule {
        path_segments: vec!["missing".into()],
        target_key: "meta".into(),
        default_value: None,
        optional: false,
        pattern: None,
        capture_index: None,
    }])
    .apply(vec![record])
    .unwrap_err();

    match err {
        ZiError::Validation { message } => {
            assert!(message.contains("missing payload path"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn ZiFTMetadataExtractFactoryBuildsOperator() {
    let operator = ZiFMetadataExtractFactory(&json!({
        "keys": {
            "payload.value": {
                "name": "stored"
            }
        }
    }))
    .unwrap();

    let record = ZiCRecord::ZiFNew(None, json!({"value": 10}));
    let batch = operator.apply(vec![record]).unwrap();

    let metadata = batch[0].metadata.as_ref().unwrap();
    assert_eq!(metadata.get("stored"), Some(&json!(10)));
}
