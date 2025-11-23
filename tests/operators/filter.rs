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

use regex::Regex;
use serde_json::{json, Value};
use Zi::errors::ZiError;
use Zi::operators::filter::*;
use Zi::record::ZiCRecord;

#[test]
fn ZiFTFilterContainsNoneCaseInsensitiveRejects() {
    let operator = ZiCFilterContainsNone::ZiFNew(
        ZiCFieldPath::ZiFParse("payload.text").unwrap(),
        vec!["BLOCK".into()],
        true,
    );
    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "safe"})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "Block entry"})),
    ];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].id.as_deref(), Some("1"));
}

#[test]
fn ZiFTFilterIsNullIncludesMissingWhenEnabled() {
    let operator =
        ZiCFilterIsNull::ZiFNew(ZiCFieldPath::ZiFParse("payload.optional").unwrap(), true);
    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"optional": null})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({})),
        ZiCRecord::ZiFNew(Some("3".into()), json!({"optional": "value"})),
    ];

    let output = operator.apply(batch).unwrap();
    let ids: Vec<_> = output.iter().map(|record| record.id.as_deref()).collect();
    assert_eq!(ids, vec![Some("1"), Some("2")]);
}

#[test]
fn ZiFTFilterIsNullExcludesMissingWhenDisabled() {
    let operator =
        ZiCFilterIsNull::ZiFNew(ZiCFieldPath::ZiFParse("payload.optional").unwrap(), false);
    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"optional": null})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({})),
    ];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].id.as_deref(), Some("1"));
}

#[test]
fn ZiFTFilterIsNullFactoryParsesConfig() {
    let config = json!({"path": "metadata.flag", "include_missing": false});
    let operator = ZiFFilterIsNullFactory(&config).unwrap();

    let mut record_null = ZiCRecord::ZiFNew(None, json!({"field": 1}));
    record_null
        .ZiFMetadataMut()
        .insert("flag".into(), Value::Null);
    let batch = vec![record_null, ZiCRecord::ZiFNew(None, json!({}))];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);

    let err = ZiFFilterIsNullFactory(&json!({"include_missing": true})).unwrap_err();
    match err {
        ZiError::Validation { message } => {
            assert!(message.contains("requires string 'path'"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn ZiFTFilterGreaterThanFiltersNumericValues() {
    let operator =
        ZiCFilterGreaterThan::ZiFNew(ZiCFieldPath::ZiFParse("payload.score").unwrap(), 0.5);
    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"score": 0.6})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({"score": 0.4})),
        ZiCRecord::ZiFNew(Some("3".into()), json!({"score": "0.9"})),
    ];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].id.as_deref(), Some("1"));
}

#[test]
fn ZiFTFilterGreaterThanFactoryParsesConfig() {
    let config = json!({"path": "metadata.score", "threshold": 0.2});
    let operator = ZiFFilterGreaterThanFactory(&config).unwrap();

    let mut record = ZiCRecord::ZiFNew(None, json!({"any": true}));
    record.ZiFMetadataMut().insert("score".into(), json!(0.3));
    let batch = vec![record, ZiCRecord::ZiFNew(None, json!({"any": true}))];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);

    let err = ZiFFilterGreaterThanFactory(&json!({"path": "payload.score"})).unwrap_err();
    match err {
        ZiError::Validation { message } => {
            assert!(message.contains("threshold"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn ZiFTFilterLessThanFiltersNumericValues() {
    let operator =
        ZiCFilterLessThan::ZiFNew(ZiCFieldPath::ZiFParse("payload.score").unwrap(), 0.5);
    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"score": 0.4})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({"score": 0.6})),
        ZiCRecord::ZiFNew(Some("3".into()), json!({"score": "0.1"})),
    ];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].id.as_deref(), Some("1"));
}

#[test]
fn ZiFTFilterLessThanFactoryParsesConfig() {
    let config = json!({"path": "metadata.score", "threshold": 0.8});
    let operator = ZiFFilterLessThanFactory(&config).unwrap();

    let mut record = ZiCRecord::ZiFNew(None, json!({"any": true}));
    record.ZiFMetadataMut().insert("score".into(), json!(0.5));
    let batch = vec![record, ZiCRecord::ZiFNew(None, json!({"any": true}))];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);

    let err = ZiFFilterLessThanFactory(&json!({"path": "payload.score"})).unwrap_err();
    match err {
        ZiError::Validation { message } => {
            assert!(message.contains("threshold"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn ZiFTFilterBetweenFiltersValuesInRange() {
    let operator =
        ZiCFilterBetween::ZiFNew(ZiCFieldPath::ZiFParse("payload.score").unwrap(), 0.3, 0.7);
    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"score": 0.5})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({"score": 0.2})),
        ZiCRecord::ZiFNew(Some("3".into()), json!({"score": 0.9})),
    ];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].id.as_deref(), Some("1"));
}

#[test]
fn ZiFTFilterBetweenFactoryParsesConfig() {
    let config = json!({"path": "metadata.score", "min": 0.1, "max": 0.6});
    let operator = ZiFFilterBetweenFactory(&config).unwrap();

    let mut record = ZiCRecord::ZiFNew(None, json!({"any": true}));
    record.ZiFMetadataMut().insert("score".into(), json!(0.4));
    let batch = vec![record, ZiCRecord::ZiFNew(None, json!({"any": true}))];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);

    let err = ZiFFilterBetweenFactory(&json!({"path": "payload.score", "min": 0.7})).unwrap_err();
    match err {
        ZiError::Validation { message } => {
            assert!(message.contains("max"));
        }
        other => panic!("unexpected error: {other:?}"),
    }

    let err = ZiFFilterBetweenFactory(&json!({
        "path": "payload.score",
        "min": 0.8,
        "max": 0.2,
    }))
    .unwrap_err();
    match err {
        ZiError::Validation { message } => {
            assert!(message.contains("may not exceed"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn ZiFTFilterAnyMatchesAcrossPaths() {
    let operator = ZiCFilterAny::ZiFNew(
        vec![
            ZiCFieldPath::ZiFParse("payload.primary").unwrap(),
            ZiCFieldPath::ZiFParse("metadata.tag").unwrap(),
        ],
        json!("match"),
    );

    let record1 = ZiCRecord::ZiFNew(Some("1".into()), json!({"primary": "match"}));
    let mut record2 = ZiCRecord::ZiFNew(Some("2".into()), json!({"primary": "other"}));
    record2
        .ZiFMetadataMut()
        .insert("tag".into(), json!("match"));
    let record3 = ZiCRecord::ZiFNew(Some("3".into()), json!({"primary": "nope"}));

    let batch = vec![record1, record2, record3];
    let output = operator.apply(batch).unwrap();

    let ids: Vec<_> = output.iter().map(|r| r.id.as_deref()).collect();
    assert_eq!(ids, vec![Some("1"), Some("2")]);
}

#[test]
fn ZiFTFilterAnyFactoryParsesConfig() {
    let config = json!({
        "paths": ["payload.primary", "metadata.tag"],
        "equals": "match",
    });
    let operator = ZiFFilterAnyFactory(&config).unwrap();

    let record = ZiCRecord::ZiFNew(None, json!({"primary": "match"}));
    let batch = vec![record.clone(), {
        let mut alt = ZiCRecord::ZiFNew(None, json!({"primary": "other"}));
        alt.ZiFMetadataMut().insert("tag".into(), json!("match"));
        alt
    }];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 2);

    let err = ZiFFilterAnyFactory(&json!({"equals": "match"})).unwrap_err();
    match err {
        ZiError::Validation { message } => assert!(message.contains("paths")),
        other => panic!("unexpected error: {other:?}"),
    }

    let err = ZiFFilterAnyFactory(&json!({"paths": [], "equals": "match"})).unwrap_err();
    match err {
        ZiError::Validation { message } => assert!(message.contains("may not be empty")),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn ZiFTFilterNotEqualsFiltersOutMatches() {
    let operator = ZiCFilterNotEquals::ZiFNew(
        ZiCFieldPath::ZiFParse("payload.lang").unwrap(),
        json!("en"),
    );
    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"lang": "en"})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({"lang": "zh"})),
    ];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].id.as_deref(), Some("2"));
}

#[test]
fn ZiFTFilterNotEqualsFactoryParsesConfig() {
    let config = json!({"path": "metadata.category", "equals": "blocked"});
    let operator = ZiFFilterNotEqualsFactory(&config).unwrap();

    let mut record = ZiCRecord::ZiFNew(None, json!({"lang": "en"}));
    record
        .ZiFMetadataMut()
        .insert("category".into(), json!("news"));
    let batch = vec![record];

    let output = operator apply(batch).unwrap();
    assert_eq!(output.len(), 1);

    let err = ZiFFilterNotEqualsFactory(&json!({"path": "payload.lang"})).unwrap_err();
    match err {
        ZiError::Validation { message } => assert!(message.contains("'equals'")),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn ZiFTFilterStartsWithMatchesPrefix() {
    let operator = ZiCFilterStartsWith::ZiFNew(
        ZiCFieldPath::ZiFParse("payload.text").unwrap(),
        "hello".into(),
    );
    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "hello world"})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "world"})),
    ];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].id.as_deref(), Some("1"));
}

#[test]
fn ZiFTFilterStartsWithFactoryParsesConfig() {
    let config = json!({"path": "metadata.tags", "prefix": "vip"});
    let operator = ZiFFilterStartsWithFactory(&config).unwrap();

    let mut record = ZiCRecord::ZiFNew(None, json!({"lang": "en"}));
    record
        .ZiFMetadataMut()
        .insert("tags".into(), json!(["vip:gold", "priority"]));
    let batch = vec![record, ZiCRecord::ZiFNew(None, json!({"lang": "en"}))];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);

    let err = ZiFFilterStartsWithFactory(&json!({"path": "payload.text"})).unwrap_err();
    match err {
        ZiError::Validation { message } => {
            assert!(message.contains("prefix"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn ZiFTFilterContainsNoneRejectsConfiguredSubstrings() {
    let operator = ZiCFilterContainsNone::ZiFNew(
        ZiCFieldPath::ZiFParse("payload.text").unwrap(),
        vec!["bad".into(), "spam".into()],
        false,
    );
    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "clean text"})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "bad content"})),
        ZiCRecord::ZiFNew(Some("3".into()), json!({"text": "spam mail"})),
    ];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].id.as_deref(), Some("1"));
}

#[test]
fn ZiFTFilterContainsNoneFactoryParsesConfig() {
    let config = json!({
        "path": "metadata.tags",
        "contains_none": ["blocked", "spam"],
    });
    let operator = ZiFFilterContainsNoneFactory(&config).unwrap();

    let mut record = ZiCRecord::ZiFNew(None, json!({"text": "hello"}));
    record
        .ZiFMetadataMut()
        .insert("tags".into(), json!(["clean", "priority"]));
    let output = operator.apply(vec![record]).unwrap();
    assert_eq!(output.len(), 1);

    let mut record = ZiCRecord::ZiFNew(None, json!({"text": "hello"}));
    record
        .ZiFMetadataMut()
        .insert("tags".into(), json!(["blocked", "beta"]));
    let output = operator.apply(vec![record]).unwrap();
    assert_eq!(output.len(), 0);

    let err = ZiFFilterContainsNoneFactory(&json!({"path": "payload.text"})).unwrap_err();
    assert!(matches!(err, ZiError::Validation { message } if message.contains("requires array 'contains_none'")));

    let err = ZiFFilterContainsNoneFactory(&json!({
        "path": "payload.text",
        "contains_none": [],
    }))
    .unwrap_err();
    assert!(matches!(err, ZiError::Validation { message } if message.contains("may not be empty")));

    let err = ZiFFilterContainsNoneFactory(&json!({
        "path": "payload.text",
        "contains_none": ["ok", 1],
    }))
    .unwrap_err();
    assert!(matches!(err, ZiError::Validation { message } if message.contains("must be strings")));
}

#[test]
fn ZiFTFilterContainsNoneAllowsMissingOrNull() {
    let operator = ZiCFilterContainsNone::ZiFNew(
        ZiCFieldPath::ZiFParse("metadata.notes").unwrap(),
        vec!["blocked".into()],
        false,
    );

    let record_missing = ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "clean"}));

    let mut record_null = ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "clean"}));
    record_null
        .ZiFMetadataMut()
        .insert("notes".into(), Value::Null);

    let mut record_blocked = ZiCRecord::ZiFNew(Some("3".into()), json!({"text": "blocked"}));
    record_blocked
        .ZiFMetadataMut()
        .insert("notes".into(), json!("blocked entry"));

    let output = operator
        .apply(vec![record_missing, record_null, record_blocked])
        .unwrap();
    let ids: Vec<_> = output.iter().map(|record| record.id.as_deref()).collect();
    assert_eq!(ids, vec![Some("1"), Some("2")]);
}

#[test]
fn ZiFTFilterContainsNoneHandlesStringArrays() {
    let operator = ZiCFilterContainsNone::ZiFNew(
        ZiCFieldPath::ZiFParse("metadata.tags").unwrap(),
        vec!["blocked".into(), "spam".into()],
        false,
    );

    let mut record_allowed = ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "hello"}));
    record_allowed
        .ZiFMetadataMut()
        .insert("tags".into(), json!(["primary", "beta"]));

    let mut record_blocked = ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "hello"}));
    record_blocked
        .ZiFMetadataMut()
        .insert("tags".into(), json!(["primary", "blocked"]));

    let mut record_substring = ZiCRecord::ZiFNew(Some("3".into()), json!({"text": "hello"}));
    record_substring
        .ZiFMetadataMut()
        .insert("tags".into(), json!(["preblocked", "daily"]));

    let output = operator
        .apply(vec![record_allowed, record_blocked, record_substring])
        .unwrap();
    let ids: Vec<_> = output.iter().map(|record| record.id.as_deref()).collect();
    assert_eq!(ids, vec![Some("1")]);
}

#[test]
fn ZiFTFilterContainsAllCaseInsensitiveRequiresAll() {
    let operator = ZiCFilterContainsAll::ZiFNew(
        ZiCFieldPath::ZiFParse("payload.text").unwrap(),
        vec!["HELLO".into(), "WORLD".into()],
        true,
    );
    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "hello world"})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "HELLO"})),
    ];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].id.as_deref(), Some("1"));
}

#[test]
fn ZiFTFilterContainsAnyCaseInsensitiveMatches() {
    let operator = ZiCFilterContainsAny::ZiFNew(
        ZiCFieldPath::ZiFParse("payload.text").unwrap(),
        vec!["HELLO".into(), "WORLD".into()],
        true,
    );
    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "Hello there"})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "goodbye"})),
    ];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].id.as_deref(), Some("1"));
}

#[test]
fn ZiFTFilterContainsMatchesCaseInsensitive() {
    let operator = ZiCFilterContains::ZiFNew(
        ZiCFieldPath::ZiFParse("payload.text").unwrap(),
        "NEWS".into(),
        true,
    );
    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "breaking news"})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "BREAKING"})),
    ];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].id.as_deref(), Some("1"));
}

#[test]
fn ZiFTFilterEqualsRetainsMatchingRecords() {
    let operator =
        ZiCFilterEquals::ZiFNew(ZiCFieldPath::ZiFParse("payload.lang").unwrap(), json!("en"));
    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"lang": "en"})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({"lang": "zh"})),
    ];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].id.as_deref(), Some("1"));
}

#[test]
fn ZiFTFilterContainsHandlesStringArrays() {
    let operator = ZiCFilterContains::ZiFNew(
        ZiCFieldPath::ZiFParse("metadata.tags").unwrap(),
        "news".into(),
        false,
    );

    let mut record_match = ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "hello"}));
    record_match
        .ZiFMetadataMut()
        .insert("tags".into(), json!(["breaking news", "daily"]));

    let mut record_substring = ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "hello"}));
    record_substring
        .ZiFMetadataMut()
        .insert("tags".into(), json!(["newsworthy", "weekly"]));

    let mut record_miss = ZiCRecord::ZiFNew(Some("3".into()), json!({"text": "hello"}));
    record_miss
        .ZiFMetadataMut()
        .insert("tags".into(), json!(["daily", "gamma"]));

    let output = operator
        .apply(vec![record_match, record_substring, record_miss])
        .unwrap();
    let ids: Vec<_> = output.iter().map(|record| record.id.as_deref()).collect();
    assert_eq!(ids, vec![Some("1"), Some("2")]);
}

#[test]
fn ZiFTFilterEqualsSupportsMetadataPaths() {
    let mut record = ZiCRecord::ZiFNew(Some("1".into()), json!({"lang": "en"}));
    record
        .ZiFMetadataMut()
        .insert("category".to_string(), json!("news"));

    let batch = vec![record];
    let operator = ZiCFilterEquals::ZiFNew(
        ZiCFieldPath::ZiFParse("metadata.category").unwrap(),
        json!("news"),
    );

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);
}

#[test]
fn ZiFTFilterContainsMatchesSubstrings() {
    let operator = ZiCFilterContains::ZiFNew(
        ZiCFieldPath::ZiFParse("payload.text").unwrap(),
        "news".into(),
        false,
    );
    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "breaking news"})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "sports"})),
    ];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].id.as_deref(), Some("1"));
}

#[test]
fn ZiFTFilterContainsFactoryParsesConfig() {
    let config = json!({"path": "metadata.tags", "contains": "vip"});
    let operator = ZiFFilterContainsFactory(&config).unwrap();

    let mut record = ZiCRecord::ZiFNew(None, json!({"lang": "en"}));
    record
        .ZiFMetadataMut()
        .insert("tags".into(), json!(["vip", "priority"]));
    let batch = vec![record, ZiCRecord::ZiFNew(None, json!({"lang": "en"}))];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);

    let err = ZiFFilterContainsFactory(&json!({"path": "payload.text"})).unwrap_err();
    match err {
        ZiError::Validation { message } => {
            assert!(message.contains("requires string 'contains'"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
