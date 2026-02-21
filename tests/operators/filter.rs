//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
//!
//! This file is part of Zi.
//! The Zi project belongs to the Dunimd project team.

use zi::operators::filter::*;
use zi::record::ZiCRecord;
use serde_json::json;

#[test]
fn filter_contains_factory_parses_config() {
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
        zi::errors::ZiError::Validation { message } => {
            assert!(message.contains("requires string 'contains'"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn filter_starts_with_matches_prefix() {
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
fn filter_starts_with_factory_parses_config() {
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
        zi::errors::ZiError::Validation { message } => {
            assert!(message.contains("prefix"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn filter_contains_none_rejects_configured_substrings() {
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
fn filter_contains_none_factory_parses_config() {
    let config = json!({
        "path": "metadata.tags",
        "contains_none": ["blocked", "spam"]
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
    match err {
        zi::errors::ZiError::Validation { message } => {
            assert!(message.contains("requires array 'contains_none'"));
        }
        other => panic!("unexpected error: {other:?}"),
    }

    let err = ZiFFilterContainsNoneFactory(&json!({
        "path": "payload.text",
        "contains_none": []
    }))
    .unwrap_err();
    match err {
        zi::errors::ZiError::Validation { message } => {
            assert!(message.contains("may not be empty"));
        }
        other => panic!("unexpected error: {other:?}"),
    }

    let err = ZiFFilterContainsNoneFactory(&json!({
        "path": "payload.text",
        "contains_none": ["ok", 1]
    }))
    .unwrap_err();
    match err {
        zi::errors::ZiError::Validation { message } => {
            assert!(message.contains("must be strings"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn filter_contains_none_allows_missing_or_null() {
    let operator = ZiCFilterContainsNone::ZiFNew(
        ZiCFieldPath::ZiFParse("metadata.notes").unwrap(),
        vec!["blocked".into()],
        false,
    );

    let mut record = ZiCRecord::ZiFNew(None, json!({}));
    record
        .ZiFMetadataMut()
        .insert("tags".into(), json!(["vip_gold", "priority"]));
    let batch = vec![record, ZiCRecord::ZiFNew(None, json!({"lang": "en"}))];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);

    let err = ZiFFilterRegexFactory(&json!({"path": "payload.text"})).unwrap_err();
    match err {
        zi::errors::ZiError::Validation { message } => {
            assert!(message.contains("pattern"));
        }
        other => panic!("unexpected error: {other:?}"),
    }

    let err =
        ZiFFilterRegexFactory(&json!({"path": "payload.text", "pattern": "["})).unwrap_err();
    match err {
        zi::errors::ZiError::Validation { message } => {
            assert!(message.contains("invalid regex"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn filter_is_null_includes_missing_when_enabled() {
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
fn filter_is_null_excludes_missing_when_disabled() {
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
fn filter_is_null_factory_parses_config() {
    let config = json!({"path": "metadata.flag", "include_missing": false});
    let operator = ZiFFilterIsNullFactory(&config).unwrap();

    let mut record_null = ZiCRecord::ZiFNew(None, json!({"field": 1}));
    record_null
        .ZiFMetadataMut()
        .insert("flag".into(), serde_json::Value::Null);
    let batch = vec![record_null, ZiCRecord::ZiFNew(None, json!({}))];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);

    let err = ZiFFilterIsNullFactory(&json!({"include_missing": true})).unwrap_err();
    match err {
        zi::errors::ZiError::Validation { message } => {
            assert!(message.contains("requires string 'path'"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn filter_greater_than_filters_numeric_values() {
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
fn filter_greater_than_factory_parses_config() {
    let config = json!({"path": "metadata.score", "threshold": 0.2});
    let operator = ZiFFilterGreaterThanFactory(&config).unwrap();

    let mut record = ZiCRecord::ZiFNew(None, json!({"any": true}));
    record.ZiFMetadataMut().insert("score".into(), json!(0.3));
    let batch = vec![record, ZiCRecord::ZiFNew(None, json!({"any": true}))];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);

    let err = ZiFFilterGreaterThanFactory(&json!({"path": "payload.score"})).unwrap_err();
    match err {
        zi::errors::ZiError::Validation { message } => {
            assert!(message.contains("threshold"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn filter_less_than_filters_numeric_values() {
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
fn filter_less_than_factory_parses_config() {
    let config = json!({"path": "metadata.score", "threshold": 0.8});
    let operator = ZiFFilterLessThanFactory(&config).unwrap();

    let mut record = ZiCRecord::ZiFNew(None, json!({"any": true}));
    record.ZiFMetadataMut().insert("score".into(), json!(0.5));
    let batch = vec![record, ZiCRecord::ZiFNew(None, json!({"any": true}))];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);

    let err = ZiFFilterLessThanFactory(&json!({"path": "payload.score"})).unwrap_err();
    match err {
        zi::errors::ZiError::Validation { message } => {
            assert!(message.contains("threshold"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn filter_between_filters_values_in_range() {
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
fn filter_between_factory_parses_config() {
    let config = json!({"path": "metadata.score", "min": 0.1, "max": 0.6});
    let operator = ZiFFilterBetweenFactory(&config).unwrap();

    let mut record = ZiCRecord::ZiFNew(None, json!({"any": true}));
    record.ZiFMetadataMut().insert("score".into(), json!(0.4));
    let batch = vec![record, ZiCRecord::ZiFNew(None, json!({"any": true}))];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);

    let err =
        ZiFFilterBetweenFactory(&json!({"path": "payload.score", "min": 0.7})).unwrap_err();
    match err {
        zi::errors::ZiError::Validation { message } => {
            assert!(message.contains("max"));
        }
        other => panic!("unexpected error: {other:?}"),
    }

    let err =
        ZiFFilterBetweenFactory(&json!({"path": "payload.score", "min": 0.8, "max": 0.2}))
            .unwrap_err();
    match err {
        zi::errors::ZiError::Validation { message } => {
            assert!(message.contains("may not exceed"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn filter_any_matches_across_paths() {
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
fn filter_any_factory_parses_config() {
    let config = json!({
        "paths": ["payload.primary", "metadata.tag"],
        "equals": "match"
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
        zi::errors::ZiError::Validation { message } => assert!(message.contains("paths")),
        other => panic!("unexpected error: {other:?}"),
    }

    let err = ZiFFilterAnyFactory(&json!({"paths": [], "equals": "match"})).unwrap_err();
    match err {
        zi::errors::ZiError::Validation { message } => assert!(message.contains("may not be empty")),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn filter_not_equals_filters_out_matches() {
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
fn filter_not_equals_factory_parses_config() {
    let config = json!({"path": "metadata.category", "equals": "blocked"});
    let operator = ZiFFilterNotEqualsFactory(&config).unwrap();

    let mut record = ZiCRecord::ZiFNew(None, json!({"lang": "en"}));
    record
        .ZiFMetadataMut()
        .insert("category".into(), json!("news"));
    let batch = vec![record];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);

    let err = ZiFFilterNotEqualsFactory(&json!({"path": "payload.lang"})).unwrap_err();
    match err {
        zi::errors::ZiError::Validation { message } => assert!(message.contains("'equals'")),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn filter_not_in_excludes_disallowed_values() {
    let operator = ZiCFilterNotIn::ZiFNew(
        ZiCFieldPath::ZiFParse("payload.lang").unwrap(),
        vec![json!("en"), json!("fr")],
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
fn filter_not_in_factory_parses_config() {
    let config = json!({
        "path": "metadata.tag",
        "values": ["blocked", "spam"]
    });
    let operator = ZiFFilterNotInFactory(&config).unwrap();

    let mut record = ZiCRecord::ZiFNew(None, json!({"lang": "en"}));
    record.ZiFMetadataMut().insert("tag".into(), json!("clean"));
    let output = operator.apply(vec![record]).unwrap();
    assert_eq!(output.len(), 1);

    let err = ZiFFilterNotInFactory(&json!({"path": "payload.lang"})).unwrap_err();
    match err {
        zi::errors::ZiError::Validation { message } => assert!(message.contains("'values'")),
        other => panic!("unexpected error: {other:?}"),
    }

    let err =
        ZiFFilterNotInFactory(&json!({"path": "payload.lang", "values": []})).unwrap_err();
    match err {
        zi::errors::ZiError::Validation { message } => assert!(message.contains("may not be empty")),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn filter_length_range_factory_parses_config() {
    let config = json!({
        "path": "payload.text",
        "min": 3,
        "max": 8
    });
    let operator = ZiFFilterLengthRangeFactory(&config).unwrap();

    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "ok"})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "perfect"})),
    ];

    let out = operator.apply(batch).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].id.as_deref(), Some("2"));

    let err = ZiFFilterLengthRangeFactory(&json!({"path": "payload.text"})).unwrap_err();
    match err {
        zi::errors::ZiError::Validation { message } => assert!(message.contains("at least one of")),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn filter_token_range_filters_by_token_count() {
    let op = ZiCFilterTokenRange::ZiFNew(
        ZiCFieldPath::ZiFParse("payload.text").unwrap(),
        Some(2),
        Some(3),
    );
    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "one"})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "two tokens"})),
        ZiCRecord::ZiFNew(Some("3".into()), json!({"text": "this has four tokens"})),
    ];

    let out = op.apply(batch).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].id.as_deref(), Some("2"));
}

#[test]
fn filter_token_range_factory_parses_config() {
    let config = json!({
        "path": "metadata.summary",
        "min": 2
    });
    let operator = ZiFFilterTokenRangeFactory(&config).unwrap();

    let mut record = ZiCRecord::ZiFNew(None, json!({}));
    record
        .ZiFMetadataMut()
        .insert("summary".into(), json!("short summary"));
    let out = operator.apply(vec![record]).unwrap();
    assert_eq!(out.len(), 1);

    let err = ZiFFilterTokenRangeFactory(&json!({"path": "payload.text"})).unwrap_err();
    match err {
        zi::errors::ZiError::Validation { message } => assert!(message.contains("at least one")),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn filter_equals_factory_parses_config() {
    let config = json!({"path": "payload.tag", "equals": 1});
    let operator = ZiFFilterEqualsFactory(&config).unwrap();

    let batch = vec![
        ZiCRecord::ZiFNew(None, json!({"tag": 1})),
        ZiCRecord::ZiFNew(None, json!({"tag": 2})),
    ];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);
}

#[test]
fn field_path_errors_on_invalid_prefix() {
    let err = ZiCFieldPath::ZiFParse("data.field").unwrap_err();
    match err {
        zi::errors::ZiError::Validation { message } => {
            assert!(message.contains("payload"));
        }
        _ => panic!("unexpected error kind"),
    }
}
