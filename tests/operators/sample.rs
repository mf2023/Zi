//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
//!
//! This file is part of Zi.
//! The Zi project belongs to the Dunimd project team.

use zi::operators::sample::*;
use zi::record::{ZiCMetadata, ZiCRecord};
use serde_json::json;

#[test]
fn sample_random_deterministic_with_seed() {
    let op = ZiCSampleRandom::ZiFNew(Some(0.5), None, 42, None, None, None);
    let mut batch = Vec::new();
    for i in 0..10 {
        batch.push(ZiCRecord::ZiFNew(
            Some(i.to_string()),
            json!({"text": format!("row {i}")}),
        ));
    }
    let out = op.apply(batch).unwrap();
    assert!(out.len() >= 3 && out.len() <= 7);
}

#[test]
fn sample_random_count_uses_hash_order() {
    let op = ZiCSampleRandom::ZiFNew(None, Some(3), 99, None, None, None);
    let mut batch = Vec::new();
    for i in 0..6 {
        batch.push(ZiCRecord::ZiFNew(Some(i.to_string()), json!({"v": i})));
    }
    let out = op.apply(batch).unwrap();
    let ids: Vec<_> = out.iter().map(|r| r.id.clone().unwrap()).collect();
    assert_eq!(ids, vec!["0", "3", "5"]);
}

#[test]
fn sample_random_weight_prefers_higher_weights() {
    let op = ZiCSampleRandom::ZiFNew(None, Some(2), 123, Some("w".into()), None, None);
    let mut batch = Vec::new();
    for i in 0..4 {
        let weight = if i < 2 { 0.1 } else { 5.0 };
        let mut record = ZiCRecord::ZiFNew(Some(format!("r{i}")), json!({"idx": i}));
        record.ZiFMetadataMut().insert("w".into(), json!(weight));
        batch.push(record);
    }
    let out = op.apply(batch).unwrap();
    let ids: Vec<_> = out
        .iter()
        .map(|r| r.id.as_deref().unwrap().to_string())
        .collect();
    assert!(ids.contains(&"r2".to_string()));
    assert!(ids.contains(&"r3".to_string()));
}

#[test]
fn sample_random_group_minimums_respected() {
    let op =
        ZiCSampleRandom::ZiFNew(Some(0.5), Some(4), 7, None, Some("bucket".into()), Some(1));

    let mut batch = Vec::new();
    for i in 0..6 {
        let mut record = ZiCRecord::ZiFNew(Some(format!("r{i}")), json!({"idx": i}));
        let bucket = if i % 2 == 0 { "even" } else { "odd" };
        record
            .ZiFMetadataMut()
            .insert("bucket".into(), json!(bucket));
        batch.push(record);
    }

    let out = op.apply(batch).unwrap();
    assert_eq!(out.len(), 4);
    let mut counts = std::collections::HashMap::new();
    for record in &out {
        let bucket = record.metadata.as_ref().unwrap()["bucket"]
            .as_str()
            .unwrap();
        *counts.entry(bucket).or_insert(0) += 1;
    }
    assert!(counts.get("even").copied().unwrap_or_default() >= 1);
    assert!(counts.get("odd").copied().unwrap_or_default() >= 1);
}

#[test]
fn sample_random_min_without_group_errors() {
    let op = ZiCSampleRandom::ZiFNew(Some(0.5), Some(2), 11, None, None, Some(1));
    let result = op.apply(Vec::new());
    assert!(result.is_err());
}

#[test]
fn sample_top_picks_highest_values() {
    let op = ZiCSampleTop::ZiFNew("quality".into(), 2);
    let a = ZiCRecord::ZiFNew(Some("a".into()), json!(null)).ZiFWithMetadata({
        let mut m = ZiCMetadata::new();
        m.insert("quality".into(), json!(0.8));
        m
    });
    let b = ZiCRecord::ZiFNew(Some("b".into()), json!(null)).ZiFWithMetadata({
        let mut m = ZiCMetadata::new();
        m.insert("quality".into(), json!(0.4));
        m
    });
    let c = ZiCRecord::ZiFNew(Some("c".into()), json!(null)).ZiFWithMetadata({
        let mut m = ZiCMetadata::new();
        m.insert("quality".into(), json!(0.9));
        m
    });
    let out = op.apply(vec![a, b, c]).unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].id.as_deref(), Some("c"));
    assert_eq!(out[1].id.as_deref(), Some("a"));
}
