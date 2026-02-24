//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
//!
//! This file is part of Zi.
//! The Zi project belongs to the Dunimd project team.

//! Test for sample operators

use zix::operators::sample::sample_random_factory;
use zix::ZiRecord;
use serde_json::json;

#[test]
fn sample_random_factory_basic() {
    let config = json!({"rate": 0.5, "seed": 42});
    let operator = sample_random_factory(&config).unwrap();
    let mut batch = Vec::new();
    for i in 0..10 {
        batch.push(ZiRecord::new(Some(i.to_string()), json!({"v": i})));
    }
    let out = operator.apply(batch).unwrap();
    assert!(out.len() >= 3 && out.len() <= 7);
}
