//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
//!
//! This file is part of Zi.
//! The Zi project belongs to the Dunimd project team.

//! Test for augment operators

use zix::operators::augment::augment_noise_factory;
use zix::ZiRecord;
use serde_json::json;

#[test]
fn augment_noise_basic() {
    let config = json!({"path": "payload.text", "intensity": 0.1, "seed": 42});
    let operator = augment_noise_factory(&config).unwrap();
    let rec = ZiRecord::new(None, json!({"text": "hello world"}));
    let out = operator.apply(vec![rec]).unwrap();
    assert!(!out.is_empty());
}
