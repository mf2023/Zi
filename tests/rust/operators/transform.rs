//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
//!
//! This file is part of Zi.
//! The Zi project belongs to the Dunimd project team.

//! Test for transform operators

use zix::operators::transform::transform_normalize_factory;
use zix::ZiRecord;
use serde_json::json;

#[test]
fn transform_normalize_basic() {
    let config = json!({"path": "payload.text", "lowercase": true});
    let operator = transform_normalize_factory(&config).unwrap();
    let rec = ZiRecord::new(None, json!({"text": "  Hello   WORLD "}));
    let out = operator.apply(vec![rec]).unwrap();
    assert_eq!(out[0].payload["text"], json!("hello world"));
}
