//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
//!
//! This file is part of Zi.
//! The Zi project belongs to the Dunimd project team.

use zi::operators::filter::ZiCFieldPath;
use zi::operators::transform::*;
use zi::record::ZiCRecord;
use serde_json::json;

#[test]
fn normalize_basic() {
    let op = _TransformNormalize::ZiFNew(ZiCFieldPath::ZiFParse("payload.text").unwrap(), true);
    let rec = ZiCRecord::ZiFNew(None, json!({"text": "  Hello   WORLD "}));
    let out = op.apply(vec![rec]).unwrap();
    assert_eq!(out[0].payload["text"], json!("hello world"));
}
