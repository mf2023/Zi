//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
//!
//! This file is part of Zi.
//! The Zi project belongs to the Dunimd project team.

use zi::operators::filter::ZiCFieldPath;
use zi::operators::augment::*;
use zi::record::ZiCRecord;
use serde_json::json;

#[test]
fn synonym_replaces_words() {
    let op = _AugmentSynonym::ZiFNew(
        ZiCFieldPath::ZiFParse("payload.text").unwrap(),
        vec![_SynonymEntry {
            word: "good".into(),
            replacements: vec!["great".into(), "nice".into()],
        }],
        42,
    );
    let batch = vec![ZiCRecord::ZiFNew(None, json!({"text": "A good day"}))];
    let out = op.apply(batch).unwrap();
    let text = out[0].payload["text"].as_str().unwrap();
    assert!(text.contains("good") || text.contains("great") || text.contains("nice"));
}

#[test]
fn noise_flips_characters() {
    let op = _AugmentNoise::ZiFNew(ZiCFieldPath::ZiFParse("payload.text").unwrap(), 1.0, 1);
    let batch = vec![ZiCRecord::ZiFNew(None, json!({"text": "a1Z"}))];
    let out = op.apply(batch).unwrap();
    let text = out[0].payload["text"].as_str().unwrap();
    assert_ne!(text, "a1Z");
}
