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

use serde_json::{json, Value};
use Zi::operators::quality::*;
use Zi::record::ZiCRecord;

#[test]
fn ZiFTQualityScoreAndFilterChain() {
    let scorer = ZiCQualityScore::ZiFNew(
        ZiCFieldPath::ZiFParse("payload.text").unwrap(),
        "quality".into(),
    );
    let filter = ZiCQualityFilter::ZiFNew("quality".into(), 0.5);
    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "hello world"})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "@@@@@@"})),
    ];
    let scored = scorer.apply(batch).unwrap();
    let filtered = filter.apply(scored).unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].id.as_deref(), Some("1"));
}

#[test]
fn ZiFTQualityToxicityScoresText() {
    let operator = ZiCToxicityScore::ZiFNew(
        ZiCFieldPath::ZiFParse("payload.text").unwrap(),
        "tox".into(),
        vec![
            _ZiCToxicTerm::from_tokens(vec!["hate".into()], 1.0),
            _ZiCToxicTerm::from_tokens(vec!["love".into()], 0.2),
        ],
    );

    let batch = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "I hate this"})),
        ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "Full of love"})),
    ];

    let scored = operator.apply(batch).unwrap();
    let first = scored[0].metadata.as_ref().unwrap()["tox"].as_f64().unwrap();
    let second = scored[1].metadata.as_ref().unwrap()["tox"].as_f64().unwrap();
    assert!(first > second);
    assert!(first > 0.0);
}

#[test]
fn ZiFTQualityToxicityDetectsObfuscatedTerms() {
    let operator = ZiCToxicityScore::ZiFNew(
        ZiCFieldPath::ZiFParse("payload.text").unwrap(),
        "tox".into(),
        vec![_ZiCToxicTerm::from_tokens(vec!["hate".into()], 1.0)],
    );

    let batch = vec![ZiCRecord::ZiFNew(
        Some("1".into()),
        json!({"text": "I h4te you!!!"}),
    )];

    let scored = operator.apply(batch).unwrap();
    let score = scored[0].metadata.as_ref().unwrap()["tox"].as_f64().unwrap();
    assert!(score > 0.2, "expected obfuscated term to be detected");
}

#[test]
fn ZiFTQualityScoreTracksDetailsForNoisyText() {
    let scorer = ZiCQualityScore::ZiFNew(
        ZiCFieldPath::ZiFParse("payload.text").unwrap(),
        "quality".into(),
    )
    .ZiFWithDetails(Some("quality_details".into()));

    let clean = ZiCRecord::ZiFNew(
        Some("1".into()),
        json!({"text": "Hello world. This is balanced (test)."}),
    );
    let noisy = ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "Hello ((world!!! $$$$$"}));

    let scored = scorer.apply(vec![clean, noisy]).unwrap();
    let clean_score = scored[0]
        .metadata
        .as_ref()
        .unwrap()
        .get("quality")
        .and_then(Value::as_f64)
        .unwrap();
    let noisy_score = scored[1]
        .metadata
        .as_ref()
        .unwrap()
        .get("quality")
        .and_then(Value::as_f64)
        .unwrap();

    assert!(clean_score > noisy_score);

    let noisy_details = scored[1]
        .metadata
        .as_ref()
        .unwrap()
        .get("quality_details")
        .and_then(Value::as_object)
        .expect("details should be present");
    let contributions = noisy_details
        .get("contributions")
        .and_then(Value::as_object)
        .expect("contributions should be present");

    assert!(
        contributions
            .get("punctuation_balance")
            .and_then(Value::as_f64)
            .unwrap()
            < 0.02
    );
    assert!(
        contributions
            .get("symbol_ratio_penalty")
            .and_then(Value::as_f64)
            .unwrap()
            < 0.02
    );
}
