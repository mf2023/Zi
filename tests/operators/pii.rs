//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
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
use std::collections::HashSet;
use Zi::operators::pii::*;
use Zi::record::ZiCRecord;

#[test]
fn ZiFTPiiPlaceholderAndMaskRules() {
    let operator = ZiCPiiRedact::ZiFNew(
        ZiCFieldPath::ZiFParse("payload.text").unwrap(),
        vec![
            ZiCPiiRule {
                tag: "email".into(),
                pattern: Regex::new(r"[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}").unwrap(),
                strategy: ZiCPiiStrategy::Placeholder("<EMAIL>".into()),
                context_window: 5,
                store_original: true,
            },
            ZiCPiiRule {
                tag: "phone".into(),
                pattern: Regex::new(r"\b\d{11}\b").unwrap(),
                strategy: ZiCPiiStrategy::Mask {
                    mask_char: '*',
                    prefix: 3,
                    suffix: 2,
                },
                context_window: 3,
                store_original: false,
            },
        ],
        Some("pii".into()),
        HashSet::new(),
    );
    let batch = vec![ZiCRecord::ZiFNew(
        None,
        json!({"text": "mail me at a@b.com, phone 13800138000"}),
    )];
    let output = operator.apply(batch).unwrap();
    let text = output[0].payload["text"].as_str().unwrap();
    assert!(text.contains("<EMAIL>"));
    assert!(text.contains("138****00"));
    let metadata = output[0].metadata.as_ref().unwrap();
    let pii = metadata.get("pii").unwrap().as_array().unwrap();
    assert_eq!(pii.len(), 2);
    assert_eq!(pii[0]["original"], Value::String("a@b.com".into()));
    assert_eq!(pii[1]["strategy"], Value::String("mask".into()));
}

#[test]
fn ZiFTPiiHashStrategyRedactsSensitiveText() {
    let operator = ZiCPiiRedact::ZiFNew(
        ZiCFieldPath::ZiFParse("payload.text").unwrap(),
        vec![ZiCPiiRule {
            tag: "account".into(),
            pattern: Regex::new(r"acct-\d{4}").unwrap(),
            strategy: ZiCPiiStrategy::Hash {
                salt: 42,
                prefix: 4,
                suffix: 4,
            },
            context_window: 2,
            store_original: false,
        }],
        Some("pii".into()),
        HashSet::new(),
    );
    let batch = vec![ZiCRecord::ZiFNew(
        None,
        json!({"text": "acct-1234 should be redacted"}),
    )];
    let output = operator.apply(batch).unwrap();
    let text = output[0].payload["text"].as_str().unwrap();
    assert!(text.contains("acct-"));
    assert!(!text.contains("acct-1234"));
}

#[test]
fn ZiFTPiiFactoryBuildsWithCustomRules() {
    let config = json!({
        "path": "payload.text",
        "store_key": "pii",
        "custom": [
            {
                "tag": "id",
                "pattern": r"\bID-\d{6}\b",
                "strategy": "placeholder",
                "placeholder": "<ID>"
            }
        ]
    });
    let operator = ZiFPiiRedactFactory(&config).unwrap();
    let batch = vec![ZiCRecord::ZiFNew(
        None,
        json!({"text": "email a@b.com, id ID-123456"}),
    )];
    let output = operator.apply(batch).unwrap();
    let text = output[0].payload["text"].as_str().unwrap();
    assert!(text.contains("<EMAIL>"));
    assert!(text.contains("<ID>"));
}
