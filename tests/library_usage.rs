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
use std::path::Path;

use Zi::io::ZiCIO;
use Zi::pipeline::ZiCPipelineBuilder;

#[test]
fn library_end_to_end_cleans_dataset() {
    let fixture_path = Path::new("tests/fixtures/library_input.jsonl");
    assert!(fixture_path.exists(), "expected fixture to exist");

    let batch = ZiCIO::ZiFLoadJsonl(fixture_path).expect("load fixture");
    assert_eq!(batch.len(), 4, "fixture should contain four records");

    let builder = ZiCPipelineBuilder::with_defaults();
    let config = json!([
        {
            "operator": "transform.normalize",
            "config": {"path": "payload.text", "lowercase": true}
        },
        {
            "operator": "pii.redact",
            "config": {
                "path": "payload.text",
                "store_key": "pii",
                "custom": [
                    {
                        "tag": "fr_phone",
                        "pattern": r"\+33(?:\s?\d){9}",
                        "strategy": "mask",
                        "mask_char": "*",
                        "prefix": 3,
                        "suffix": 2
                    }
                ]
            }
        },
        {
            "operator": "filter.contains_none",
            "config": {
                "path": "payload.text",
                "contains_none": ["buy now"],
                "case_insensitive": true
            }
        },
        {
            "operator": "quality.score",
            "config": {"path": "payload.text", "key": "quality", "details_key": "quality_details"}
        },
        {
            "operator": "quality.filter",
            "config": {"key": "quality", "min": 0.6}
        },
        {
            "operator": "dedup.simhash",
            "config": {"path": "payload.text", "threshold": 0.9}
        }
    ]);

    let pipeline = builder
        .build_from_config(config.as_array().expect("config array"))
        .expect("build pipeline");

    let processed = pipeline.run(batch).expect("run pipeline");
    assert_eq!(
        processed.len(),
        2,
        "cleaned dataset should keep two high-quality uniques"
    );

    println!("=== Cleaned Records ===");
    for record in &processed {
        let id = record.id.as_deref().unwrap_or("<no-id>");
        let text = record.payload["text"]
            .as_str()
            .expect("text should be string");
        println!("record {id}: {text}");
        if let Some(metadata) = &record.metadata {
            let printable = Value::Object(metadata.clone());
            println!(
                "  metadata: {}",
                serde_json::to_string_pretty(&printable).unwrap()
            );
        }
    }

    let ids: Vec<_> = processed
        .iter()
        .map(|record| record.id.as_deref().unwrap_or_default().to_string())
        .collect();
    assert_eq!(
        ids,
        vec!["1", "4"],
        "expected duplicate and spam records removed"
    );

    let record_by_id = |needle: &str| {
        processed
            .iter()
            .find(|record| record.id.as_deref() == Some(needle))
            .unwrap_or_else(|| panic!("record {needle} should be present"))
    };

    let rec1 = record_by_id("1");
    let text1 = rec1.payload["text"]
        .as_str()
        .expect("text should be string");
    assert_eq!(
        text1, "hello world! contact me at <EMAIL>",
        "email should be normalized and redacted"
    );
    let meta1 = rec1.metadata.as_ref().expect("metadata should exist");
    let quality1 = meta1
        .get("quality")
        .and_then(serde_json::Value::as_f64)
        .expect("quality score should exist");
    assert!(quality1 >= 0.6 && quality1 <= 1.0);
    let pii1 = meta1
        .get("pii")
        .and_then(serde_json::Value::as_array)
        .expect("pii matches should be recorded");
    assert!(pii1.iter().any(|entry| entry["tag"] == "email"));

    let rec4 = record_by_id("4");
    let text4 = rec4.payload["text"]
        .as_str()
        .expect("text should be string");
    assert!(
        text4.contains('*'),
        "phone number should be masked with asterisks"
    );
    assert!(
        !text4.contains("6 12 34 56 78"),
        "original phone digits should not remain"
    );
    let meta4 = rec4.metadata.as_ref().expect("metadata should exist");
    let quality4 = meta4
        .get("quality")
        .and_then(serde_json::Value::as_f64)
        .expect("quality score should exist");
    assert!(quality4 >= 0.6 && quality4 <= 1.0);
    let pii4 = meta4
        .get("pii")
        .and_then(serde_json::Value::as_array)
        .expect("pii matches should be recorded");
    assert!(pii4.iter().any(|entry| entry["tag"] == "fr_phone"));

    for record in &processed {
        let text = record.payload["text"]
            .as_str()
            .expect("text should be string");
        assert_eq!(text, text.trim(), "text should be trimmed");
        let lowered_check = text.replace("<EMAIL>", "<email>").replace("<ID>", "<id>");
        assert_eq!(
            lowered_check,
            lowered_check.to_lowercase(),
            "text should be lowercase aside from placeholders"
        );

        let metadata = record.metadata.as_ref().expect("metadata should exist");
        let details = metadata
            .get("quality_details")
            .and_then(serde_json::Value::as_object)
            .expect("quality details should exist");
        assert!(details.contains_key("components"));
        assert!(details.contains_key("contributions"));
    }
}
