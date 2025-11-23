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

use std::path::Path;

use serde_json::json;

use Zi::io::ZiCIO;
use Zi::pipeline::ZiCPipelineBuilder;

#[test]
fn semantic_dedup_pipeline_marks_duplicates_in_metadata() {
    let fixture_path = Path::new("tests/fixtures/semantic_dedup.jsonl");
    let batch = ZiCIO::ZiFLoadJsonl(fixture_path).expect("load fixture");
    assert_eq!(batch.len(), 5, "fixture should contain five records");

    let config = json!([
        {
            "operator": "transform.normalize",
            "config": {
                "path": "payload.text",
                "lowercase": true
            }
        },
        {
            "operator": "dedup.semantic",
            "config": {
                "path": "payload.text",
                "threshold": 0.6,
                "details_key": "semantic_dup",
                "max_matches": 5
            }
        }
    ]);

    let pipeline = ZiCPipelineBuilder::with_defaults()
        .build_from_config(config.as_array().unwrap())
        .expect("build pipeline");

    let out = pipeline.run(batch).expect("run pipeline");

    // We expect near-duplicate fox sentences to collapse into a single kept record.
    assert!(
        out.len() < 5,
        "semantic dedup should drop at least one record"
    );

    // Find the kept fox record and inspect its semantic_dup metadata.
    let fox = out
        .iter()
        .find(|rec| {
            rec.payload
                .get("text")
                .and_then(|v| v.as_str())
                .map(|t| t.contains("quick brown fox"))
                .unwrap_or(false)
        })
        .expect("one fox record should remain");

    println!("Kept fox record: id={:?}, payload={}", fox.id, fox.payload);
    println!("Metadata: {:?}", fox.metadata);

    let metadata = fox.metadata.as_ref().expect("metadata should exist");
    let details = metadata
        .get("semantic_dup")
        .and_then(|v| v.as_object())
        .expect("semantic_dup details should be present");

    assert_eq!(details.get("duplicate"), Some(&json!(true)));

    let matches = details
        .get("matches")
        .and_then(|v| v.as_array())
        .expect("matches array present");

    assert!(
        !matches.is_empty(),
        "there should be at least one semantic match"
    );

    // All similarities should be between 0.6 and 1.0
    for entry in matches {
        let obj = entry.as_object().expect("match entry object");
        let sim = obj
            .get("similarity")
            .and_then(|v| v.as_f64())
            .expect("similarity value");
        assert!(sim >= 0.6 && sim <= 1.0);
    }
}
