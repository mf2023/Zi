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

use serde_json::json;
use std::io::Write;
use tempfile::NamedTempFile;

use Zi::io::ZiCIO;
use Zi::metrics::ZiCQualityMetrics;
use Zi::orbit::runtime::{ZiCDataVisibility, ZiCPluginPolicy};
use Zi::orbit::{ZiCInProcessOrbit, ZiCPluginDescriptor};
use Zi::pipeline::ZiCPipelineBuilder;

#[test]
fn orbit_pipeline_filters_enriches_and_limits_jsonl() {
    let mut input = NamedTempFile::new().unwrap();
    let content = r#"{"id": "1", "payload": {"lang": "en", "text": "hello world", "score": 0.8}}
{"id": "2", "payload": {"lang": "zh", "text": "nihao", "score": 0.2}}
{"id": "3", "payload": {"lang": "en", "text": "world", "score": 0.6}}
"#;
    input.write_all(content.as_bytes()).unwrap();
    input.flush().unwrap();

    let builder = ZiCPipelineBuilder::with_defaults();
    let config = json!([
        {
            "operator": "metadata.enrich",
            "config": {"entries": {"source": "integration", "drop": true, "tags": ["primary", "beta"], "description": "integration dataset"}}
        },
        {
            "operator": "metadata.rename",
            "config": {"keys": {"source": "origin"}}
        },
        {
            "operator": "metadata.remove",
            "config": {"keys": ["drop"]}
        },
        {
            "operator": "metadata.copy",
            "config": {"keys": {"origin": "origin_backup"}}
        },
        {
            "operator": "metadata.require",
            "config": {"keys": ["origin", "origin_backup"]}
        },
        {
            "operator": "metadata.extract",
            "config": {"keys": {"payload.lang": {"name": "lang"}}}
        },
        {
            "operator": "metadata.keep",
            "config": {"keys": ["origin", "origin_backup", "lang", "tags", "description"]}
        },
        {
            "operator": "filter.length_range",
            "config": {"path": "payload.text", "min": 8, "max": 32}
        },
        {
            "operator": "filter.range",
            "config": {"path": "payload.score", "min": 0.4, "max": 1.0}
        },
        {
            "operator": "filter.in",
            "config": {"path": "metadata.lang", "values": ["en", "fr"]}
        },
        {
            "operator": "filter.array_contains",
            "config": {"path": "metadata.tags", "element": "primary"}
        },
        {
            "operator": "filter.not_in",
            "config": {"path": "metadata.origin", "values": ["blocked", "spam"]}
        },
        {
            "operator": "filter.starts_with",
            "config": {"path": "payload.text", "prefix": "hell"}
        },
        {
            "operator": "filter.ends_with",
            "config": {"path": "payload.text", "suffix": "ld"}
        },
        {
            "operator": "filter.regex",
            "config": {"path": "payload.text", "pattern": "^hell"}
        },
        {
            "operator": "filter.greater_than",
            "config": {"path": "payload.score", "threshold": 0.4}
        },
        {
            "operator": "filter.token_range",
            "config": {"path": "payload.text", "min": 2}
        },
        {
            "operator": "filter.less_than",
            "config": {"path": "payload.score", "threshold": 0.95}
        },
        {
            "operator": "filter.between",
            "config": {"path": "payload.score", "min": 0.4, "max": 0.85}
        },
        {
            "operator": "filter.any",
            "config": {"paths": ["payload.text", "metadata.origin"], "equals": "hello world"}
        },
        {
            "operator": "filter.not_equals",
            "config": {"path": "metadata.origin", "equals": "blocked"}
        },
        {
            "operator": "filter.not_exists",
            "config": {"path": "metadata.drop"}
        },
        {
            "operator": "filter.exists",
            "config": {"path": "metadata.lang"}
        },
        {
            "operator": "filter.contains",
            "config": {"path": "payload.text", "contains": "HELL", "case_insensitive": true}
        },
        {
            "operator": "filter.contains_all",
            "config": {"path": "metadata.tags", "contains_all": ["primary", "beta"]}
        },
        {
            "operator": "filter.contains_any",
            "config": {"path": "metadata.tags", "contains_any": ["primary", "beta"]}
        },
        {
            "operator": "filter.contains_none",
            "config": {"path": "metadata.description", "contains_none": ["blocked", "deprecated"]}
        },
        {
            "operator": "filter.equals",
            "config": {"path": "payload.lang", "equals": "en"}
        },
        {"operator": "limit", "config": {"count": 1}}
    ]);

    // Build an Orbit-backed pipeline using the same config.
    let mut orbit = ZiCInProcessOrbit::ZiFNew();
    builder.ZiFRegisterOperatorsIntoOrbit(&mut orbit);

    let plugin_id = "test.orbit_pipeline";
    let descriptor = ZiCPluginDescriptor {
        id: plugin_id.to_string(),
        version: Zi::orbit::runtime::ZiCPluginVersion::parse("1.0.0").unwrap(),
        exports: Vec::new(),
        policy: ZiCPluginPolicy {
            allowed_capabilities: Vec::new(),
            can_access_versions: false,
            default_visibility: ZiCDataVisibility::Full,
        },
        dependencies: vec![],
        state: Zi::orbit::runtime::ZiCPluginState::Loaded,
        load_time: std::time::SystemTime::now(),
    };
    orbit.ZiFRegisterPlugin(descriptor);

    let orbit_pipeline = builder
        .ZiFBuildOrbitPipeline(plugin_id, config.as_array().expect("config must be array"))
        .unwrap();

    let batch = ZiCIO::ZiFLoadJsonl(input.path()).unwrap();

    let mut metrics = ZiCQualityMetrics::default();
    let processed = orbit_pipeline
        .run(&mut orbit, &mut metrics, None, batch)
        .unwrap();

    assert_eq!(processed.len(), 1);
    assert_eq!(processed[0].id.as_deref(), Some("1"));
    let metadata = processed[0].metadata.as_ref().unwrap();
    assert_eq!(metadata["origin"], json!("integration"));
    assert!(metadata.get("source").is_none());
    assert!(metadata.get("drop").is_none());
    assert_eq!(metadata["origin_backup"], json!("integration"));
    assert_eq!(metadata["lang"], json!("en"));
    assert_eq!(metadata["description"], json!("integration dataset"));
    assert_eq!(processed[0].payload["score"], json!(0.8));
}
