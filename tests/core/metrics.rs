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

use serde_json::{json, Map};
use Zi::metrics::ZiCQualityMetrics;
use Zi::record::ZiCRecord;

#[test]
fn ZiFTMetricsComputeFromRecords() {
    let records = vec![
        ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "hello world"})).ZiFWithMetadata({
            let mut map = Map::new();
            map.insert("lang".into(), json!("en"));
            map.insert("toxicity".into(), json!(0.2));
            map.insert("quality".into(), json!(0.82));
            map.insert("verified".into(), json!(true));
            map.insert(
                "pii".into(),
                json!([{ "tag": "email", "strategy": "placeholder" }]),
            );
            map
        }),
        ZiCRecord::ZiFNew(Some("2".into()), json!("simple line")).ZiFWithMetadata({
            let mut map = Map::new();
            map.insert("language".into(), json!("fr"));
            map.insert("toxicity".into(), json!(0.4));
            map.insert("quality".into(), json!(0.64));
            map.insert(
                "pii".into(),
                json!([{ "tag": "phone", "strategy": "mask" }]),
            );
            map.insert("verified".into(), json!(false));
            map
        }),
    ];

    let metrics = ZiCQualityMetrics::ZiFCompute(&records);
    assert_eq!(metrics.total_records, 2);
    assert!(metrics.average_payload_chars > 0.0);
    assert!(metrics.average_payload_tokens > 0.0);
    assert_eq!(metrics.records_with_metadata, 2);
    assert!(metrics.metadata_average_keys >= 2.0);
    assert_eq!(metrics.metadata_coverage_ratio, 1.0);
    assert_eq!(metrics.language_counts.get("en"), Some(&1));
    assert_eq!(metrics.language_counts.get("fr"), Some(&1));
    assert_eq!(metrics.metadata_key_counts.get("lang"), Some(&1));
    assert_eq!(metrics.metadata_key_counts.get("language"), Some(&1));
    assert!((metrics.toxicity_average - 0.3).abs() < f64::EPSILON);
    assert_eq!(metrics.toxicity_max, 0.4);
    assert_eq!(metrics.pii_total_matches, 2);
    assert_eq!(metrics.pii_tag_counts.get("email"), Some(&1));
    assert_eq!(metrics.pii_tag_counts.get("phone"), Some(&1));
    assert_eq!(metrics.pii_strategy_counts.get("placeholder"), Some(&1));
    assert_eq!(metrics.pii_strategy_counts.get("mask"), Some(&1));
    assert_eq!(metrics.metadata_flag_true_counts.get("verified"), Some(&1));
    let quality_stats = metrics
        .metadata_numeric_stats
        .get("quality")
        .expect("quality stats present");
    assert!((quality_stats.mean - 0.73).abs() < 1e-6);
    let toxicity_stats = metrics
        .metadata_numeric_stats
        .get("toxicity")
        .expect("toxicity stats present");
    assert_eq!(toxicity_stats.max, metrics.toxicity_max);
    assert!((metrics.payload_char_stats.mean - metrics.average_payload_chars).abs() < f64::EPSILON);
    assert!(metrics.payload_token_stats.stddev >= 0.0);
    assert!(metrics.toxicity_stats.max >= metrics.toxicity_stats.min);

    let json_output = metrics.ZiFAsJson();
    assert!(json_output.is_object());
}
