//! Copyright © 2025 Dunimd Team. All Rights Reserved.
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

use std::collections::HashMap;

use serde::Serialize;
use serde_json::Value;

use crate::record::ZiCRecord;

#[derive(Debug, Serialize, Default, PartialEq)]
pub struct ZiCQualityMetrics {
    pub total_records: usize,
    pub average_payload_chars: f64,
    pub average_payload_tokens: f64,
    pub metadata_key_counts: HashMap<String, usize>,
    pub language_counts: HashMap<String, usize>,
    pub toxicity_average: f64,
    pub toxicity_max: f64,
}

impl ZiCQualityMetrics {
    #[allow(non_snake_case)]
    pub fn ZiFCompute(records: &[ZiCRecord]) -> Self {
        let mut metrics = ZiCQualityMetrics::default();
        metrics.total_records = records.len();

        if records.is_empty() {
            return metrics;
        }

        let mut total_chars = 0usize;
        let mut total_tokens = 0usize;
        let mut toxicity_total = 0.0f64;
        let mut toxicity_count = 0usize;

        for record in records {
            let payload_string = match &record.payload {
                Value::String(text) => text.clone(),
                other => other.to_string(),
            };
            total_chars += payload_string.chars().count();
            total_tokens += payload_string
                .split_whitespace()
                .filter(|token| !token.is_empty())
                .count();

            if let Some(metadata) = &record.metadata {
                for (key, value) in metadata {
                    *metrics.metadata_key_counts.entry(key.clone()).or_insert(0) += 1;

                    if key.eq_ignore_ascii_case("lang") || key.eq_ignore_ascii_case("language") {
                        if let Some(lang) = value.as_str() {
                            *metrics.language_counts.entry(lang.to_string()).or_insert(0) += 1;
                        }
                    }

                    if let Some(score) = value.as_f64() {
                        toxicity_total += score;
                        toxicity_count += 1;
                        if score > metrics.toxicity_max {
                            metrics.toxicity_max = score;
                        }
                    }
                }
            }
        }

        metrics.average_payload_chars = total_chars as f64 / records.len() as f64;
        metrics.average_payload_tokens = total_tokens as f64 / records.len() as f64;
        metrics.toxicity_average = if toxicity_count > 0 {
            toxicity_total / toxicity_count as f64
        } else {
            0.0
        };

        metrics
    }

    #[allow(non_snake_case)]
    pub fn ZiFAsJson(&self) -> Value {
        serde_json::to_value(self).unwrap_or_else(|_| Value::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::ZiCRecord;
    use serde_json::json;

    #[test]
    fn compute_metrics_from_records() {
        let records = vec![
            ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "hello world"})).ZiFWithMetadata({
                let mut map = serde_json::Map::new();
                map.insert("lang".into(), json!("en"));
                map.insert("toxicity".into(), json!(0.2));
                map
            }),
            ZiCRecord::ZiFNew(Some("2".into()), json!("simple line")).ZiFWithMetadata({
                let mut map = serde_json::Map::new();
                map.insert("language".into(), json!("fr"));
                map.insert("toxicity".into(), json!(0.4));
                map
            }),
        ];

        let metrics = ZiCQualityMetrics::ZiFCompute(&records);
        assert_eq!(metrics.total_records, 2);
        assert!(metrics.average_payload_chars > 0.0);
        assert!(metrics.average_payload_tokens > 0.0);
        assert_eq!(metrics.language_counts.get("en"), Some(&1));
        assert_eq!(metrics.language_counts.get("fr"), Some(&1));
        assert_eq!(metrics.metadata_key_counts.get("lang"), Some(&1));
        assert_eq!(metrics.metadata_key_counts.get("language"), Some(&1));
        assert!((metrics.toxicity_average - 0.3).abs() < f64::EPSILON);
        assert_eq!(metrics.toxicity_max, 0.4);

        let json_output = metrics.ZiFAsJson();
        assert!(json_output.is_object());
    }
}
