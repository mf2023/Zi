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

use std::collections::HashMap;
use std::f64;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::record::ZiCRecord;

#[derive(Clone, Debug, Serialize, Deserialize, Default, PartialEq)]
pub struct ZiCStatisticSummary {
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub median: f64,
    pub p95: f64,
    pub stddev: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default, PartialEq)]
pub struct ZiCQualityMetrics {
    pub total_records: usize,
    pub average_payload_chars: f64,
    pub average_payload_tokens: f64,
    pub records_with_metadata: usize,
    pub metadata_average_keys: f64,
    pub metadata_coverage_ratio: f64,
    pub metadata_key_counts: HashMap<String, usize>,
    pub metadata_flag_true_counts: HashMap<String, usize>,
    pub metadata_numeric_stats: HashMap<String, ZiCStatisticSummary>,
    pub language_counts: HashMap<String, usize>,
    pub toxicity_average: f64,
    pub toxicity_max: f64,
    pub payload_char_stats: ZiCStatisticSummary,
    pub payload_token_stats: ZiCStatisticSummary,
    pub toxicity_stats: ZiCStatisticSummary,
    pub pii_total_matches: usize,
    pub pii_tag_counts: HashMap<String, usize>,
    pub pii_strategy_counts: HashMap<String, usize>,
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
        let mut char_lengths = Vec::with_capacity(records.len());
        let mut token_counts = Vec::with_capacity(records.len());
        let mut toxicity_scores = Vec::new();
        let mut metadata_numeric_values: HashMap<String, Vec<f64>> = HashMap::new();
        let mut metadata_flag_true_counts: HashMap<String, usize> = HashMap::new();

        for record in records {
            let payload_string = match &record.payload {
                Value::String(text) => text.clone(),
                other => other.to_string(),
            };
            let char_count = payload_string.chars().count();
            let token_count = payload_string
                .split_whitespace()
                .filter(|token| !token.is_empty())
                .count();

            total_chars += char_count;
            total_tokens += token_count;
            char_lengths.push(char_count as f64);
            token_counts.push(token_count as f64);

            if let Some(metadata) = &record.metadata {
                metrics.records_with_metadata += 1;
                let metadata_len = metadata.len() as f64;

                for (key, value) in metadata {
                    *metrics.metadata_key_counts.entry(key.clone()).or_insert(0) += 1;

                    let key_lower = key.to_ascii_lowercase();

                    if key_lower == "lang" || key_lower == "language" {
                        if let Some(lang) = value.as_str() {
                            *metrics.language_counts.entry(lang.to_string()).or_insert(0) += 1;
                        }
                    }

                    if key_lower.contains("tox") {
                        if let Some(score) = value.as_f64() {
                            toxicity_scores.push(score);
                            if score > metrics.toxicity_max {
                                metrics.toxicity_max = score;
                            }
                        }
                    }

                    if key_lower.contains("pii") {
                        if let Value::Array(matches) = value {
                            for entry in matches {
                                if let Value::Object(obj) = entry {
                                    if let Some(tag) = obj.get("tag").and_then(Value::as_str) {
                                        metrics.pii_total_matches += 1;
                                        *metrics
                                            .pii_tag_counts
                                            .entry(tag.to_string())
                                            .or_insert(0) += 1;
                                        if let Some(strategy) =
                                            obj.get("strategy").and_then(Value::as_str)
                                        {
                                            *metrics
                                                .pii_strategy_counts
                                                .entry(strategy.to_string())
                                                .or_insert(0) += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    match value {
                        Value::Number(number) => {
                            if let Some(float) = number.as_f64() {
                                metadata_numeric_values
                                    .entry(key.clone())
                                    .or_default()
                                    .push(float);
                            }
                        }
                        Value::Bool(true) => {
                            *metadata_flag_true_counts.entry(key.clone()).or_insert(0) += 1;
                        }
                        _ => {}
                    }
                }

                // Track per-record metadata saturation for coverage stats
                metrics.metadata_average_keys += metadata_len;
            }
        }

        metrics.average_payload_chars = total_chars as f64 / records.len() as f64;
        metrics.average_payload_tokens = total_tokens as f64 / records.len() as f64;
        metrics.metadata_average_keys = if metrics.records_with_metadata > 0 {
            metrics.metadata_average_keys / metrics.records_with_metadata as f64
        } else {
            0.0
        };
        metrics.metadata_coverage_ratio =
            metrics.records_with_metadata as f64 / metrics.total_records.max(1) as f64;

        metrics.payload_char_stats = ZiCStatisticSummary::from_slice(&char_lengths);
        metrics.payload_token_stats = ZiCStatisticSummary::from_slice(&token_counts);

        if !toxicity_scores.is_empty() {
            metrics.toxicity_stats = ZiCStatisticSummary::from_slice(&toxicity_scores);
            metrics.toxicity_average = metrics.toxicity_stats.mean;
        }

        metrics.metadata_flag_true_counts = metadata_flag_true_counts;
        metrics.metadata_numeric_stats = metadata_numeric_values
            .into_iter()
            .map(|(key, values)| (key, ZiCStatisticSummary::from_slice(&values)))
            .collect();

        metrics
    }

    #[allow(non_snake_case)]
    pub fn ZiFAsJson(&self) -> Value {
        serde_json::to_value(self).unwrap_or_else(|_| Value::Null)
    }
}

impl ZiCStatisticSummary {
    pub fn from_slice(values: &[f64]) -> Self {
        if values.is_empty() {
            return ZiCStatisticSummary::default();
        }

        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let len = sorted.len();
        let min = sorted.first().copied().unwrap_or(0.0);
        let max = sorted.last().copied().unwrap_or(0.0);
        let mean = values.iter().sum::<f64>() / len as f64;
        let variance = if len > 1 {
            values
                .iter()
                .map(|v| {
                    let diff = *v - mean;
                    diff * diff
                })
                .sum::<f64>()
                / len as f64
        } else {
            0.0
        };
        let stddev = variance.sqrt();

        let median = if len % 2 == 1 {
            sorted[len / 2]
        } else {
            (sorted[len / 2 - 1] + sorted[len / 2]) / 2.0
        };

        let p95_index = ((len as f64 - 1.0) * 0.95).round() as usize;
        let p95 = sorted[p95_index.min(len - 1)];

        ZiCStatisticSummary {
            min,
            max,
            mean,
            median,
            p95,
            stddev,
        }
    }
}

