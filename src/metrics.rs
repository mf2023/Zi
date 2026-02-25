//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
//!
//! This file is part of Zi.
//! The Zi project belongs to the Dunimd Team.
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

//! # Zi Metrics Module
//!
//! This module provides data quality metrics and statistical summaries for
//! evaluating and monitoring data processing pipelines.
//!
//! ## Metrics Types
//!
//! - **ZiQualityMetrics**: Quality indicators computed from record batches
//!   including toxicity scores, quality scores, duplicates, and error counts
//! - **ZiStatisticSummary**: Statistical summaries of field distributions
//!
//! ## Usage
//!
//! ```rust
//! use zi::metrics::{ZiQualityMetrics, ZiStatisticSummary};
//! use zi::record::{ZiRecord, ZiRecordBatch};
//!
//! // Compute quality metrics for a batch
//! let metrics = ZiQualityMetrics::compute(&batch);
//! println!("Quality score: {}", metrics.quality_score_average);
//! ```

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::record::ZiRecord;

/// Container for data quality metrics computed from a batch of records.
///
/// ZiQualityMetrics provides a comprehensive view of data quality by computing
/// various indicators from a collection of records. These metrics are useful for:
///
/// - **Quality Assessment**: Understanding the overall quality of data
/// - **Monitoring**: Tracking quality trends over time
/// - **Filtering**: Identifying low-quality records for removal
/// - **Reporting**: Generating quality reports for stakeholders
///
/// # Computation
///
/// Metrics are computed synchronously by iterating over the record batch.
/// For large datasets, consider computing metrics in parallel or sampling.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZiQualityMetrics {
    /// Total number of records in the batch
    pub total_records: usize,
    /// Average number of characters in payloads
    pub average_payload_chars: f64,
    /// Average number of tokens (whitespace-separated words) in payloads
    pub average_payload_tokens: f64,
    /// Average toxicity score across all records (0.0 to 1.0)
    pub toxicity_average: f64,
    /// Maximum toxicity score across all records (0.0 to 1.0)
    pub toxicity_max: f64,
    /// Average quality score across all records (0.0 to 1.0)
    pub quality_score_average: f64,
    /// Number of duplicate records detected
    pub duplicate_count: usize,
    /// Number of records with empty payloads
    pub empty_count: usize,
    /// Number of records with processing errors
    pub error_count: usize,
}

impl ZiQualityMetrics {
    #[allow(non_snake_case)]
    pub fn compute(batch: &[ZiRecord]) -> Self {
        if batch.is_empty() {
            return Self::default();
        }

        let total_records = batch.len();
        let mut total_chars = 0usize;
        let mut total_tokens = 0usize;
        let mut toxicity_sum = 0.0_f64;
        let mut toxicity_max = 0.0_f64;
        let mut quality_sum = 0.0_f64;
        let mut empty_count = 0usize;

        for record in batch {
            let payload_str = match &record.payload {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };

            let char_count = payload_str.chars().count();
            let token_count = payload_str.split_whitespace().count();

            total_chars += char_count;
            total_tokens += token_count;

            if char_count == 0 {
                empty_count += 1;
            }

            if let Some(metadata) = &record.metadata {
                if let Some(toxicity) = metadata.get("toxicity").and_then(|v| v.as_f64()) {
                    toxicity_sum += toxicity;
                    toxicity_max = toxicity_max.max(toxicity);
                }
                if let Some(score) = metadata.get("quality_score").and_then(|v| v.as_f64()) {
                    quality_sum += score;
                }
            }
        }

        let average_payload_chars = total_chars as f64 / total_records as f64;
        let average_payload_tokens = total_tokens as f64 / total_records as f64;

        Self {
            total_records,
            average_payload_chars,
            average_payload_tokens,
            toxicity_average: toxicity_sum / total_records as f64,
            toxicity_max,
            quality_score_average: quality_sum / total_records as f64,
            duplicate_count: 0,
            empty_count,
            error_count: 0,
        }
    }

    #[allow(non_snake_case)]
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZiStatisticSummary {
    pub count: usize,
    pub mean: f64,
    pub std_dev: f64,
    pub min: f64,
    pub max: f64,
    pub median: f64,
    pub p25: f64,
    pub p75: f64,
    pub p95: f64,
    pub p99: f64,
}

impl ZiStatisticSummary {
    pub fn from_slice(values: &[f64]) -> Self {
        if values.is_empty() {
            return Self::default();
        }

        let count = values.len();
        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let sum: f64 = sorted.iter().sum();
        let mean = sum / count as f64;

        let variance: f64 = sorted.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / count as f64;
        let std_dev = variance.sqrt();

        let min = sorted[0];
        let max = sorted[count - 1];
        let median = sorted[count / 2];
        let p25 = sorted[(count as f64 * 0.25) as usize];
        let p75 = sorted[(count as f64 * 0.75) as usize];
        let p95 = sorted[(count as f64 * 0.95) as usize];
        let p99 = sorted[(count as f64 * 0.99) as usize];

        Self {
            count,
            mean,
            std_dev,
            min,
            max,
            median,
            p25,
            p75,
            p95,
            p99,
        }
    }
}
