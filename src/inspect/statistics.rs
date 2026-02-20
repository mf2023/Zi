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

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::record::ZiCRecordBatch;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZiCNumericStats {
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

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZiCStringStats {
    pub count: usize,
    pub empty_count: usize,
    pub min_length: usize,
    pub max_length: usize,
    pub avg_length: f64,
    pub unique_count: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZiCStatistics {
    pub numeric_stats: std::collections::HashMap<String, ZiCNumericStats>,
    pub string_stats: std::collections::HashMap<String, ZiCStringStats>,
}

impl ZiCStatistics {
    #[allow(non_snake_case)]
    pub fn ZiFCompute(batch: &ZiCRecordBatch) -> Self {
        let mut stats = Self::default();

        let mut numeric_values: std::collections::HashMap<String, Vec<f64>> = 
            std::collections::HashMap::new();
        let mut string_values: std::collections::HashMap<String, Vec<String>> = 
            std::collections::HashMap::new();

        for record in batch {
            Self::extract_values("payload", &record.payload, &mut numeric_values, &mut string_values);
        }

        for (field, values) in numeric_values {
            if !values.is_empty() {
                stats.numeric_stats.insert(field, Self::compute_numeric_stats(&values));
            }
        }

        for (field, values) in string_values {
            if !values.is_empty() {
                stats.string_stats.insert(field, Self::compute_string_stats(&values));
            }
        }

        stats
    }

    fn extract_values(
        path: &str,
        value: &Value,
        numeric: &mut std::collections::HashMap<String, Vec<f64>>,
        strings: &mut std::collections::HashMap<String, Vec<String>>,
    ) {
        match value {
            Value::Number(n) => {
                if let Some(f) = n.as_f64() {
                    numeric.entry(path.to_string()).or_default().push(f);
                }
            }
            Value::String(s) => {
                strings.entry(path.to_string()).or_default().push(s.clone());
            }
            Value::Object(map) => {
                for (k, v) in map {
                    let new_path = format!("{}.{}", path, k);
                    Self::extract_values(&new_path, v, numeric, strings);
                }
            }
            Value::Array(arr) => {
                for (i, v) in arr.iter().enumerate() {
                    let new_path = format!("{}[{}]", path, i);
                    Self::extract_values(&new_path, v, numeric, strings);
                }
            }
            _ => {}
        }
    }

    fn compute_numeric_stats(values: &[f64]) -> ZiCNumericStats {
        if values.is_empty() {
            return ZiCNumericStats::default();
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
        let p25 = sorted[((count as f64 * 0.25) as usize).min(count - 1)];
        let p75 = sorted[((count as f64 * 0.75) as usize).min(count - 1)];
        let p95 = sorted[((count as f64 * 0.95) as usize).min(count - 1)];
        let p99 = sorted[((count as f64 * 0.99) as usize).min(count - 1)];

        ZiCNumericStats {
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

    fn compute_string_stats(values: &[String]) -> ZiCStringStats {
        if values.is_empty() {
            return ZiCStringStats::default();
        }

        let count = values.len();
        let empty_count = values.iter().filter(|s| s.is_empty()).count();
        let lengths: Vec<usize> = values.iter().map(|s| s.chars().count()).collect();
        
        let min_length = *lengths.iter().min().unwrap_or(&0);
        let max_length = *lengths.iter().max().unwrap_or(&0);
        let avg_length = lengths.iter().sum::<usize>() as f64 / count as f64;

        let unique_count = values.iter().collect::<std::collections::HashSet<_>>().len();

        ZiCStringStats {
            count,
            empty_count,
            min_length,
            max_length,
            avg_length,
            unique_count,
        }
    }
}
