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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::operator::ZiOperator;
use crate::record::{ZiRecord, ZiRecordBatch};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZiHistogram {
    pub bins: Vec<ZiHistogramBin>,
    pub total_count: usize,
    pub bin_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiHistogramBin {
    pub start: f64,
    pub end: f64,
    pub count: usize,
    pub percentage: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZiPercentiles {
    pub p5: f64,
    pub p10: f64,
    pub p25: f64,
    pub p50: f64,
    pub p75: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZiValueDistribution {
    pub field: String,
    pub total_count: usize,
    pub unique_count: usize,
    pub null_count: usize,
    pub value_counts: HashMap<String, usize>,
    pub top_values: Vec<(String, usize)>,
    pub histogram: Option<ZiHistogram>,
    pub percentiles: Option<ZiPercentiles>,
}

#[derive(Debug)]
pub struct ZiDistributionAnalyzer {
    field: String,
    top_k: usize,
    histogram_bins: usize,
}

impl ZiDistributionAnalyzer {
    #[allow(non_snake_case)]
    pub fn new(field: String, top_k: usize, histogram_bins: usize) -> Self {
        Self {
            field,
            top_k,
            histogram_bins,
        }
    }

    fn extract_value(&self, record: &ZiRecord) -> Option<Value> {
        let parts: Vec<&str> = self.field.split('.').collect();
        if parts.len() < 2 {
            return None;
        }

        let mut current = &record.payload;
        for part in &parts[1..] {
            match current {
                Value::Object(map) => {
                    current = map.get(*part)?;
                }
                Value::Array(arr) => {
                    if let Ok(idx) = part.parse::<usize>() {
                        if idx < arr.len() {
                            current = &arr[idx];
                        } else {
                            return None;
                        }
                    } else {
                        return None;
                    }
                }
                _ => return None,
            }
        }
        Some(current.clone())
    }

    fn compute_histogram(&self, values: &[f64]) -> ZiHistogram {
        if values.is_empty() {
            return ZiHistogram::default();
        }

        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let min = sorted[0];
        let max = sorted[sorted.len() - 1];
        let range = max - min;

        if range == 0.0 {
            return ZiHistogram {
                bins: vec![ZiHistogramBin {
                    start: min,
                    end: max,
                    count: values.len(),
                    percentage: 100.0,
                }],
                total_count: values.len(),
                bin_count: 1,
            };
        }

        let bin_width = range / self.histogram_bins as f64;
        let mut bins: Vec<ZiHistogramBin> = (0..self.histogram_bins)
            .map(|i| ZiHistogramBin {
                start: min + i as f64 * bin_width,
                end: min + (i + 1) as f64 * bin_width,
                count: 0,
                percentage: 0.0,
            })
            .collect();

        for &value in &sorted {
            let bin_idx = ((value - min) / bin_width).floor() as usize;
            let bin_idx = bin_idx.min(self.histogram_bins - 1);
            bins[bin_idx].count += 1;
        }

        let total = values.len();
        for bin in &mut bins {
            bin.percentage = (bin.count as f64 / total as f64) * 100.0;
        }

        ZiHistogram {
            bins,
            total_count: total,
            bin_count: self.histogram_bins,
        }
    }

    fn compute_percentiles(&self, values: &[f64]) -> ZiPercentiles {
        if values.is_empty() {
            return ZiPercentiles::default();
        }

        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let n = sorted.len();
        let percentile = |p: f64| sorted[((n as f64 * p) as usize).min(n - 1)];

        ZiPercentiles {
            p5: percentile(0.05),
            p10: percentile(0.10),
            p25: percentile(0.25),
            p50: percentile(0.50),
            p75: percentile(0.75),
            p90: percentile(0.90),
            p95: percentile(0.95),
            p99: percentile(0.99),
        }
    }
}

impl ZiOperator for ZiDistributionAnalyzer {
    fn name(&self) -> &'static str {
        "distribution.analyze"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let mut value_counts: HashMap<String, usize> = HashMap::new();
        let mut numeric_values: Vec<f64> = Vec::new();
        let mut null_count = 0;

        for record in &batch {
            match self.extract_value(record) {
                Some(Value::Null) | None => {
                    null_count += 1;
                }
                Some(Value::Number(n)) => {
                    if let Some(f) = n.as_f64() {
                        numeric_values.push(f);
                    }
                    let key = n.to_string();
                    *value_counts.entry(key).or_insert(0) += 1;
                }
                Some(Value::String(s)) => {
                    *value_counts.entry(s.clone()).or_insert(0) += 1;
                }
                Some(Value::Bool(b)) => {
                    let key = b.to_string();
                    *value_counts.entry(key).or_insert(0) += 1;
                }
                Some(other) => {
                    let key = other.to_string();
                    *value_counts.entry(key).or_insert(0) += 1;
                }
            }
        }

        let total_count = batch.len();
        let unique_count = value_counts.len();

        let mut top_values: Vec<(String, usize)> = value_counts
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        top_values.sort_by(|a, b| b.1.cmp(&a.1));
        top_values.truncate(self.top_k);

        let histogram = if !numeric_values.is_empty() {
            Some(self.compute_histogram(&numeric_values))
        } else {
            None
        };

        let percentiles = if !numeric_values.is_empty() {
            Some(self.compute_percentiles(&numeric_values))
        } else {
            None
        };

        let distribution = ZiValueDistribution {
            field: self.field.clone(),
            total_count,
            unique_count,
            null_count,
            value_counts,
            top_values,
            histogram,
            percentiles,
        };

        let mut result = batch;
        if let Some(first) = result.first_mut() {
            let meta = first.metadata_mut();
            meta.insert(
                "distribution".to_string(),
                serde_json::to_value(&distribution).unwrap_or(Value::Null),
            );
        }

        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn distribution_analyzer_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("distribution.analyze config must be object"))?;

    let field = obj
        .get("field")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("distribution.analyze requires string 'field'"))?
        .to_string();

    let top_k = obj
        .get("top_k")
        .and_then(Value::as_u64)
        .unwrap_or(10) as usize;

    let histogram_bins = obj
        .get("histogram_bins")
        .and_then(Value::as_u64)
        .unwrap_or(10) as usize;

    Ok(Box::new(ZiDistributionAnalyzer::new(field, top_k, histogram_bins)))
}

#[derive(Debug)]
pub struct ZiDistributionReport {
    fields: Vec<String>,
}

impl ZiDistributionReport {
    #[allow(non_snake_case)]
    pub fn new(fields: Vec<String>) -> Self {
        Self { fields }
    }
}

impl ZiOperator for ZiDistributionReport {
    fn name(&self) -> &'static str {
        "distribution.report"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let mut report: HashMap<String, ZiValueDistribution> = HashMap::new();

        for field in &self.fields {
            let analyzer = ZiDistributionAnalyzer::new(field.clone(), 10, 10);
            
            let mut value_counts: HashMap<String, usize> = HashMap::new();
            let mut numeric_values: Vec<f64> = Vec::new();
            let mut null_count = 0;

            for record in &batch {
                let parts: Vec<&str> = field.split('.').collect();
                if parts.len() < 2 {
                    continue;
                }

                let mut current = &record.payload;
                for part in &parts[1..] {
                    if let Value::Object(map) = current {
                        current = match map.get(*part) {
                            Some(v) => v,
                            None => &Value::Null,
                        };
                    } else {
                        current = &Value::Null;
                        break;
                    }
                }

                match current {
                    Value::Null => {
                        null_count += 1;
                    }
                    Value::Number(n) => {
                        if let Some(f) = n.as_f64() {
                            numeric_values.push(f);
                        }
                        let key = n.to_string();
                        *value_counts.entry(key).or_insert(0) += 1;
                    }
                    Value::String(s) => {
                        *value_counts.entry(s.clone()).or_insert(0) += 1;
                    }
                    Value::Bool(b) => {
                        let key = b.to_string();
                        *value_counts.entry(key).or_insert(0) += 1;
                    }
                    other => {
                        let key = other.to_string();
                        *value_counts.entry(key).or_insert(0) += 1;
                    }
                }
            }

            let total_count = batch.len();
            let unique_count = value_counts.len();

            let mut top_values: Vec<(String, usize)> = value_counts
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect();
            top_values.sort_by(|a, b| b.1.cmp(&a.1));
            top_values.truncate(10);

            let histogram = if !numeric_values.is_empty() {
                Some(analyzer.compute_histogram(&numeric_values))
            } else {
                None
            };

            let percentiles = if !numeric_values.is_empty() {
                Some(analyzer.compute_percentiles(&numeric_values))
            } else {
                None
            };

            report.insert(
                field.clone(),
                ZiValueDistribution {
                    field: field.clone(),
                    total_count,
                    unique_count,
                    null_count,
                    value_counts,
                    top_values,
                    histogram,
                    percentiles,
                },
            );
        }

        let mut result = batch;
        if let Some(first) = result.first_mut() {
            let meta = first.metadata_mut();
            meta.insert(
                "distribution_report".to_string(),
                serde_json::to_value(&report).unwrap_or(Value::Null),
            );
        }

        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn distribution_report_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("distribution.report config must be object"))?;

    let fields = obj
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("distribution.report requires array 'fields'"))?
        .iter()
        .map(|v| {
            v.as_str()
                .ok_or_else(|| ZiError::validation("fields must be strings"))
                .map(|s| s.to_string())
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Box::new(ZiDistributionReport::new(fields)))
}

#[derive(Debug)]
pub struct ZiCorrelation {
    field_x: String,
    field_y: String,
}

impl ZiCorrelation {
    #[allow(non_snake_case)]
    pub fn new(field_x: String, field_y: String) -> Self {
        Self { field_x, field_y }
    }

    fn extract_numeric(&self, record: &ZiRecord, field: &str) -> Option<f64> {
        let parts: Vec<&str> = field.split('.').collect();
        if parts.len() < 2 {
            return None;
        }

        let mut current = &record.payload;
        for part in &parts[1..] {
            if let Value::Object(map) = current {
                current = map.get(*part)?;
            } else {
                return None;
            }
        }

        match current {
            Value::Number(n) => n.as_f64(),
            _ => None,
        }
    }
}

impl ZiOperator for ZiCorrelation {
    fn name(&self) -> &'static str {
        "distribution.correlation"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let mut x_values: Vec<f64> = Vec::new();
        let mut y_values: Vec<f64> = Vec::new();

        for record in &batch {
            if let (Some(x), Some(y)) = (
                self.extract_numeric(record, &self.field_x),
                self.extract_numeric(record, &self.field_y),
            ) {
                x_values.push(x);
                y_values.push(y);
            }
        }

        let correlation = if x_values.len() > 1 {
            let n = x_values.len() as f64;
            let sum_x: f64 = x_values.iter().sum();
            let sum_y: f64 = y_values.iter().sum();
            let sum_xy: f64 = x_values.iter().zip(&y_values).map(|(x, y)| x * y).sum();
            let sum_x2: f64 = x_values.iter().map(|x| x * x).sum();
            let sum_y2: f64 = y_values.iter().map(|y| y * y).sum();

            let numerator = n * sum_xy - sum_x * sum_y;
            let denominator = ((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y)).sqrt();

            if denominator != 0.0 {
                Some(numerator / denominator)
            } else {
                None
            }
        } else {
            None
        };

        let mut result = batch;
        if let Some(first) = result.first_mut() {
            let meta = first.metadata_mut();
            meta.insert(
                "correlation".to_string(),
                serde_json::json!({
                    "field_x": self.field_x,
                    "field_y": self.field_y,
                    "pearson": correlation,
                    "sample_size": x_values.len(),
                }),
            );
        }

        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn correlation_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("distribution.correlation config must be object"))?;

    let field_x = obj
        .get("field_x")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("distribution.correlation requires string 'field_x'"))?
        .to_string();

    let field_y = obj
        .get("field_y")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("distribution.correlation requires string 'field_y'"))?
        .to_string();

    Ok(Box::new(ZiCorrelation::new(field_x, field_y)))
}
