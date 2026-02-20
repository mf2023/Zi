//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
//!
//! This file is part of Zi.
//! The Zi project belongs to the Dunimd project team.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//!     http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::record::{ZiCRecord, ZiCRecordBatch};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZiCFieldProfile {
    pub name: String,
    pub count: usize,
    pub null_count: usize,
    pub type_distribution: HashMap<String, usize>,
    pub unique_count: Option<usize>,
    pub sample_values: Vec<Value>,
    pub frequency_distribution: Vec<(Value, usize)>,
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
    pub avg_value: Option<f64>,
    pub std_dev: Option<f64>,
    pub anomalies: Vec<ZiCAnomaly>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCAnomaly {
    pub anomaly_type: String,
    pub description: String,
    pub severity: ZiCAnomalySeverity,
    pub affected_count: usize,
    pub sample_indices: Vec<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiCAnomalySeverity {
    Low,
    Medium,
    High,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZiCProfileReport {
    pub total_records: usize,
    pub total_fields: usize,
    pub field_profiles: Vec<ZiCFieldProfile>,
    pub avg_record_size: f64,
    pub memory_estimate: usize,
    pub duplicate_count: usize,
    pub empty_record_count: usize,
    pub completeness_score: f64,
    pub quality_score: f64,
    pub text_statistics: Option<ZiCTextStatistics>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZiCTextStatistics {
    pub total_chars: usize,
    pub total_words: usize,
    pub avg_chars_per_record: f64,
    pub avg_words_per_record: f64,
    pub char_distribution: HashMap<char, usize>,
    pub word_frequency: Vec<(String, usize)>,
    pub language_distribution: HashMap<String, usize>,
    pub ngram_distribution: HashMap<String, usize>,
}

#[derive(Clone, Debug)]
pub struct ZiCProfilerConfig {
    pub sample_size: usize,
    pub max_unique_tracking: usize,
    pub max_frequency_items: usize,
    pub detect_anomalies: bool,
    pub anomaly_threshold: f64,
    pub compute_text_stats: bool,
    pub ngram_size: usize,
}

impl Default for ZiCProfilerConfig {
    fn default() -> Self {
        Self {
            sample_size: 100,
            max_unique_tracking: 10000,
            max_frequency_items: 20,
            detect_anomalies: true,
            anomaly_threshold: 3.0,
            compute_text_stats: true,
            ngram_size: 2,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ZiCProfiler {
    config: ZiCProfilerConfig,
}

impl ZiCProfiler {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        Self {
            config: ZiCProfilerConfig::default(),
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFWithConfig(mut self, config: ZiCProfilerConfig) -> Self {
        self.config = config;
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFProfile(&self, batch: &ZiCRecordBatch) -> ZiCProfileReport {
        if batch.is_empty() {
            return ZiCProfileReport::default();
        }

        let total_records = batch.len();
        let mut field_profiles: HashMap<String, ZiCFieldProfileBuilder> = HashMap::new();
        let mut total_size = 0usize;
        let mut seen_hashes: HashSet<u64> = HashSet::new();
        let mut duplicate_count = 0usize;
        let mut empty_count = 0usize;
        let mut text_stats_builder = ZiCTextStatsBuilder::new(self.config.ngram_size);

        for (idx, record) in batch.iter().enumerate() {
            let record_size = Self::estimate_record_size(record);
            total_size += record_size;
            
            let record_hash = self.hash_record(record);
            if seen_hashes.contains(&record_hash) {
                duplicate_count += 1;
            } else {
                seen_hashes.insert(record_hash);
            }

            if self.is_empty_record(record) {
                empty_count += 1;
            }

            self.profile_record(record, idx, &mut field_profiles, &mut text_stats_builder);
        }

        let field_profiles: Vec<ZiCFieldProfile> = field_profiles
            .into_values()
            .map(|b| b.build(&self.config))
            .collect();
        
        let total_fields = field_profiles.len();
        
        let completeness_score = self.calculate_completeness(&field_profiles, total_records);
        let quality_score = self.calculate_quality_score(
            &field_profiles,
            total_records,
            duplicate_count,
            empty_count,
        );

        let text_statistics = if self.config.compute_text_stats {
            Some(text_stats_builder.build(self.config.max_frequency_items))
        } else {
            None
        };

        ZiCProfileReport {
            total_records,
            total_fields,
            field_profiles,
            avg_record_size: total_size as f64 / total_records as f64,
            memory_estimate: total_size,
            duplicate_count,
            empty_record_count: empty_count,
            completeness_score,
            quality_score,
            text_statistics,
        }
    }

    fn hash_record(&self, record: &ZiCRecord) -> u64 {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;
        
        let mut hasher = DefaultHasher::new();
        record.id.hash(&mut hasher);
        record.payload.to_string().hash(&mut hasher);
        if let Some(meta) = &record.metadata {
            if let Ok(meta_str) = serde_json::to_string(meta) {
                meta_str.hash(&mut hasher);
            }
        }
        hasher.finish()
    }

    fn is_empty_record(&self, record: &ZiCRecord) -> bool {
        match &record.payload {
            Value::Null => true,
            Value::Object(map) => map.is_empty(),
            Value::Array(arr) => arr.is_empty(),
            Value::String(s) => s.trim().is_empty(),
            _ => false,
        }
    }

    fn profile_record(
        &self,
        record: &ZiCRecord,
        idx: usize,
        profiles: &mut HashMap<String, ZiCFieldProfileBuilder>,
        text_stats: &mut ZiCTextStatsBuilder,
    ) {
        self.profile_value("payload", &record.payload, idx, profiles, text_stats);
        
        if let Some(meta) = &record.metadata {
            for (key, value) in meta {
                let path = format!("metadata.{}", key);
                self.profile_value(&path, value, idx, profiles, text_stats);
            }
        }
    }

    fn profile_value(
        &self,
        path: &str,
        value: &Value,
        _idx: usize,
        profiles: &mut HashMap<String, ZiCFieldProfileBuilder>,
        text_stats: &mut ZiCTextStatsBuilder,
    ) {
        let profile = profiles.entry(path.to_string()).or_insert_with(|| {
            ZiCFieldProfileBuilder::new(path.to_string())
        });

        profile.count += 1;

        match value {
            Value::Null => {
                profile.null_count += 1;
            }
            Value::Bool(_b) => {
                *profile.type_distribution.entry("bool".to_string()).or_insert(0) += 1;
                profile.track_unique(value.clone());
                profile.track_frequency(value.clone());
            }
            Value::Number(n) => {
                *profile.type_distribution.entry("number".to_string()).or_insert(0) += 1;
                profile.track_unique(value.clone());
                profile.track_frequency(value.clone());
                
                if let Some(f) = n.as_f64() {
                    profile.track_numeric(f);
                }
            }
            Value::String(s) => {
                *profile.type_distribution.entry("string".to_string()).or_insert(0) += 1;
                profile.track_unique(value.clone());
                profile.track_frequency(value.clone());
                profile.track_string(s);
                
                if self.config.compute_text_stats && path == "payload" {
                    text_stats.process_text(s);
                }
            }
            Value::Array(_arr) => {
                *profile.type_distribution.entry("array".to_string()).or_insert(0) += 1;
                profile.track_unique(value.clone());
            }
            Value::Object(_obj) => {
                *profile.type_distribution.entry("object".to_string()).or_insert(0) += 1;
                profile.track_unique(value.clone());
            }
        }

        if profile.sample_values.len() < self.config.sample_size && !value.is_null() {
            profile.sample_values.push(value.clone());
        }
    }

    fn calculate_completeness(&self, profiles: &[ZiCFieldProfile], total_records: usize) -> f64 {
        if profiles.is_empty() || total_records == 0 {
            return 0.0;
        }

        let total_expected = profiles.len() * total_records;
        let total_actual: usize = profiles.iter().map(|p| p.count - p.null_count).sum();

        total_actual as f64 / total_expected as f64
    }

    fn calculate_quality_score(
        &self,
        profiles: &[ZiCFieldProfile],
        total_records: usize,
        duplicate_count: usize,
        empty_count: usize,
    ) -> f64 {
        if total_records == 0 {
            return 0.0;
        }

        let completeness = self.calculate_completeness(profiles, total_records);
        let uniqueness = 1.0 - (duplicate_count as f64 / total_records as f64);
        let non_empty_ratio = 1.0 - (empty_count as f64 / total_records as f64);

        let anomaly_penalty: f64 = profiles
            .iter()
            .map(|p| {
                let anomaly_count = p.anomalies.iter().map(|a| a.affected_count).sum::<usize>();
                anomaly_count as f64 / total_records as f64
            })
            .sum::<f64>()
            / profiles.len().max(1) as f64;

        let score = completeness * 0.3 + uniqueness * 0.25 + non_empty_ratio * 0.25 + (1.0 - anomaly_penalty) * 0.2;
        score.max(0.0).min(1.0)
    }

    fn estimate_record_size(record: &ZiCRecord) -> usize {
        let payload_size = record.payload.to_string().len();
        let meta_size = record.metadata.as_ref()
            .and_then(|m| serde_json::to_string(m).ok())
            .map(|s| s.len())
            .unwrap_or(0);
        payload_size + meta_size + 64
    }
}

#[derive(Clone)]
struct ZiCFieldProfileBuilder {
    name: String,
    count: usize,
    null_count: usize,
    type_distribution: HashMap<String, usize>,
    unique_values: HashSet<String>,
    frequency_map: HashMap<String, usize>,
    sample_values: Vec<Value>,
    numeric_values: Vec<f64>,
    string_lengths: Vec<usize>,
    min_numeric: Option<f64>,
    max_numeric: Option<f64>,
    sum_numeric: f64,
}

impl ZiCFieldProfileBuilder {
    fn new(name: String) -> Self {
        Self {
            name,
            count: 0,
            null_count: 0,
            type_distribution: HashMap::new(),
            unique_values: HashSet::new(),
            frequency_map: HashMap::new(),
            sample_values: Vec::new(),
            numeric_values: Vec::new(),
            string_lengths: Vec::new(),
            min_numeric: None,
            max_numeric: None,
            sum_numeric: 0.0,
        }
    }

    fn track_unique(&mut self, value: Value) {
        let key = value.to_string();
        self.unique_values.insert(key);
    }

    fn track_frequency(&mut self, value: Value) {
        let key = value.to_string();
        *self.frequency_map.entry(key).or_insert(0) += 1;
    }

    fn track_numeric(&mut self, n: f64) {
        self.numeric_values.push(n);
        self.sum_numeric += n;
        
        self.min_numeric = Some(self.min_numeric.map_or(n, |m| m.min(n)));
        self.max_numeric = Some(self.max_numeric.map_or(n, |m| m.max(n)));
    }

    fn track_string(&mut self, s: &str) {
        self.string_lengths.push(s.len());
    }

    fn build(self, config: &ZiCProfilerConfig) -> ZiCFieldProfile {
        let unique_count = if self.unique_values.len() <= config.max_unique_tracking {
            Some(self.unique_values.len())
        } else {
            None
        };

        let anomalies = if config.detect_anomalies {
            self.clone().detect_anomalies(config.anomaly_threshold)
        } else {
            Vec::new()
        };

        let mut frequency_distribution: Vec<(Value, usize)> = self.frequency_map
            .into_iter()
            .map(|(k, v)| {
                let value = serde_json::from_str(&k).unwrap_or(Value::String(k));
                (value, v)
            })
            .collect();
        
        frequency_distribution.sort_by(|a, b| b.1.cmp(&a.1));
        frequency_distribution.truncate(config.max_frequency_items);

        let (min_value, max_value, avg_value, std_dev) = if !self.numeric_values.is_empty() {
            let min = self.numeric_values.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = self.numeric_values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            let sum: f64 = self.numeric_values.iter().sum();
            let avg = sum / self.numeric_values.len() as f64;
            
            let variance: f64 = self.numeric_values
                .iter()
                .map(|x| (x - avg).powi(2))
                .sum::<f64>() / self.numeric_values.len() as f64;
            let std = variance.sqrt();

            (
                Some(Value::Number(serde_json::Number::from_f64(min).unwrap_or_else(|| serde_json::Number::from(0)))),
                Some(Value::Number(serde_json::Number::from_f64(max).unwrap_or_else(|| serde_json::Number::from(0)))),
                Some(avg),
                Some(std),
            )
        } else {
            (None, None, None, None)
        };

        ZiCFieldProfile {
            name: self.name,
            count: self.count,
            null_count: self.null_count,
            type_distribution: self.type_distribution,
            unique_count,
            sample_values: self.sample_values,
            frequency_distribution,
            min_value,
            max_value,
            avg_value,
            std_dev,
            anomalies,
        }
    }

    fn detect_anomalies(&self, threshold: f64) -> Vec<ZiCAnomaly> {
        let mut anomalies = Vec::new();

        let null_rate = self.null_count as f64 / self.count.max(1) as f64;
        if null_rate > 0.5 {
            anomalies.push(ZiCAnomaly {
                anomaly_type: "high_null_rate".to_string(),
                description: format!("Null rate is {:.1}%, exceeds 50%", null_rate * 100.0),
                severity: ZiCAnomalySeverity::High,
                affected_count: self.null_count,
                sample_indices: Vec::new(),
            });
        } else if null_rate > 0.2 {
            anomalies.push(ZiCAnomaly {
                anomaly_type: "elevated_null_rate".to_string(),
                description: format!("Null rate is {:.1}%, exceeds 20%", null_rate * 100.0),
                severity: ZiCAnomalySeverity::Medium,
                affected_count: self.null_count,
                sample_indices: Vec::new(),
            });
        }

        if let (Some(_min), Some(_max)) = (self.min_numeric, self.max_numeric) {
            if !self.numeric_values.is_empty() {
                let avg = self.sum_numeric / self.numeric_values.len() as f64;
                let variance: f64 = self.numeric_values
                    .iter()
                    .map(|x| (x - avg).powi(2))
                    .sum::<f64>() / self.numeric_values.len() as f64;
                let std = variance.sqrt();
                
                if std > 0.0 {
                    let outlier_count = self.numeric_values
                        .iter()
                        .filter(|x| (*(*x) - avg).abs() > threshold * std)
                        .count();
                    
                    if outlier_count > 0 {
                        let outlier_rate = outlier_count as f64 / self.numeric_values.len() as f64;
                        let severity = if outlier_rate > 0.1 {
                            ZiCAnomalySeverity::High
                        } else if outlier_rate > 0.05 {
                            ZiCAnomalySeverity::Medium
                        } else {
                            ZiCAnomalySeverity::Low
                        };

                        anomalies.push(ZiCAnomaly {
                            anomaly_type: "numeric_outliers".to_string(),
                            description: format!("{} outliers detected (>{:.1} std from mean)", outlier_count, threshold),
                            severity,
                            affected_count: outlier_count,
                            sample_indices: Vec::new(),
                        });
                    }
                }
            }
        }

        if !self.string_lengths.is_empty() {
            let avg_len: f64 = self.string_lengths.iter().sum::<usize>() as f64 / self.string_lengths.len() as f64;
            let variance: f64 = self.string_lengths
                .iter()
                .map(|l| (*l as f64 - avg_len).powi(2))
                .sum::<f64>() / self.string_lengths.len() as f64;
            let std_len = variance.sqrt();

            if std_len > avg_len {
                anomalies.push(ZiCAnomaly {
                    anomaly_type: "high_length_variance".to_string(),
                    description: format!("String length variance is high (avg: {:.1}, std: {:.1})", avg_len, std_len),
                    severity: ZiCAnomalySeverity::Medium,
                    affected_count: self.string_lengths.len(),
                    sample_indices: Vec::new(),
                });
            }
        }

        anomalies
    }
}

struct ZiCTextStatsBuilder {
    total_chars: usize,
    total_words: usize,
    char_distribution: HashMap<char, usize>,
    word_frequency: HashMap<String, usize>,
    ngram_distribution: HashMap<String, usize>,
    ngram_size: usize,
}

impl ZiCTextStatsBuilder {
    fn new(ngram_size: usize) -> Self {
        Self {
            total_chars: 0,
            total_words: 0,
            char_distribution: HashMap::new(),
            word_frequency: HashMap::new(),
            ngram_distribution: HashMap::new(),
            ngram_size,
        }
    }

    fn process_text(&mut self, text: &str) {
        self.total_chars += text.chars().count();

        let words: Vec<&str> = text.split_whitespace().collect();
        self.total_words += words.len();

        for c in text.chars() {
            *self.char_distribution.entry(c).or_insert(0) += 1;
        }

        for word in words {
            let normalized = word.to_lowercase();
            *self.word_frequency.entry(normalized).or_insert(0) += 1;
        }

        let chars: Vec<char> = text.chars().collect();
        for window in chars.windows(self.ngram_size) {
            let ngram: String = window.iter().collect();
            *self.ngram_distribution.entry(ngram).or_insert(0) += 1;
        }
    }

    fn build(self, max_items: usize) -> ZiCTextStatistics {
        let record_count = if self.total_words > 0 { 1 } else { 1 };
        
        let mut word_frequency: Vec<(String, usize)> = self.word_frequency
            .into_iter()
            .collect();
        word_frequency.sort_by(|a, b| b.1.cmp(&a.1));
        word_frequency.truncate(max_items);

        let mut ngram_distribution = self.ngram_distribution;
        ngram_distribution.retain(|_, v| *v > 1);

        ZiCTextStatistics {
            total_chars: self.total_chars,
            total_words: self.total_words,
            avg_chars_per_record: self.total_chars as f64 / record_count as f64,
            avg_words_per_record: self.total_words as f64 / record_count as f64,
            char_distribution: self.char_distribution,
            word_frequency,
            language_distribution: HashMap::new(),
            ngram_distribution,
        }
    }
}
