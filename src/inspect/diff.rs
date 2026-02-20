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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCDiffChange {
    pub path: String,
    pub old_value: Option<Value>,
    pub new_value: Option<Value>,
    pub change_type: ZiCChangeType,
    pub similarity: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ZiCChangeType {
    Added,
    Removed,
    Modified,
    Unchanged,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZiCDiffStats {
    pub total_records_old: usize,
    pub total_records_new: usize,
    pub records_added: usize,
    pub records_removed: usize,
    pub records_modified: usize,
    pub records_unchanged: usize,
    pub fields_added: usize,
    pub fields_removed: usize,
    pub fields_modified: usize,
    pub similarity_score: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZiCDiffReport {
    pub stats: ZiCDiffStats,
    pub changes: Vec<ZiCDiffChange>,
    pub field_changes: Vec<ZiCFieldChange>,
    pub record_diffs: Vec<ZiCRecordDiff>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCFieldChange {
    pub field_path: String,
    pub change_count: usize,
    pub old_type: Option<String>,
    pub new_type: Option<String>,
    pub sample_old_values: Vec<Value>,
    pub sample_new_values: Vec<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCRecordDiff {
    pub record_id: String,
    pub diff_type: ZiCChangeType,
    pub field_changes: Vec<ZiCDiffChange>,
    pub similarity: f64,
}

#[derive(Clone, Debug)]
pub struct ZiCDifferConfig {
    pub max_changes: usize,
    pub max_field_samples: usize,
    pub compute_similarity: bool,
    pub similarity_threshold: f64,
    pub track_field_changes: bool,
    pub ignore_fields: HashSet<String>,
}

impl Default for ZiCDifferConfig {
    fn default() -> Self {
        Self {
            max_changes: 1000,
            max_field_samples: 10,
            compute_similarity: true,
            similarity_threshold: 0.8,
            track_field_changes: true,
            ignore_fields: HashSet::new(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ZiCDiffer {
    config: ZiCDifferConfig,
}

impl ZiCDiffer {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        Self {
            config: ZiCDifferConfig::default(),
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFWithConfig(mut self, config: ZiCDifferConfig) -> Self {
        self.config = config;
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFWithMaxChanges(mut self, max: usize) -> Self {
        self.config.max_changes = max;
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFIgnoreField(mut self, field: &str) -> Self {
        self.config.ignore_fields.insert(field.to_string());
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFDiff(&self, old: &ZiCRecordBatch, new: &ZiCRecordBatch) -> ZiCDiffReport {
        let old_map: HashMap<String, &ZiCRecord> = old
            .iter()
            .filter_map(|r| r.id.as_ref().map(|id| (id.clone(), r)))
            .collect();

        let new_map: HashMap<String, &ZiCRecord> = new
            .iter()
            .filter_map(|r| r.id.as_ref().map(|id| (id.clone(), r)))
            .collect();

        let mut stats = ZiCDiffStats {
            total_records_old: old.len(),
            total_records_new: new.len(),
            ..Default::default()
        };

        let mut changes = Vec::new();
        let mut field_change_tracker: HashMap<String, ZiCFieldChangeBuilder> = HashMap::new();
        let mut record_diffs = Vec::new();

        let old_ids: HashSet<&String> = old_map.keys().collect();
        let new_ids: HashSet<&String> = new_map.keys().collect();

        stats.records_removed = old_ids.difference(&new_ids).count();
        stats.records_added = new_ids.difference(&old_ids).count();

        for id in new_ids.difference(&old_ids) {
            if changes.len() >= self.config.max_changes {
                break;
            }
            if let Some(record) = new_map.get(*id) {
                let diff = ZiCRecordDiff {
                    record_id: id.to_string(),
                    diff_type: ZiCChangeType::Added,
                    field_changes: vec![ZiCDiffChange {
                        path: "record".to_string(),
                        old_value: None,
                        new_value: Some(record.payload.clone()),
                        change_type: ZiCChangeType::Added,
                        similarity: None,
                    }],
                    similarity: 0.0,
                };
                record_diffs.push(diff);
            }
        }

        for id in old_ids.difference(&new_ids) {
            if changes.len() >= self.config.max_changes {
                break;
            }
            if let Some(record) = old_map.get(*id) {
                let diff = ZiCRecordDiff {
                    record_id: id.to_string(),
                    diff_type: ZiCChangeType::Removed,
                    field_changes: vec![ZiCDiffChange {
                        path: "record".to_string(),
                        old_value: Some(record.payload.clone()),
                        new_value: None,
                        change_type: ZiCChangeType::Removed,
                        similarity: None,
                    }],
                    similarity: 0.0,
                };
                record_diffs.push(diff);
            }
        }

        for (id, new_record) in &new_map {
            if let Some(old_record) = old_map.get(id) {
                let mut field_changes = Vec::new();
                let record_similarity = self.diff_records(
                    old_record,
                    new_record,
                    &mut field_changes,
                    &mut field_change_tracker,
                );

                if !field_changes.is_empty() {
                    stats.records_modified += 1;
                    
                    let diff = ZiCRecordDiff {
                        record_id: id.clone(),
                        diff_type: ZiCChangeType::Modified,
                        field_changes: field_changes.clone(),
                        similarity: record_similarity,
                    };
                    record_diffs.push(diff);

                    for change in field_changes {
                        if changes.len() >= self.config.max_changes {
                            break;
                        }
                        changes.push(change);
                    }
                } else {
                    stats.records_unchanged += 1;
                }
            }
        }

        let field_changes: Vec<ZiCFieldChange> = field_change_tracker
            .into_values()
            .map(|b| b.build(self.config.max_field_samples))
            .collect();

        stats.fields_added = field_changes.iter()
            .filter(|fc| fc.old_type.is_none() && fc.new_type.is_some())
            .count();
        stats.fields_removed = field_changes.iter()
            .filter(|fc| fc.old_type.is_some() && fc.new_type.is_none())
            .count();
        stats.fields_modified = field_changes.iter()
            .filter(|fc| fc.old_type.is_some() && fc.new_type.is_some() && fc.old_type != fc.new_type)
            .count();

        stats.similarity_score = self.calculate_similarity_score(&stats, old.len(), new.len());

        ZiCDiffReport {
            stats,
            changes,
            field_changes,
            record_diffs,
        }
    }

    fn diff_records(
        &self,
        old: &ZiCRecord,
        new: &ZiCRecord,
        changes: &mut Vec<ZiCDiffChange>,
        field_tracker: &mut HashMap<String, ZiCFieldChangeBuilder>,
    ) -> f64 {
        self.diff_values("payload", &old.payload, &new.payload, changes, field_tracker);

        if changes.is_empty() {
            1.0
        } else {
            let total_fields = self.count_fields(&old.payload) + self.count_fields(&new.payload);
            if total_fields == 0 {
                0.0
            } else {
                1.0 - (changes.len() as f64 / total_fields as f64 / 2.0)
            }
        }
    }

    fn diff_values(
        &self,
        path: &str,
        old: &Value,
        new: &Value,
        changes: &mut Vec<ZiCDiffChange>,
        field_tracker: &mut HashMap<String, ZiCFieldChangeBuilder>,
    ) {
        if self.config.ignore_fields.contains(path) {
            return;
        }

        if old == new {
            return;
        }

        match (old, new) {
            (Value::Object(old_map), Value::Object(new_map)) => {
                for (key, old_val) in old_map {
                    let new_path = format!("{}.{}", path, key);
                    if self.config.ignore_fields.contains(&new_path) {
                        continue;
                    }
                    if let Some(new_val) = new_map.get(key) {
                        self.diff_values(&new_path, old_val, new_val, changes, field_tracker);
                    } else {
                        self.track_field_change(&new_path, old_val, &Value::Null, field_tracker);
                        changes.push(ZiCDiffChange {
                            path: new_path,
                            old_value: Some(old_val.clone()),
                            new_value: None,
                            change_type: ZiCChangeType::Removed,
                            similarity: None,
                        });
                    }
                }
                for (key, new_val) in new_map {
                    let new_path = format!("{}.{}", path, key);
                    if self.config.ignore_fields.contains(&new_path) {
                        continue;
                    }
                    if !old_map.contains_key(key) {
                        self.track_field_change(&new_path, &Value::Null, new_val, field_tracker);
                        changes.push(ZiCDiffChange {
                            path: new_path,
                            old_value: None,
                            new_value: Some(new_val.clone()),
                            change_type: ZiCChangeType::Added,
                            similarity: None,
                        });
                    }
                }
            }
            (Value::Array(old_arr), Value::Array(new_arr)) => {
                if old_arr.len() != new_arr.len() {
                    let similarity = if self.config.compute_similarity {
                        Some(self.array_similarity(old_arr, new_arr))
                    } else {
                        None
                    };
                    changes.push(ZiCDiffChange {
                        path: path.to_string(),
                        old_value: Some(old.clone()),
                        new_value: Some(new.clone()),
                        change_type: ZiCChangeType::Modified,
                        similarity,
                    });
                } else {
                    for (i, (old_item, new_item)) in old_arr.iter().zip(new_arr.iter()).enumerate() {
                        let item_path = format!("{}[{}]", path, i);
                        self.diff_values(&item_path, old_item, new_item, changes, field_tracker);
                    }
                }
            }
            (Value::String(old_s), Value::String(new_s)) => {
                let similarity = if self.config.compute_similarity {
                    Some(self.string_similarity(old_s, new_s))
                } else {
                    None
                };
                
                self.track_field_change(path, old, new, field_tracker);
                changes.push(ZiCDiffChange {
                    path: path.to_string(),
                    old_value: Some(old.clone()),
                    new_value: Some(new.clone()),
                    change_type: ZiCChangeType::Modified,
                    similarity,
                });
            }
            _ => {
                self.track_field_change(path, old, new, field_tracker);
                changes.push(ZiCDiffChange {
                    path: path.to_string(),
                    old_value: Some(old.clone()),
                    new_value: Some(new.clone()),
                    change_type: ZiCChangeType::Modified,
                    similarity: None,
                });
            }
        }
    }

    fn track_field_change(
        &self,
        path: &str,
        old_value: &Value,
        new_value: &Value,
        tracker: &mut HashMap<String, ZiCFieldChangeBuilder>,
    ) {
        if !self.config.track_field_changes {
            return;
        }

        let builder = tracker.entry(path.to_string()).or_insert_with(|| {
            ZiCFieldChangeBuilder::new(path.to_string())
        });

        builder.old_type = Some(self.value_type(old_value));
        builder.new_type = Some(self.value_type(new_value));
        builder.change_count += 1;
        
        if builder.sample_old_values.len() < self.config.max_field_samples {
            if !old_value.is_null() {
                builder.sample_old_values.push(old_value.clone());
            }
        }
        if builder.sample_new_values.len() < self.config.max_field_samples {
            if !new_value.is_null() {
                builder.sample_new_values.push(new_value.clone());
            }
        }
    }

    fn value_type(&self, value: &Value) -> String {
        match value {
            Value::Null => "null".to_string(),
            Value::Bool(_) => "bool".to_string(),
            Value::Number(n) => {
                if n.is_i64() {
                    "integer".to_string()
                } else {
                    "float".to_string()
                }
            }
            Value::String(_) => "string".to_string(),
            Value::Array(_) => "array".to_string(),
            Value::Object(_) => "object".to_string(),
        }
    }

    fn count_fields(&self, value: &Value) -> usize {
        match value {
            Value::Object(map) => {
                map.len() + map.values().map(|v| self.count_fields(v)).sum::<usize>()
            }
            Value::Array(arr) => arr.iter().map(|v| self.count_fields(v)).sum(),
            _ => 1,
        }
    }

    fn string_similarity(&self, s1: &str, s2: &str) -> f64 {
        if s1 == s2 {
            return 1.0;
        }
        
        let len1 = s1.chars().count();
        let len2 = s2.chars().count();
        
        if len1 == 0 && len2 == 0 {
            return 1.0;
        }
        if len1 == 0 || len2 == 0 {
            return 0.0;
        }

        let distance = self.levenshtein_distance(s1, s2);
        1.0 - (distance as f64 / (len1.max(len2) as f64))
    }

    fn levenshtein_distance(&self, s1: &str, s2: &str) -> usize {
        let chars1: Vec<char> = s1.chars().collect();
        let chars2: Vec<char> = s2.chars().collect();
        
        let len1 = chars1.len();
        let len2 = chars2.len();

        if len1 == 0 {
            return len2;
        }
        if len2 == 0 {
            return len1;
        }

        let mut matrix = vec![vec![0; len2 + 1]; len1 + 1];

        for i in 0..=len1 {
            matrix[i][0] = i;
        }
        for j in 0..=len2 {
            matrix[0][j] = j;
        }

        for i in 1..=len1 {
            for j in 1..=len2 {
                let cost = if chars1[i - 1] == chars2[j - 1] { 0 } else { 1 };
                matrix[i][j] = (matrix[i - 1][j] + 1)
                    .min(matrix[i][j - 1] + 1)
                    .min(matrix[i - 1][j - 1] + cost);
            }
        }

        matrix[len1][len2]
    }

    fn array_similarity(&self, arr1: &[Value], arr2: &[Value]) -> f64 {
        if arr1.len() == 0 && arr2.len() == 0 {
            return 1.0;
        }

        let set1: HashSet<String> = arr1.iter().map(|v| v.to_string()).collect();
        let set2: HashSet<String> = arr2.iter().map(|v| v.to_string()).collect();

        let intersection = set1.intersection(&set2).count();
        let union = set1.union(&set2).count();

        if union == 0 {
            1.0
        } else {
            intersection as f64 / union as f64
        }
    }

    fn calculate_similarity_score(&self, stats: &ZiCDiffStats, old_count: usize, new_count: usize) -> f64 {
        if old_count == 0 && new_count == 0 {
            return 1.0;
        }

        let total = old_count.max(new_count);
        if total == 0 {
            return 1.0;
        }

        let unchanged_weight = stats.records_unchanged as f64 / total as f64;
        let modified_weight = stats.records_modified as f64 / total as f64 * 0.5;

        (unchanged_weight + modified_weight).min(1.0)
    }
}

struct ZiCFieldChangeBuilder {
    field_path: String,
    change_count: usize,
    old_type: Option<String>,
    new_type: Option<String>,
    sample_old_values: Vec<Value>,
    sample_new_values: Vec<Value>,
}

impl ZiCFieldChangeBuilder {
    fn new(field_path: String) -> Self {
        Self {
            field_path,
            change_count: 0,
            old_type: None,
            new_type: None,
            sample_old_values: Vec::new(),
            sample_new_values: Vec::new(),
        }
    }

    fn build(self, max_samples: usize) -> ZiCFieldChange {
        let mut sample_old = self.sample_old_values;
        sample_old.truncate(max_samples);
        let mut sample_new = self.sample_new_values;
        sample_new.truncate(max_samples);

        ZiCFieldChange {
            field_path: self.field_path,
            change_count: self.change_count,
            old_type: self.old_type,
            new_type: self.new_type,
            sample_old_values: sample_old,
            sample_new_values: sample_new,
        }
    }
}
