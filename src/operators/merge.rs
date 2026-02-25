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

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::operator::ZiOperator;
use crate::record::{ZiRecord, ZiRecordBatch};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiMergeFieldMode {
    Align,
    Strict,
    Loose,
}

impl Default for ZiMergeFieldMode {
    fn default() -> Self {
        Self::Align
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiMergeConflictStrategy {
    First,
    Last,
    Concat,
    Error,
}

impl Default for ZiMergeConflictStrategy {
    fn default() -> Self {
        Self::First
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct ZiMergeConcat {
    field_mode: ZiMergeFieldMode,
}

impl ZiMergeConcat {
    #[allow(non_snake_case)]
    pub fn new(field_mode: ZiMergeFieldMode) -> Self {
        Self { field_mode }
    }
}

impl ZiOperator for ZiMergeConcat {
    fn name(&self) -> &'static str {
        "merge.concat"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        if batch.is_empty() {
            return Ok(batch);
        }

        match self.field_mode {
            ZiMergeFieldMode::Align => {
                let mut all_fields: HashSet<String> = HashSet::new();
                for record in &batch {
                    if let Value::Object(map) = &record.payload {
                        for key in map.keys() {
                            all_fields.insert(key.clone());
                        }
                    }
                }

                let mut result = Vec::with_capacity(batch.len());
                for mut record in batch {
                    if let Value::Object(ref mut map) = record.payload {
                        for field in &all_fields {
                            if !map.contains_key(field) {
                                map.insert(field.clone(), Value::Null);
                            }
                        }
                    }
                    result.push(record);
                }
                Ok(result)
            }
            ZiMergeFieldMode::Strict => {
                let first_fields: HashSet<String> = if let Some(first) = batch.first() {
                    if let Value::Object(map) = &first.payload {
                        map.keys().cloned().collect()
                    } else {
                        HashSet::new()
                    }
                } else {
                    HashSet::new()
                };

                for (i, record) in batch.iter().enumerate() {
                    if let Value::Object(map) = &record.payload {
                        let current_fields: HashSet<String> = map.keys().cloned().collect();
                        if current_fields != first_fields {
                            return Err(ZiError::validation(format!(
                                "field mismatch at record {}: expected {:?}, got {:?}",
                                i, first_fields, current_fields
                            )));
                        }
                    }
                }
                Ok(batch)
            }
            ZiMergeFieldMode::Loose => Ok(batch),
        }
    }
}

#[allow(non_snake_case)]
pub fn merge_concat_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let field_mode = config
        .get("field_mode")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();

    Ok(Box::new(ZiMergeConcat::new(field_mode)))
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct ZiMergeBatch {
    field_mode: ZiMergeFieldMode,
    conflict_strategy: ZiMergeConflictStrategy,
}

impl ZiMergeBatch {
    #[allow(non_snake_case)]
    pub fn new(
        field_mode: ZiMergeFieldMode,
        conflict_strategy: ZiMergeConflictStrategy,
    ) -> Self {
        Self {
            field_mode,
            conflict_strategy,
        }
    }

    fn merge_records(&self, records: Vec<ZiRecord>) -> Result<ZiRecord> {
        if records.is_empty() {
            return Err(ZiError::validation("cannot merge empty records"));
        }

        if records.len() == 1 {
            return Ok(records.into_iter().next().unwrap());
        }

        let first = &records[0];
        let mut merged_payload: serde_json::Map<String, Value> = match &first.payload {
            Value::Object(map) => map.clone(),
            _ => {
                let mut map = serde_json::Map::new();
                map.insert("value".to_string(), first.payload.clone());
                map
            }
        };

        let mut merged_metadata = first.metadata.clone().unwrap_or_default();

        for record in records.iter().skip(1) {
            if let Value::Object(map) = &record.payload {
                for (key, value) in map {
                    if merged_payload.contains_key(key) {
                        match &self.conflict_strategy {
                            ZiMergeConflictStrategy::First => {}
                            ZiMergeConflictStrategy::Last => {
                                merged_payload.insert(key.clone(), value.clone());
                            }
                            ZiMergeConflictStrategy::Concat => {
                                let existing = merged_payload.get(key).cloned().unwrap_or(Value::Null);
                                let combined = self.concat_values(&existing, value);
                                merged_payload.insert(key.clone(), combined);
                            }
                            ZiMergeConflictStrategy::Error => {
                                return Err(ZiError::validation(format!(
                                    "field conflict: {}",
                                    key
                                )));
                            }
                        }
                    } else {
                        merged_payload.insert(key.clone(), value.clone());
                    }
                }
            }

            if let Some(meta) = &record.metadata {
                for (key, value) in meta {
                    if !merged_metadata.contains_key(key) {
                        merged_metadata.insert(key.clone(), value.clone());
                    }
                }
            }
        }

        Ok(ZiRecord::new(first.id.clone(), Value::Object(merged_payload))
            .with_metadata(merged_metadata))
    }

    fn concat_values(&self, a: &Value, b: &Value) -> Value {
        match (a, b) {
            (Value::String(s1), Value::String(s2)) => {
                Value::String(format!("{} {}", s1, s2))
            }
            (Value::Array(arr1), Value::Array(arr2)) => {
                let mut combined = arr1.clone();
                combined.extend(arr2.clone());
                Value::Array(combined)
            }
            (Value::Array(arr), Value::String(s)) => {
                let mut combined = arr.clone();
                combined.push(Value::String(s.clone()));
                Value::Array(combined)
            }
            (Value::String(s), Value::Array(arr)) => {
                let mut combined = vec![Value::String(s.clone())];
                combined.extend(arr.clone());
                Value::Array(combined)
            }
            _ => b.clone(),
        }
    }
}

impl ZiOperator for ZiMergeBatch {
    fn name(&self) -> &'static str {
        "merge.batch"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let mut groups: HashMap<String, Vec<ZiRecord>> = HashMap::new();

        for record in batch {
            let key = record.id.clone().unwrap_or_else(|| {
                format!("{:?}", blake3::hash(
                    serde_json::to_string(&record.payload).unwrap_or_default().as_bytes()
                ))
            });
            groups.entry(key).or_default().push(record);
        }

        let mut result = Vec::with_capacity(groups.len());
        for (_, records) in groups {
            result.push(self.merge_records(records)?);
        }

        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn merge_batch_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let field_mode = config
        .get("field_mode")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();

    let conflict_strategy = config
        .get("conflict_strategy")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();

    Ok(Box::new(ZiMergeBatch::new(field_mode, conflict_strategy)))
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct ZiMergeUnion {
    key_field: Option<String>,
    seed: u64,
}

impl ZiMergeUnion {
    #[allow(non_snake_case)]
    pub fn new(key_field: Option<String>, seed: u64) -> Self {
        Self { key_field, seed }
    }

    fn compute_hash(&self, record: &ZiRecord) -> String {
        if let Some(ref key_field) = self.key_field {
            let parts: Vec<&str> = key_field.split('.').collect();
            if parts.len() >= 2 {
                let mut current = &record.payload;
                for part in &parts[1..] {
                    if let Value::Object(map) = current {
                        current = map.get(*part).unwrap_or(&Value::Null);
                    } else {
                        break;
                    }
                }
                return format!("{:?}", blake3::hash(
                    current.to_string().as_bytes()
                ));
            }
        }

        format!("{:?}", blake3::hash(
            serde_json::to_string(&record.payload).unwrap_or_default().as_bytes()
        ))
    }
}

impl ZiOperator for ZiMergeUnion {
    fn name(&self) -> &'static str {
        "merge.union"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        let mut seen: HashSet<String> = HashSet::new();
        let mut result = Vec::new();

        for record in batch {
            let hash = self.compute_hash(&record);
            if !seen.contains(&hash) {
                seen.insert(hash);
                result.push(record);
            }
        }

        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn merge_union_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let key_field = config
        .get("key_field")
        .and_then(Value::as_str)
        .map(|s| s.to_string());

    let seed = config
        .get("seed")
        .and_then(Value::as_u64)
        .unwrap_or(0xCAFEBABE);

    Ok(Box::new(ZiMergeUnion::new(key_field, seed)))
}

#[derive(Debug)]
pub struct ZiMergeIntersect {
    key_field: Option<String>,
}

impl ZiMergeIntersect {
    #[allow(non_snake_case)]
    pub fn new(key_field: Option<String>) -> Self {
        Self { key_field }
    }

    fn compute_hash(&self, record: &ZiRecord) -> String {
        if let Some(ref key_field) = self.key_field {
            let parts: Vec<&str> = key_field.split('.').collect();
            if parts.len() >= 2 {
                let mut current = &record.payload;
                for part in &parts[1..] {
                    if let Value::Object(map) = current {
                        current = map.get(*part).unwrap_or(&Value::Null);
                    } else {
                        break;
                    }
                }
                return format!("{:?}", blake3::hash(
                    current.to_string().as_bytes()
                ));
            }
        }

        format!("{:?}", blake3::hash(
            serde_json::to_string(&record.payload).unwrap_or_default().as_bytes()
        ))
    }
}

impl ZiOperator for ZiMergeIntersect {
    fn name(&self) -> &'static str {
        "merge.intersect"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        let mut hash_counts: HashMap<String, usize> = HashMap::new();
        let mut hash_to_record: HashMap<String, ZiRecord> = HashMap::new();

        for record in batch {
            let hash = self.compute_hash(&record);
            *hash_counts.entry(hash.clone()).or_insert(0) += 1;
            hash_to_record.entry(hash).or_insert(record);
        }

        let result: ZiRecordBatch = hash_counts
            .into_iter()
            .filter(|(_, count)| *count > 1)
            .filter_map(|(hash, _)| hash_to_record.remove(&hash))
            .collect();

        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn merge_intersect_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let key_field = config
        .get("key_field")
        .and_then(Value::as_str)
        .map(|s| s.to_string());

    Ok(Box::new(ZiMergeIntersect::new(key_field)))
}

#[derive(Debug)]
pub struct ZiMergeDifference {
    key_field: Option<String>,
}

impl ZiMergeDifference {
    #[allow(non_snake_case)]
    pub fn new(key_field: Option<String>) -> Self {
        Self { key_field }
    }

    fn compute_hash(&self, record: &ZiRecord) -> String {
        if let Some(ref key_field) = self.key_field {
            let parts: Vec<&str> = key_field.split('.').collect();
            if parts.len() >= 2 {
                let mut current = &record.payload;
                for part in &parts[1..] {
                    if let Value::Object(map) = current {
                        current = map.get(*part).unwrap_or(&Value::Null);
                    } else {
                        break;
                    }
                }
                return format!("{:?}", blake3::hash(
                    current.to_string().as_bytes()
                ));
            }
        }

        format!("{:?}", blake3::hash(
            serde_json::to_string(&record.payload).unwrap_or_default().as_bytes()
        ))
    }
}

impl ZiOperator for ZiMergeDifference {
    fn name(&self) -> &'static str {
        "merge.difference"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        let mut hash_counts: HashMap<String, usize> = HashMap::new();
        let mut hash_to_record: HashMap<String, ZiRecord> = HashMap::new();

        for record in batch {
            let hash = self.compute_hash(&record);
            *hash_counts.entry(hash.clone()).or_insert(0) += 1;
            hash_to_record.entry(hash).or_insert(record);
        }

        let result: ZiRecordBatch = hash_counts
            .into_iter()
            .filter(|(_, count)| *count == 1)
            .filter_map(|(hash, _)| hash_to_record.remove(&hash))
            .collect();

        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn merge_difference_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let key_field = config
        .get("key_field")
        .and_then(Value::as_str)
        .map(|s| s.to_string());

    Ok(Box::new(ZiMergeDifference::new(key_field)))
}

#[derive(Debug)]
pub struct ZiMergeZip {
    fill_missing: bool,
    default_value: Value,
}

impl ZiMergeZip {
    #[allow(non_snake_case)]
    pub fn new(fill_missing: bool, default_value: Value) -> Self {
        Self {
            fill_missing,
            default_value,
        }
    }
}

impl ZiOperator for ZiMergeZip {
    fn name(&self) -> &'static str {
        "merge.zip"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let mut field_values: HashMap<String, Vec<Value>> = HashMap::new();
        let mut all_fields: HashSet<String> = HashSet::new();

        for record in &batch {
            if let Value::Object(map) = &record.payload {
                for key in map.keys() {
                    all_fields.insert(key.clone());
                }
            }
        }

        for record in &batch {
            if let Value::Object(map) = &record.payload {
                for field in &all_fields {
                    let value = map.get(field).cloned().unwrap_or_else(|| {
                        if self.fill_missing {
                            self.default_value.clone()
                        } else {
                            Value::Null
                        }
                    });
                    field_values.entry(field.clone()).or_default().push(value);
                }
            }
        }

        let mut result = Vec::new();
        let mut first_record = batch.into_iter().next().unwrap();

        let mut new_payload = serde_json::Map::new();
        for (field, values) in field_values {
            new_payload.insert(field, Value::Array(values));
        }

        first_record.payload = Value::Object(new_payload);
        result.push(first_record);

        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn merge_zip_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let fill_missing = config
        .get("fill_missing")
        .and_then(Value::as_bool)
        .unwrap_or(true);

    let default_value = config
        .get("default_value")
        .cloned()
        .unwrap_or(Value::Null);

    Ok(Box::new(ZiMergeZip::new(fill_missing, default_value)))
}

pub fn merge_batches(batches: Vec<ZiRecordBatch>) -> ZiRecordBatch {
    batches.into_iter().flatten().collect()
}
