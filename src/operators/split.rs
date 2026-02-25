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

use rand::seq::SliceRandom;
use rand::SeedableRng;
use rand::rngs::StdRng;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::operator::ZiOperator;
use crate::record::{ZiRecord, ZiRecordBatch};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiSplitConfig {
    pub ratios: Vec<f64>,
    pub names: Vec<String>,
    pub seed: u64,
}

impl ZiSplitConfig {
    #[allow(non_snake_case)]
    pub fn new(ratios: Vec<f64>, names: Vec<String>, seed: u64) -> Result<Self> {
        if ratios.is_empty() {
            return Err(ZiError::validation("split ratios cannot be empty"));
        }

        if ratios.len() != names.len() {
            return Err(ZiError::validation(
                "split ratios and names must have the same length",
            ));
        }

        let sum: f64 = ratios.iter().sum();
        if (sum - 1.0).abs() > 0.001 {
            return Err(ZiError::validation(format!(
                "split ratios must sum to 1.0, got {}",
                sum
            )));
        }

        for (i, ratio) in ratios.iter().enumerate() {
            if *ratio < 0.0 {
                return Err(ZiError::validation(format!(
                    "split ratio at index {} is negative: {}",
                    i, ratio
                )));
            }
        }

        Ok(Self { ratios, names, seed })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZiSplitResult {
    pub splits: HashMap<String, ZiRecordBatch>,
    pub counts: HashMap<String, usize>,
    pub total: usize,
}

impl ZiSplitResult {
    pub fn new() -> Self {
        Self {
            splits: HashMap::new(),
            counts: HashMap::new(),
            total: 0,
        }
    }

    pub fn add(&mut self, name: String, batch: ZiRecordBatch) {
        let count = batch.len();
        self.counts.insert(name.clone(), count);
        self.total += count;
        self.splits.insert(name, batch);
    }

    pub fn get(&self, name: &str) -> Option<&ZiRecordBatch> {
        self.splits.get(name)
    }

    pub fn into_batches(self) -> Vec<(String, ZiRecordBatch)> {
        self.splits.into_iter().collect()
    }
}

#[derive(Debug)]
pub struct ZiSplitRandom {
    config: ZiSplitConfig,
}

impl ZiSplitRandom {
    #[allow(non_snake_case)]
    pub fn new(config: ZiSplitConfig) -> Self {
        Self { config }
    }
}

impl ZiOperator for ZiSplitRandom {
    fn name(&self) -> &'static str {
        "split.random"
    }

    fn apply(&self, mut batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let mut rng = StdRng::seed_from_u64(self.config.seed);
        batch.shuffle(&mut rng);

        let total = batch.len();
        let mut result = ZiSplitResult::new();
        let mut start = 0;

        for (i, ratio) in self.config.ratios.iter().enumerate() {
            let count = if i == self.config.ratios.len() - 1 {
                total - start
            } else {
                ((total as f64) * ratio).round() as usize
            };

            let end = (start + count).min(total);
            let split_batch: ZiRecordBatch = batch[start..end].to_vec();

            for record in &split_batch {
                let mut new_record = record.clone();
                let meta = new_record.metadata_mut();
                meta.insert(
                    "split".to_string(),
                    Value::String(self.config.names[i].clone()),
                );
            }

            result.add(self.config.names[i].clone(), split_batch);
            start = end;
        }

        for (_, split_batch) in result.splits {
            batch.extend(split_batch);
        }

        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn split_random_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("split.random config must be object"))?;

    let (ratios, names) = if let Some(test_size) = obj.get("test_size").and_then(Value::as_f64) {
        let train_size = 1.0 - test_size;
        (vec![train_size, test_size], vec!["train".to_string(), "test".to_string()])
    } else {
        let ratios = obj
            .get("ratios")
            .and_then(Value::as_array)
            .ok_or_else(|| ZiError::validation("split.random requires array 'ratios' or 'test_size'"))?
            .iter()
            .map(|v| v.as_f64().ok_or_else(|| ZiError::validation("ratios must be numbers")))
            .collect::<Result<Vec<_>>>()?;

        let names = obj
            .get("names")
            .and_then(Value::as_array)
            .ok_or_else(|| ZiError::validation("split.random requires array 'names'"))?
            .iter()
            .map(|v| v.as_str().ok_or_else(|| ZiError::validation("names must be strings")))
            .map(|r| r.map(|s| s.to_string()))
            .collect::<Result<Vec<_>>>()?;
        (ratios, names)
    };

    let seed = obj
        .get("seed")
        .and_then(Value::as_u64)
        .unwrap_or(42);

    let split_config = ZiSplitConfig::new(ratios, names, seed)?;
    Ok(Box::new(ZiSplitRandom::new(split_config)))
}

#[derive(Debug)]
pub struct ZiSplitStratified {
    config: ZiSplitConfig,
    stratify_field: String,
}

impl ZiSplitStratified {
    #[allow(non_snake_case)]
    pub fn new(config: ZiSplitConfig, stratify_field: String) -> Self {
        Self {
            config,
            stratify_field,
        }
    }

    fn get_field_value(&self, record: &ZiRecord) -> String {
        let parts: Vec<&str> = self.stratify_field.split('.').collect();
        if parts.len() < 2 {
            return String::new();
        }

        let mut current = &record.payload;
        for part in &parts[1..] {
            if let Value::Object(map) = current {
                current = map.get(*part).unwrap_or(&Value::Null);
            } else {
                break;
            }
        }

        match current {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            _ => String::new(),
        }
    }
}

impl ZiOperator for ZiSplitStratified {
    fn name(&self) -> &'static str {
        "split.stratified"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let mut groups: HashMap<String, ZiRecordBatch> = HashMap::new();
        for record in batch {
            let key = self.get_field_value(&record);
            groups.entry(key).or_default().push(record);
        }

        let mut result_splits: HashMap<String, ZiRecordBatch> = HashMap::new();
        for name in &self.config.names {
            result_splits.insert(name.clone(), Vec::new());
        }

        let mut rng = StdRng::seed_from_u64(self.config.seed);

        for (_, mut group) in groups {
            group.shuffle(&mut rng);

            let total = group.len();
            let mut start = 0;

            for (i, ratio) in self.config.ratios.iter().enumerate() {
                let count = if i == self.config.ratios.len() - 1 {
                    total - start
                } else {
                    ((total as f64) * ratio).round() as usize
                };

                let end = (start + count).min(total);
                let split_batch: ZiRecordBatch = group[start..end].to_vec();

                for record in &split_batch {
                    let mut new_record = record.clone();
                    let meta = new_record.metadata_mut();
                    meta.insert(
                        "split".to_string(),
                        Value::String(self.config.names[i].clone()),
                    );
                }

                if let Some(split) = result_splits.get_mut(&self.config.names[i]) {
                    split.extend(split_batch);
                }

                start = end;
            }
        }

        let mut result = Vec::new();
        for (_, split_batch) in result_splits {
            result.extend(split_batch);
        }

        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn split_stratified_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("split.stratified config must be object"))?;

    let ratios = obj
        .get("ratios")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("split.stratified requires array 'ratios'"))?
        .iter()
        .map(|v| v.as_f64().ok_or_else(|| ZiError::validation("ratios must be numbers")))
        .collect::<Result<Vec<_>>>()?;

    let names = obj
        .get("names")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("split.stratified requires array 'names'"))?
        .iter()
        .map(|v| v.as_str().ok_or_else(|| ZiError::validation("names must be strings")))
        .map(|r| r.map(|s| s.to_string()))
        .collect::<Result<Vec<_>>>()?;

    let seed = obj
        .get("seed")
        .and_then(Value::as_u64)
        .unwrap_or(42);

    let stratify_field = obj
        .get("stratify_field")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("split.stratified requires string 'stratify_field'"))?
        .to_string();

    let split_config = ZiSplitConfig::new(ratios, names, seed)?;
    Ok(Box::new(ZiSplitStratified::new(split_config, stratify_field)))
}

#[derive(Debug)]
pub struct ZiSplitSequential {
    config: ZiSplitConfig,
}

impl ZiSplitSequential {
    #[allow(non_snake_case)]
    pub fn new(config: ZiSplitConfig) -> Self {
        Self { config }
    }
}

impl ZiOperator for ZiSplitSequential {
    fn name(&self) -> &'static str {
        "split.sequential"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let total = batch.len();
        let mut result = Vec::new();
        let mut start = 0;

        for (i, ratio) in self.config.ratios.iter().enumerate() {
            let count = if i == self.config.ratios.len() - 1 {
                total - start
            } else {
                ((total as f64) * ratio).round() as usize
            };

            let end = (start + count).min(total);
            let split_batch: ZiRecordBatch = batch[start..end].to_vec();

            for mut record in split_batch {
                let meta = record.metadata_mut();
                meta.insert(
                    "split".to_string(),
                    Value::String(self.config.names[i].clone()),
                );
                result.push(record);
            }

            start = end;
        }

        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn split_sequential_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("split.sequential config must be object"))?;

    let ratios = obj
        .get("ratios")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("split.sequential requires array 'ratios'"))?
        .iter()
        .map(|v| v.as_f64().ok_or_else(|| ZiError::validation("ratios must be numbers")))
        .collect::<Result<Vec<_>>>()?;

    let names = obj
        .get("names")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("split.sequential requires array 'names'"))?
        .iter()
        .map(|v| v.as_str().ok_or_else(|| ZiError::validation("names must be strings")))
        .map(|r| r.map(|s| s.to_string()))
        .collect::<Result<Vec<_>>>()?;

    let seed = obj
        .get("seed")
        .and_then(Value::as_u64)
        .unwrap_or(42);

    let split_config = ZiSplitConfig::new(ratios, names, seed)?;
    Ok(Box::new(ZiSplitSequential::new(split_config)))
}

#[derive(Debug)]
pub struct ZiSplitKFold {
    k: usize,
    seed: u64,
}

impl ZiSplitKFold {
    #[allow(non_snake_case)]
    pub fn new(k: usize, seed: u64) -> Result<Self> {
        if k < 2 {
            return Err(ZiError::validation("k-fold requires k >= 2"));
        }
        Ok(Self { k, seed })
    }
}

impl ZiOperator for ZiSplitKFold {
    fn name(&self) -> &'static str {
        "split.kfold"
    }

    fn apply(&self, mut batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let mut rng = StdRng::seed_from_u64(self.seed);
        batch.shuffle(&mut rng);

        let fold_size = batch.len() / self.k;
        let remainder = batch.len() % self.k;

        let mut result = Vec::new();
        let mut start = 0;

        for fold in 0..self.k {
            let extra = if fold < remainder { 1 } else { 0 };
            let end = start + fold_size + extra;

            for (idx, record) in batch[start..end].iter().enumerate() {
                let mut new_record = record.clone();
                let meta = new_record.metadata_mut();
                meta.insert("fold".to_string(), Value::Number(fold.into()));
                meta.insert(
                    "fold_index".to_string(),
                    Value::Number(idx.into()),
                );
                result.push(new_record);
            }

            start = end;
        }

        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn split_k_fold_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("split.kfold config must be object"))?;

    let k = obj
        .get("k")
        .and_then(Value::as_u64)
        .ok_or_else(|| ZiError::validation("split.kfold requires integer 'k'"))?
        as usize;

    let seed = obj
        .get("seed")
        .and_then(Value::as_u64)
        .unwrap_or(42);

    Ok(Box::new(ZiSplitKFold::new(k, seed)?))
}

#[derive(Debug)]
pub struct ZiSplitChunk {
    chunk_size: usize,
}

impl ZiSplitChunk {
    #[allow(non_snake_case)]
    pub fn new(chunk_size: usize) -> Result<Self> {
        if chunk_size == 0 {
            return Err(ZiError::validation("chunk_size must be positive"));
        }
        Ok(Self { chunk_size })
    }
}

impl ZiOperator for ZiSplitChunk {
    fn name(&self) -> &'static str {
        "split.chunk"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let mut result = Vec::new();
        let num_chunks = (batch.len() + self.chunk_size - 1) / self.chunk_size;

        for (chunk_idx, chunk) in batch.chunks(self.chunk_size).enumerate() {
            for record in chunk {
                let mut new_record = record.clone();
                let meta = new_record.metadata_mut();
                meta.insert("chunk".to_string(), Value::Number(chunk_idx.into()));
                meta.insert(
                    "total_chunks".to_string(),
                    Value::Number(num_chunks.into()),
                );
                result.push(new_record);
            }
        }

        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn split_chunk_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("split.chunk config must be object"))?;

    let chunk_size = obj
        .get("chunk_size")
        .and_then(Value::as_u64)
        .ok_or_else(|| ZiError::validation("split.chunk requires integer 'chunk_size'"))?
        as usize;

    Ok(Box::new(ZiSplitChunk::new(chunk_size)?))
}

#[allow(non_snake_case)]
pub fn split_into_batches(
    batch: &ZiRecordBatch,
    ratios: &[f64],
    names: &[String],
    seed: u64,
) -> Result<HashMap<String, ZiRecordBatch>> {
    if batch.is_empty() {
        return Ok(HashMap::new());
    }

    let config = ZiSplitConfig::new(ratios.to_vec(), names.to_vec(), seed)?;
    let mut rng = StdRng::seed_from_u64(config.seed);

    let mut shuffled = batch.clone();
    shuffled.shuffle(&mut rng);

    let total = shuffled.len();
    let mut result: HashMap<String, ZiRecordBatch> = HashMap::new();
    let mut start = 0;

    for (i, ratio) in config.ratios.iter().enumerate() {
        let count = if i == config.ratios.len() - 1 {
            total - start
        } else {
            ((total as f64) * ratio).round() as usize
        };

        let end = (start + count).min(total);
        let split_batch: ZiRecordBatch = shuffled[start..end].to_vec();
        result.insert(config.names[i].clone(), split_batch);
        start = end;
    }

    Ok(result)
}
