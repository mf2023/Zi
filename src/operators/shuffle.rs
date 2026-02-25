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

use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::operator::ZiOperator;
use crate::record::ZiRecordBatch;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiShuffleAlgorithm {
    FisherYates,
    Block { block_size: usize },
    Reservoir,
}

impl Default for ZiShuffleAlgorithm {
    fn default() -> Self {
        Self::FisherYates
    }
}

#[derive(Debug)]
pub struct ZiShuffle {
    seed: Option<u64>,
    algorithm: ZiShuffleAlgorithm,
}

impl ZiShuffle {
    #[allow(non_snake_case)]
    pub fn new(seed: Option<u64>, algorithm: ZiShuffleAlgorithm) -> Self {
        Self { seed, algorithm }
    }

    fn shuffle_fisher_yates(&self, batch: ZiRecordBatch) -> ZiRecordBatch {
        let mut rng = match self.seed {
            Some(s) => StdRng::seed_from_u64(s),
            None => StdRng::from_entropy(),
        };

        let mut result = batch;
        result.shuffle(&mut rng);
        result
    }

    fn shuffle_block(&self, batch: ZiRecordBatch, block_size: usize) -> ZiRecordBatch {
        let mut rng = match self.seed {
            Some(s) => StdRng::seed_from_u64(s),
            None => StdRng::from_entropy(),
        };

        let mut blocks: Vec<ZiRecordBatch> = batch
            .chunks(block_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        blocks.shuffle(&mut rng);

        for block in &mut blocks {
            block.shuffle(&mut rng);
        }

        blocks.into_iter().flatten().collect()
    }

    fn shuffle_reservoir(&self, batch: ZiRecordBatch) -> ZiRecordBatch {
        let mut rng = match self.seed {
            Some(s) => StdRng::seed_from_u64(s),
            None => StdRng::from_entropy(),
        };

        let n = batch.len();
        if n <= 1 {
            return batch;
        }

        let mut result = batch;
        for i in (1..n).rev() {
            let j = rng.gen_range(0..i + 1);
            result.swap(i, j);
        }

        result
    }
}

impl ZiOperator for ZiShuffle {
    fn name(&self) -> &'static str {
        "shuffle"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        if batch.is_empty() {
            return Ok(batch);
        }

        match &self.algorithm {
            ZiShuffleAlgorithm::FisherYates => Ok(self.shuffle_fisher_yates(batch)),
            ZiShuffleAlgorithm::Block { block_size } => {
                Ok(self.shuffle_block(batch, *block_size))
            }
            ZiShuffleAlgorithm::Reservoir => Ok(self.shuffle_reservoir(batch)),
        }
    }
}

#[allow(non_snake_case)]
pub fn shuffle_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("shuffle config must be object"))?;

    let seed = obj.get("seed").and_then(Value::as_u64);

    let algorithm = if let Some(block_size) = obj.get("block_size").and_then(Value::as_u64) {
        ZiShuffleAlgorithm::Block {
            block_size: block_size as usize,
        }
    } else if obj
        .get("algorithm")
        .and_then(Value::as_str)
        .map(|s| s == "reservoir")
        .unwrap_or(false)
    {
        ZiShuffleAlgorithm::Reservoir
    } else {
        ZiShuffleAlgorithm::FisherYates
    };

    Ok(Box::new(ZiShuffle::new(seed, algorithm)))
}

#[derive(Debug)]
pub struct ZiShuffleDeterministic {
    seed: u64,
}

impl ZiShuffleDeterministic {
    #[allow(non_snake_case)]
    pub fn new(seed: u64) -> Self {
        Self { seed }
    }
}

impl ZiOperator for ZiShuffleDeterministic {
    fn name(&self) -> &'static str {
        "shuffle.deterministic"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let mut rng = StdRng::seed_from_u64(self.seed);
        let mut result = batch;
        result.shuffle(&mut rng);
        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn shuffle_deterministic_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("shuffle.deterministic config must be object"))?;

    let seed = obj
        .get("seed")
        .and_then(Value::as_u64)
        .ok_or_else(|| ZiError::validation("shuffle.deterministic requires integer 'seed'"))?;

    Ok(Box::new(ZiShuffleDeterministic::new(seed)))
}

#[derive(Debug)]
pub struct ZiShuffleBlock {
    block_size: usize,
    seed: Option<u64>,
}

impl ZiShuffleBlock {
    #[allow(non_snake_case)]
    pub fn new(block_size: usize, seed: Option<u64>) -> Self {
        Self { block_size, seed }
    }
}

impl ZiOperator for ZiShuffleBlock {
    fn name(&self) -> &'static str {
        "shuffle.block"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let mut rng = match self.seed {
            Some(s) => StdRng::seed_from_u64(s),
            None => StdRng::from_entropy(),
        };

        let mut blocks: Vec<ZiRecordBatch> = batch
            .chunks(self.block_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        blocks.shuffle(&mut rng);

        for block in &mut blocks {
            block.shuffle(&mut rng);
        }

        Ok(blocks.into_iter().flatten().collect())
    }
}

#[allow(non_snake_case)]
pub fn shuffle_block_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("shuffle.block config must be object"))?;

    let block_size = obj
        .get("block_size")
        .and_then(Value::as_u64)
        .ok_or_else(|| ZiError::validation("shuffle.block requires integer 'block_size'"))?
        as usize;

    let seed = obj.get("seed").and_then(Value::as_u64);

    Ok(Box::new(ZiShuffleBlock::new(block_size, seed)))
}

#[derive(Debug)]
pub struct ZiShuffleStratified {
    field: String,
    seed: Option<u64>,
}

impl ZiShuffleStratified {
    #[allow(non_snake_case)]
    pub fn new(field: String, seed: Option<u64>) -> Self {
        Self { field, seed }
    }

    fn get_field_value(&self, record: &crate::record::ZiRecord) -> String {
        let parts: Vec<&str> = self.field.split('.').collect();
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

impl ZiOperator for ZiShuffleStratified {
    fn name(&self) -> &'static str {
        "shuffle.stratified"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        use std::collections::HashMap;

        if batch.is_empty() {
            return Ok(batch);
        }

        let mut rng = match self.seed {
            Some(s) => StdRng::seed_from_u64(s),
            None => StdRng::from_entropy(),
        };

        let mut groups: HashMap<String, ZiRecordBatch> = HashMap::new();
        for record in batch {
            let key = self.get_field_value(&record);
            groups.entry(key).or_default().push(record);
        }

        for (_, group) in &mut groups {
            group.shuffle(&mut rng);
        }

        let mut group_iters: Vec<_> = groups.into_values().map(|g| g.into_iter()).collect();
        let mut result = Vec::new();

        while !group_iters.is_empty() {
            group_iters.retain(|iter| iter.as_slice().len() > 0);
            
            let mut non_empty: Vec<_> = group_iters.iter_mut().filter(|g| g.len() > 0).collect();
            if non_empty.is_empty() {
                break;
            }

            non_empty.shuffle(&mut rng);

            for iter in non_empty {
                if let Some(record) = iter.next() {
                    result.push(record);
                }
            }
        }

        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn shuffle_stratified_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("shuffle.stratified config must be object"))?;

    let field = obj
        .get("field")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("shuffle.stratified requires string 'field'"))?
        .to_string();

    let seed = obj.get("seed").and_then(Value::as_u64);

    Ok(Box::new(ZiShuffleStratified::new(field, seed)))
}

#[derive(Debug)]
pub struct ZiShuffleWindow {
    window_size: usize,
    seed: Option<u64>,
}

impl ZiShuffleWindow {
    #[allow(non_snake_case)]
    pub fn new(window_size: usize, seed: Option<u64>) -> Self {
        Self { window_size, seed }
    }
}

impl ZiOperator for ZiShuffleWindow {
    fn name(&self) -> &'static str {
        "shuffle.window"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        if batch.is_empty() || self.window_size <= 1 {
            return Ok(batch);
        }

        let mut rng = match self.seed {
            Some(s) => StdRng::seed_from_u64(s),
            None => StdRng::from_entropy(),
        };

        let n = batch.len();
        let mut result = batch;

        for i in 0..n {
            let window_start = i.saturating_sub(self.window_size / 2);
            let window_end = (i + self.window_size / 2 + 1).min(n);
            let j = rng.gen_range(window_start..window_end);
            result.swap(i, j);
        }

        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn shuffle_window_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("shuffle.window config must be object"))?;

    let window_size = obj
        .get("window_size")
        .and_then(Value::as_u64)
        .ok_or_else(|| ZiError::validation("shuffle.window requires integer 'window_size'"))?
        as usize;

    let seed = obj.get("seed").and_then(Value::as_u64);

    Ok(Box::new(ZiShuffleWindow::new(window_size, seed)))
}
