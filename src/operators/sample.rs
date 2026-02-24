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

use std::collections::HashMap;

use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::operator::ZiOperator;
use crate::record::{ZiRecord, ZiRecordBatch};

fn _sample_stable_hash(record: &ZiRecord, seed: u64) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    record.id.hash(&mut hasher);
    serde_json::to_string(&record.payload)
        .unwrap_or_default()
        .hash(&mut hasher);
    if let Some(metadata) = &record.metadata {
        serde_json::to_string(metadata)
            .unwrap_or_default()
            .hash(&mut hasher);
    }
    hasher.finish()
}

fn _value_to_group_key(value: &Value) -> String {
    match value {
        Value::String(s) => s.to_ascii_lowercase(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        Value::Array(_) | Value::Object(_) => {
            serde_json::to_string(value).unwrap_or_else(|_| "other".into())
        }
    }
}

#[derive(Debug)]
pub struct ZiSampleRandom {
    ratio: Option<f64>,
    count: Option<usize>,
    seed: u64,
    weight_key: Option<String>,
    group_key: Option<String>,
    min_per_group: Option<usize>,
}

impl ZiSampleRandom {
    #[allow(non_snake_case)]
    pub fn new(
        ratio: Option<f64>,
        count: Option<usize>,
        seed: u64,
        weight_key: Option<String>,
        group_key: Option<String>,
        min_per_group: Option<usize>,
    ) -> Self {
        Self {
            ratio,
            count,
            seed,
            weight_key,
            group_key,
            min_per_group,
        }
    }
}

impl ZiOperator for ZiSampleRandom {
    fn name(&self) -> &'static str {
        "sample.random"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        if self.ratio.is_none() && self.count.is_none() {
            return Err(ZiError::validation(
                "sample.random requires 'ratio' or 'count'",
            ));
        }

        if let Some(r) = self.ratio {
            if !(0.0..=1.0).contains(&r) {
                return Err(ZiError::validation(
                    "sample.random 'ratio' must be in [0,1]",
                ));
            }
        }

        if self.min_per_group.is_some() && self.group_key.is_none() {
            return Err(ZiError::validation(
                "sample.random 'min_per_group' requires 'group_key'",
            ));
        }

        let mut grouped: HashMap<String, Vec<(f64, u64, ZiRecord)>> = HashMap::new();
        let mut total_records = 0usize;

        for record in batch.into_iter() {
            let base_hash = _sample_stable_hash(&record, self.seed);
            let unit = ((base_hash as f64) + 1.0) / ((u64::MAX as f64) + 2.0);
            let random_unit = unit.clamp(f64::MIN_POSITIVE, 1.0);

            let weight = if let Some(key) = &self.weight_key {
                record
                    .metadata
                    .as_ref()
                    .and_then(|m| m.get(key))
                    .and_then(Value::as_f64)
                    .filter(|w| *w > 0.0)
                    .unwrap_or(1.0)
            } else {
                1.0
            };

            if weight <= 0.0 {
                continue;
            }

            let score = if self.weight_key.is_some() {
                random_unit.powf(1.0 / weight)
            } else {
                random_unit
            };

            if !score.is_finite() {
                continue;
            }

            let group = if let Some(key) = &self.group_key {
                record
                    .metadata
                    .as_ref()
                    .and_then(|m| m.get(key))
                    .map(_value_to_group_key)
                    .unwrap_or_else(|| "__missing__".to_string())
            } else {
                "__default__".to_string()
            };

            grouped
                .entry(group)
                .or_default()
                .push((score, base_hash, record));
            total_records += 1;
        }

        if total_records == 0 {
            return Ok(Vec::new());
        }

        let mut target = self.count.unwrap_or(total_records);
        let ratio_target = self.ratio.map(|ratio| {
            if ratio <= 0.0 {
                0
            } else {
                let raw = (total_records as f64) * ratio;
                raw.max(1.0).round() as usize
            }
        });

        if let Some(ratio_target) = ratio_target {
            if self.count.is_some() {
                target = target.max(ratio_target);
            } else {
                target = ratio_target;
            }
        }

        target = target.min(total_records);

        if target == 0 {
            return Ok(Vec::new());
        }

        if self.group_key.is_none() {
            let mut combined: Vec<(f64, u64, ZiRecord)> =
                grouped.into_values().flatten().collect();
            combined.sort_by(|a, b| {
                b.0.partial_cmp(&a.0)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| b.1.cmp(&a.1))
            });
            return Ok(combined
                .into_iter()
                .take(target)
                .map(|(_, _, record)| record)
                .collect());
        }

        let mut groups: Vec<(String, Vec<(f64, u64, ZiRecord)>)> = grouped.into_iter().collect();
        groups.sort_by(|a, b| a.0.cmp(&b.0));

        for (_, items) in groups.iter_mut() {
            items.sort_by(|a, b| {
                b.0.partial_cmp(&a.0)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| b.1.cmp(&a.1))
            });
        }

        let mut selected: Vec<ZiRecord> = Vec::with_capacity(target);
        let mut remaining = target;
        let min_each = self.min_per_group.unwrap_or(0);

        if min_each > 0 {
            for (_, items) in groups.iter_mut() {
                if remaining == 0 {
                    break;
                }
                if items.is_empty() {
                    continue;
                }
                let take = min_each.min(items.len()).min(remaining);
                if take > 0 {
                    let drained: Vec<_> = items.drain(..take).collect();
                    remaining -= drained.len();
                    selected.extend(drained.into_iter().map(|(_, _, record)| record));
                }
            }
        }

        if remaining > 0 {
            let mut leftovers: Vec<(f64, u64, ZiRecord)> = groups
                .into_iter()
                .flat_map(|(_, items)| items.into_iter())
                .collect();
            leftovers.sort_by(|a, b| {
                b.0.partial_cmp(&a.0)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| b.1.cmp(&a.1))
            });
            selected.extend(
                leftovers
                    .into_iter()
                    .take(remaining)
                    .map(|(_, _, record)| record),
            );
        }

        selected.truncate(target);
        Ok(selected)
    }
}

#[allow(non_snake_case)]
pub fn sample_random_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("sample.random config must be object"))?;
    let ratio = obj.get("ratio").and_then(Value::as_f64);
    let count = obj.get("count").and_then(Value::as_u64).map(|v| v as usize);
    let seed = obj
        .get("seed")
        .and_then(Value::as_u64)
        .unwrap_or(0xCAFEBABE);
    let weight_key = obj
        .get("weight_key")
        .and_then(Value::as_str)
        .map(|s| s.to_string());
    let group_key = obj
        .get("group_key")
        .and_then(Value::as_str)
        .map(|s| s.to_string());
    let min_per_group = obj
        .get("min_per_group")
        .and_then(Value::as_u64)
        .map(|v| v as usize);
    Ok(Box::new(ZiSampleRandom::new(
        ratio,
        count,
        seed,
        weight_key,
        group_key,
        min_per_group,
    )))
}

#[derive(Debug)]
pub struct ZiSampleTop {
    key: String,
    count: usize,
}

impl ZiSampleTop {
    #[allow(non_snake_case)]
    pub fn new(key: String, count: usize) -> Self {
        Self { key, count }
    }
}

impl ZiOperator for ZiSampleTop {
    fn name(&self) -> &'static str {
        "sample.top"
    }

    fn apply(&self, mut batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        batch.sort_by(|a, b| {
            let av = a
                .metadata
                .as_ref()
                .and_then(|m| m.get(&self.key))
                .and_then(Value::as_f64)
                .unwrap_or(f64::MIN);
            let bv = b
                .metadata
                .as_ref()
                .and_then(|m| m.get(&self.key))
                .and_then(Value::as_f64)
                .unwrap_or(f64::MIN);
            bv.partial_cmp(&av).unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(batch.into_iter().take(self.count).collect())
    }
}

#[allow(non_snake_case)]
pub fn sample_top_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("sample.top config must be object"))?;
    let key = obj
        .get("key")
        .and_then(Value::as_str)
        .or_else(|| obj.get("path").and_then(Value::as_str))
        .unwrap_or("quality")
        .to_string();
    let count = obj
        .get("count")
        .and_then(Value::as_u64)
        .or_else(|| obj.get("n").and_then(Value::as_u64))
        .ok_or_else(|| ZiError::validation("sample.top requires unsigned integer 'count' or 'n'"))?
        as usize;
    Ok(Box::new(ZiSampleTop::new(key, count)))
}

#[derive(Debug, Clone)]
pub enum ZiBalanceStrategy {
    Undersample,
    Oversample,
    Hybrid,
}

#[derive(Debug)]
pub struct ZiSampleBalanced {
    label_field: String,
    max_per_class: Option<usize>,
    min_per_class: Option<usize>,
    strategy: ZiBalanceStrategy,
    seed: u64,
}

impl ZiSampleBalanced {
    #[allow(non_snake_case)]
    pub fn new(
        label_field: String,
        max_per_class: Option<usize>,
        min_per_class: Option<usize>,
        strategy: ZiBalanceStrategy,
        seed: u64,
    ) -> Self {
        Self {
            label_field,
            max_per_class,
            min_per_class,
            strategy,
            seed,
        }
    }

    fn get_label(&self, record: &ZiRecord) -> String {
        let parts: Vec<&str> = self.label_field.split('.').collect();
        if parts.len() < 2 {
            return "unknown".to_string();
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
            _ => "unknown".to_string(),
        }
    }
}

impl ZiOperator for ZiSampleBalanced {
    fn name(&self) -> &'static str {
        "sample.balanced"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        use rand::seq::SliceRandom;
        use rand::SeedableRng;
        use rand::rngs::StdRng;

        if batch.is_empty() {
            return Ok(batch);
        }

        let mut rng = StdRng::seed_from_u64(self.seed);

        let mut groups: HashMap<String, ZiRecordBatch> = HashMap::new();
        for record in batch {
            let label = self.get_label(&record);
            groups.entry(label).or_default().push(record);
        }

        for (_, group) in groups.iter_mut() {
            group.shuffle(&mut rng);
        }

        let class_counts: Vec<usize> = groups.values().map(|g| g.len()).collect();
        let min_count = class_counts.iter().min().copied().unwrap_or(0);
        let max_count = class_counts.iter().max().copied().unwrap_or(0);

        let target_per_class = match &self.strategy {
            ZiBalanceStrategy::Undersample => {
                self.max_per_class.unwrap_or(min_count).min(min_count)
            }
            ZiBalanceStrategy::Oversample => {
                self.min_per_class.unwrap_or(max_count).max(max_count)
            }
            ZiBalanceStrategy::Hybrid => {
                let target = (min_count + max_count) / 2;
                if let Some(max) = self.max_per_class {
                    target.min(max)
                } else if let Some(min) = self.min_per_class {
                    target.max(min)
                } else {
                    target
                }
            }
        };

        let mut result = Vec::new();

        for (_, group) in groups {
            let count = group.len();

            if count >= target_per_class {
                let taken: Vec<_> = group.into_iter().take(target_per_class).collect();
                result.extend(taken);
            } else {
                result.extend(group.clone());

                let needed = target_per_class - count;
                for _ in 0..needed {
                    if let Some(record) = group.choose(&mut rng) {
                        result.push(record.clone());
                    }
                }
            }
        }

        result.shuffle(&mut rng);
        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn sample_balanced_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("sample.balanced config must be object"))?;

    let label_field = obj
        .get("label_field")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("sample.balanced requires string 'label_field'"))?
        .to_string();

    let max_per_class = obj.get("max_per_class").and_then(Value::as_u64).map(|v| v as usize);
    let min_per_class = obj.get("min_per_class").and_then(Value::as_u64).map(|v| v as usize);

    let strategy = match obj.get("strategy").and_then(Value::as_str) {
        Some("undersample") => ZiBalanceStrategy::Undersample,
        Some("oversample") => ZiBalanceStrategy::Oversample,
        Some("hybrid") | _ => ZiBalanceStrategy::Hybrid,
    };

    let seed = obj
        .get("seed")
        .and_then(Value::as_u64)
        .unwrap_or(0xCAFEBABE);

    Ok(Box::new(ZiSampleBalanced::new(
        label_field,
        max_per_class,
        min_per_class,
        strategy,
        seed,
    )))
}

#[derive(Debug)]
pub struct ZiSampleByDistribution {
    field: String,
    target_distribution: HashMap<String, f64>,
    total_count: usize,
    seed: u64,
}

impl ZiSampleByDistribution {
    #[allow(non_snake_case)]
    pub fn new(
        field: String,
        target_distribution: HashMap<String, f64>,
        total_count: usize,
        seed: u64,
    ) -> Self {
        Self {
            field,
            target_distribution,
            total_count,
            seed,
        }
    }

    fn get_field_value(&self, record: &ZiRecord) -> String {
        let parts: Vec<&str> = self.field.split('.').collect();
        if parts.len() < 2 {
            return "unknown".to_string();
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
            _ => "unknown".to_string(),
        }
    }
}

impl ZiOperator for ZiSampleByDistribution {
    fn name(&self) -> &'static str {
        "sample.by_distribution"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        use rand::seq::SliceRandom;
        use rand::SeedableRng;
        use rand::rngs::StdRng;

        if batch.is_empty() {
            return Ok(batch);
        }

        let mut rng = StdRng::seed_from_u64(self.seed);

        let mut groups: HashMap<String, ZiRecordBatch> = HashMap::new();
        for record in batch {
            let value = self.get_field_value(&record);
            groups.entry(value).or_default().push(record);
        }

        for (_, group) in groups.iter_mut() {
            group.shuffle(&mut rng);
        }

        let mut result = Vec::with_capacity(self.total_count);

        let dist_sum: f64 = self.target_distribution.values().sum();
        let normalized_dist: HashMap<String, f64> = if dist_sum > 0.0 {
            self.target_distribution
                .iter()
                .map(|(k, v)| (k.clone(), v / dist_sum))
                .collect()
        } else {
            let uniform = 1.0 / groups.len() as f64;
            groups.keys().map(|k| (k.clone(), uniform)).collect()
        };

        for (label, ratio) in &normalized_dist {
            let target_count = (*ratio * self.total_count as f64).round() as usize;

            if let Some(group) = groups.get_mut(label) {
                let take = target_count.min(group.len());
                result.extend(group.drain(..take));
            }
        }

        result.shuffle(&mut rng);
        result.truncate(self.total_count);
        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn sample_by_distribution_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("sample.by_distribution config must be object"))?;

    let field = obj
        .get("field")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("sample.by_distribution requires string 'field'"))?
        .to_string();

    let target_distribution = obj
        .get("target_distribution")
        .and_then(Value::as_object)
        .ok_or_else(|| ZiError::validation("sample.by_distribution requires object 'target_distribution'"))?
        .iter()
        .map(|(k, v)| {
            v.as_f64()
                .ok_or_else(|| ZiError::validation("distribution values must be numbers"))
                .map(|f| (k.clone(), f))
        })
        .collect::<Result<HashMap<_, _>>>()?;

    let total_count = obj
        .get("total_count")
        .and_then(Value::as_u64)
        .ok_or_else(|| ZiError::validation("sample.by_distribution requires integer 'total_count'"))?
        as usize;

    let seed = obj
        .get("seed")
        .and_then(Value::as_u64)
        .unwrap_or(0xCAFEBABE);

    Ok(Box::new(ZiSampleByDistribution::new(
        field,
        target_distribution,
        total_count,
        seed,
    )))
}

#[derive(Debug)]
pub struct ZiSampleByLength {
    text_field: String,
    min_length: usize,
    max_length: usize,
    target_count: Option<usize>,
    seed: u64,
}

impl ZiSampleByLength {
    #[allow(non_snake_case)]
    pub fn new(
        text_field: String,
        min_length: usize,
        max_length: usize,
        target_count: Option<usize>,
        seed: u64,
    ) -> Self {
        Self {
            text_field,
            min_length,
            max_length,
            target_count,
            seed,
        }
    }

    fn get_text_length(&self, record: &ZiRecord) -> usize {
        let parts: Vec<&str> = self.text_field.split('.').collect();
        if parts.len() < 2 {
            return 0;
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
            Value::String(s) => s.chars().count(),
            _ => 0,
        }
    }
}

impl ZiOperator for ZiSampleByLength {
    fn name(&self) -> &'static str {
        "sample.by_length"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        use rand::seq::SliceRandom;
        use rand::SeedableRng;
        use rand::rngs::StdRng;

        if batch.is_empty() {
            return Ok(batch);
        }

        let mut rng = StdRng::seed_from_u64(self.seed);

        let mut filtered: Vec<ZiRecord> = batch
            .into_iter()
            .filter(|record| {
                let len = self.get_text_length(record);
                len >= self.min_length && len <= self.max_length
            })
            .collect();

        filtered.shuffle(&mut rng);

        if let Some(count) = self.target_count {
            filtered.truncate(count);
        }

        Ok(filtered)
    }
}

#[allow(non_snake_case)]
pub fn sample_by_length_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("sample.by_length config must be object"))?;

    let text_field = obj
        .get("text_field")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("sample.by_length requires string 'text_field'"))?
        .to_string();

    let min_length = obj
        .get("min_length")
        .and_then(Value::as_u64)
        .unwrap_or(0) as usize;

    let max_length = obj
        .get("max_length")
        .and_then(Value::as_u64)
        .unwrap_or(usize::MAX as u64) as usize;

    let target_count = obj.get("target_count").and_then(Value::as_u64).map(|v| v as usize);

    let seed = obj
        .get("seed")
        .and_then(Value::as_u64)
        .unwrap_or(0xCAFEBABE);

    Ok(Box::new(ZiSampleByLength::new(
        text_field,
        min_length,
        max_length,
        target_count,
        seed,
    )))
}

#[derive(Debug)]
pub struct ZiSampleStratified {
    field: String,
    count: usize,
    seed: u64,
}

impl ZiSampleStratified {
    #[allow(non_snake_case)]
    pub fn new(field: String, count: usize, seed: u64) -> Self {
        Self { field, count, seed }
    }

    fn get_field_value(&self, record: &ZiRecord) -> String {
        let parts: Vec<&str> = self.field.split('.').collect();
        if parts.len() < 2 {
            return "unknown".to_string();
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
            _ => "unknown".to_string(),
        }
    }
}

impl ZiOperator for ZiSampleStratified {
    fn name(&self) -> &'static str {
        "sample.stratified"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        use rand::seq::SliceRandom;
        use rand::SeedableRng;
        use rand::rngs::StdRng;

        if batch.is_empty() {
            return Ok(batch);
        }

        let mut rng = StdRng::seed_from_u64(self.seed);

        let mut groups: HashMap<String, ZiRecordBatch> = HashMap::new();
        for record in batch {
            let value = self.get_field_value(&record);
            groups.entry(value).or_default().push(record);
        }

        for (_, group) in groups.iter_mut() {
            group.shuffle(&mut rng);
        }

        let total = groups.values().map(|g| g.len()).sum::<usize>();
        let ratio = self.count as f64 / total as f64;

        let mut result = Vec::with_capacity(self.count);

        for (_, group) in groups {
            let target = (group.len() as f64 * ratio).round() as usize;
            result.extend(group.into_iter().take(target));
        }

        result.shuffle(&mut rng);
        result.truncate(self.count);
        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn sample_stratified_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("sample.stratified config must be object"))?;

    let field = obj
        .get("field")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("sample.stratified requires string 'field'"))?
        .to_string();

    let count = obj
        .get("count")
        .and_then(Value::as_u64)
        .ok_or_else(|| ZiError::validation("sample.stratified requires integer 'count'"))?
        as usize;

    let seed = obj
        .get("seed")
        .and_then(Value::as_u64)
        .unwrap_or(0xCAFEBABE);

    Ok(Box::new(ZiSampleStratified::new(field, count, seed)))
}


