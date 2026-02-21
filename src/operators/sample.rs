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
use crate::operator::ZiCOperator;
use crate::record::{ZiCRecord, ZiCRecordBatch};

fn _sample_stable_hash(record: &ZiCRecord, seed: u64) -> u64 {
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
pub struct ZiCSampleRandom {
    ratio: Option<f64>,
    count: Option<usize>,
    seed: u64,
    weight_key: Option<String>,
    group_key: Option<String>,
    min_per_group: Option<usize>,
}

impl ZiCSampleRandom {
    #[allow(non_snake_case)]
    pub fn ZiFNew(
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

impl ZiCOperator for ZiCSampleRandom {
    fn name(&self) -> &'static str {
        "sample.random"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
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

        let mut grouped: HashMap<String, Vec<(f64, u64, ZiCRecord)>> = HashMap::new();
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
            let mut combined: Vec<(f64, u64, ZiCRecord)> =
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

        let mut groups: Vec<(String, Vec<(f64, u64, ZiCRecord)>)> = grouped.into_iter().collect();
        groups.sort_by(|a, b| a.0.cmp(&b.0));

        for (_, items) in groups.iter_mut() {
            items.sort_by(|a, b| {
                b.0.partial_cmp(&a.0)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| b.1.cmp(&a.1))
            });
        }

        let mut selected: Vec<ZiCRecord> = Vec::with_capacity(target);
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
            let mut leftovers: Vec<(f64, u64, ZiCRecord)> = groups
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
pub fn ZiFSampleRandomFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
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
    Ok(Box::new(ZiCSampleRandom::ZiFNew(
        ratio,
        count,
        seed,
        weight_key,
        group_key,
        min_per_group,
    )))
}

#[derive(Debug)]
pub struct ZiCSampleTop {
    key: String,
    count: usize,
}

impl ZiCSampleTop {
    #[allow(non_snake_case)]
    pub fn ZiFNew(key: String, count: usize) -> Self {
        Self { key, count }
    }
}

impl ZiCOperator for ZiCSampleTop {
    fn name(&self) -> &'static str {
        "sample.top"
    }

    fn apply(&self, mut batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
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
pub fn ZiFSampleTopFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("sample.top config must be object"))?;
    let key = obj
        .get("key")
        .and_then(Value::as_str)
        .unwrap_or("quality")
        .to_string();
    let count = obj
        .get("count")
        .and_then(Value::as_u64)
        .ok_or_else(|| ZiError::validation("sample.top requires unsigned integer 'count'"))?
        as usize;
    Ok(Box::new(ZiCSampleTop::ZiFNew(key, count)))
}


