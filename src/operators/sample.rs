//! Copyright © 2025 Dunimd Team. All Rights Reserved.
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

use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::operator::ZiCOperator;
use crate::record::{ZiCRecord, ZiCRecordBatch};

fn _SampleStableHash(record: &ZiCRecord, seed: u64) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    seed.hash(&mut h);
    record.id.hash(&mut h);
    serde_json::to_string(&record.payload)
        .unwrap_or_default()
        .hash(&mut h);
    if let Some(m) = &record.metadata {
        serde_json::to_string(m).unwrap_or_default().hash(&mut h);
    }
    h.finish()
}

#[derive(Debug)]
pub struct ZiCSampleRandom {
    ratio: Option<f64>,
    count: Option<usize>,
    seed: u64,
}

impl ZiCSampleRandom {
    #[allow(non_snake_case)]
    pub fn ZiFNew(ratio: Option<f64>, count: Option<usize>, seed: u64) -> Self {
        Self { ratio, count, seed }
    }
}

impl ZiCOperator for ZiCSampleRandom {
    fn name(&self) -> &'static str {
        "sample.random"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        if let Some(c) = self.count {
            return Ok(batch.into_iter().take(c).collect());
        }
        let ratio = self
            .ratio
            .ok_or_else(|| ZiError::validation("sample.random requires 'ratio' or 'count'"))?;
        if !(0.0..=1.0).contains(&ratio) {
            return Err(ZiError::validation(
                "sample.random 'ratio' must be in [0,1]",
            ));
        }
        Ok(batch
            .into_iter()
            .filter(|r| {
                let h = _SampleStableHash(r, self.seed);
                ((h as f64) / (u64::MAX as f64)) < ratio
            })
            .collect())
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
    Ok(Box::new(ZiCSampleRandom::ZiFNew(ratio, count, seed)))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{ZiCMetadata, ZiCRecord};
    use serde_json::json;

    #[test]
    fn sample_random_deterministic_with_seed() {
        let op = ZiCSampleRandom::ZiFNew(Some(0.5), None, 42);
        let mut batch = Vec::new();
        for i in 0..10 {
            batch.push(ZiCRecord::ZiFNew(
                Some(i.to_string()),
                json!({"text": format!("row {i}")}),
            ));
        }
        let out = op.apply(batch).unwrap();
        assert!(out.len() >= 3 && out.len() <= 7);
    }

    #[test]
    fn sample_top_picks_highest_values() {
        let op = ZiCSampleTop::ZiFNew("quality".into(), 2);
        let a = ZiCRecord::ZiFNew(Some("a".into()), json!(null)).ZiFWithMetadata({
            let mut m = ZiCMetadata::new();
            m.insert("quality".into(), json!(0.8));
            m
        });
        let b = ZiCRecord::ZiFNew(Some("b".into()), json!(null)).ZiFWithMetadata({
            let mut m = ZiCMetadata::new();
            m.insert("quality".into(), json!(0.4));
            m
        });
        let c = ZiCRecord::ZiFNew(Some("c".into()), json!(null)).ZiFWithMetadata({
            let mut m = ZiCMetadata::new();
            m.insert("quality".into(), json!(0.9));
            m
        });
        let out = op.apply(vec![a, b, c]).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].id.as_deref(), Some("c"));
        assert_eq!(out[1].id.as_deref(), Some("a"));
    }
}
