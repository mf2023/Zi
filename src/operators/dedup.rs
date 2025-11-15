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
use std::collections::HashSet;

use crate::errors::{Result, ZiError};
use crate::operator::ZiCOperator;
use crate::operators::filter::ZiCFieldPath;
use crate::record::ZiCRecordBatch;

#[allow(non_snake_case)]
fn ZiFHash64(s: &str) -> u64 {
    // simple 64-bit hash (FNV-1a)
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

#[allow(non_snake_case)]
fn ZiFSimhash64(tokens: &[String]) -> u64 {
    let mut vec = [0i64; 64];
    for t in tokens {
        let mut x = ZiFHash64(t);
        for i in 0..64 {
            let bit = (x & 1) != 0;
            vec[i] += if bit { 1 } else { -1 };
            x >>= 1;
        }
    }
    let mut out: u64 = 0;
    for i in (0..64).rev() {
        out <<= 1;
        if vec[i] >= 0 {
            out |= 1;
        }
    }
    out
}

#[allow(non_snake_case)]
fn ZiFHamming(a: u64, b: u64) -> u32 {
    (a ^ b).count_ones()
}

#[allow(non_snake_case)]
fn ZiFTokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect()
}

#[allow(non_snake_case)]
fn ZiFJaccard(a: &HashSet<String>, b: &HashSet<String>) -> f64 {
    if a.is_empty() && b.is_empty() {
        return 1.0;
    }
    let intersection = a.intersection(b).count() as f64;
    let union = (a.len() + b.len()) as f64 - intersection;
    if union == 0.0 {
        0.0
    } else {
        intersection / union
    }
}

#[derive(Debug)]
struct _DedupSimHash {
    path: ZiCFieldPath,
    threshold: f64,
}

impl _DedupSimHash {
    fn new(path: ZiCFieldPath, threshold: f64) -> Self {
        Self { path, threshold }
    }
}

impl ZiCOperator for _DedupSimHash {
    fn name(&self) -> &'static str {
        "dedup.simhash"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        let mut seen: Vec<(u64, usize)> = Vec::new();
        let mut out = Vec::new();
        'outer: for (idx, rec) in batch.into_iter().enumerate() {
            if let Some(Value::String(text)) = self.path.ZiFResolve(&rec) {
                let tokens = ZiFTokenize(text);
                let sh = ZiFSimhash64(&tokens);
                for (prev, _) in &seen {
                    let dist = ZiFHamming(*prev, sh) as f64 / 64.0;
                    if 1.0 - dist >= self.threshold {
                        continue 'outer; // duplicate, drop
                    }
                }
                seen.push((sh, idx));
                out.push(rec);
            } else {
                out.push(rec);
            }
        }
        Ok(out)
    }
}

#[allow(non_snake_case)]
pub fn ZiFDedupSimhashFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("dedup.simhash config must be object"))?;
    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("dedup.simhash requires string 'path'"))?;
    let threshold = obj.get("threshold").and_then(Value::as_f64).unwrap_or(0.85);
    if !(0.0..=1.0).contains(&threshold) {
        return Err(ZiError::validation(
            "dedup.simhash 'threshold' must be in [0,1]",
        ));
    }
    let field_path = ZiCFieldPath::ZiFParse(path)?;
    Ok(Box::new(_DedupSimHash::new(field_path, threshold)))
}

#[derive(Debug)]
struct _DedupMinHash {
    path: ZiCFieldPath,
    threshold: f64,
    k: usize,
    bands: usize,
}

impl _DedupMinHash {
    fn new(path: ZiCFieldPath, threshold: f64, k: usize, bands: usize) -> Self {
        Self {
            path,
            threshold,
            k,
            bands,
        }
    }

    fn signature(&self, tokens: &[String]) -> Vec<u64> {
        let mut sig = vec![u64::MAX; self.k];
        for (i, seed) in (0..self.k).enumerate() {
            let s = (seed as u64).wrapping_add(0x9E3779B185EBCA87u64);
            for t in tokens {
                let mut h = ZiFHash64(t);
                h ^= s;
                if h < sig[i] {
                    sig[i] = h;
                }
            }
        }
        sig
    }

        #[allow(non_snake_case)]
    fn _DedupMinHashJaccard(a: &[String], b: &[String]) -> f64 {
        use std::collections::HashSet;
        let sa: HashSet<&String> = a.iter().collect();
        let sb: HashSet<&String> = b.iter().collect();
        let inter = sa.intersection(&sb).count() as f64;
        let uni = sa.union(&sb).count() as f64;
        if uni == 0.0 {
            0.0
        } else {
            inter / uni
        }
    }
}

impl ZiCOperator for _DedupMinHash {
    fn name(&self) -> &'static str {
        "dedup.minhash"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        use std::collections::HashMap;
        let rows_per_band = (self.k.max(1) + self.bands.max(1) - 1) / self.bands.max(1);
        let mut buckets: HashMap<(usize, u64), Vec<(usize, Vec<String>)>> = HashMap::new();
        let mut out = Vec::new();
        'outer: for (idx, rec) in batch.into_iter().enumerate() {
            let tokens = match self.path.ZiFResolve(&rec) {
                Some(Value::String(text)) => ZiFTokenize(text),
                _ => {
                    out.push(rec);
                    continue;
                }
            };
            let sig = self.signature(&tokens);
            let mut candidates = Vec::new();
            for b in 0..self.bands.max(1) {
                let start = b * rows_per_band;
                let end = (start + rows_per_band).min(self.k);
                if start >= end {
                    break;
                }
                let mut acc: u64 = 0;
                for i in start..end {
                    acc = acc.wrapping_mul(1315423911) ^ sig[i];
                }
                let key = (b, acc);
                if let Some(list) = buckets.get(&key) {
                    candidates.extend(list.iter().cloned());
                }
                buckets.entry(key).or_default().push((idx, tokens.clone()));
            }
            for (_cidx, ctoks) in candidates {
                let j = Self::_DedupMinHashJaccard(&tokens, &ctoks);
                if j >= self.threshold {
                    continue 'outer;
                }
            }
            out.push(rec);
        }
        Ok(out)
    }
}

#[allow(non_snake_case)]
pub fn ZiFDedupMinhashFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("dedup.minhash config must be object"))?;
    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("dedup.minhash requires string 'path'"))?;
    let threshold = obj.get("threshold").and_then(Value::as_f64).unwrap_or(0.8);
    let k = obj.get("k").and_then(Value::as_u64).unwrap_or(64) as usize;
    let bands = obj.get("bands").and_then(Value::as_u64).unwrap_or(8) as usize;
    if !(0.0..=1.0).contains(&threshold) {
        return Err(ZiError::validation(
            "dedup.minhash 'threshold' must be in [0,1]",
        ));
    }
    if k == 0 || bands == 0 {
        return Err(ZiError::validation(
            "dedup.minhash 'k' and 'bands' must be positive",
        ));
    }
    let field_path = ZiCFieldPath::ZiFParse(path)?;
    Ok(Box::new(_DedupMinHash::new(
        field_path, threshold, k, bands,
    )))
}

#[derive(Debug)]
struct _DedupSemantic {
    path: ZiCFieldPath,
    threshold: f64,
}

impl _DedupSemantic {
    fn new(path: ZiCFieldPath, threshold: f64) -> Self {
        Self { path, threshold }
    }
}

impl ZiCOperator for _DedupSemantic {
    fn name(&self) -> &'static str {
        "dedup.semantic"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        let mut seen: Vec<HashSet<String>> = Vec::new();
        let mut out = Vec::new();

        'outer: for rec in batch.into_iter() {
            if let Some(Value::String(text)) = self.path.ZiFResolve(&rec) {
                let token_set: HashSet<String> = ZiFTokenize(text).into_iter().collect();
                for existing in &seen {
                    if ZiFJaccard(&token_set, existing) >= self.threshold {
                        continue 'outer;
                    }
                }
                seen.push(token_set);
                out.push(rec);
            } else {
                out.push(rec);
            }
        }

        Ok(out)
    }
}

#[allow(non_snake_case)]
pub fn ZiFDedupSemanticFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("dedup.semantic config must be object"))?;
    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("dedup.semantic requires string 'path'"))?;
    let threshold = obj.get("threshold").and_then(Value::as_f64).unwrap_or(0.7);
    if !(0.0..=1.0).contains(&threshold) {
        return Err(ZiError::validation(
            "dedup.semantic 'threshold' must be in [0,1]",
        ));
    }
    let field_path = ZiCFieldPath::ZiFParse(path)?;
    Ok(Box::new(_DedupSemantic::new(field_path, threshold)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::ZiCRecord;
    use serde_json::json;

    #[test]
    fn simhash_dedup_removes_near_duplicates() {
        let op = _DedupSimHash::new(ZiCFieldPath::ZiFParse("payload.text").unwrap(), 0.9);
        let batch = vec![
            ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "Hello world!"})),
            ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "hello   world"})),
            ZiCRecord::ZiFNew(Some("3".into()), json!({"text": "Different text"})),
        ];
        let out = op.apply(batch).unwrap();
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn minhash_dedup_removes_similar() {
        let op = _DedupMinHash::new(ZiCFieldPath::ZiFParse("payload.text").unwrap(), 0.8, 32, 8);
        let batch = vec![
            ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "Alpha beta gamma"})),
            ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "alpha  beta   gamma"})),
            ZiCRecord::ZiFNew(Some("3".into()), json!({"text": "delta epsilon"})),
        ];
        let out = op.apply(batch).unwrap();
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn semantic_dedup_removes_near_duplicates() {
        let op = _DedupSemantic::new(ZiCFieldPath::ZiFParse("payload.text").unwrap(), 0.5);
        let batch = vec![
            ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "Large language model"})),
            ZiCRecord::ZiFNew(
                Some("2".into()),
                json!({"text": "Large language models"}),
            ),
            ZiCRecord::ZiFNew(Some("3".into()), json!({"text": "Small cats"})),
        ];
        let out = op.apply(batch).unwrap();
        assert_eq!(out.len(), 2);
    }
}
