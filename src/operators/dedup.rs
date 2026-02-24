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

use serde_json::{Map, Number, Value};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

#[cfg(feature = "embedding")]
use std::sync::OnceLock;

use crate::errors::{Result, ZiError};
use crate::operator::ZiOperator;
use crate::operators::filter::ZiFieldPath;
use crate::record::{ZiMetadata, ZiRecordBatch};

#[cfg(feature = "embedding")]
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};

#[allow(non_snake_case)]
fn hash64(s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

#[allow(non_snake_case)]
fn simhash64(tokens: &[String]) -> u64 {
    let mut vec = [0i64; 64];
    for t in tokens {
        let mut x = hash64(t);
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
fn hamming(a: u64, b: u64) -> u32 {
    (a ^ b).count_ones()
}

#[allow(non_snake_case)]
fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect()
}

#[derive(Debug)]
struct _DedupSimHash {
    path: ZiFieldPath,
    threshold: f64,
}

impl _DedupSimHash {
    fn new(path: ZiFieldPath, threshold: f64) -> Self {
        Self { path, threshold }
    }
}

impl ZiOperator for _DedupSimHash {
    fn name(&self) -> &'static str {
        "dedup.simhash"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        let mut seen: Vec<(u64, usize)> = Vec::new();
        let mut out = Vec::new();
        'outer: for (idx, rec) in batch.into_iter().enumerate() {
            if let Some(Value::String(text)) = self.path.resolve(&rec) {
                let tokens = tokenize(text);
                let sh = simhash64(&tokens);
                for (prev, _) in &seen {
                    let dist = hamming(*prev, sh) as f64 / 64.0;
                    if 1.0 - dist >= self.threshold {
                        continue 'outer;
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
pub fn dedup_simhash_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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
    let field_path = ZiFieldPath::parse(path)?;
    Ok(Box::new(_DedupSimHash::new(field_path, threshold)))
}

#[derive(Debug)]
struct _DedupMinHash {
    path: ZiFieldPath,
    threshold: f64,
    k: usize,
    bands: usize,
}

impl _DedupMinHash {
    fn new(path: ZiFieldPath, threshold: f64, k: usize, bands: usize) -> Self {
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
                let mut h = hash64(t);
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

impl ZiOperator for _DedupMinHash {
    fn name(&self) -> &'static str {
        "dedup.minhash"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        use std::collections::HashMap;
        let rows_per_band = (self.k.max(1) + self.bands.max(1) - 1) / self.bands.max(1);
        let mut buckets: HashMap<(usize, u64), Vec<(usize, Vec<String>)>> = HashMap::new();
        let mut out = Vec::new();
        'outer: for (idx, rec) in batch.into_iter().enumerate() {
            let tokens = match self.path.resolve(&rec) {
                Some(Value::String(text)) => tokenize(text),
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
pub fn dedup_minhash_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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
    let field_path = ZiFieldPath::parse(path)?;
    Ok(Box::new(_DedupMinHash::new(
        field_path, threshold, k, bands,
    )))
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum ZiEmbeddingModel {
    AllMiniLML6V2,
    BGEBaseENV15,
    BGESmallENV15,
    NomicEmbedText,
    MultiQAMiniLML6CosV1,
}

impl Default for ZiEmbeddingModel {
    fn default() -> Self {
        Self::AllMiniLML6V2
    }
}

impl ZiEmbeddingModel {
    #[cfg(feature = "embedding")]
    fn to_fastembed(&self) -> EmbeddingModel {
        match self {
            ZiEmbeddingModel::AllMiniLML6V2 => EmbeddingModel::AllMiniLML6V2,
            ZiEmbeddingModel::BGEBaseENV15 => EmbeddingModel::BGEBaseENV15,
            ZiEmbeddingModel::BGESmallENV15 => EmbeddingModel::BGESmallENV15,
            ZiEmbeddingModel::NomicEmbedText => EmbeddingModel::NomicEmbedTextV15,
            ZiEmbeddingModel::MultiQAMiniLML6CosV1 => EmbeddingModel::MultilingualE5Small,
        }
    }
}

#[derive(Debug)]
struct _DedupSemantic {
    path: ZiFieldPath,
    threshold: f64,
    details_key: Option<String>,
    max_duplicates: usize,
    use_embedding: bool,
    embedding_model: ZiEmbeddingModel,
}

impl _DedupSemantic {
    fn new(path: ZiFieldPath, threshold: f64) -> Self {
        Self {
            path,
            threshold,
            details_key: None,
            max_duplicates: 50,
            use_embedding: false,
            embedding_model: ZiEmbeddingModel::default(),
        }
    }

    fn with_details(mut self, details_key: Option<String>, max_duplicates: usize) -> Self {
        self.details_key = details_key;
        self.max_duplicates = max_duplicates;
        self
    }

    fn with_embedding(mut self, use_embedding: bool, model: ZiEmbeddingModel) -> Self {
        self.use_embedding = use_embedding;
        self.embedding_model = model;
        self
    }
}

#[derive(Debug)]
struct _ZiCSemanticSeen {
    weights: HashMap<String, f64>,
    norm: f64,
    out_index: usize,
}

#[cfg(feature = "embedding")]
static EMBEDDING_MODEL: OnceLock<TextEmbedding> = OnceLock::new();

#[cfg(feature = "embedding")]
fn get_embedding_model(model: &ZiEmbeddingModel) -> Option<&'static TextEmbedding> {
    Some(EMBEDDING_MODEL.get_or_init(|| {
        TextEmbedding::try_new(InitOptions::new(model.to_fastembed()))
            .ok()
            .unwrap_or_else(|| {
                TextEmbedding::try_new(Default::default()).unwrap()
            })
    }))
}

#[derive(Debug)]
struct _EmbeddingSeen {
    embedding: Vec<f32>,
    out_index: usize,
}

#[cfg(feature = "embedding")]
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    dot / (norm_a * norm_b)
}

impl ZiOperator for _DedupSemantic {
    fn name(&self) -> &'static str {
        "dedup.semantic"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        if self.use_embedding {
            self.apply_with_embeddings(batch)
        } else {
            self.apply_with_tfidf(batch)
        }
    }
}

impl _DedupSemantic {
    fn apply_with_tfidf(&self, records: ZiRecordBatch) -> Result<ZiRecordBatch> {
        let mut tokenized: Vec<Option<Vec<String>>> = Vec::with_capacity(records.len());
        let mut doc_freq: HashMap<String, usize> = HashMap::new();
        let mut total_docs = 0usize;

        for record in &records {
            if let Some(Value::String(text)) = self.path.resolve(record) {
                let tokens = tokenize(text);
                if !tokens.is_empty() {
                    total_docs += 1;
                    let mut unique = HashSet::new();
                    for token in &tokens {
                        if unique.insert(token) {
                            *doc_freq.entry(token.clone()).or_insert(0) += 1;
                        }
                    }
                }
                tokenized.push(Some(tokens));
            } else {
                tokenized.push(None);
            }
        }

        let mut out = Vec::new();
        let mut seen_vectors: Vec<_ZiCSemanticSeen> = Vec::new();

        for (record, maybe_tokens) in records.into_iter().zip(tokenized.into_iter()) {
            let tokens = match maybe_tokens {
                Some(tokens) => tokens,
                None => {
                    let mut record = record;
                    if let Some(details_key) = &self.details_key {
                        _semantic_details_set_empty(record.metadata_mut(), details_key);
                    }
                    out.push(record);
                    continue;
                }
            };

            if tokens.is_empty() || total_docs == 0 {
                let mut record = record;
                if let Some(details_key) = &self.details_key {
                    _semantic_details_set_empty(record.metadata_mut(), details_key);
                }
                out.push(record);
                continue;
            }

            let mut term_counts: HashMap<String, usize> = HashMap::new();
            for token in &tokens {
                *term_counts.entry(token.clone()).or_insert(0) += 1;
            }

            let token_len = tokens.len() as f64;
            let mut weights: HashMap<String, f64> = HashMap::new();
            let mut norm_sq = 0.0f64;

            for (token, count) in term_counts {
                let tf = count as f64 / token_len;
                let df = doc_freq.get(&token).copied().unwrap_or(1) as f64;
                let idf = ((total_docs as f64 + 1.0) / (df + 1.0)).ln() + 1.0;
                let weight = tf * idf;
                norm_sq += weight * weight;
                weights.insert(token, weight);
            }

            let norm = norm_sq.sqrt();
            if norm == 0.0 {
                let mut record = record;
                if let Some(details_key) = &self.details_key {
                    _semantic_details_set_empty(record.metadata_mut(), details_key);
                }
                let out_index = out.len();
                seen_vectors.push(_ZiCSemanticSeen {
                    weights,
                    norm,
                    out_index,
                });
                out.push(record);
                continue;
            }

            let mut duplicate_of: Option<(usize, f64)> = None;
            for (idx, seen) in seen_vectors.iter().enumerate() {
                if seen.norm == 0.0 {
                    continue;
                }
                let mut dot = 0.0f64;
                for (token, weight) in &weights {
                    if let Some(other) = seen.weights.get(token) {
                        dot += weight * other;
                    }
                }
                let cosine = dot / (norm * seen.norm);
                if cosine >= self.threshold {
                    duplicate_of = Some((idx, cosine));
                    break;
                }
            }

            if let Some((seen_idx, similarity)) = duplicate_of {
                if let Some(details_key) = &self.details_key {
                    let seen = &mut seen_vectors[seen_idx];
                    let kept_record = &mut out[seen.out_index];
                    _semantic_details_add_match(
                        kept_record.metadata_mut(),
                        details_key,
                        record.id.as_deref(),
                        similarity,
                        self.max_duplicates,
                    );
                }
                continue;
            }

            let mut record = record;
            if let Some(details_key) = &self.details_key {
                _semantic_details_set_empty(record.metadata_mut(), details_key);
            }

            let out_index = out.len();
            seen_vectors.push(_ZiCSemanticSeen {
                weights,
                norm,
                out_index,
            });
            out.push(record);
        }

        Ok(out)
    }

    #[cfg(feature = "embedding")]
    fn apply_with_embeddings(&self, records: ZiRecordBatch) -> Result<ZiRecordBatch> {
        let model = match get_embedding_model(&self.embedding_model) {
            Some(m) => m,
            None => return self.apply_with_tfidf(records),
        };

        let texts: Vec<String> = records
            .iter()
            .map(|rec| {
                self.path
                    .resolve(rec)
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_default()
            })
            .collect();

        let embeddings = model.embed(texts.clone(), None).unwrap_or_default();

        let mut out = Vec::new();
        let mut seen_vectors: Vec<_EmbeddingSeen> = Vec::new();

        for (idx, mut record) in records.into_iter().enumerate() {
            let embedding = if idx < embeddings.len() {
                embeddings[idx].clone()
            } else {
                vec![]
            };

            if embedding.is_empty() {
                if let Some(details_key) = &self.details_key {
                    _semantic_details_set_empty(record.metadata_mut(), details_key);
                }
                out.push(record);
                continue;
            }

            let mut duplicate_of: Option<(usize, f32)> = None;
            for (seen_idx, seen) in seen_vectors.iter().enumerate() {
                let sim = cosine_similarity(&embedding, &seen.embedding);
                if sim >= self.threshold as f32 {
                    duplicate_of = Some((seen_idx, sim));
                    break;
                }
            }

            if let Some((seen_idx, similarity)) = duplicate_of {
                if let Some(details_key) = &self.details_key {
                    let seen = &seen_vectors[seen_idx];
                    let kept_record = &mut out[seen.out_index];
                    _semantic_details_add_match(
                        kept_record.metadata_mut(),
                        details_key,
                        record.id.as_deref(),
                        similarity as f64,
                        self.max_duplicates,
                    );
                }
                continue;
            }

            if let Some(details_key) = &self.details_key {
                _semantic_details_set_empty(record.metadata_mut(), details_key);
            }

            let out_index = out.len();
            seen_vectors.push(_EmbeddingSeen {
                embedding,
                out_index,
            });
            out.push(record);
        }

        Ok(out)
    }

    #[cfg(not(feature = "embedding"))]
    fn apply_with_embeddings(&self, records: ZiRecordBatch) -> Result<ZiRecordBatch> {
        self.apply_with_tfidf(records)
    }
}

#[allow(non_snake_case)]
pub fn dedup_semantic_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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
    let details_key = obj
        .get("details_key")
        .and_then(Value::as_str)
        .map(|s| s.to_string());
    let max_matches = obj.get("max_matches").and_then(Value::as_u64).unwrap_or(25) as usize;
    let use_embedding = obj.get("use_embedding").and_then(Value::as_bool).unwrap_or(false);
    let embedding_model = obj
        .get("embedding_model")
        .and_then(Value::as_str)
        .map(|s| match s.to_lowercase().as_str() {
            "all-minilm-l6-v2" => ZiEmbeddingModel::AllMiniLML6V2,
            "bge-base-en-v1.5" => ZiEmbeddingModel::BGEBaseENV15,
            "bge-small-en-v1.5" => ZiEmbeddingModel::BGESmallENV15,
            "nomic-embed-text" => ZiEmbeddingModel::NomicEmbedText,
            "multi-qa-minilm-l6-cos-v1" => ZiEmbeddingModel::MultiQAMiniLML6CosV1,
            _ => ZiEmbeddingModel::default(),
        })
        .unwrap_or_default();
    
    let field_path = ZiFieldPath::parse(path)?;
    Ok(Box::new(
        _DedupSemantic::new(field_path, threshold)
            .with_details(details_key, max_matches)
            .with_embedding(use_embedding, embedding_model),
    ))
}

fn _semantic_default_details() -> Map<String, Value> {
    let mut obj = Map::new();
    obj.insert("duplicate".into(), Value::Bool(false));
    obj.insert("max_similarity".into(), Value::Null);
    obj.insert("matches".into(), Value::Array(Vec::new()));
    obj
}

fn _semantic_details_set_empty(metadata: &mut ZiMetadata, key: &str) {
    metadata.insert(key.to_string(), Value::Object(_semantic_default_details()));
}

fn _semantic_details_mut<'a>(
    metadata: &'a mut ZiMetadata,
    key: &str,
) -> &'a mut Map<String, Value> {
    let entry = metadata
        .entry(key.to_string())
        .or_insert_with(|| Value::Object(_semantic_default_details()));

    if !entry.is_object() {
        *entry = Value::Object(_semantic_default_details());
    }

    match entry {
        Value::Object(map) => map,
        _ => unreachable!("semantic details should be an object"),
    }
}

fn _semantic_details_add_match(
    metadata: &mut ZiMetadata,
    key: &str,
    duplicate_id: Option<&str>,
    similarity: f64,
    max_duplicates: usize,
) {
    let details = _semantic_details_mut(metadata, key);
    details.insert("duplicate".into(), Value::Bool(true));

    if similarity.is_finite() {
        let current_max = details
            .get("max_similarity")
            .and_then(Value::as_f64)
            .unwrap_or(f64::NEG_INFINITY);
        if similarity > current_max {
            details.insert(
                "max_similarity".into(),
                Number::from_f64(similarity)
                    .map(Value::Number)
                    .unwrap_or(Value::Null),
            );
        }
    }

    let matches_value = details
        .entry("matches".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));

    let new_entry = {
        let mut obj = Map::new();
        if let Some(id) = duplicate_id {
            obj.insert("id".into(), Value::String(id.to_string()));
        }
        obj.insert(
            "similarity".into(),
            Number::from_f64(similarity)
                .map(Value::Number)
                .unwrap_or(Value::Null),
        );
        Value::Object(obj)
    };

    if let Value::Array(matches) = matches_value {
        if let Some(id) = duplicate_id {
            if let Some(existing) = matches.iter_mut().find(|value| {
                value
                    .as_object()
                    .and_then(|obj| obj.get("id"))
                    .and_then(Value::as_str)
                    == Some(id)
            }) {
                if let Some(obj) = existing.as_object_mut() {
                    obj.insert(
                        "similarity".into(),
                        Number::from_f64(similarity)
                            .map(Value::Number)
                            .unwrap_or(Value::Null),
                    );
                }
                return;
            }
        }

        if matches.len() < max_duplicates {
            matches.push(new_entry);
            return;
        }

        if let Some((idx, min_sim)) = matches
            .iter()
            .enumerate()
            .filter_map(|(idx, value)| {
                value
                    .as_object()
                    .and_then(|obj| obj.get("similarity"))
                    .and_then(Value::as_f64)
                    .map(|sim| (idx, sim))
            })
            .min_by(|a, b| match a.1.partial_cmp(&b.1) {
                Some(order) => order,
                None => Ordering::Equal,
            })
        {
            if similarity > min_sim {
                matches[idx] = new_entry;
            }
        }
    } else {
        *matches_value = Value::Array(vec![new_entry]);
    }
}
