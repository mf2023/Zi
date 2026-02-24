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
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tiktoken_rs::CoreBPE;

use crate::errors::{Result, ZiError};
use crate::operator::ZiOperator;
use crate::record::{ZiRecord, ZiRecordBatch};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiTokenizerType {
    Cl100kBase,
    P50kBase,
    P50kEdit,
    R50kBase,
    O200kBase,
    Whitespace,
    Character,
    Word,
}

impl Default for ZiTokenizerType {
    fn default() -> Self {
        Self::Cl100kBase
    }
}

impl ZiTokenizerType {
    pub fn from_model(model: &str) -> Self {
        match model.to_lowercase().as_str() {
            "cl100k_base" => Self::Cl100kBase,
            "p50k_base" => Self::P50kBase,
            "p50k_edit" => Self::P50kEdit,
            "r50k_base" => Self::R50kBase,
            "o200k_base" => Self::O200kBase,
            "whitespace" => Self::Whitespace,
            "character" => Self::Character,
            "word" => Self::Word,
            "gpt-4" | "gpt-4-turbo" | "gpt-4o" | "gpt-3.5-turbo" => Self::Cl100kBase,
            "gpt-4o-mini" | "gpt-5" | "o1" | "o3" | "o4" => Self::O200kBase,
            _ => Self::Cl100kBase,
        }
    }
}

static CL100K_BPE: OnceLock<CoreBPE> = OnceLock::new();
static P50K_BPE: OnceLock<CoreBPE> = OnceLock::new();
static P50K_EDIT_BPE: OnceLock<CoreBPE> = OnceLock::new();
static R50K_BPE: OnceLock<CoreBPE> = OnceLock::new();
static O200K_BPE: OnceLock<CoreBPE> = OnceLock::new();

fn get_bpe(tokenizer_type: &ZiTokenizerType) -> Option<&'static CoreBPE> {
    match tokenizer_type {
        ZiTokenizerType::Cl100kBase => {
            Some(CL100K_BPE.get_or_init(|| {
                tiktoken_rs::cl100k_base().unwrap_or_else(|_| {
                    tiktoken_rs::get_bpe_from_model("gpt-4").unwrap()
                })
            }))
        }
        ZiTokenizerType::P50kBase => {
            Some(P50K_BPE.get_or_init(|| {
                tiktoken_rs::p50k_base().unwrap_or_else(|_| {
                    tiktoken_rs::get_bpe_from_model("text-davinci-003").unwrap()
                })
            }))
        }
        ZiTokenizerType::P50kEdit => {
            Some(P50K_EDIT_BPE.get_or_init(|| {
                tiktoken_rs::p50k_edit().unwrap_or_else(|_| {
                    tiktoken_rs::get_bpe_from_model("text-davinci-edit-001").unwrap()
                })
            }))
        }
        ZiTokenizerType::R50kBase => {
            Some(R50K_BPE.get_or_init(|| {
                tiktoken_rs::r50k_base().unwrap_or_else(|_| {
                    tiktoken_rs::get_bpe_from_model("davinci").unwrap()
                })
            }))
        }
        ZiTokenizerType::O200kBase => {
            Some(O200K_BPE.get_or_init(|| {
                tiktoken_rs::o200k_base().unwrap_or_else(|_| {
                    tiktoken_rs::get_bpe_from_model("gpt-4o").unwrap()
                })
            }))
        }
        _ => None,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiTokenCountConfig {
    pub text_field: String,
    pub output_field: String,
    pub tokenizer_type: ZiTokenizerType,
}

impl Default for ZiTokenCountConfig {
    fn default() -> Self {
        Self {
            text_field: "payload.text".to_string(),
            output_field: "metadata.token_count".to_string(),
            tokenizer_type: ZiTokenizerType::Cl100kBase,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZiTokenStats {
    pub total_tokens: usize,
    pub min_tokens: usize,
    pub max_tokens: usize,
    pub avg_tokens: f64,
    pub median_tokens: f64,
    pub p25_tokens: usize,
    pub p75_tokens: usize,
    pub p95_tokens: usize,
    pub p99_tokens: usize,
    pub record_count: usize,
}

#[derive(Debug)]
pub struct ZiTokenCounter {
    config: ZiTokenCountConfig,
}

impl ZiTokenCounter {
    #[allow(non_snake_case)]
    pub fn new(config: ZiTokenCountConfig) -> Self {
        Self { config }
    }

    fn extract_text(&self, record: &ZiRecord) -> String {
        let parts: Vec<&str> = self.config.text_field.split('.').collect();
        if parts.len() < 2 {
            return String::new();
        }

        let mut current = &record.payload;
        for part in &parts[1..] {
            match current {
                Value::Object(map) => {
                    current = map.get(*part).unwrap_or(&Value::Null);
                }
                Value::Array(arr) => {
                    if let Ok(idx) = part.parse::<usize>() {
                        if idx < arr.len() {
                            current = &arr[idx];
                        } else {
                            return String::new();
                        }
                    } else {
                        return String::new();
                    }
                }
                _ => return String::new(),
            }
        }

        match current {
            Value::String(s) => s.clone(),
            Value::Array(arr) => arr
                .iter()
                .filter_map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join(" "),
            Value::Object(map) => map
                .values()
                .filter_map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join(" "),
            _ => String::new(),
        }
    }

    fn count_tokens(&self, text: &str) -> usize {
        match self.config.tokenizer_type {
            ZiTokenizerType::Whitespace => text.split_whitespace().count(),
            ZiTokenizerType::Character => text.chars().count(),
            ZiTokenizerType::Word => {
                let word_count = text.split_whitespace().count();
                let chinese_chars = text
                    .chars()
                    .filter(|c| '\u{4e00}' <= *c && *c <= '\u{9fff}')
                    .count();
                word_count + chinese_chars
            }
            ZiTokenizerType::Cl100kBase
            | ZiTokenizerType::P50kBase
            | ZiTokenizerType::P50kEdit
            | ZiTokenizerType::R50kBase
            | ZiTokenizerType::O200kBase => {
                if let Some(bpe) = get_bpe(&self.config.tokenizer_type) {
                    bpe.encode_with_special_tokens(text).len()
                } else {
                    text.split_whitespace().count()
                }
            }
        }
    }

    fn set_token_count(&self, record: &mut ZiRecord, count: usize) {
        let parts: Vec<&str> = self.config.output_field.split('.').collect();
        if parts.len() < 2 {
            return;
        }

        match parts[0] {
            "metadata" => {
                let meta = record.metadata_mut();
                meta.insert(parts[1].to_string(), Value::Number(count.into()));
            }
            "payload" => {
                if let Value::Object(ref mut map) = record.payload {
                    map.insert(parts[1].to_string(), Value::Number(count.into()));
                }
            }
            _ => {}
        }
    }
}

impl ZiOperator for ZiTokenCounter {
    fn name(&self) -> &'static str {
        "token.count"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        batch
            .into_iter()
            .map(|mut record| {
                let text = self.extract_text(&record);
                let count = self.count_tokens(&text);
                self.set_token_count(&mut record, count);
                Ok(record)
            })
            .collect()
    }
}

#[allow(non_snake_case)]
pub fn token_count_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let config: ZiTokenCountConfig = serde_json::from_value(config.clone())
        .map_err(|e| ZiError::validation(format!("invalid token count config: {}", e)))?;
    Ok(Box::new(ZiTokenCounter::new(config)))
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct ZiTokenStatsOperator {
    text_field: String,
    tokenizer_type: ZiTokenizerType,
    output_field: String,
}

impl ZiTokenStatsOperator {
    #[allow(non_snake_case)]
    pub fn new(
        text_field: String,
        tokenizer_type: ZiTokenizerType,
        output_field: String,
    ) -> Self {
        Self {
            text_field,
            tokenizer_type,
            output_field,
        }
    }

    fn extract_text(&self, record: &ZiRecord) -> String {
        let parts: Vec<&str> = self.text_field.split('.').collect();
        if parts.len() < 2 {
            return String::new();
        }

        let mut current = &record.payload;
        for part in &parts[1..] {
            if let Value::Object(map) = current {
                current = map.get(*part).unwrap_or(&Value::Null);
            } else {
                return String::new();
            }
        }

        match current {
            Value::String(s) => s.clone(),
            _ => String::new(),
        }
    }

    fn count_tokens(&self, text: &str) -> usize {
        match self.tokenizer_type {
            ZiTokenizerType::Whitespace => text.split_whitespace().count(),
            ZiTokenizerType::Character => text.chars().count(),
            ZiTokenizerType::Word => {
                let word_count = text.split_whitespace().count();
                let chinese_chars = text
                    .chars()
                    .filter(|c| '\u{4e00}' <= *c && *c <= '\u{9fff}')
                    .count();
                word_count + chinese_chars
            }
            ZiTokenizerType::Cl100kBase
            | ZiTokenizerType::P50kBase
            | ZiTokenizerType::P50kEdit
            | ZiTokenizerType::R50kBase
            | ZiTokenizerType::O200kBase => {
                if let Some(bpe) = get_bpe(&self.tokenizer_type) {
                    bpe.encode_with_special_tokens(text).len()
                } else {
                    text.split_whitespace().count()
                }
            }
        }
    }

    fn compute_stats(&self, token_counts: &[usize]) -> ZiTokenStats {
        if token_counts.is_empty() {
            return ZiTokenStats::default();
        }

        let mut sorted = token_counts.to_vec();
        sorted.sort();

        let total: usize = sorted.iter().sum();
        let count = sorted.len();

        ZiTokenStats {
            total_tokens: total,
            min_tokens: sorted[0],
            max_tokens: sorted[count - 1],
            avg_tokens: total as f64 / count as f64,
            median_tokens: sorted[count / 2] as f64,
            p25_tokens: sorted[(count as f64 * 0.25) as usize],
            p75_tokens: sorted[(count as f64 * 0.75) as usize],
            p95_tokens: sorted[(count as f64 * 0.95).min(count as f64 - 1.0) as usize],
            p99_tokens: sorted[(count as f64 * 0.99).min(count as f64 - 1.0) as usize],
            record_count: count,
        }
    }
}

impl ZiOperator for ZiTokenStatsOperator {
    fn name(&self) -> &'static str {
        "token.stats"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let token_counts: Vec<usize> = batch
            .iter()
            .map(|record| {
                let text = self.extract_text(record);
                self.count_tokens(&text)
            })
            .collect();

        let stats = self.compute_stats(&token_counts);

        let mut result = Vec::with_capacity(batch.len());
        for (i, mut record) in batch.into_iter().enumerate() {
            let meta = record.metadata_mut();
            meta.insert(
                "token_count".to_string(),
                Value::Number(token_counts[i].into()),
            );
            result.push(record);
        }

        if let Some(first) = result.first_mut() {
            let meta = first.metadata_mut();
            meta.insert(
                "token_stats".to_string(),
                serde_json::to_value(&stats).unwrap_or(Value::Null),
            );
        }

        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn token_stats_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("token.stats config must be object"))?;

    let text_field = obj
        .get("text_field")
        .and_then(Value::as_str)
        .unwrap_or("payload.text")
        .to_string();

    let tokenizer_type = obj
        .get("tokenizer_type")
        .and_then(Value::as_str)
        .map(|s| ZiTokenizerType::from_model(s))
        .unwrap_or_default();

    let output_field = obj
        .get("output_field")
        .and_then(Value::as_str)
        .unwrap_or("metadata.token_stats")
        .to_string();

    Ok(Box::new(ZiTokenStatsOperator::new(
        text_field,
        tokenizer_type,
        output_field,
    )))
}

#[derive(Debug)]
pub struct ZiTokenFilter {
    text_field: String,
    min_tokens: usize,
    max_tokens: usize,
    tokenizer_type: ZiTokenizerType,
}

impl ZiTokenFilter {
    #[allow(non_snake_case)]
    pub fn new(
        text_field: String,
        min_tokens: usize,
        max_tokens: usize,
        tokenizer_type: ZiTokenizerType,
    ) -> Self {
        Self {
            text_field,
            min_tokens,
            max_tokens,
            tokenizer_type,
        }
    }

    fn extract_text(&self, record: &ZiRecord) -> String {
        let parts: Vec<&str> = self.text_field.split('.').collect();
        if parts.len() < 2 {
            return String::new();
        }

        let mut current = &record.payload;
        for part in &parts[1..] {
            if let Value::Object(map) = current {
                current = map.get(*part).unwrap_or(&Value::Null);
            } else {
                return String::new();
            }
        }

        match current {
            Value::String(s) => s.clone(),
            _ => String::new(),
        }
    }

    fn count_tokens(&self, text: &str) -> usize {
        match self.tokenizer_type {
            ZiTokenizerType::Whitespace => text.split_whitespace().count(),
            ZiTokenizerType::Character => text.chars().count(),
            ZiTokenizerType::Word => {
                let word_count = text.split_whitespace().count();
                let chinese_chars = text
                    .chars()
                    .filter(|c| '\u{4e00}' <= *c && *c <= '\u{9fff}')
                    .count();
                word_count + chinese_chars
            }
            ZiTokenizerType::Cl100kBase
            | ZiTokenizerType::P50kBase
            | ZiTokenizerType::P50kEdit
            | ZiTokenizerType::R50kBase
            | ZiTokenizerType::O200kBase => {
                if let Some(bpe) = get_bpe(&self.tokenizer_type) {
                    bpe.encode_with_special_tokens(text).len()
                } else {
                    text.split_whitespace().count()
                }
            }
        }
    }
}

impl ZiOperator for ZiTokenFilter {
    fn name(&self) -> &'static str {
        "token.filter"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| {
                let text = self.extract_text(record);
                let count = self.count_tokens(&text);
                count >= self.min_tokens && count <= self.max_tokens
            })
            .collect())
    }
}

#[allow(non_snake_case)]
pub fn token_filter_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("token.filter config must be object"))?;

    let text_field = obj
        .get("text_field")
        .and_then(Value::as_str)
        .unwrap_or("payload.text")
        .to_string();

    let min_tokens = obj
        .get("min_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0) as usize;

    let max_tokens = obj
        .get("max_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(u64::MAX) as usize;

    let tokenizer_type = obj
        .get("tokenizer_type")
        .and_then(Value::as_str)
        .map(|s| ZiTokenizerType::from_model(s))
        .unwrap_or_default();

    Ok(Box::new(ZiTokenFilter::new(
        text_field,
        min_tokens,
        max_tokens,
        tokenizer_type,
    )))
}

#[derive(Debug)]
pub struct ZiTokenHistogram {
    text_field: String,
    tokenizer_type: ZiTokenizerType,
    bucket_size: usize,
}

impl ZiTokenHistogram {
    #[allow(non_snake_case)]
    pub fn new(text_field: String, tokenizer_type: ZiTokenizerType, bucket_size: usize) -> Self {
        Self {
            text_field,
            tokenizer_type,
            bucket_size,
        }
    }

    fn extract_text(&self, record: &ZiRecord) -> String {
        let parts: Vec<&str> = self.text_field.split('.').collect();
        if parts.len() < 2 {
            return String::new();
        }

        let mut current = &record.payload;
        for part in &parts[1..] {
            if let Value::Object(map) = current {
                current = map.get(*part).unwrap_or(&Value::Null);
            } else {
                return String::new();
            }
        }

        match current {
            Value::String(s) => s.clone(),
            _ => String::new(),
        }
    }

    fn count_tokens(&self, text: &str) -> usize {
        match self.tokenizer_type {
            ZiTokenizerType::Whitespace => text.split_whitespace().count(),
            ZiTokenizerType::Character => text.chars().count(),
            ZiTokenizerType::Word => {
                let word_count = text.split_whitespace().count();
                let chinese_chars = text
                    .chars()
                    .filter(|c| '\u{4e00}' <= *c && *c <= '\u{9fff}')
                    .count();
                word_count + chinese_chars
            }
            ZiTokenizerType::Cl100kBase
            | ZiTokenizerType::P50kBase
            | ZiTokenizerType::P50kEdit
            | ZiTokenizerType::R50kBase
            | ZiTokenizerType::O200kBase => {
                if let Some(bpe) = get_bpe(&self.tokenizer_type) {
                    bpe.encode_with_special_tokens(text).len()
                } else {
                    text.split_whitespace().count()
                }
            }
        }
    }
}

impl ZiOperator for ZiTokenHistogram {
    fn name(&self) -> &'static str {
        "token.histogram"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let mut histogram: HashMap<usize, usize> = HashMap::new();

        for record in &batch {
            let text = self.extract_text(record);
            let count = self.count_tokens(&text);
            let bucket = count / self.bucket_size;
            *histogram.entry(bucket).or_insert(0) += 1;
        }

        let mut result = batch;

        if let Some(first) = result.first_mut() {
            let meta = first.metadata_mut();
            let hist_value: Value = histogram
                .iter()
                .map(|(bucket, count)| {
                    (
                        format!("{}-{}", bucket * self.bucket_size, (bucket + 1) * self.bucket_size - 1),
                        Value::Number((*count).into()),
                    )
                })
                .collect::<serde_json::Map<String, Value>>()
                .into();
            meta.insert("token_histogram".to_string(), hist_value);
        }

        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn token_histogram_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("token.histogram config must be object"))?;

    let text_field = obj
        .get("text_field")
        .and_then(Value::as_str)
        .unwrap_or("payload.text")
        .to_string();

    let tokenizer_type = obj
        .get("tokenizer_type")
        .and_then(Value::as_str)
        .map(|s| ZiTokenizerType::from_model(s))
        .unwrap_or_default();

    let bucket_size = obj
        .get("bucket_size")
        .and_then(Value::as_u64)
        .unwrap_or(100) as usize;

    Ok(Box::new(ZiTokenHistogram::new(
        text_field,
        tokenizer_type,
        bucket_size,
    )))
}

#[allow(non_snake_case)]
pub fn count_tokens(text: &str, tokenizer_type: &ZiTokenizerType) -> usize {
    match tokenizer_type {
        ZiTokenizerType::Whitespace => text.split_whitespace().count(),
        ZiTokenizerType::Character => text.chars().count(),
        ZiTokenizerType::Word => {
            let word_count = text.split_whitespace().count();
            let chinese_chars = text
                .chars()
                .filter(|c| '\u{4e00}' <= *c && *c <= '\u{9fff}')
                .count();
            word_count + chinese_chars
        }
        ZiTokenizerType::Cl100kBase
        | ZiTokenizerType::P50kBase
        | ZiTokenizerType::P50kEdit
        | ZiTokenizerType::R50kBase
        | ZiTokenizerType::O200kBase => {
            if let Some(bpe) = get_bpe(tokenizer_type) {
                bpe.encode_with_special_tokens(text).len()
            } else {
                text.split_whitespace().count()
            }
        }
    }
}
