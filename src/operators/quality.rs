//! Copyright 2025 Dunimd Team. All Rights Reserved.
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

use serde_json::{Map, Value};

use crate::errors::{Result, ZiError};
use crate::operator::ZiCOperator;
use crate::operators::filter::ZiCFieldPath;
use crate::record::ZiCRecordBatch;

#[allow(non_snake_case)]
fn _ZiFJsonNumber(value: f64) -> Value {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or_else(|| Value::Number(serde_json::Number::from(0)))
}

fn _normalize_toxic_token(token: &str) -> String {
    token
        .chars()
        .filter_map(|ch| {
            let lower = ch.to_ascii_lowercase();
            let mapped = match lower {
                '0' => 'o',
                '1' | '!' => 'i',
                '2' => 'z',
                '3' => 'e',
                '4' | '@' => 'a',
                '5' | '$' => 's',
                '6' => 'g',
                '7' => 't',
                '8' => 'b',
                '9' => 'g',
                '+' => 't',
                '|' => 'l',
                '(' => 'c',
                ')' => 'o',
                '{' | '}' => 'o',
                '[' | ']' => 'i',
                _ => lower,
            };
            if mapped.is_ascii_alphanumeric() {
                Some(mapped)
            } else {
                None
            }
        })
        .collect()
}

#[derive(Clone, Debug)]
struct _ZiCQualityScoreWeights {
    ascii_ratio: f64,
    entropy: f64,
    readability: f64,
    unique_tokens: f64,
    nonprint_cleanliness: f64,
    repeated_bigram_cleanliness: f64,
    char_run_cleanliness: f64,
    avg_word_len_penalty: f64,
    punctuation_balance: f64,
    symbol_ratio_penalty: f64,
}

impl Default for _ZiCQualityScoreWeights {
    fn default() -> Self {
        Self {
            ascii_ratio: 0.20,
            entropy: 0.15,
            readability: 0.15,
            unique_tokens: 0.15,
            nonprint_cleanliness: 0.10,
            repeated_bigram_cleanliness: 0.08,
            char_run_cleanliness: 0.08,
            avg_word_len_penalty: 0.05,
            punctuation_balance: 0.03,
            symbol_ratio_penalty: 0.01,
        }
    }
}

impl _ZiCQualityScoreWeights {
    fn total(&self) -> f64 {
        self.ascii_ratio
            + self.entropy
            + self.readability
            + self.unique_tokens
            + self.nonprint_cleanliness
            + self.repeated_bigram_cleanliness
            + self.char_run_cleanliness
            + self.avg_word_len_penalty
            + self.punctuation_balance
            + self.symbol_ratio_penalty
    }

    fn to_map(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("ascii_ratio".into(), _ZiFJsonNumber(self.ascii_ratio));
        map.insert("entropy".into(), _ZiFJsonNumber(self.entropy));
        map.insert("readability".into(), _ZiFJsonNumber(self.readability));
        map.insert("unique_tokens".into(), _ZiFJsonNumber(self.unique_tokens));
        map.insert(
            "nonprint_cleanliness".into(),
            _ZiFJsonNumber(self.nonprint_cleanliness),
        );
        map.insert(
            "repeated_bigram_cleanliness".into(),
            _ZiFJsonNumber(self.repeated_bigram_cleanliness),
        );
        map.insert(
            "char_run_cleanliness".into(),
            _ZiFJsonNumber(self.char_run_cleanliness),
        );
        map.insert(
            "avg_word_len_penalty".into(),
            _ZiFJsonNumber(self.avg_word_len_penalty),
        );
        map.insert(
            "punctuation_balance".into(),
            _ZiFJsonNumber(self.punctuation_balance),
        );
        map.insert(
            "symbol_ratio_penalty".into(),
            _ZiFJsonNumber(self.symbol_ratio_penalty),
        );
        map
    }

    fn from_json(obj: &Map<String, Value>) -> Result<Self> {
        let mut weights = _ZiCQualityScoreWeights::default();
        for (key, value) in obj {
            let weight = value.as_f64().ok_or_else(|| {
                ZiError::validation(format!("quality.score weight '{key}' must be a number"))
            })?;
            if !weight.is_finite() || weight < 0.0 {
                return Err(ZiError::validation(format!(
                    "quality.score weight '{key}' must be a finite, non-negative number"
                )));
            }
            match key.as_str() {
                "ascii_ratio" => weights.ascii_ratio = weight,
                "entropy" => weights.entropy = weight,
                "readability" => weights.readability = weight,
                "unique_tokens" => weights.unique_tokens = weight,
                "nonprint_cleanliness" => weights.nonprint_cleanliness = weight,
                "repeated_bigram_cleanliness" => weights.repeated_bigram_cleanliness = weight,
                "char_run_cleanliness" => weights.char_run_cleanliness = weight,
                "avg_word_len_penalty" => weights.avg_word_len_penalty = weight,
                "punctuation_balance" => weights.punctuation_balance = weight,
                "symbol_ratio_penalty" => weights.symbol_ratio_penalty = weight,
                _ => {
                    return Err(ZiError::validation(format!(
                        "quality.score weights does not support key '{key}'"
                    )))
                }
            }
        }

        Ok(weights)
    }
}

#[derive(Debug, Default)]
struct _ZiCQualityScoreComponents {
    ascii_ratio: f64,
    entropy: f64,
    readability: f64,
    unique_tokens: f64,
    nonprint_cleanliness: f64,
    repeated_bigram_cleanliness: f64,
    char_run_cleanliness: f64,
    avg_word_len_penalty: f64,
    punctuation_balance: f64,
    symbol_ratio_penalty: f64,
}

impl _ZiCQualityScoreComponents {
    fn from_text(text: &str) -> Self {
        if text.trim().is_empty() {
            return Self::default();
        }

        let chars: Vec<char> = text.chars().collect();
        let len = chars.len() as f64;
        if len == 0.0 {
            return Self::default();
        }

        let ascii_ratio = chars.iter().filter(|c| (**c as u32) <= 0x7F).count() as f64 / len;
        let nonprint_ratio = chars
            .iter()
            .filter(|c| (**c as u32) < 0x20 && **c != '\n' && **c != '\t')
            .count() as f64
            / len;

        // Character entropy (normalized)
        let mut freq: std::collections::HashMap<char, usize> = std::collections::HashMap::new();
        for c in &chars {
            *freq.entry(*c).or_insert(0) += 1;
        }
        let entropy = {
            let mut h = 0.0f64;
            for count in freq.values() {
                let p = *count as f64 / len;
                h -= p * p.max(f64::MIN_POSITIVE).ln();
            }
            let max_entropy = (freq.len() as f64).max(1.0).ln();
            if max_entropy > 0.0 {
                (h / max_entropy).clamp(0.0, 1.0)
            } else {
                0.0
            }
        };

        let tokens: Vec<&str> = text
            .split(|c: char| !c.is_alphanumeric())
            .filter(|token| !token.is_empty())
            .collect();
        let token_count = tokens.len() as f64;
        let avg_word_len = if token_count > 0.0 {
            tokens
                .iter()
                .map(|token| token.chars().count() as f64)
                .sum::<f64>()
                / token_count
        } else {
            0.0
        };
        let unique_tokens = {
            let set: std::collections::HashSet<&str> = tokens.iter().copied().collect();
            if token_count > 0.0 {
                set.len() as f64 / token_count
            } else {
                0.0
            }
        };

        let max_run = {
            let mut max_run = 1usize;
            let mut run = 1usize;
            for pair in chars.windows(2) {
                if pair[0] == pair[1] {
                    run += 1;
                    if run > max_run {
                        max_run = run;
                    }
                } else {
                    run = 1;
                }
            }
            max_run as f64
        };

        let repeated_bigrams = {
            let mut counts: std::collections::HashMap<(char, char), usize> =
                std::collections::HashMap::new();
            for pair in chars.windows(2) {
                *counts.entry((pair[0], pair[1])).or_insert(0) += 1;
            }
            counts.values().filter(|&&c| c > 1).count() as f64 / len.max(1.0)
        };

        let sentence_count = text.matches(&['.', '!', '?'][..]).count().max(1) as f64;
        let syllable_estimate = tokens
            .iter()
            .map(|token| -> f64 {
                let vowels = ['a', 'e', 'i', 'o', 'u', 'y'];
                let mut count = 0;
                let lowercase = token.to_lowercase();
                let chars: Vec<char> = lowercase.chars().collect();
                for (idx, ch) in chars.iter().enumerate() {
                    if vowels.contains(ch) {
                        if idx == 0 || !vowels.contains(&chars[idx - 1]) {
                            count += 1;
                        }
                    }
                }
                count.max(1) as f64
            })
            .sum::<f64>();
        let readability = if token_count > 0.0 {
            let words_per_sentence = token_count / sentence_count;
            let syllables_per_word = syllable_estimate / token_count;
            let score = 206.835 - 1.015 * words_per_sentence - 84.6 * syllables_per_word;
            (score / 100.0).clamp(0.0, 1.0)
        } else {
            0.0
        };

        let punctuation_balance = {
            let mut punctuation_counts: std::collections::HashMap<char, isize> =
                std::collections::HashMap::new();
            let pairs = [("()", '(', ')'), ("[]", '[', ']'), ("{}", '{', '}')];
            for (_, open, close) in &pairs {
                punctuation_counts.insert(*open, 0);
                punctuation_counts.insert(*close, 0);
            }
            for ch in text.chars() {
                if let Some(count) = punctuation_counts.get_mut(&ch) {
                    *count += 1;
                }
            }
            let mut imbalance = 0.0;
            let mut total = 0.0;
            for (_, open, close) in &pairs {
                let open_count = *punctuation_counts.get(open).unwrap_or(&0) as f64;
                let close_count = *punctuation_counts.get(close).unwrap_or(&0) as f64;
                imbalance += (open_count - close_count).abs();
                total += open_count + close_count;
            }
            if total <= 0.0 {
                1.0
            } else {
                (1.0 - (imbalance / total).clamp(0.0, 1.0)).clamp(0.0, 1.0)
            }
        };

        let symbol_ratio_penalty = {
            let symbols = text
                .chars()
                .filter(|ch| {
                    !(ch.is_alphanumeric()
                        || ch.is_whitespace()
                        || [',', '.', '!', '?', ';', ':', '-', '\'', '"'].contains(ch))
                })
                .count() as f64;
            (1.0 - (symbols / len.max(1.0)).min(1.0)).clamp(0.0, 1.0)
        };

        Self {
            ascii_ratio,
            entropy,
            readability,
            unique_tokens,
            nonprint_cleanliness: (1.0 - nonprint_ratio).clamp(0.0, 1.0),
            repeated_bigram_cleanliness: (1.0 - repeated_bigrams.clamp(0.0, 1.0)).clamp(0.0, 1.0),
            char_run_cleanliness: (1.0 - (max_run - 1.0) / len.min(200.0)).clamp(0.0, 1.0),
            avg_word_len_penalty: if token_count > 0.0 {
                (1.0 - (avg_word_len / 20.0).clamp(0.0, 1.0)).clamp(0.0, 1.0)
            } else {
                0.0
            },
            punctuation_balance,
            symbol_ratio_penalty,
        }
    }

    fn score(&self, weights: &_ZiCQualityScoreWeights) -> f64 {
        let mut score = self.ascii_ratio * weights.ascii_ratio
            + self.entropy * weights.entropy
            + self.readability * weights.readability
            + self.unique_tokens * weights.unique_tokens
            + self.nonprint_cleanliness * weights.nonprint_cleanliness
            + self.repeated_bigram_cleanliness * weights.repeated_bigram_cleanliness
            + self.char_run_cleanliness * weights.char_run_cleanliness
            + self.avg_word_len_penalty * weights.avg_word_len_penalty
            + self.punctuation_balance * weights.punctuation_balance
            + self.symbol_ratio_penalty * weights.symbol_ratio_penalty;

        if !score.is_finite() {
            score = 0.0;
        }

        score.clamp(0.0, 1.0)
    }

    fn to_component_map(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("ascii_ratio".into(), _ZiFJsonNumber(self.ascii_ratio));
        map.insert("entropy".into(), _ZiFJsonNumber(self.entropy));
        map.insert("readability".into(), _ZiFJsonNumber(self.readability));
        map.insert("unique_tokens".into(), _ZiFJsonNumber(self.unique_tokens));
        map.insert(
            "nonprint_cleanliness".into(),
            _ZiFJsonNumber(self.nonprint_cleanliness),
        );
        map.insert(
            "repeated_bigram_cleanliness".into(),
            _ZiFJsonNumber(self.repeated_bigram_cleanliness),
        );
        map.insert(
            "char_run_cleanliness".into(),
            _ZiFJsonNumber(self.char_run_cleanliness),
        );
        map.insert(
            "avg_word_len_penalty".into(),
            _ZiFJsonNumber(self.avg_word_len_penalty),
        );
        map.insert(
            "punctuation_balance".into(),
            _ZiFJsonNumber(self.punctuation_balance),
        );
        map.insert(
            "symbol_ratio_penalty".into(),
            _ZiFJsonNumber(self.symbol_ratio_penalty),
        );
        map
    }

    fn to_contribution_map(&self, weights: &_ZiCQualityScoreWeights) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert(
            "ascii_ratio".into(),
            _ZiFJsonNumber(self.ascii_ratio * weights.ascii_ratio),
        );
        map.insert(
            "entropy".into(),
            _ZiFJsonNumber(self.entropy * weights.entropy),
        );
        map.insert(
            "readability".into(),
            _ZiFJsonNumber(self.readability * weights.readability),
        );
        map.insert(
            "unique_tokens".into(),
            _ZiFJsonNumber(self.unique_tokens * weights.unique_tokens),
        );
        map.insert(
            "nonprint_cleanliness".into(),
            _ZiFJsonNumber(self.nonprint_cleanliness * weights.nonprint_cleanliness),
        );
        map.insert(
            "repeated_bigram_cleanliness".into(),
            _ZiFJsonNumber(self.repeated_bigram_cleanliness * weights.repeated_bigram_cleanliness),
        );
        map.insert(
            "char_run_cleanliness".into(),
            _ZiFJsonNumber(self.char_run_cleanliness * weights.char_run_cleanliness),
        );
        map.insert(
            "avg_word_len_penalty".into(),
            _ZiFJsonNumber(self.avg_word_len_penalty * weights.avg_word_len_penalty),
        );
        map.insert(
            "punctuation_balance".into(),
            _ZiFJsonNumber(self.punctuation_balance * weights.punctuation_balance),
        );
        map.insert(
            "symbol_ratio_penalty".into(),
            _ZiFJsonNumber(self.symbol_ratio_penalty * weights.symbol_ratio_penalty),
        );
        map
    }

    fn to_details_map(&self, weights: &_ZiCQualityScoreWeights, score: f64) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("score".into(), _ZiFJsonNumber(score));
        map.insert("components".into(), Value::Object(self.to_component_map()));
        map.insert("weights".into(), Value::Object(weights.to_map()));
        map.insert(
            "contributions".into(),
            Value::Object(self.to_contribution_map(weights)),
        );
        map.insert("weight_sum".into(), _ZiFJsonNumber(weights.total()));
        map
    }
}

#[derive(Debug)]
pub struct ZiCQualityScore {
    path: ZiCFieldPath,
    target_key: String,
    details_key: Option<String>,
    weights: _ZiCQualityScoreWeights,
}

impl ZiCQualityScore {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, target_key: String) -> Self {
        Self {
            path,
            target_key,
            details_key: None,
            weights: _ZiCQualityScoreWeights::default(),
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFWithDetails(mut self, details_key: Option<String>) -> Self {
        self.details_key = details_key;
        self
    }

    #[allow(non_snake_case)]
    fn ZiFWithWeights(mut self, weights: _ZiCQualityScoreWeights) -> Self {
        self.weights = weights;
        self
    }
}

impl ZiCOperator for ZiCQualityScore {
    fn name(&self) -> &'static str {
        "quality.score"
    }

    fn apply(&self, mut batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        for record in &mut batch {
            if let Some(Value::String(text)) = self.path.ZiFResolve(record) {
                let components = _ZiCQualityScoreComponents::from_text(text);
                let score = components.score(&self.weights);
                if let Some(number) = serde_json::Number::from_f64(score) {
                    record
                        .ZiFMetadataMut()
                        .insert(self.target_key.clone(), Value::Number(number));
                }

                if let Some(details_key) = &self.details_key {
                    let details = components.to_details_map(&self.weights, score);
                    record
                        .ZiFMetadataMut()
                        .insert(details_key.clone(), Value::Object(details));
                }
            }
        }
        Ok(batch)
    }
}

#[derive(Debug)]
pub struct ZiCQualityFilter {
    key: String,
    min: f64,
}

impl ZiCQualityFilter {
    #[allow(non_snake_case)]
    pub fn ZiFNew(key: String, min: f64) -> Self {
        Self { key, min }
    }
}

impl ZiCOperator for ZiCQualityFilter {
    fn name(&self) -> &'static str {
        "quality.filter"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|r| {
                r.metadata
                    .as_ref()
                    .and_then(|m| m.get(&self.key))
                    .and_then(Value::as_f64)
                    .map(|v| v >= self.min)
                    .unwrap_or(false)
            })
            .collect())
    }
}

#[allow(non_snake_case)]
pub fn ZiFQualityScoreFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("quality.score config must be object"))?;
    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("quality.score requires string 'path'"))?;
    let key = obj
        .get("key")
        .and_then(Value::as_str)
        .unwrap_or("quality")
        .to_string();
    let field_path = ZiCFieldPath::ZiFParse(path)?;
    let mut operator = ZiCQualityScore::ZiFNew(field_path, key);

    if let Some(Value::String(details_key)) = obj.get("details_key") {
        operator = operator.ZiFWithDetails(Some(details_key.to_string()));
    }

    if let Some(weights_obj) = obj.get("weights").and_then(Value::as_object) {
        let weights = _ZiCQualityScoreWeights::from_json(weights_obj)?;
        operator = operator.ZiFWithWeights(weights);
    }

    Ok(Box::new(operator))
}

#[allow(non_snake_case)]
pub fn ZiFQualityFilterFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("quality.filter config must be object"))?;
    let key = obj
        .get("key")
        .and_then(Value::as_str)
        .unwrap_or("quality")
        .to_string();
    let min = obj
        .get("min")
        .and_then(Value::as_f64)
        .ok_or_else(|| ZiError::validation("quality.filter requires numeric 'min'"))?;
    Ok(Box::new(ZiCQualityFilter::ZiFNew(key, min)))
}

#[allow(non_upper_case_globals)]
const _ZiCDEFAULT_TOXIC_LEXICON: &[(&str, f64)] = &[
    ("kill", 1.0),
    ("hate", 0.8),
    ("abuse", 0.8),
    ("stupid", 0.6),
    ("idiot", 0.6),
    ("violence", 0.7),
    ("shut up", 0.7),
    ("terrible person", 0.9),
];

#[derive(Debug, Clone)]
pub(crate) struct _ZiCToxicTerm {
    tokens: Vec<String>,
    normalized_tokens: Vec<String>,
    weight: f64,
}

impl _ZiCToxicTerm {
    fn from_tokens(tokens: Vec<String>, weight: f64) -> Self {
        let normalized_tokens = tokens
            .iter()
            .map(|token| _normalize_toxic_token(token))
            .collect();
        Self {
            tokens,
            normalized_tokens,
            weight,
        }
    }
}

#[derive(Debug)]
pub struct ZiCToxicityScore {
    path: ZiCFieldPath,
    target_key: String,
    lexicon: Vec<_ZiCToxicTerm>,
}

impl ZiCToxicityScore {
    #[allow(non_snake_case)]
    pub(crate) fn ZiFNew(
        path: ZiCFieldPath,
        target_key: String,
        lexicon: Vec<_ZiCToxicTerm>,
    ) -> Self {
        let normalized = lexicon
            .into_iter()
            .map(|term| {
                let normalized_tokens = term
                    .tokens
                    .iter()
                    .map(|token| _normalize_toxic_token(token))
                    .collect();
                _ZiCToxicTerm {
                    tokens: term.tokens,
                    normalized_tokens,
                    weight: term.weight,
                }
            })
            .collect();

        Self {
            path,
            target_key,
            lexicon: normalized,
        }
    }

    fn score_text(&self, text: &str) -> f64 {
        let alpha_tokens: Vec<String> = text
            .split(|c: char| !c.is_alphanumeric())
            .filter(|token| !token.is_empty())
            .map(|token| token.to_lowercase())
            .collect();

        let normalized_tokens: Vec<String> = alpha_tokens
            .iter()
            .map(|token| _normalize_toxic_token(token))
            .collect();

        if alpha_tokens.is_empty() {
            return 0.0;
        }

        let raw_tokens: Vec<&str> = text.split_whitespace().collect();
        let uppercase_tokens = raw_tokens
            .iter()
            .filter(|token| {
                let has_alpha = token.chars().any(|c| c.is_alphabetic());
                has_alpha && token == &&token.to_uppercase()
            })
            .count();
        let uppercase_ratio = uppercase_tokens as f64 / raw_tokens.len().max(1) as f64;

        let exclamations = text.chars().filter(|c| *c == '!').count();

        let negations: [&str; 6] = ["not", "never", "no", "without", "ain't", "hardly"];
        let mut match_weight_total = 0.0f64;
        let mut matches_count = 0usize;

        for term in &self.lexicon {
            let term_len = term.tokens.len();
            if term_len == 0 || term_len > alpha_tokens.len() {
                continue;
            }

            for window_start in 0..=alpha_tokens.len() - term_len {
                if normalized_tokens[window_start..window_start + term_len]
                    .iter()
                    .zip(&term.normalized_tokens)
                    .all(|(candidate, term_token)| candidate == term_token)
                {
                    let mut weight = term.weight;
                    if window_start > 0
                        && negations.contains(&alpha_tokens[window_start - 1].as_str())
                    {
                        weight *= 0.4;
                    } else if window_start > 1
                        && negations.contains(&alpha_tokens[window_start - 2].as_str())
                    {
                        weight *= 0.7;
                    }
                    match_weight_total += weight;
                    matches_count += 1;
                }
            }
        }

        let lexicon_size = self.lexicon.len().max(1) as f64;
        let weight_component = (match_weight_total / lexicon_size).min(1.0);
        let frequency_component =
            (matches_count as f64 / alpha_tokens.len().max(1) as f64).min(1.0);
        let emphasis_component = ((uppercase_ratio.min(1.0) * 0.5)
            + ((exclamations.min(6) as f64) / 6.0 * 0.3))
            .min(0.8);

        let base = 0.6 * weight_component + 0.25 * frequency_component + 0.15 * emphasis_component;
        base.clamp(0.0, 1.0)
    }
}

impl ZiCOperator for ZiCToxicityScore {
    fn name(&self) -> &'static str {
        "quality.toxicity"
    }

    fn apply(&self, mut batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        for record in &mut batch {
            if let Some(Value::String(text)) = self.path.ZiFResolve(record) {
                let score = self.score_text(text);
                let number = serde_json::Number::from_f64(score)
                    .unwrap_or_else(|| serde_json::Number::from(0));
                record
                    .ZiFMetadataMut()
                    .insert(self.target_key.clone(), Value::Number(number));
            }
        }
        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn ZiFToxicityFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("quality.toxicity config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("quality.toxicity requires string 'path'"))?;
    let key = obj
        .get("key")
        .and_then(Value::as_str)
        .unwrap_or("toxicity")
        .to_string();

    let lexicon = obj
        .get("lexicon")
        .and_then(Value::as_array)
        .map(|items| -> Result<Vec<_ZiCToxicTerm>> {
            items
                .iter()
                .map(|item| {
                    let obj = item.as_object().ok_or_else(|| {
                        ZiError::validation("quality.toxicity lexicon entries must be objects")
                    })?;
                    let text = obj
                        .get("word")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            ZiError::validation("quality.toxicity lexicon entry missing 'word'")
                        })?
                        .to_lowercase();
                    let tokens: Vec<String> = text
                        .split_whitespace()
                        .map(|s| s.to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
                    if tokens.is_empty() {
                        return Err(ZiError::validation(
                            "quality.toxicity lexicon entry must contain alphabetic characters",
                        ));
                    }
                    let weight = obj
                        .get("weight")
                        .and_then(Value::as_f64)
                        .unwrap_or(1.0)
                        .clamp(0.0, 1.0);
                    Ok(_ZiCToxicTerm::from_tokens(tokens, weight))
                })
                .collect()
        })
        .transpose()?;

    let lexicon = lexicon.unwrap_or_else(|| {
        _ZiCDEFAULT_TOXIC_LEXICON
            .iter()
            .map(|(word, weight)| {
                let mut tokens: Vec<String> = word
                    .split_whitespace()
                    .map(|s| s.to_lowercase())
                    .filter(|s| !s.is_empty())
                    .collect();
                if tokens.is_empty() {
                    tokens.push(word.to_lowercase());
                }
                _ZiCToxicTerm::from_tokens(tokens, *weight)
            })
            .collect()
    });

    let field_path = ZiCFieldPath::ZiFParse(path)?;
    Ok(Box::new(ZiCToxicityScore::ZiFNew(field_path, key, lexicon)))
}

