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

use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::operator::ZiCOperator;
use crate::operators::filter::ZiCFieldPath;
use crate::record::ZiCRecordBatch;

#[allow(non_snake_case)]
fn _ZiCQualityScoreCompute(text: &str) -> f64 {
    let len = text.chars().count() as f64;
    if len == 0.0 {
        return 0.0;
    }
    let ascii = text.chars().filter(|c| (*c as u32) <= 0x7F).count() as f64;
    let nonprint = text
        .chars()
        .filter(|c| (*c as u32) < 0x20 && *c != '\n' && *c != '\t')
        .count() as f64;
    let repeats = {
        let mut max_run = 1;
        let mut run = 1;
        let mut prev = '\0';
        for ch in text.chars() {
            if ch == prev {
                run += 1;
            } else {
                run = 1;
                prev = ch;
            }
            if run > max_run {
                max_run = run;
            }
        }
        max_run as f64
    };
    let ascii_ratio = ascii / len;
    let nonprint_ratio = nonprint / len;
    let repeat_penalty = (repeats - 1.0) / len.min(100.0);
    let base = 0.6 * ascii_ratio + 0.4 * (1.0 - nonprint_ratio);
    (base - repeat_penalty).clamp(0.0, 1.0)
}

#[derive(Debug)]
pub struct ZiCQualityScore {
    path: ZiCFieldPath,
    target_key: String,
}

impl ZiCQualityScore {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, target_key: String) -> Self {
        Self { path, target_key }
    }
}

impl ZiCOperator for ZiCQualityScore {
    fn name(&self) -> &'static str {
        "quality.score"
    }

    fn apply(&self, mut batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        for record in &mut batch {
            if let Some(Value::String(text)) = self.path.ZiFResolve(record) {
                let score = _ZiCQualityScoreCompute(text);
                record.ZiFMetadataMut().insert(
                    self.target_key.clone(),
                    Value::Number(serde_json::Number::from_f64(score).unwrap()),
                );
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
    Ok(Box::new(ZiCQualityScore::ZiFNew(field_path, key)))
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
];

#[derive(Debug, Clone)]
pub(crate) struct _ZiCToxicTerm {
    word: String,
    weight: f64,
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
        Self {
            path,
            target_key,
            lexicon,
        }
    }

    fn score_text(&self, text: &str) -> f64 {
        let tokens: Vec<String> = text
            .split(|c: char| !c.is_alphabetic())
            .filter(|token| !token.is_empty())
            .map(|token| token.to_lowercase())
            .collect();

        if tokens.is_empty() {
            return 0.0;
        }

        let mut total = 0.0;
        for token in tokens {
            for term in &self.lexicon {
                if token == term.word {
                    total += term.weight;
                }
            }
        }

        (total / self.lexicon.len() as f64).min(1.0)
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
                    let word = obj
                        .get("word")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            ZiError::validation("quality.toxicity lexicon entry missing 'word'")
                        })?
                        .to_lowercase();
                    let weight = obj
                        .get("weight")
                        .and_then(Value::as_f64)
                        .unwrap_or(1.0)
                        .clamp(0.0, 1.0);
                    Ok(_ZiCToxicTerm { word, weight })
                })
                .collect()
        })
        .transpose()?;

    let lexicon = lexicon.unwrap_or_else(|| {
        _ZiCDEFAULT_TOXIC_LEXICON
            .iter()
            .map(|(word, weight)| _ZiCToxicTerm {
                word: word.to_string(),
                weight: *weight,
            })
            .collect()
    });

    let field_path = ZiCFieldPath::ZiFParse(path)?;
    Ok(Box::new(ZiCToxicityScore::ZiFNew(field_path, key, lexicon)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::ZiCRecord;
    use serde_json::json;

    #[test]
    fn score_and_filter() {
        let s = ZiCQualityScore::ZiFNew(
            ZiCFieldPath::ZiFParse("payload.text").unwrap(),
            "quality".into(),
        );
        let f = ZiCQualityFilter::ZiFNew("quality".into(), 0.5);
        let batch = vec![
            ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "hello world"})),
            ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "@@@@@@"})),
        ];
        let scored = s.apply(batch).unwrap();
        let filtered = f.apply(scored).unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id.as_deref(), Some("1"));
    }

    #[test]
    fn toxicity_scores_text() {
        let op = ZiCToxicityScore::ZiFNew(
            ZiCFieldPath::ZiFParse("payload.text").unwrap(),
            "tox".into(),
            vec![
                _ZiCToxicTerm {
                    word: "hate".into(),
                    weight: 1.0,
                },
                _ZiCToxicTerm {
                    word: "love".into(),
                    weight: 0.2,
                },
            ],
        );

        let batch = vec![
            ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "I hate this"})),
            ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "Full of love"})),
        ];

        let scored = op.apply(batch).unwrap();
        let first = scored[0].metadata.as_ref().unwrap()["tox"]
            .as_f64()
            .unwrap();
        let second = scored[1].metadata.as_ref().unwrap()["tox"]
            .as_f64()
            .unwrap();
        assert!(first > second);
        assert!(first > 0.0);
    }
}
