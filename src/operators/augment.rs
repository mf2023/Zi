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

use rand::prelude::*;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::operator::ZiOperator;
use crate::operators::filter::ZiFieldPath;
use crate::record::ZiRecordBatch;

#[derive(Debug, Clone)]
pub struct _SynonymEntry {
    pub word: String,
    pub replacements: Vec<String>,
}

#[derive(Debug)]
pub struct _AugmentSynonym {
    path: ZiFieldPath,
    synonyms: Vec<_SynonymEntry>,
    seed: u64,
}

impl _AugmentSynonym {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, synonyms: Vec<_SynonymEntry>, seed: u64) -> Self {
        Self {
            path,
            synonyms,
            seed,
        }
    }

    #[allow(non_snake_case)]
    fn replace(&self, text: &str, rng: &mut SmallRng) -> String {
        if self.synonyms.is_empty() {
            return text.to_string();
        }

        let mut words: Vec<String> = text.split_whitespace().map(|w| w.to_string()).collect();
        for entry in &self.synonyms {
            let probability = 1.0 / (entry.replacements.len() as f64 + 1.0);
            for word in &mut words {
                if word.eq_ignore_ascii_case(&entry.word) && rng.gen_bool(probability) {
                    if let Some(replacement) = entry.replacements.choose(rng) {
                        *word = replacement.clone();
                    }
                }
            }
        }

        words.join(" ")
    }
}

impl ZiOperator for _AugmentSynonym {
    fn name(&self) -> &'static str {
        "augment.synonym"
    }

    fn apply(&self, mut batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        let mut rng = SmallRng::seed_from_u64(self.seed);
        for record in &mut batch {
            if let Some(Value::String(text)) = self.path.resolve(record) {
                let augmented = self.replace(text.as_str(), &mut rng);
                let _ = self.path.set_value(record, Value::String(augmented));
            }
        }
        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn augment_synonym_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("augment.synonym config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("augment.synonym requires string 'path'"))?;

    let entries = obj
        .get("synonyms")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("augment.synonym requires array 'synonyms'"))?;

    if entries.is_empty() {
        return Err(ZiError::validation(
            "augment.synonym 'synonyms' may not be empty",
        ));
    }

    let synonyms = entries
        .iter()
        .map(|entry| {
            let obj = entry
                .as_object()
                .ok_or_else(|| ZiError::validation("augment.synonym entries must be objects"))?;
            let word = obj
                .get("word")
                .and_then(Value::as_str)
                .ok_or_else(|| ZiError::validation("augment.synonym entry missing 'word'"))?
                .to_lowercase();
            let replacements = obj
                .get("replacements")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    ZiError::validation("augment.synonym entry requires array 'replacements'")
                })?;
            if replacements.is_empty() {
                return Err(ZiError::validation(
                    "augment.synonym 'replacements' may not be empty",
                ));
            }
            let replacements = replacements
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .ok_or_else(|| {
                            ZiError::validation("augment.synonym replacements must be strings")
                        })
                        .map(|s| s.to_string())
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(_SynonymEntry { word, replacements })
        })
        .collect::<Result<Vec<_>>>()?;

    let seed = obj
        .get("seed")
        .and_then(Value::as_u64)
        .unwrap_or(0x1bad_b002);

    let field_path = ZiFieldPath::parse(path)?;
    Ok(Box::new(_AugmentSynonym::new(
        field_path, synonyms, seed,
    )))
}

#[derive(Debug)]
pub struct _AugmentNoise {
    path: ZiFieldPath,
    intensity: f64,
    seed: u64,
}

impl _AugmentNoise {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, intensity: f64, seed: u64) -> Self {
        Self {
            path,
            intensity,
            seed,
        }
    }
}

impl ZiOperator for _AugmentNoise {
    fn name(&self) -> &'static str {
        "augment.noise"
    }

    fn apply(&self, mut batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        let mut rng = SmallRng::seed_from_u64(self.seed);
        for record in &mut batch {
            if let Some(Value::String(text)) = self.path.resolve(record) {
                let toggled = text
                    .chars()
                    .map(|ch| {
                        if ch.is_alphabetic() && rng.gen_bool(self.intensity) {
                            if ch.is_lowercase() {
                                ch.to_ascii_uppercase()
                            } else {
                                ch.to_ascii_lowercase()
                            }
                        } else if ch.is_ascii_digit() && rng.gen_bool(self.intensity) {
                            ((ch as u8 - b'0' + 1) % 10 + b'0') as char
                        } else {
                            ch
                        }
                    })
                    .collect::<String>();
                let _ = self.path.set_value(record, Value::String(toggled));
            }
        }
        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn augment_noise_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("augment.noise config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("augment.noise requires string 'path'"))?;

    let intensity = obj.get("intensity").and_then(Value::as_f64).unwrap_or(0.1);

    if !(0.0..=1.0).contains(&intensity) {
        return Err(ZiError::validation(
            "augment.noise 'intensity' must be in [0,1]",
        ));
    }

    let seed = obj
        .get("seed")
        .and_then(Value::as_u64)
        .unwrap_or(0xfeed_f00d);

    let field_path = ZiFieldPath::parse(path)?;
    Ok(Box::new(_AugmentNoise::new(field_path, intensity, seed)))
}


