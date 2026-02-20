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

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::errors::Result;
use crate::record::ZiCRecordBatch;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCAugmentationConfig {
    pub methods: Vec<ZiCAugmentationMethod>,
    pub preserve_original: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiCAugmentationMethod {
    Shuffle,
    Duplicate { count: usize },
    Noise { ratio: f64 },
}

impl Default for ZiCAugmentationConfig {
    fn default() -> Self {
        Self {
            methods: vec![ZiCAugmentationMethod::Duplicate { count: 1 }],
            preserve_original: true,
        }
    }
}

#[derive(Debug)]
pub struct ZiCAugmenter {
    config: ZiCAugmentationConfig,
}

impl ZiCAugmenter {
    #[allow(non_snake_case)]
    pub fn ZiFNew(config: ZiCAugmentationConfig) -> Self {
        Self { config }
    }

    #[allow(non_snake_case)]
    pub fn ZiFAugment(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        let mut augmented = Vec::new();

        if self.config.preserve_original {
            augmented.extend(batch.clone());
        }

        for method in &self.config.methods {
            let method_results = self.apply_method(&batch, method)?;
            augmented.extend(method_results);
        }

        Ok(augmented)
    }

    fn apply_method(&self, batch: &ZiCRecordBatch, method: &ZiCAugmentationMethod) -> Result<ZiCRecordBatch> {
        match method {
            ZiCAugmentationMethod::Shuffle => {
                let mut shuffled = batch.clone();
                use rand::seq::SliceRandom;
                let mut rng = rand::thread_rng();
                shuffled.shuffle(&mut rng);
                
                for (i, record) in shuffled.iter_mut().enumerate() {
                    record.ZiFMetadataMut()
                        .insert("augmented".to_string(), Value::Bool(true));
                    record.ZiFMetadataMut()
                        .insert("augmentation_method".to_string(), Value::String("shuffle".to_string()));
                    if let Some(id) = &record.id {
                        record.id = Some(format!("{}_shuf_{}", id, i));
                    }
                }
                
                Ok(shuffled)
            }
            ZiCAugmentationMethod::Duplicate { count } => {
                let mut duplicated = Vec::new();
                
                for record in batch {
                    for i in 0..*count {
                        let mut new_record = record.clone();
                        if let Some(id) = &record.id {
                            new_record.id = Some(format!("{}_dup_{}", id, i));
                        }
                        new_record.ZiFMetadataMut()
                            .insert("augmented".to_string(), Value::Bool(true));
                        new_record.ZiFMetadataMut()
                            .insert("augmentation_method".to_string(), Value::String("duplicate".to_string()));
                        duplicated.push(new_record);
                    }
                }
                
                Ok(duplicated)
            }
            ZiCAugmentationMethod::Noise { ratio } => {
                let mut noisy = batch.clone();
                
                for record in noisy.iter_mut() {
                    if let Value::String(ref mut text) = record.payload {
                        let chars: Vec<char> = text.chars().collect();
                        let noise_count = (chars.len() as f64 * ratio) as usize;
                        
                        for _ in 0..noise_count.min(chars.len()) {
                            let idx = rand::random::<usize>() % chars.len();
                            text.replace_range(idx..idx+1, "");
                        }
                    }
                    
                    record.ZiFMetadataMut()
                        .insert("augmented".to_string(), Value::Bool(true));
                    record.ZiFMetadataMut()
                        .insert("augmentation_method".to_string(), Value::String("noise".to_string()));
                }
                
                Ok(noisy)
            }
        }
    }
}
