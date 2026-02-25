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

//! # Data Augmentation Module
//!
//! This module provides data augmentation capabilities for increasing dataset diversity
//! through various transformation techniques.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::errors::Result;
use crate::record::ZiRecordBatch;

/// Configuration for data augmentation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiAugmentationConfig {
    /// List of augmentation methods to apply.
    pub methods: Vec<ZiAugmentationMethod>,
    /// Whether to preserve original records in output.
    pub preserve_original: bool,
}

/// Augmentation methods supported.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiAugmentationMethod {
    /// Randomly shuffle record order.
    Shuffle,
    /// Create duplicate records with new IDs.
    Duplicate { count: usize },
    /// Add random noise by removing characters.
    Noise { ratio: f64 },
}

impl Default for ZiAugmentationConfig {
    fn default() -> Self {
        Self {
            methods: vec![ZiAugmentationMethod::Duplicate { count: 1 }],
            preserve_original: true,
        }
    }
}

/// Data augmenter for applying augmentation techniques.
#[derive(Debug)]
pub struct ZiAugmenter {
    config: ZiAugmentationConfig,
}

impl ZiAugmenter {
    /// Creates a new augmenter with the given configuration.
    #[allow(non_snake_case)]
    pub fn new(config: ZiAugmentationConfig) -> Self {
        Self { config }
    }

    /// Augments all records in a batch.
    #[allow(non_snake_case)]
    pub fn augment(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
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

    fn apply_method(&self, batch: &ZiRecordBatch, method: &ZiAugmentationMethod) -> Result<ZiRecordBatch> {
        match method {
            ZiAugmentationMethod::Shuffle => {
                let mut shuffled = batch.clone();
                use rand::seq::SliceRandom;
                let mut rng = rand::thread_rng();
                shuffled.shuffle(&mut rng);
                
                for (i, record) in shuffled.iter_mut().enumerate() {
                    record.metadata_mut()
                        .insert("augmented".to_string(), Value::Bool(true));
                    record.metadata_mut()
                        .insert("augmentation_method".to_string(), Value::String("shuffle".to_string()));
                    if let Some(id) = &record.id {
                        record.id = Some(format!("{}_shuf_{}", id, i));
                    }
                }
                
                Ok(shuffled)
            }
            ZiAugmentationMethod::Duplicate { count } => {
                let mut duplicated = Vec::new();
                
                for record in batch {
                    for i in 0..*count {
                        let mut new_record = record.clone();
                        if let Some(id) = &record.id {
                            new_record.id = Some(format!("{}_dup_{}", id, i));
                        }
                        new_record.metadata_mut()
                            .insert("augmented".to_string(), Value::Bool(true));
                        new_record.metadata_mut()
                            .insert("augmentation_method".to_string(), Value::String("duplicate".to_string()));
                        duplicated.push(new_record);
                    }
                }
                
                Ok(duplicated)
            }
            ZiAugmentationMethod::Noise { ratio } => {
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
                    
                    record.metadata_mut()
                        .insert("augmented".to_string(), Value::Bool(true));
                    record.metadata_mut()
                        .insert("augmentation_method".to_string(), Value::String("noise".to_string()));
                }
                
                Ok(noisy)
            }
        }
    }
}
