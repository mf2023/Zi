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
use crate::record::{ZiCRecord, ZiCRecordBatch};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCAnnotationConfig {
    pub field: String,
    pub annotation_type: ZiCAnnotationType,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiCAnnotationType {
    Label { name: String },
    Score { name: String },
    Tag { name: String },
}

impl Default for ZiCAnnotationConfig {
    fn default() -> Self {
        Self {
            field: "payload".to_string(),
            annotation_type: ZiCAnnotationType::Label { name: "default".to_string() },
        }
    }
}

#[derive(Debug)]
pub struct ZiCAnnotator {
    config: ZiCAnnotationConfig,
}

impl ZiCAnnotator {
    #[allow(non_snake_case)]
    pub fn ZiFNew(config: ZiCAnnotationConfig) -> Self {
        Self { config }
    }

    #[allow(non_snake_case)]
    pub fn ZiFAnnotate(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        batch.into_iter().map(|record| self.annotate_record(record)).collect()
    }

    fn annotate_record(&self, mut record: ZiCRecord) -> Result<ZiCRecord> {
        let annotation_key = match &self.config.annotation_type {
            ZiCAnnotationType::Label { name } => format!("label_{}", name),
            ZiCAnnotationType::Score { name } => format!("score_{}", name),
            ZiCAnnotationType::Tag { name } => format!("tag_{}", name),
        };

        let annotation_value = self.compute_annotation(&record);

        record.ZiFMetadataMut()
            .insert(annotation_key, annotation_value);

        Ok(record)
    }

    fn compute_annotation(&self, record: &ZiCRecord) -> Value {
        match &self.config.annotation_type {
            ZiCAnnotationType::Label { name } => {
                Value::String(format!("{}_auto", name))
            }
            ZiCAnnotationType::Score { .. } => {
                let text = match &record.payload {
                    Value::String(s) => s.clone(),
                    Value::Object(map) => {
                        map.values()
                            .filter_map(|v| v.as_str())
                            .collect::<Vec<_>>()
                            .join(" ")
                    }
                    _ => String::new(),
                };
                
                let score = (text.len() as f64 / 1000.0).min(1.0);
                Value::Number(serde_json::Number::from_f64(score).unwrap_or_else(|| serde_json::Number::from(0)))
            }
            ZiCAnnotationType::Tag { name } => {
                Value::Array(vec![Value::String(format!("{}_tagged", name))])
            }
        }
    }
}
