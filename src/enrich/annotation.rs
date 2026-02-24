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

//! # Data Annotation Module
//!
//! This module provides annotation capabilities for adding metadata labels, scores,
//! and tags to data records.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::errors::Result;
use crate::record::{ZiRecord, ZiRecordBatch};

/// Configuration for data annotation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiAnnotationConfig {
    /// Target field path for annotation.
    pub field: String,
    /// Type of annotation to generate.
    pub annotation_type: ZiAnnotationType,
}

/// Types of annotations supported.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiAnnotationType {
    /// Categorical label annotation.
    Label { name: String },
    /// Numerical score annotation.
    Score { name: String },
    /// Multi-label tag annotation.
    Tag { name: String },
}

impl Default for ZiAnnotationConfig {
    fn default() -> Self {
        Self {
            field: "payload".to_string(),
            annotation_type: ZiAnnotationType::Label { name: "default".to_string() },
        }
    }
}

/// Annotator for adding metadata annotations to records.
#[derive(Debug)]
pub struct ZiAnnotator {
    config: ZiAnnotationConfig,
}

impl ZiAnnotator {
    /// Creates a new annotator with the given configuration.
    #[allow(non_snake_case)]
    pub fn new(config: ZiAnnotationConfig) -> Self {
        Self { config }
    }

    /// Annotates all records in a batch.
    #[allow(non_snake_case)]
    pub fn annotate(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        batch.into_iter().map(|record| self.annotate_record(record)).collect()
    }

    fn annotate_record(&self, mut record: ZiRecord) -> Result<ZiRecord> {
        let annotation_key = match &self.config.annotation_type {
            ZiAnnotationType::Label { name } => format!("label_{}", name),
            ZiAnnotationType::Score { name } => format!("score_{}", name),
            ZiAnnotationType::Tag { name } => format!("tag_{}", name),
        };

        let annotation_value = self.compute_annotation(&record);

        record.metadata_mut()
            .insert(annotation_key, annotation_value);

        Ok(record)
    }

    fn compute_annotation(&self, record: &ZiRecord) -> Value {
        match &self.config.annotation_type {
            ZiAnnotationType::Label { name } => {
                Value::String(format!("{}_auto", name))
            }
            ZiAnnotationType::Score { .. } => {
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
            ZiAnnotationType::Tag { name } => {
                Value::Array(vec![Value::String(format!("{}_tagged", name))])
            }
        }
    }
}
