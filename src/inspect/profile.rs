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

use crate::record::{ZiCRecord, ZiCRecordBatch};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZiCFieldProfile {
    pub name: String,
    pub count: usize,
    pub null_count: usize,
    pub type_distribution: std::collections::HashMap<String, usize>,
    pub unique_count: Option<usize>,
    pub sample_values: Vec<Value>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZiCProfileReport {
    pub total_records: usize,
    pub total_fields: usize,
    pub field_profiles: Vec<ZiCFieldProfile>,
    pub avg_record_size: f64,
    pub memory_estimate: usize,
}

#[derive(Clone, Debug, Default)]
pub struct ZiCProfiler {
    sample_size: usize,
    max_unique_tracking: usize,
}

impl ZiCProfiler {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        Self {
            sample_size: 100,
            max_unique_tracking: 10000,
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFProfile(&self, batch: &ZiCRecordBatch) -> ZiCProfileReport {
        if batch.is_empty() {
            return ZiCProfileReport::default();
        }

        let total_records = batch.len();
        let mut field_profiles: std::collections::HashMap<String, ZiCFieldProfile> = 
            std::collections::HashMap::new();
        let mut total_size = 0usize;

        for record in batch {
            let record_size = Self::estimate_record_size(record);
            total_size += record_size;
            self.profile_record(record, &mut field_profiles);
        }

        let field_profiles: Vec<ZiCFieldProfile> = field_profiles.into_values().collect();
        let total_fields = field_profiles.len();

        ZiCProfileReport {
            total_records,
            total_fields,
            field_profiles,
            avg_record_size: total_size as f64 / total_records as f64,
            memory_estimate: total_size,
        }
    }

    fn profile_record(&self, record: &ZiCRecord, profiles: &mut std::collections::HashMap<String, ZiCFieldProfile>) {
        self.profile_value("payload", &record.payload, profiles);
        if let Some(meta) = &record.metadata {
            for (key, value) in meta {
                let path = format!("metadata.{}", key);
                self.profile_value(&path, value, profiles);
            }
        }
    }

    fn profile_value(&self, path: &str, value: &Value, profiles: &mut std::collections::HashMap<String, ZiCFieldProfile>) {
        let profile = profiles.entry(path.to_string()).or_insert_with(|| ZiCFieldProfile {
            name: path.to_string(),
            count: 0,
            null_count: 0,
            type_distribution: std::collections::HashMap::new(),
            unique_count: None,
            sample_values: Vec::new(),
        });

        profile.count += 1;

        let type_name = match value {
            Value::Null => {
                profile.null_count += 1;
                "null"
            }
            Value::Bool(_) => "bool",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        };

        *profile.type_distribution.entry(type_name.to_string()).or_insert(0) += 1;

        if profile.sample_values.len() < self.sample_size && !value.is_null() {
            profile.sample_values.push(value.clone());
        }
    }

    fn estimate_record_size(record: &ZiCRecord) -> usize {
        let payload_size = record.payload.to_string().len();
        let meta_size = record.metadata.as_ref().map(|m| m.to_string().len()).unwrap_or(0);
        payload_size + meta_size + 64
    }
}
