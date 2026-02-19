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
pub struct ZiCSynthesisConfig {
    pub template: String,
    pub count: usize,
    pub seed: Option<u64>,
}

impl Default for ZiCSynthesisConfig {
    fn default() -> Self {
        Self {
            template: "{{text}}".to_string(),
            count: 100,
            seed: None,
        }
    }
}

#[derive(Debug)]
pub struct ZiCSynthesizer {
    config: ZiCSynthesisConfig,
}

impl ZiCSynthesizer {
    #[allow(non_snake_case)]
    pub fn ZiFNew(config: ZiCSynthesisConfig) -> Self {
        Self { config }
    }

    #[allow(non_snake_case)]
    pub fn ZiFSynthesize(&self, batch: &ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        let mut synthesized = Vec::new();

        for record in batch {
            for i in 0..self.config.count {
                let new_record = self.synthesize_record(record, i)?;
                synthesized.push(new_record);
            }
        }

        Ok(synthesized)
    }

    fn synthesize_record(&self, record: &ZiCRecord, index: usize) -> Result<ZiCRecord> {
        let mut new_record = record.clone();
        
        if let Some(id) = &record.id {
            new_record.id = Some(format!("{}_synth_{}", id, index));
        }

        new_record.ZiFMetadataMut()
            .insert("synthesized".to_string(), Value::Bool(true));
        new_record.ZiFMetadataMut()
            .insert("synthesis_index".to_string(), Value::Number(index.into()));

        Ok(new_record)
    }
}
