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

use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::operator::ZiCOperator;
use crate::record::ZiCRecordBatch;

/// Truncates the incoming batch to at most `count` records.
#[derive(Debug)]
pub struct ZiCLimit {
    count: usize,
}

impl ZiCLimit {
    #[allow(non_snake_case)]
    pub fn ZiFNew(count: usize) -> Self {
        Self { count }
    }
}

impl ZiCOperator for ZiCLimit {
    fn name(&self) -> &'static str {
        "limit"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch.into_iter().take(self.count).collect())
    }
}

#[allow(non_snake_case)]
pub fn ZiFLimitFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let count = config
        .get("count")
        .and_then(Value::as_u64)
        .ok_or_else(|| ZiError::validation("limit requires unsigned integer 'count'"))?;

    Ok(Box::new(ZiCLimit::ZiFNew(count as usize)))
}

