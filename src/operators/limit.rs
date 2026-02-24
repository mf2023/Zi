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
use crate::operator::ZiOperator;
use crate::record::ZiRecordBatch;

/// Truncates the incoming batch to at most `count` records.
#[derive(Debug)]
pub struct ZiLimit {
    count: usize,
}

impl ZiLimit {
    #[allow(non_snake_case)]
    pub fn new(count: usize) -> Self {
        Self { count }
    }
}

impl ZiOperator for ZiLimit {
    fn name(&self) -> &'static str {
        "limit"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch.into_iter().take(self.count).collect())
    }
}

#[allow(non_snake_case)]
pub fn limit_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let count = config
        .get("count")
        .and_then(Value::as_u64)
        .ok_or_else(|| ZiError::validation("limit requires unsigned integer 'count'"))?;

    Ok(Box::new(ZiLimit::new(count as usize)))
}

