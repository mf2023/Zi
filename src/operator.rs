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

use crate::errors::{Result, ZiError};
use crate::record::ZiRecordBatch;

/// Contracts that every Zi Core operator must fulfill.
pub trait ZiOperator: std::fmt::Debug {
    /// Unique, human-readable name for the operator.
    fn name(&self) -> &'static str;

    /// Applies the operator to an incoming batch of records.
    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch>;
}

/// Convenience helper to execute an operator while normalizing errors.
#[allow(non_snake_case)]
pub fn execute_operator(
    operator: &dyn ZiOperator,
    batch: ZiRecordBatch,
) -> Result<ZiRecordBatch> {
    operator
        .apply(batch)
        .map_err(|err| ZiError::operator(operator.name(), err.to_string()))
}

