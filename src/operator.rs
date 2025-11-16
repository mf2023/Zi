//! Copyright © 2025 Wenze Wei. All Rights Reserved.
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

use crate::errors::{Result, ZiError};
use crate::record::ZiCRecordBatch;

/// Contracts that every Zi Core operator must fulfill.
pub trait ZiCOperator: std::fmt::Debug {
    /// Unique, human-readable name for the operator.
    fn name(&self) -> &'static str;

    /// Applies the operator to an incoming batch of records.
    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch>;
}

/// Convenience helper to execute an operator while normalizing errors.
#[allow(non_snake_case)]
pub fn ZiFExecuteOperator(
    operator: &dyn ZiCOperator,
    batch: ZiCRecordBatch,
) -> Result<ZiCRecordBatch> {
    operator
        .apply(batch)
        .map_err(|err| ZiError::operator(operator.name(), err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::ZiCRecord;
    use serde_json::json;

    #[derive(Debug)]
    struct PassThrough;

    impl ZiCOperator for PassThrough {
        fn name(&self) -> &'static str {
            "pass"
        }

        fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
            Ok(batch)
        }
    }

    #[derive(Debug)]
    struct Failing;

    impl ZiCOperator for Failing {
        fn name(&self) -> &'static str {
            "fail"
        }

        fn apply(&self, _batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
            Err(ZiError::validation("boom"))
        }
    }

    #[test]
    fn execute_success_returns_same_batch() {
        let batch = vec![ZiCRecord::ZiFNew(Some("id".into()), json!(1))];
        let result = ZiFExecuteOperator(&PassThrough, batch.clone()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].payload, json!(1));
    }

    #[test]
    fn execute_error_wraps_with_operator_name() {
        let batch = vec![ZiCRecord::ZiFNew(None, json!(null))];
        let err = ZiFExecuteOperator(&Failing, batch).unwrap_err();

        match err {
            ZiError::Operator { operator, message } => {
                assert_eq!(operator, "fail");
                assert!(message.contains("boom"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
