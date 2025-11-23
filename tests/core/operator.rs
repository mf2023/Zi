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

use serde_json::json;
use Zi::operator::{ZiCOperator, ZiFExecuteOperator};
use Zi::record::{ZiCRecord, ZiCRecordBatch};
use Zi::errors::{Result, ZiError};

#[derive(Debug)]
struct ZiCTPassThrough;

impl ZiCOperator for ZiCTPassThrough {
    fn name(&self) -> &'static str {
        "pass"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch)
    }
}

#[derive(Debug)]
struct ZiCTFailing;

impl ZiCOperator for ZiCTFailing {
    fn name(&self) -> &'static str {
        "fail"
    }

    fn apply(&self, _batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Err(ZiError::validation("boom"))
    }
}

#[test]
fn ZiFTOperatorExecuteSuccessReturnsSameBatch() {
    let batch = vec![ZiCRecord::ZiFNew(Some("id".into()), json!(1))];
    let result = ZiFExecuteOperator(&ZiCTPassThrough, batch.clone()).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].payload, json!(1));
}

#[test]
fn ZiFTOperatorExecuteErrorWrapsWithOperatorName() {
    let batch = vec![ZiCRecord::ZiFNew(None, json!(null))];
    let err = ZiFExecuteOperator(&ZiCTFailing, batch).unwrap_err();

    match err {
        ZiError::Operator { operator, message } => {
            assert_eq!(operator, "fail");
            assert!(message.contains("boom"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
