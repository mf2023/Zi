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

use serde_json::json;
use Zi::operators::limit::{ZiCLimit, ZiFLimitFactory};
use Zi::record::ZiCRecord;

#[test]
fn ZiFTLimitTruncatesBatch() {
    let operator = ZiCLimit::ZiFNew(1);
    let batch = vec![
        ZiCRecord::ZiFNew(Some("a".into()), json!(1)),
        ZiCRecord::ZiFNew(Some("b".into()), json!(2)),
    ];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].id.as_deref(), Some("a"));
}

#[test]
fn ZiFTLimitFactoryReadsConfig() {
    let operator = ZiFLimitFactory(&json!({"count": 2})).unwrap();
    let batch = vec![
        ZiCRecord::ZiFNew(None, json!(1)),
        ZiCRecord::ZiFNew(None, json!(2)),
        ZiCRecord::ZiFNew(None, json!(3)),
    ];

    let output = operator.apply(batch).unwrap();
    assert_eq!(output.len(), 2);
}
