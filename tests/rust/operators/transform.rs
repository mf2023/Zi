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

use zix::operators::transform::transform_normalize_factory;
use zix::ZiRecord;
use serde_json::json;

#[test]
fn transform_normalize_basic() {
    let config = json!({"path": "payload.text", "lowercase": true});
    let operator = transform_normalize_factory(&config).unwrap();
    let rec = ZiRecord::new(None, json!({"text": "  Hello   WORLD "}));
    let out = operator.apply(vec![rec]).unwrap();
    assert_eq!(out[0].payload["text"], json!("hello world"));
}
