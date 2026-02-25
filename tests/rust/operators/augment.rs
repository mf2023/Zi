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

use zix::operators::augment::augment_noise_factory;
use zix::ZiRecord;
use serde_json::json;

#[test]
fn augment_noise_basic() {
    let config = json!({"path": "payload.text", "intensity": 0.1, "seed": 42});
    let operator = augment_noise_factory(&config).unwrap();
    let rec = ZiRecord::new(None, json!({"text": "hello world"}));
    let out = operator.apply(vec![rec]).unwrap();
    assert!(!out.is_empty());
}
