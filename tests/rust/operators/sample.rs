//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
//!
//! This file is part of Zi.
//! The Zi project belongs to the Dunimd Team.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//!
//! You may obtain a copy of the License at
//!
//!     http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

//! # Zi Operator Tests - Sample
//!
//! This module contains tests for sample operators in the Zi framework.
//! Sample operators are used for random sampling, stratified sampling, and
//! other sampling strategies.
//!
//! ## Running Tests
//!
//! ```bash
//! cargo test --test sample
//! ```

use zix::operators::sample::sample_random_factory;
use zix::ZiRecord;
use serde_json::json;

/// Tests the random sampling operator factory.
///
/// Verifies that a random sample operator can be created from configuration
/// and correctly samples records based on the specified ratio.
#[test]
fn sample_random_factory_basic() {
    let config = json!({"ratio": 0.5, "seed": 42});
    let operator = sample_random_factory(&config).unwrap();
    let mut batch = Vec::new();
    for i in 0..10 {
        batch.push(ZiRecord::new(Some(i.to_string()), json!({"v": i})));
    }
    let out = operator.apply(batch).unwrap();
    assert!(out.len() >= 3 && out.len() <= 7);
}
