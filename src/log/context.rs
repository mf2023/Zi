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

use std::collections::HashMap;

use serde_json::Value;

thread_local! {
    static LOG_CONTEXT: std::cell::RefCell<HashMap<String, Value>> =
        std::cell::RefCell::new(HashMap::new());
}

#[derive(Debug, Default)]
pub struct ZiCLogContext;

impl ZiCLogContext {
    /// Add or update contextual key/value pairs.
    #[allow(non_snake_case)]
    pub fn ZiFPut<I, K, V>(pairs: I)
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<Value>,
    {
        LOG_CONTEXT.with(|ctx| {
            let mut map = ctx.borrow_mut();
            for (k, v) in pairs {
                map.insert(k.into(), v.into());
            }
        });
    }

    /// Get a snapshot of the current logging context.
    #[allow(non_snake_case)]
    pub fn ZiFGet() -> HashMap<String, Value> {
        LOG_CONTEXT.with(|ctx| ctx.borrow().clone())
    }

    /// Clear the logging context.
    #[allow(non_snake_case)]
    pub fn ZiFClear() {
        LOG_CONTEXT.with(|ctx| {
            ctx.borrow_mut().clear();
        });
    }
}
