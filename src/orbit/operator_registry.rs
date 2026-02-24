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

//! # Operator Registry Module
//!
//! This module provides the operator registry for ZiOrbit, managing the registration
//! and lookup of operator factories at runtime.
//!
//! ## Registry Operations
//!
//! - Register operator factories with unique names
//! - Query available operators
//! - Create operator instances from configuration

use std::collections::HashMap;

use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::operator::ZiOperator;

/// Type alias for an operator factory used by ZiOrbit. Given a JSON config
/// value, it returns a boxed operator ready to be applied to a record batch.
pub type OperatorFactory = fn(&Value) -> Result<Box<dyn ZiOperator + Send + Sync>>;

/// Registry mapping operator names to their factory functions.
#[derive(Debug, Default)]
pub struct ZiOperatorRegistry {
    inner: HashMap<String, OperatorFactory>,
}

impl ZiOperatorRegistry {
    #[allow(non_snake_case)]
    pub fn new() -> Self {
        ZiOperatorRegistry {
            inner: HashMap::new(),
        }
    }

    #[allow(non_snake_case)]
    pub fn register(&mut self, name: &str, factory: OperatorFactory) {
        self.inner.insert(name.to_string(), factory);
    }

    pub fn get(&self, name: &str) -> Result<OperatorFactory> {
        self.inner
            .get(name)
            .copied()
            .ok_or_else(|| ZiError::internal(format!("unknown operator: {}", name)))
    }
}
