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

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCDSLNode {
    pub operator: String,
    pub config: Value,
    pub input: Option<String>,
    pub output: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZiCDSLProgram {
    pub nodes: Vec<ZiCDSLNode>,
}

impl ZiCDSLProgram {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        Self::default()
    }

    #[allow(non_snake_case)]
    pub fn ZiFAddNode(mut self, node: ZiCDSLNode) -> Self {
        self.nodes.push(node);
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFToJson(&self) -> crate::errors::Result<String> {
        serde_json::to_string_pretty(self)
            .map_err(|e| crate::errors::ZiError::internal(format!("Failed to serialize program: {}", e)))
    }

    #[allow(non_snake_case)]
    pub fn ZiFFromJson(json: &str) -> crate::errors::Result<Self> {
        serde_json::from_str(json)
            .map_err(|e| crate::errors::ZiError::validation(format!("Invalid program JSON: {}", e)))
    }
}
