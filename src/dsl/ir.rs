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

//! # DSL Intermediate Representation (IR)
//!
//! This module defines the intermediate representation (IR) for the Zi DSL.
//! The IR is the in-memory representation of a parsed DSL program, consisting
//! of a list of operator nodes with their configurations.
//!
//! ## Data Flow
//!
//! ```text
//! DSL Source Code (JSON/YAML/Text)
//!            |
//!            v
//!        Parser
//!            |
//!            v
//!     ZiDSLProgram (IR)
//!            |
//!            v
//!        Compiler
//!            |
//!            v
//!   ZiCompiledPipeline
//!            |
//!            v
//!     Execution
//! ```
//!
//! ## IR Structure
//!
//! A ZiDSLProgram consists of:
//! - **nodes**: A vector of ZiDSLNode, each representing a single operator
//! - Each node contains:
//!   - **operator**: The operator type (e.g., "filter.equals", "quality.score")
//!   - **config**: JSON configuration for the operator
//!   - **input**: Optional input field path
//!   - **output**: Optional output field path

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Represents a single node in the DSL pipeline.
///
/// Each node corresponds to an operator that will be applied to the data.
/// The node contains the operator name and its configuration, along with
/// optional input/output field specifications.
///
/// # Fields
///
/// - `operator`: The operator type identifier (e.g., "filter.equals", "quality.score")
/// - `config`: JSON object containing operator-specific configuration
/// - `input`: Optional path to the input field (e.g., "payload.text")
/// - `output`: Optional path to the output field (e.g., "metadata.result")
///
/// # Example
///
/// ```json
/// {
///   "operator": "filter.equals",
///   "config": {"path": "payload.lang", "value": "en"},
///   "input": "payload",
///   "output": "filtered_payload"
/// }
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiDSLNode {
    /// The operator type identifier.
    /// This string maps to a factory function in the compiler.
    pub operator: String,
    /// Operator-specific configuration as JSON.
    /// The structure depends on the operator type.
    pub config: Value,
    /// Optional dot-notation path to input field.
    /// If None, the entire record is used as input.
    pub input: Option<String>,
    /// Optional dot-notation path to output field.
    /// If None, the result replaces the input.
    pub output: Option<String>,
}

/// The intermediate representation of a complete DSL program.
///
/// ZiDSLProgram is a collection of nodes that define a data processing pipeline.
/// Nodes are executed in order, with each node receiving the output of the previous node.
///
/// # Fields
///
/// - `nodes`: Vector of ZiDSLNode representing the pipeline steps
///
/// # Construction
///
/// Programs can be constructed programmatically using the builder pattern:
///
/// ```rust
/// use zi::dsl::ir::{ZiDSLNode, ZiDSLProgram};
/// use serde_json::json;
///
/// let program = ZiDSLProgram::new()
///     .add_node(ZiDSLNode {
///         operator: "filter.equals".to_string(),
///         config: json!({"path": "payload.lang", "value": "en"}),
///         input: None,
///         output: None,
///     })
///     .add_node(ZiDSLNode {
///         operator: "limit".to_string(),
///         config: json!({"max_records": 100}),
///         input: None,
///         output: None,
///     });
/// ```
///
/// # Serialization
///
/// Programs can be serialized to JSON for storage or transmission:
///
/// ```rust
/// let json = program.to_json()?;
/// let program = ZiDSLProgram::from_json(&json)?;
/// ```
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZiDSLProgram {
    /// Ordered list of operator nodes in the pipeline.
    /// Each node is executed in sequence.
    pub nodes: Vec<ZiDSLNode>,
}

impl ZiDSLProgram {
    /// Creates a new empty DSL program.
    ///
    /// # Returns
    ///
    /// A new ZiDSLProgram with an empty nodes vector
    #[allow(non_snake_case)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a node to the end of the pipeline.
    ///
    /// This method returns a new program with the node appended,
    /// following the builder pattern for immutability.
    ///
    /// # Arguments
    ///
    /// - `node`: The ZiDSLNode to add to the pipeline
    ///
    /// # Returns
    ///
    /// A new ZiDSLProgram with the node appended
    #[allow(non_snake_case)]
    pub fn add_node(mut self, node: ZiDSLNode) -> Self {
        self.nodes.push(node);
        self
    }

    /// Serializes the program to a pretty-printed JSON string.
    ///
    /// # Returns
    ///
    /// Result containing the JSON string or an error
    #[allow(non_snake_case)]
    pub fn to_json(&self) -> crate::errors::Result<String> {
        serde_json::to_string_pretty(self)
            .map_err(|e| crate::errors::ZiError::internal(format!("Failed to serialize program: {}", e)))
    }

    /// Deserializes a program from a JSON string.
    ///
    /// # Arguments
    ///
    /// - `json`: The JSON string to parse
    ///
    /// # Returns
    ///
    /// Result containing the parsed ZiDSLProgram or an error
    #[allow(non_snake_case)]
    pub fn from_json(json: &str) -> crate::errors::Result<Self> {
        serde_json::from_str(json)
            .map_err(|e| crate::errors::ZiError::validation(format!("Invalid program JSON: {}", e)))
    }
}
