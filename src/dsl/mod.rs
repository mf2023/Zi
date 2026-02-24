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

//! # DSL (Domain Specific Language) Module
//!
//! This module provides a declarative domain-specific language for defining data processing
//! pipelines in the Zi framework. It enables users to define complex data transformation
//! workflows using a human-readable text format that gets parsed and compiled into executable
//! pipeline code.
//!
//! ## Architecture
//!
//! The DSL system consists of three main components:
//! - **Parser** ([parser.rs](parser/index.html)): Converts DSL source code (JSON, YAML, or custom syntax)
//!   into an intermediate representation (IR)
//! - **IR** ([ir.rs](ir/index.html)): The intermediate representation that represents the
//!   parsed pipeline structure
//! - **Compiler** ([compiler.rs](compiler/index.html)): Converts the IR into executable
//!   operator pipelines that can process data
//!
//! ## DSL Formats Supported
//!
//! The parser supports multiple input formats:
//! - **JSON**: Array of operator objects or object with "steps"/"pipeline" key
//! - **YAML**: Same structure as JSON using YAML syntax
//! - **Simple Syntax**: Line-based syntax with operator names and inline configuration
//!
//! ## Usage Example (JSON)
//!
//! ```json
//! [
//!   {
//!     "operator": "filter.equals",
//!     "config": {"path": "payload.lang", "value": "en"}
//!   },
//!   {
//!     "operator": "quality.score",
//!     "config": {"min_score": 0.8}
//!   },
//!   {
//!     "operator": "limit",
//!     "config": {"max_records": 1000}
//!   }
//! ]
//! ```
//!
//! ## Usage Example (Simple Syntax)
//!
//! ```text
//! filter.equals path=payload.lang value=en
//! quality.score min_score=0.8
//! limit max_records=1000
//! ```

pub mod parser;
pub mod ir;
pub mod compiler;

pub use parser::{ZiDSLParser, ZiParseResult, ZiDSLParserConfig};
pub use ir::{ZiDSLNode, ZiDSLProgram};
pub use compiler::{ZiDSLCompiler, ZiCompiledPipeline};
