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

//! # Zi Error Module
//!
//! This module defines the error types and utilities used throughout the Zi
//! framework for consistent error handling and reporting.
//!
//! ## Error Handling Philosophy
//!
//! Zi uses a structured error approach with the following principles:
//!
//! - **Explicit Error Types**: Each error variant represents a specific category
//!   of failure, making it easier to handle errors appropriately
//! - **Context-Rich**: Errors include relevant context (operator names, stage
//!   names, detailed messages) to aid debugging
//! - **Recoverable**: Most errors are recoverable, allowing pipelines to handle
//!   failures gracefully
//! - **Serde Support**: Errors can be serialized/deserialized for logging,
//!   persistence, and network transmission
//!
//! ## Error Categories
//!
//! - **Io**: Filesystem and network errors
//! - **Schema**: Data structure and type mismatches
//! - **Validation**: Input validation failures
//! - **Operator**: Failures in operator implementations
//! - **Pipeline**: Pipeline orchestration failures
//! - **Serde**: Serialization/deserialization errors
//! - **Zip**: ZIP archive operation errors
//! - **Internal**: Unexpected internal failures
//!
//! ## Usage
//!
//! ```rust
//! use zi::errors::{Result, ZiError};
//!
//! fn example() -> Result<String> {
//!     // Validation error
//!     if some_condition {
//!         return Err(ZiError::validation("invalid input"));
//!     }
//!
//!     // Operator error with context
//!     let result = some_operation().map_err(|e| {
//!         ZiError::operator("my.operator", e.to_string())
//!     })?;
//!
//!     Ok(result)
//! }
//! ```

use std::io;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use zip::result::ZipError;

/// Convience result type used throughout Zi Core.
///
/// This is a type alias for `std::result::Result<T, ZiError>` that provides
/// a more concise way to write function signatures that return Zi errors.
///
/// # Example
///
/// ```rust
/// use zi::errors::Result;
///
/// fn process_data(input: &str) -> Result<String> {
///     if input.is_empty() {
///         return Err(ZiError::validation("input cannot be empty"));
///     }
///     Ok(input.to_uppercase())
/// }
/// ```
pub type Result<T> = std::result::Result<T, ZiError>;

/// Canonical error enumeration for Zi Core.
#[derive(Debug, Error, Serialize, Deserialize)]
pub enum ZiError {
    /// Errors originating from filesystem or network IO.
    #[error("io error: {0}")]
    Io(String),

    /// Errors caused by malformed schema or incompatible data layout.
    #[error("schema error: {message}")]
    Schema { message: String },

    /// Validation errors triggered by invalid parameters or inputs.
    #[error("validation error: {message}")]
    Validation { message: String },

    /// Any failure raised by an operator implementation.
    #[error("operator '{operator}' failed: {message}")]
    Operator { operator: String, message: String },

    /// Failures that occur while orchestrating a pipeline.
    #[error("pipeline error at stage '{stage}': {message}")]
    Pipeline { stage: String, message: String },

    /// Wrapper for serde-style serialization issues.
    #[error("serialization error: {0}")]
    Serde(String),

    /// Errors originating from ZIP file operations.
    #[error("zip error: {0}")]
    Zip(String),

    /// Catch-all variant for unexpected situations.
    #[error("internal error: {0}")]
    Internal(String),
}

impl From<io::Error> for ZiError {
    fn from(err: io::Error) -> Self {
        ZiError::Io(err.to_string())
    }
}

impl From<serde_json::Error> for ZiError {
    fn from(err: serde_json::Error) -> Self {
        ZiError::Serde(err.to_string())
    }
}

impl From<ZipError> for ZiError {
    fn from(err: ZipError) -> Self {
        ZiError::Zip(err.to_string())
    }
}

impl ZiError {
    /// Helper to construct simple validation errors.
    pub fn validation<T: Into<String>>(message: T) -> Self {
        ZiError::Validation {
            message: message.into(),
        }
    }

    /// Helper to construct schema errors.
    pub fn schema<T: Into<String>>(message: T) -> Self {
        ZiError::Schema {
            message: message.into(),
        }
    }

    /// Helper to construct operator errors.
    pub fn operator(name: impl Into<String>, message: impl Into<String>) -> Self {
        ZiError::Operator {
            operator: name.into(),
            message: message.into(),
        }
    }

    /// Helper to construct pipeline errors.
    pub fn pipeline(stage: impl Into<String>, message: impl Into<String>) -> Self {
        ZiError::Pipeline {
            stage: stage.into(),
            message: message.into(),
        }
    }

    /// Helper to construct internal errors.
    pub fn internal<T: Into<String>>(message: T) -> Self {
        ZiError::Internal(message.into())
    }
}
