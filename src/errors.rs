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

use std::io;

use thiserror::Error;
use zip::result::ZipError;

/// Convience result type used throughout Zi Core.
pub type Result<T> = std::result::Result<T, ZiError>;

/// Canonical error enumeration for Zi Core.
#[derive(Debug, Error)]
pub enum ZiError {
    /// Errors originating from filesystem or network IO.
    #[error("io error: {0}")]
    Io(#[from] io::Error),

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
    Serde(#[from] serde_json::Error),

    /// Errors originating from ZIP file operations.
    #[error("zip error: {0}")]
    Zip(#[from] ZipError),

    /// Catch-all variant for unexpected situations.
    #[error("internal error: {0}")]
    Internal(String),
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
