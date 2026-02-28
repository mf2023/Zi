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

//! # Zi Operator Module
//!
//! This module defines the core operator trait and execution utilities for
//! the Zi data processing framework. Operators are the fundamental building
//! blocks that transform data within Zi pipelines.
//!
//! ## Operator Design
//!
//! Zi follows a batch-oriented processing model where operators receive a
//! collection of records (ZiRecordBatch), process them, and return a new
//! collection of records. This design enables:
//!
//! - **Parallel Processing**: Multiple records can be processed concurrently
//! - **Streaming Support**: Operators can emit fewer, equal, or more records
//!   than they receive
//! - **Error Isolation**: Failures in one operator don't necessarily crash
//!   the entire pipeline
//!
//! ## Implementing Custom Operators
//!
//! Any type implementing the `ZiOperator` trait can be used in Zi pipelines.
//! The trait requires implementing two methods:
//!
//! - `name()`: Returns a static string identifier for the operator
//! - `apply()`: Takes a batch of records and returns processed results
//!
//! ```rust
//! use zi::operator::ZiOperator;
//! use zi::record::{ZiRecord, ZiRecordBatch};
//! use zi::errors::Result;
//!
//! struct UppercaseOperator;
//!
//! impl ZiOperator for UppercaseOperator {
//!     fn name(&self) -> &'static str {
//!         "transform.uppercase"
//!     }
//!
//!     fn apply(&self, mut batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
//!         for record in &mut batch {
//!             if let Some(text) = record.payload.as_str() {
//!                 record.payload = serde_json::json!({
//!                     "text": text.to_uppercase()
//!                 });
//!             }
//!         }
//!         Ok(batch)
//!     }
//! }
//! ```
//!
//! ## Error Handling
//!
//! Operators return `Result<ZiRecordBatch, ZiError>` to handle failures
//! gracefully. The `execute_operator` helper function wraps operator execution
//! and adds operator context to error messages.

use crate::errors::{Result, ZiError};
use crate::record::ZiRecordBatch;

/// Contracts that every Zi Core operator must fulfill.
///
/// The ZiOperator trait is the core abstraction for data processing in Zi.
/// It defines the interface that all operators must implement to participate
/// in Zi pipelines.
///
/// # Design Philosophy
///
/// Operators are designed to be:
/// - **Stateless**: Operators should not maintain internal state between
///   invocations, enabling parallel execution
/// - **Deterministic**: Same input should always produce same output
/// - **Composable**: Operators can be chained together in pipelines
///
/// # Batch Processing
///
/// Operators operate on batches of records rather than individual records.
/// This design choice enables:
/// - Reduced function call overhead
/// - Better cache locality
/// - Natural parallelism at the batch level
pub trait ZiOperator: std::fmt::Debug {
    /// Unique, human-readable name for the operator.
    ///
    /// This name is used for:
    /// - Logging and debugging
    /// - Error messages
    /// - Pipeline visualization
    /// - Operator registry lookup
    fn name(&self) -> &'static str;

    /// Applies the operator to an incoming batch of records.
    ///
    /// # Arguments
    ///
    /// - `batch`: A vector of ZiRecord to process
    ///
    /// # Returns
    ///
    /// Returns a Result containing:
    /// - Ok(`Vec<ZiRecord>`): The processed batch (may contain 0, equal, or more records)
    /// - Err(ZiError): If processing failed
    ///
    /// # Notes
    ///
    /// - The operator may filter records (return empty Vec)
    /// - The operator may split records (return more records than input)
    /// - The operator may merge records (return fewer records than input)
    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch>;
}

/// Convenience helper to execute an operator while normalizing errors.
///
/// This function wraps operator execution to provide consistent error
/// handling. It captures the operator name and includes it in any
/// error that occurs during processing.
///
/// # Arguments
///
/// - `operator`: Reference to the ZiOperator to execute
/// - `batch`: Input record batch
///
/// # Returns
///
/// Result containing processed batch or ZiError with operator context
///
/// # Example
///
/// ```rust
/// use zi::operator::{ZiOperator, execute_operator};
/// use zi::record::ZiRecordBatch;
/// use zi::errors::Result;
///
/// fn process_with_op(op: &dyn ZiOperator, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
///     execute_operator(op, batch)
/// }
/// ```
#[allow(non_snake_case)]
pub fn execute_operator(
    operator: &dyn ZiOperator,
    batch: ZiRecordBatch,
) -> Result<ZiRecordBatch> {
    operator
        .apply(batch)
        .map_err(|err| ZiError::operator(operator.name(), err.to_string()))
}

