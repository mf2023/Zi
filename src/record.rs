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

//! # Zi Record Module
//!
//! This module provides the core data structures for representing individual
//! data records in the Zi framework. ZiRecord is the fundamental unit of data
//! that flows through Zi pipelines.
//!
//! ## Design Principles
//!
//! - **Flexibility**: Records use JSON (serde_json::Value) for payloads, enabling
//!   storage of structured and semi-structured data without strict schemas
//! - **Extensibility**: Optional metadata field allows attaching arbitrary
//!   key-value attributes to records
//! - **Immutability-friendly**: Clone-derivable design supports functional
//!   transformation patterns
//!
//! ## Usage Example
//!
//! ```rust
//! use zi::record::{ZiRecord, ZiMetadata};
//! use serde_json::json;
//!
//! // Create a simple record
//! let record = ZiRecord::new("id-001", json!({"text": "hello world"}));
//!
//! // Create a record with metadata
//! let mut metadata = ZiMetadata::new();
//! metadata.insert("source".to_string(), json!("api"));
//! metadata.insert("score".to_string(), json!(0.95));
//!
//! let record = ZiRecord::new("id-002", json!({"text": "example"}))
//!     .with_metadata(metadata);
//!
//! // Access and modify metadata
//! let record = ZiRecord::new(None, json!({"data": 42}))
//!     .with_metadata(json!({"tags": ["train", "labeled"]}));
//! ```

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Generic metadata map that may accompany a record.
///
/// This type alias represents a flexible key-value store for attaching
/// arbitrary attributes to ZiRecord instances. Common uses include:
/// - Provenance information (source, timestamp, author)
/// - Quality scores and confidence values
/// - Classification labels and tags
/// - Processing history and lineage
pub type ZiMetadata = Map<String, Value>;

/// Fundamental data unit processed by Zi Core operators.
///
/// ZiRecord is the core data structure in the Zi framework. Every piece of
/// data that flows through a Zi pipeline is represented as a ZiRecord. The
/// record contains an optional identifier, a flexible payload for storing
/// the actual data, and optional metadata for auxiliary information.
///
/// # Type Parameters
///
/// - The payload field uses serde_json::Value to support any JSON-serializable
///   data structure, including objects, arrays, strings, numbers, and booleans
///
/// # Thread Safety
///
/// ZiRecord implements Send + Sync, making it safe to share across threads
/// for concurrent processing in pipeline stages
///
/// # Serde Support
///
/// The struct derives Serialize and Deserialize, enabling:
/// - Easy persistence to disk or databases
/// - Network transmission
/// - Interoperability with other systems
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiRecord {
    /// Optional stable identifier for the record.
    ///
    /// The ID is used for tracking, deduplication, and referencing records
    /// throughout processing. While optional, providing stable IDs is
    /// recommended for production pipelines as they enable:
    /// - Deterministic output ordering
    /// - Checkpoint and recovery
    /// - Traceability and debugging
    pub id: Option<String>,

    /// Primary payload carrying user content.
    ///
    /// This field contains the main data to be processed. It uses JSON Value
    /// for maximum flexibility, supporting:
    /// - Structured objects with named fields
    /// - Arrays of values
    /// - Primitive values (strings, numbers, booleans)
    /// - Null values
    ///
    /// # Example Payloads
    ///
    /// ```json
    /// {"text": "Hello world", "language": "en"}
    /// ```
    /// ```json
    /// [{"frame": 1, "pixels": [...]}, {"frame": 2, "pixels": [...]}]
    /// ```
    /// ```json
    /// "simple text string"
    /// ```
    pub payload: Value,

    /// Additional attributes such as scores, tags, or provenance.
    ///
    /// Metadata provides a flexible way to attach auxiliary information
    /// to records without modifying the primary payload. Common metadata
    /// includes:
    /// - Quality scores and confidence values
    /// - Classification or annotation labels
    /// - Source information and timestamps
    /// - Processing history and transformations applied
    ///
    /// The metadata field is optional and uses serde_json::Map for
    /// arbitrary key-value storage. It is skipped during serialization
    /// when None to maintain backwards compatibility.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ZiMetadata>,
}

impl ZiRecord {
    /// Constructs a record with the given payload and optional identifier.
    #[allow(non_snake_case)]
    pub fn new(id: impl Into<Option<String>>, payload: Value) -> Self {
        ZiRecord {
            id: id.into(),
            payload,
            metadata: None,
        }
    }

    /// Attaches metadata to the record.
    #[allow(non_snake_case)]
    pub fn with_metadata(mut self, metadata: ZiMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Returns a mutable reference to the metadata map, creating it if necessary.
    #[allow(non_snake_case)]
    pub fn metadata_mut(&mut self) -> &mut ZiMetadata {
        if self.metadata.is_none() {
            self.metadata = Some(ZiMetadata::new());
        }
        self.metadata.as_mut().expect("metadata map missing")
    }
}

/// Convenience alias for working on batches of records.
pub type ZiRecordBatch = Vec<ZiRecord>;
