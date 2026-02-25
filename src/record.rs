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

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Generic metadata map that may accompany a record.
pub type ZiMetadata = Map<String, Value>;

/// Fundamental data unit processed by Zi Core operators.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiRecord {
    /// Optional stable identifier for the record.
    pub id: Option<String>,
    /// Primary payload carrying user content.
    pub payload: Value,
    /// Additional attributes such as scores, tags, or provenance.
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
