//! Copyright © 2025 Dunimd Team. All Rights Reserved.
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
use serde_json::{Map, Value};

/// Generic metadata map that may accompany a record.
pub type ZiCMetadata = Map<String, Value>;

/// Fundamental data unit processed by Zi Core operators.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCRecord {
    /// Optional stable identifier for the record.
    pub id: Option<String>,
    /// Primary payload carrying user content.
    pub payload: Value,
    /// Additional attributes such as scores, tags, or provenance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ZiCMetadata>,
}

impl ZiCRecord {
    /// Constructs a record with the given payload and optional identifier.
    #[allow(non_snake_case)]
    pub fn ZiFNew(id: impl Into<Option<String>>, payload: Value) -> Self {
        ZiCRecord {
            id: id.into(),
            payload,
            metadata: None,
        }
    }

    /// Attaches metadata to the record.
    #[allow(non_snake_case)]
    pub fn ZiFWithMetadata(mut self, metadata: ZiCMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Returns a mutable reference to the metadata map, creating it if necessary.
    #[allow(non_snake_case)]
    pub fn ZiFMetadataMut(&mut self) -> &mut ZiCMetadata {
        if self.metadata.is_none() {
            self.metadata = Some(ZiCMetadata::new());
        }
        self.metadata.as_mut().expect("metadata map missing")
    }
}

/// Convenience alias for working on batches of records.
pub type ZiCRecordBatch = Vec<ZiCRecord>;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn new_sets_id_and_payload_without_metadata() {
        let record = ZiCRecord::ZiFNew(Some("sample-id".to_string()), json!({"text": "hello"}));

        assert_eq!(record.id.as_deref(), Some("sample-id"));
        assert_eq!(record.payload, json!({"text": "hello"}));
        assert!(record.metadata.is_none());
    }

    #[test]
    fn with_metadata_attaches_metadata_map() {
        let mut metadata = ZiCMetadata::new();
        metadata.insert("quality".to_string(), json!(0.95));

        let record = ZiCRecord::ZiFNew(None, json!("payload")).ZiFWithMetadata(metadata.clone());

        assert!(record.id.is_none());
        assert_eq!(record.metadata.as_ref(), Some(&metadata));
    }

    #[test]
    fn metadata_mut_creates_map_on_demand() {
        let mut record = ZiCRecord::ZiFNew(None, json!(42));
        record
            .ZiFMetadataMut()
            .insert("score".to_string(), json!(0.8));

        assert_eq!(record.metadata.unwrap().get("score"), Some(&json!(0.8)));
    }

    #[test]
    fn record_batch_alias_handles_multiple_records() {
        let batch: ZiCRecordBatch = vec![
            ZiCRecord::ZiFNew(Some("a".into()), json!(1)),
            ZiCRecord::ZiFNew(Some("b".into()), json!(2)),
        ];

        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].payload, json!(1));
        assert_eq!(batch[1].payload, json!(2));
    }
}
