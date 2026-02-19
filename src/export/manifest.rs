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
use chrono::{DateTime, Utc};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCManifestFile {
    pub path: String,
    pub size: u64,
    pub hash: String,
    pub record_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCManifest {
    pub version: String,
    pub created_at: DateTime<Utc>,
    pub total_records: usize,
    pub total_size: u64,
    pub files: Vec<ZiCManifestFile>,
    pub metadata: std::collections::HashMap<String, String>,
}

impl Default for ZiCManifest {
    fn default() -> Self {
        Self {
            version: "1.0.0".to_string(),
            created_at: Utc::now(),
            total_records: 0,
            total_size: 0,
            files: Vec::new(),
            metadata: std::collections::HashMap::new(),
        }
    }
}

#[derive(Debug, Default)]
pub struct ZiCManifestBuilder {
    manifest: ZiCManifest,
}

impl ZiCManifestBuilder {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        Self {
            manifest: ZiCManifest::default(),
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFAddFile(mut self, path: &str, size: u64, hash: &str, record_count: usize) -> Self {
        self.manifest.files.push(ZiCManifestFile {
            path: path.to_string(),
            size,
            hash: hash.to_string(),
            record_count,
        });
        self.manifest.total_records += record_count;
        self.manifest.total_size += size;
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFAddMetadata(mut self, key: &str, value: &str) -> Self {
        self.manifest.metadata.insert(key.to_string(), value.to_string());
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFBuild(self) -> ZiCManifest {
        self.manifest
    }
}

impl ZiCManifest {
    #[allow(non_snake_case)]
    pub fn ZiFToJson(&self) -> crate::errors::Result<String> {
        serde_json::to_string_pretty(self)
            .map_err(|e| crate::errors::ZiError::internal(format!("Failed to serialize manifest: {}", e)))
    }

    #[allow(non_snake_case)]
    pub fn ZiFFromJson(json: &str) -> crate::errors::Result<Self> {
        serde_json::from_str(json)
            .map_err(|e| crate::errors::ZiError::validation(format!("Invalid manifest JSON: {}", e)))
    }
}
