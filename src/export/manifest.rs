//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
//!
//! This file is part of Zi.
//! The Zi project belongs to the Dunimd project team.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//!     http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::path::Path;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

use crate::errors::{Result, ZiError};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCManifestFile {
    pub path: String,
    pub size: u64,
    pub hash: String,
    pub hash_algorithm: String,
    pub record_count: usize,
    pub created_at: DateTime<Utc>,
    pub compression: Option<String>,
    pub format: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCLineageSource {
    pub path: String,
    pub hash: String,
    pub record_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCLineageTransform {
    pub operator: String,
    pub config: serde_json::Value,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCLineage {
    pub sources: Vec<ZiCLineageSource>,
    pub transforms: Vec<ZiCLineageTransform>,
    pub pipeline_hash: Option<String>,
}

impl Default for ZiCLineage {
    fn default() -> Self {
        Self {
            sources: Vec::new(),
            transforms: Vec::new(),
            pipeline_hash: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCManifestStats {
    pub total_records: usize,
    pub total_size: u64,
    pub total_files: usize,
    pub avg_record_size: f64,
    pub formats: HashMap<String, usize>,
    pub compressions: HashMap<String, usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCManifest {
    pub version: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub files: Vec<ZiCManifestFile>,
    pub lineage: ZiCLineage,
    pub metadata: HashMap<String, String>,
    pub checksum: Option<String>,
}

impl Default for ZiCManifest {
    fn default() -> Self {
        Self {
            version: "1.0.0".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            files: Vec::new(),
            lineage: ZiCLineage::default(),
            metadata: HashMap::new(),
            checksum: None,
        }
    }
}

impl ZiCManifest {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        Self::default()
    }

    #[allow(non_snake_case)]
    pub fn ZiFStats(&self) -> ZiCManifestStats {
        let total_records: usize = self.files.iter().map(|f| f.record_count).sum();
        let total_size: u64 = self.files.iter().map(|f| f.size).sum();
        let total_files = self.files.len();
        let avg_record_size = if total_records > 0 {
            total_size as f64 / total_records as f64
        } else {
            0.0
        };

        let mut formats: HashMap<String, usize> = HashMap::new();
        let mut compressions: HashMap<String, usize> = HashMap::new();

        for file in &self.files {
            *formats.entry(file.format.clone()).or_insert(0) += 1;
            if let Some(ref comp) = file.compression {
                *compressions.entry(comp.clone()).or_insert(0) += 1;
            } else {
                *compressions.entry("none".to_string()).or_insert(0) += 1;
            }
        }

        ZiCManifestStats {
            total_records,
            total_size,
            total_files,
            avg_record_size,
            formats,
            compressions,
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFToJson(&self) -> Result<String> {
        serde_json::to_string_pretty(self)
            .map_err(|e| ZiError::internal(format!("Failed to serialize manifest: {}", e)))
    }

    #[allow(non_snake_case)]
    pub fn ZiFFromJson(json: &str) -> Result<Self> {
        serde_json::from_str(json)
            .map_err(|e| ZiError::validation(format!("Invalid manifest JSON: {}", e)))
    }

    #[allow(non_snake_case)]
    pub fn ZiFSave(&self, path: &Path) -> Result<()> {
        let json = self.ZiFToJson()?;
        std::fs::write(path, json)?;
        Ok(())
    }

    #[allow(non_snake_case)]
    pub fn ZiFLoad(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Self::ZiFFromJson(&content)
    }

    #[allow(non_snake_case)]
    pub fn ZiFComputeChecksum(&mut self) -> Result<String> {
        let mut hasher = blake3::Hasher::new();
        
        for file in &self.files {
            hasher.update(file.path.as_bytes());
            hasher.update(&file.size.to_le_bytes());
            hasher.update(file.hash.as_bytes());
            hasher.update(&file.record_count.to_le_bytes());
        }

        let hash = hasher.finalize();
        let checksum = hash.to_hex().to_string();
        self.checksum = Some(checksum.clone());
        self.updated_at = Utc::now();
        Ok(checksum)
    }

    #[allow(non_snake_case)]
    pub fn ZiFVerifyChecksum(&self) -> Result<bool> {
        let expected = match &self.checksum {
            Some(c) => c.clone(),
            None => return Ok(false),
        };

        let mut temp = self.clone();
        temp.checksum = None;
        let actual = temp.ZiFComputeChecksum()?;
        Ok(expected == actual)
    }

    #[allow(non_snake_case)]
    pub fn ZiFAddFile(&mut self, file: ZiCManifestFile) {
        self.files.push(file);
        self.updated_at = Utc::now();
    }

    #[allow(non_snake_case)]
    pub fn ZiFAddSource(&mut self, source: ZiCLineageSource) {
        self.lineage.sources.push(source);
        self.updated_at = Utc::now();
    }

    #[allow(non_snake_case)]
    pub fn ZiFAddTransform(&mut self, transform: ZiCLineageTransform) {
        self.lineage.transforms.push(transform);
        self.updated_at = Utc::now();
    }

    #[allow(non_snake_case)]
    pub fn ZiFSetMetadata(&mut self, key: &str, value: &str) {
        self.metadata.insert(key.to_string(), value.to_string());
        self.updated_at = Utc::now();
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
    pub fn ZiFWithVersion(mut self, version: &str) -> Self {
        self.manifest.version = version.to_string();
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFAddFile(mut self, file: ZiCManifestFile) -> Self {
        self.manifest.files.push(file);
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFAddFileInfo(mut self, path: &str, size: u64, hash: &str, record_count: usize, format: &str) -> Self {
        self.manifest.files.push(ZiCManifestFile {
            path: path.to_string(),
            size,
            hash: hash.to_string(),
            hash_algorithm: "blake3".to_string(),
            record_count,
            created_at: Utc::now(),
            compression: None,
            format: format.to_string(),
        });
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFAddSource(mut self, path: &str, hash: &str, record_count: usize) -> Self {
        self.manifest.lineage.sources.push(ZiCLineageSource {
            path: path.to_string(),
            hash: hash.to_string(),
            record_count,
        });
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFAddTransform(mut self, operator: &str, config: serde_json::Value) -> Self {
        self.manifest.lineage.transforms.push(ZiCLineageTransform {
            operator: operator.to_string(),
            config,
            timestamp: Utc::now(),
        });
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFSetPipelineHash(mut self, hash: &str) -> Self {
        self.manifest.lineage.pipeline_hash = Some(hash.to_string());
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFAddMetadata(mut self, key: &str, value: &str) -> Self {
        self.manifest.metadata.insert(key.to_string(), value.to_string());
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFBuild(self) -> Result<ZiCManifest> {
        let mut manifest = self.manifest;
        manifest.ZiFComputeChecksum()?;
        Ok(manifest)
    }

    #[allow(non_snake_case)]
    pub fn ZiFBuildWithoutChecksum(self) -> ZiCManifest {
        self.manifest
    }
}

pub fn compute_file_hash(path: &Path) -> Result<String> {
    use std::io::Read;

    let file = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    let mut hasher = blake3::Hasher::new();
    
    let mut buffer = [0u8; 8192];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => hasher.update(&buffer[..n]),
            Err(e) => return Err(ZiError::io(format!("Failed to read file: {}", e))),
        }
    }

    Ok(hasher.finalize().to_hex().to_string())
}
