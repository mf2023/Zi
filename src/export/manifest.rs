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

//! # Dataset Manifest Module
//!
//! This module provides manifest management for datasets, including metadata tracking,
//! lineage recording, integrity verification, and statistics computation.

use std::path::Path;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

use crate::errors::Result;

/// Represents a file in the dataset manifest.
///
/// Tracks file location, size, hash, format, and other metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiManifestFile {
    /// Relative or absolute file path.
    pub path: String,
    /// File size in bytes.
    pub size: u64,
    /// Content hash for integrity verification.
    pub hash: String,
    /// Hash algorithm used (e.g., "blake3", "sha256").
    pub hash_algorithm: String,
    /// Number of records in the file.
    pub record_count: usize,
    /// File creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Compression type if applied (e.g., "gzip", "zstd").
    pub compression: Option<String>,
    /// Data format (e.g., "jsonl", "csv", "parquet").
    pub format: String,
}

/// Represents a data source in the lineage.
///
/// Records the origin of data including source file and content hash.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiLineageSource {
    /// Source file path.
    pub path: String,
    /// Source file content hash.
    pub hash: String,
    /// Number of records from this source.
    pub record_count: usize,
}

/// Represents a transformation applied to the data.
///
/// Tracks operators, configurations, and timestamps for reproducibility.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiLineageTransform {
    /// Operator name (e.g., "filter", "map", "aggregate").
    pub operator: String,
    /// Operator configuration as JSON.
    pub config: serde_json::Value,
    /// Timestamp when transform was applied.
    pub timestamp: DateTime<Utc>,
}

/// Data lineage tracking container.
///
/// Maintains history of data sources and transformations for reproducibility.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiLineage {
    /// List of data sources.
    pub sources: Vec<ZiLineageSource>,
    /// List of transformations applied.
    pub transforms: Vec<ZiLineageTransform>,
    /// Hash of the entire pipeline for identification.
    pub pipeline_hash: Option<String>,
}

impl Default for ZiLineage {
    fn default() -> Self {
        Self {
            sources: Vec::new(),
            transforms: Vec::new(),
            pipeline_hash: None,
        }
    }
}

/// Statistics computed from the manifest.
///
/// Provides aggregate metrics about the dataset.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiManifestStats {
    /// Total number of records across all files.
    pub total_records: usize,
    /// Total size in bytes.
    pub total_size: u64,
    /// Number of files in the dataset.
    pub total_files: usize,
    /// Average record size in bytes.
    pub avg_record_size: f64,
    /// Count of files per format.
    pub formats: HashMap<String, usize>,
    /// Count of files per compression type.
    pub compressions: HashMap<String, usize>,
}

/// Dataset manifest containing metadata, lineage, and statistics.
///
/// The manifest serves as the authoritative record of a dataset's contents,
/// origin, and processing history.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiManifest {
    /// Manifest version for compatibility tracking.
    pub version: String,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
    /// List of files in the dataset.
    pub files: Vec<ZiManifestFile>,
    /// Data lineage information.
    pub lineage: ZiLineage,
    /// Additional metadata key-value pairs.
    pub metadata: HashMap<String, String>,
    /// Overall checksum for integrity verification.
    pub checksum: Option<String>,
}

impl Default for ZiManifest {
    fn default() -> Self {
        Self {
            version: "1.0.0".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            files: Vec::new(),
            lineage: ZiLineage::default(),
            metadata: HashMap::new(),
            checksum: None,
        }
    }
}

impl ZiManifest {
    /// Computes aggregate statistics from the manifest.
    #[allow(non_snake_case)]
    pub fn stats(&self) -> ZiManifestStats {
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
            }
        }

        ZiManifestStats {
            total_records,
            total_size,
            total_files,
            avg_record_size,
            formats,
            compressions,
        }
    }

    /// Computes a checksum over all file hashes.
    #[allow(non_snake_case)]
    pub fn compute_checksum(&mut self) -> Result<String> {
        use blake3::Hasher;
        
        let mut hasher = Hasher::new();
        
        for file in &self.files {
            hasher.update(file.hash.as_bytes());
        }
        
        let result = hasher.finalize();
        let checksum = result.to_hex().to_string();
        
        self.checksum = Some(checksum.clone());
        Ok(checksum)
    }

    /// Verifies manifest integrity against expected checksum.
    #[allow(non_snake_case)]
    pub fn verify_checksum(&self, expected: &str) -> Result<bool> {
        let mut temp = self.clone();
        let actual = temp.compute_checksum()?;
        Ok(expected == actual)
    }

    /// Adds a file to the manifest.
    #[allow(non_snake_case)]
    pub fn add_file(&mut self, file: ZiManifestFile) {
        self.files.push(file);
        self.updated_at = Utc::now();
    }

    /// Adds a source to the lineage.
    #[allow(non_snake_case)]
    pub fn add_source(&mut self, source: ZiLineageSource) {
        self.lineage.sources.push(source);
        self.updated_at = Utc::now();
    }

    /// Adds a transform to the lineage.
    #[allow(non_snake_case)]
    pub fn add_transform(&mut self, transform: ZiLineageTransform) {
        self.lineage.transforms.push(transform);
        self.updated_at = Utc::now();
    }

    /// Sets a metadata key-value pair.
    #[allow(non_snake_case)]
    pub fn set_metadata(&mut self, key: &str, value: &str) {
        self.metadata.insert(key.to_string(), value.to_string());
        self.updated_at = Utc::now();
    }
}

/// Builder for constructing manifests programmatically.
#[derive(Debug, Default)]
pub struct ZiManifestBuilder {
    manifest: ZiManifest,
}

impl ZiManifestBuilder {
    /// Creates a new manifest builder.
    #[allow(non_snake_case)]
    pub fn new() -> Self {
        Self {
            manifest: ZiManifest::default(),
        }
    }

    /// Sets the manifest version.
    #[allow(non_snake_case)]
    pub fn with_version(mut self, version: &str) -> Self {
        self.manifest.version = version.to_string();
        self
    }

    /// Adds a file to the manifest.
    #[allow(non_snake_case)]
    pub fn add_file(mut self, file: ZiManifestFile) -> Self {
        self.manifest.files.push(file);
        self
    }

    /// Adds a file with explicit information.
    #[allow(non_snake_case)]
    pub fn add_file_info(mut self, path: &str, size: u64, hash: &str, record_count: usize, format: &str) -> Self {
        self.manifest.files.push(ZiManifestFile {
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

    /// Adds a source to the lineage.
    #[allow(non_snake_case)]
    pub fn add_source(mut self, path: &str, hash: &str, record_count: usize) -> Self {
        self.manifest.lineage.sources.push(ZiLineageSource {
            path: path.to_string(),
            hash: hash.to_string(),
            record_count,
        });
        self
    }

    /// Adds a transform to the lineage.
    #[allow(non_snake_case)]
    pub fn add_transform(mut self, operator: &str, config: serde_json::Value) -> Self {
        self.manifest.lineage.transforms.push(ZiLineageTransform {
            operator: operator.to_string(),
            config,
            timestamp: Utc::now(),
        });
        self
    }

    /// Sets the pipeline hash.
    #[allow(non_snake_case)]
    pub fn set_pipeline_hash(mut self, hash: &str) -> Self {
        self.manifest.lineage.pipeline_hash = Some(hash.to_string());
        self
    }

    /// Adds metadata key-value pair.
    #[allow(non_snake_case)]
    pub fn add_metadata(mut self, key: &str, value: &str) -> Self {
        self.manifest.metadata.insert(key.to_string(), value.to_string());
        self
    }

    /// Builds the manifest.
    pub fn build(self) -> ZiManifest {
        self.manifest
    }
}

/// Computes hash of a file using blake3 algorithm.
///
/// # Arguments
///
/// - `path`: Path to the file
///
/// # Returns
///
/// Result containing hex-encoded hash string
pub fn compute_file_hash(path: &Path) -> Result<String> {
    use blake3::Hasher;
    use std::fs::File;
    use std::io::Read;

    let mut file = File::open(path)?;
    let mut hasher = Hasher::new();
    let mut buffer = [0u8; 8192];

    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    Ok(hasher.finalize().to_hex().to_string())
}
