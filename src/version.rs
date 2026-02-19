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

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use blake3::Hasher as Blake3Hasher;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::errors::{Result, ZiError};
use crate::metrics::ZiCQualityMetrics;
use crate::operator::ZiCOperator;
use crate::record::ZiCRecord;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ZiCDataHash(pub [u8; 32]);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ZiCCodeHash(pub [u8; 32]);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ZiCEnvHash(pub [u8; 32]);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZiCTripleHash {
    pub data: ZiCDataHash,
    pub code: ZiCCodeHash,
    pub env: ZiCEnvHash,
}

impl ZiCTripleHash {
    #[allow(non_snake_case)]
    pub fn ZiFToString(&self) -> String {
        format!(
            "data={},code={},env={}",
            blake3::Hash::from(self.data.0).to_hex(),
            blake3::Hash::from(self.code.0).to_hex(),
            blake3::Hash::from(self.env.0).to_hex()
        )
    }

    #[allow(non_snake_case)]
    pub fn ZiFToCompactString(&self) -> String {
        format!(
            "{}{}{}",
            blake3::Hash::from(self.data.0).to_hex(),
            blake3::Hash::from(self.code.0).to_hex(),
            blake3::Hash::from(self.env.0).to_hex()
        )
    }
}

#[allow(non_snake_case)]
pub fn ZiFComputeDataHash(batch: &[ZiCRecord]) -> ZiCDataHash {
    let mut hasher = Blake3Hasher::new();
    for record in batch {
        if let Some(ref id) = record.id {
            hasher.update(b"id:");
            hasher.update(id.as_bytes());
        }
        hasher.update(b"\n");
        hasher.update(b"payload:");
        serde_json::to_writer(&mut hasher, &record.payload).unwrap_or_default();
        hasher.update(b"\n");
        if let Some(ref metadata) = record.metadata {
            hasher.update(b"metadata:");
            serde_json::to_writer(&mut hasher, metadata).unwrap_or_default();
            hasher.update(b"\n");
        }
        hasher.update(b"---\n");
    }
    ZiCDataHash(*hasher.finalize().as_bytes())
}

#[allow(non_snake_case)]
pub fn ZiFComputeCodeHash(operators: &[&dyn ZiCOperator]) -> ZiCCodeHash {
    let mut hasher = Blake3Hasher::new();
    for op in operators {
        hasher.update(b"operator:");
        hasher.update(op.name().as_bytes());
        hasher.update(b"\n");
    }
    ZiCCodeHash(*hasher.finalize().as_bytes())
}

#[allow(non_snake_case)]
pub fn ZiFComputeEnvHash(
    zi_version: &str,
    rust_version: &str,
    random_seed: u64,
) -> ZiCEnvHash {
    let mut hasher = Blake3Hasher::new();
    hasher.update(b"zi_version=");
    hasher.update(zi_version.as_bytes());
    hasher.update(b"\n");
    hasher.update(b"rust_version=");
    hasher.update(rust_version.as_bytes());
    hasher.update(b"\n");
    hasher.update(b"random_seed=");
    hasher.update(&random_seed.to_ne_bytes());
    hasher.update(b"\n");
    ZiCEnvHash(*hasher.finalize().as_bytes())
}

#[derive(Clone, Debug)]
pub struct ZiCVersion {
    pub id: String,
    pub parent: Option<String>,
    pub created_at: SystemTime,
    pub metadata: Map<String, Value>,
    pub metrics: ZiCQualityMetrics,
    pub digest: String,
    pub triple_hash: ZiCTripleHash,
}

#[derive(Clone, Debug, Default)]
pub struct ZiCVersionMetricsDelta {
    pub total_records_delta: isize,
    pub average_payload_chars_delta: f64,
    pub average_payload_tokens_delta: f64,
    pub toxicity_average_delta: f64,
    pub toxicity_max_delta: f64,
}

#[derive(Clone, Debug, Default)]
pub struct ZiCVersionDiff {
    pub left: String,
    pub right: String,
    pub metadata_added: Map<String, Value>,
    pub metadata_removed: Map<String, Value>,
    pub metadata_changed: HashMap<String, (Value, Value)>,
    pub metrics_delta: ZiCVersionMetricsDelta,
    pub triple_hash_changed: bool,
    pub data_hash_changed: bool,
    pub code_hash_changed: bool,
    pub env_hash_changed: bool,
}

#[derive(Debug)]
pub struct ZiCVersionStore {
    next_id: u64,
    versions: HashMap<String, ZiCVersion>,
}

#[derive(Clone, Debug)]
pub struct ZiCVersionPersistOptions {
    pub pretty: bool,
    pub atomic: bool,
    pub create_directories: bool,
}

impl Default for ZiCVersionPersistOptions {
    fn default() -> Self {
        ZiCVersionPersistOptions {
            pretty: true,
            atomic: true,
            create_directories: true,
        }
    }
}

impl ZiCVersionStore {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        ZiCVersionStore {
            next_id: 1,
            versions: HashMap::new(),
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFCreate(
        &mut self,
        parent: Option<&str>,
        metadata: Map<String, Value>,
        metrics: ZiCQualityMetrics,
        triple_hash: ZiCTripleHash,
    ) -> Result<ZiCVersion> {
        if let Some(parent_id) = parent {
            if !self.versions.contains_key(parent_id) {
                return Err(ZiError::validation(format!(
                    "version parent '{parent_id}' does not exist"
                )));
            }
        }

        let id = format!("v{:016x}", self.next_id);
        self.next_id += 1;

        let digest = _compute_digest_from_triple(&triple_hash);

        let version = ZiCVersion {
            id: id.clone(),
            parent: parent.map(|p| p.to_string()),
            created_at: SystemTime::now(),
            metadata,
            metrics: metrics.clone(),
            digest,
            triple_hash,
        };
        self.versions.insert(id.clone(), version.clone());
        Ok(version)
    }

    #[allow(non_snake_case)]
    pub fn ZiFGet(&self, id: &str) -> Option<ZiCVersion> {
        self.versions.get(id).cloned()
    }

    #[allow(non_snake_case)]
    pub fn ZiFList(&self) -> Vec<ZiCVersion> {
        let mut entries: Vec<_> = self.versions.values().cloned().collect();
        entries.sort_by_key(|version| {
            version
                .created_at
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
        });
        entries
    }

    #[allow(non_snake_case)]
    pub fn ZiFCompare(&self, left: &str, right: &str) -> Result<ZiCVersionDiff> {
        let left_version = self
            .versions
            .get(left)
            .ok_or_else(|| ZiError::validation(format!("unknown version '{left}'")))?;
        let right_version = self
            .versions
            .get(right)
            .ok_or_else(|| ZiError::validation(format!("unknown version '{right}'")))?;

        let mut added = Map::new();
        let mut removed = Map::new();
        let mut changed = HashMap::new();

        for (key, value) in &right_version.metadata {
            match left_version.metadata.get(key) {
                None => {
                    added.insert(key.clone(), value.clone());
                }
                Some(left_value) if left_value != value => {
                    changed.insert(key.clone(), (left_value.clone(), value.clone()));
                }
                _ => {}
            }
        }

        for (key, value) in &left_version.metadata {
            if !right_version.metadata.contains_key(key) {
                removed.insert(key.clone(), value.clone());
            }
        }

        let metrics_delta = ZiCVersionMetricsDelta {
            total_records_delta: right_version.metrics.total_records as isize
                - left_version.metrics.total_records as isize,
            average_payload_chars_delta: right_version.metrics.average_payload_chars
                - left_version.metrics.average_payload_chars,
            average_payload_tokens_delta: right_version.metrics.average_payload_tokens
                - left_version.metrics.average_payload_tokens,
            toxicity_average_delta: right_version.metrics.toxicity_average
                - left_version.metrics.toxicity_average,
            toxicity_max_delta: right_version.metrics.toxicity_max
                - left_version.metrics.toxicity_max,
        };

        let triple_hash_changed = left_version.triple_hash != right_version.triple_hash;

        Ok(ZiCVersionDiff {
            left: left.to_string(),
            right: right.to_string(),
            metadata_added: added,
            metadata_removed: removed,
            metadata_changed: changed,
            metrics_delta,
            triple_hash_changed,
            data_hash_changed: left_version.triple_hash.data != right_version.triple_hash.data,
            code_hash_changed: left_version.triple_hash.code != right_version.triple_hash.code,
            env_hash_changed: left_version.triple_hash.env != right_version.triple_hash.env,
        })
    }

    #[allow(non_snake_case)]
    pub fn ZiFSaveToPath(&self, path: impl AsRef<Path>) -> Result<()> {
        self.ZiFSaveToPathWithOptions(path, ZiCVersionPersistOptions::default())
    }

    #[allow(non_snake_case)]
    pub fn ZiFSaveToPathWithOptions(
        &self,
        path: impl AsRef<Path>,
        options: ZiCVersionPersistOptions,
    ) -> Result<()> {
        let path = path.as_ref();
        if options.create_directories {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent)?;
                }
            }
        }

        let payload = ZiCVersionStoreFile::from_store(self);

        if options.atomic {
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            let stem = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("versions");
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let tmp_path = parent.join(format!(".{}.tmp.{}", stem, timestamp));

            let mut file = File::create(&tmp_path)?;
            {
                let mut writer = BufWriter::new(&mut file);
                _write_store(&mut writer, &payload, options.pretty)?;
                writer.flush()?;
            }
            file.sync_all()?;

            if path.exists() {
                fs::remove_file(path)?;
            }
            fs::rename(&tmp_path, path)?;
        } else {
            let mut file = File::create(path)?;
            {
                let mut writer = BufWriter::new(&mut file);
                _write_store(&mut writer, &payload, options.pretty)?;
                writer.flush()?;
            }
            file.sync_all()?;
        }

        Ok(())
    }

    #[allow(non_snake_case)]
    pub fn ZiFLoadFromPath(path: impl AsRef<Path>) -> Result<Self> {
        Self::ZiFLoadFromPathWithValidation(path, true)
    }

    #[allow(non_snake_case)]
    pub fn ZiFLoadFromPathWithValidation(path: impl AsRef<Path>, validate: bool) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let payload: ZiCVersionStoreFile = serde_json::from_reader(reader)?;
        let store = payload.into_store();
        if validate {
            store._validate_consistency()?;
        }
        Ok(store)
    }

    fn _validate_consistency(&self) -> Result<()> {
        let mut max_id = 0u64;
        for (version_id, version) in &self.versions {
            let numeric = version_id
                .strip_prefix('v')
                .ok_or_else(|| ZiError::validation(format!("invalid version id '{version_id}'")))
                .and_then(|hex| {
                    u64::from_str_radix(hex, 16).map_err(|_| {
                        ZiError::validation(format!("invalid version id '{version_id}'"))
                    })
                })?;
            if let Some(parent) = &version.parent {
                if !self.versions.contains_key(parent) {
                    return Err(ZiError::validation(format!(
                        "version '{version_id}' references missing parent '{parent}'"
                    )));
                }
            }
            max_id = max_id.max(numeric);
        }

        if !self.versions.is_empty() && max_id >= self.next_id {
            return Err(ZiError::validation(format!(
                "version store next_id (v{next_id:016x}) must exceed max existing id (v{max_id:016x})",
                next_id = self.next_id,
                max_id = max_id
            )));
        }

        Ok(())
    }
}

#[allow(non_snake_case)]
pub fn ZiFComputeDigest(batch: &[ZiCRecord]) -> String {
    let mut hasher = DefaultHasher::new();
    for record in batch {
        record.id.hash(&mut hasher);
        record.payload.hash(&mut hasher);
        if let Some(metadata) = &record.metadata {
            metadata.hash(&mut hasher);
        }
    }
    format!("{:016x}", hasher.finish())
}

fn _compute_digest_from_triple(triple: &ZiCTripleHash) -> String {
    triple.ZiFToCompactString()
}

#[derive(Serialize, Deserialize)]
struct ZiCVersionRecord {
    id: String,
    parent: Option<String>,
    created_at_secs: u64,
    created_at_nanos: u32,
    metadata: Map<String, Value>,
    metrics: ZiCQualityMetrics,
    digest: String,
    triple_hash: ZiCTripleHashRecord,
}

#[derive(Serialize, Deserialize)]
struct ZiCTripleHashRecord {
    data: String,
    code: String,
    env: String,
}

impl ZiCTripleHashRecord {
    fn from_triple(triple: &ZiCTripleHash) -> Self {
        Self {
            data: blake3::Hash::from(triple.data.0).to_hex().to_string(),
            code: blake3::Hash::from(triple.code.0).to_hex().to_string(),
            env: blake3::Hash::from(triple.env.0).to_hex().to_string(),
        }
    }

    fn into_triple(&self) -> Result<ZiCTripleHash> {
        let data_hash = blake3::Hash::from_hex(&self.data)
            .map_err(|_| ZiError::validation("invalid data hash in version file"))?;
        let code_hash = blake3::Hash::from_hex(&self.code)
            .map_err(|_| ZiError::validation("invalid code hash in version file"))?;
        let env_hash = blake3::Hash::from_hex(&self.env)
            .map_err(|_| ZiError::validation("invalid env hash in version file"))?;
        Ok(ZiCTripleHash {
            data: ZiCDataHash(*data_hash.as_bytes()),
            code: ZiCCodeHash(*code_hash.as_bytes()),
            env: ZiCEnvHash(*env_hash.as_bytes()),
        })
    }
}

#[derive(Serialize, Deserialize)]
struct ZiCVersionStoreFile {
    next_id: u64,
    versions: Vec<ZiCVersionRecord>,
}

impl ZiCVersionStoreFile {
    fn from_store(store: &ZiCVersionStore) -> Self {
        let versions = store
            .ZiFList()
            .into_iter()
            .map(|version| {
                let duration = version
                    .created_at
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_else(|_| Duration::from_secs(0));
                ZiCVersionRecord {
                    id: version.id,
                    parent: version.parent,
                    created_at_secs: duration.as_secs(),
                    created_at_nanos: duration.subsec_nanos(),
                    metadata: version.metadata,
                    metrics: version.metrics,
                    digest: version.digest,
                    triple_hash: ZiCTripleHashRecord::from_triple(&version.triple_hash),
                }
            })
            .collect();

        ZiCVersionStoreFile {
            next_id: store.next_id,
            versions,
        }
    }

    fn into_store(self) -> ZiCVersionStore {
        let mut versions = HashMap::new();
        for record in self.versions {
            let duration = Duration::new(record.created_at_secs, record.created_at_nanos);
            let created_at = UNIX_EPOCH + duration;
            let triple_hash = record.triple_hash.into_triple().unwrap_or_else(|_| ZiCTripleHash {
                data: ZiCDataHash([0u8; 32]),
                code: ZiCCodeHash([0u8; 32]),
                env: ZiCEnvHash([0u8; 32]),
            });
            let version = ZiCVersion {
                id: record.id.clone(),
                parent: record.parent.clone(),
                created_at,
                metadata: record.metadata,
                metrics: record.metrics,
                digest: record.digest,
                triple_hash,
            };
            versions.insert(record.id, version);
        }

        ZiCVersionStore {
            next_id: self.next_id,
            versions,
        }
    }
}


fn _write_store<W: Write>(
    writer: &mut W,
    payload: &ZiCVersionStoreFile,
    pretty: bool,
) -> Result<()> {
    if pretty {
        serde_json::to_writer_pretty(writer, payload)?;
    } else {
        serde_json::to_writer(writer, payload)?;
    }
    Ok(())
}
