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
use crate::metrics::ZiQualityMetrics;
use crate::operator::ZiOperator;
use crate::record::ZiRecord;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ZiDataHash(pub [u8; 32]);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ZiCodeHash(pub [u8; 32]);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ZiEnvHash(pub [u8; 32]);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZiTripleHash {
    pub data: ZiDataHash,
    pub code: ZiCodeHash,
    pub env: ZiEnvHash,
}

impl ZiTripleHash {
    #[allow(non_snake_case)]
    pub fn to_string(&self) -> String {
        format!(
            "data={},code={},env={}",
            blake3::Hash::from(self.data.0).to_hex(),
            blake3::Hash::from(self.code.0).to_hex(),
            blake3::Hash::from(self.env.0).to_hex()
        )
    }

    #[allow(non_snake_case)]
    pub fn to_compact_string(&self) -> String {
        format!(
            "{}{}{}",
            blake3::Hash::from(self.data.0).to_hex(),
            blake3::Hash::from(self.code.0).to_hex(),
            blake3::Hash::from(self.env.0).to_hex()
        )
    }
}

#[allow(non_snake_case)]
pub fn compute_data_hash(batch: &[ZiRecord]) -> ZiDataHash {
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
    ZiDataHash(*hasher.finalize().as_bytes())
}

#[allow(non_snake_case)]
pub fn compute_code_hash(operators: &[&dyn ZiOperator]) -> ZiCodeHash {
    let mut hasher = Blake3Hasher::new();
    for op in operators {
        hasher.update(b"operator:");
        hasher.update(op.name().as_bytes());
        hasher.update(b"\n");
    }
    ZiCodeHash(*hasher.finalize().as_bytes())
}

#[allow(non_snake_case)]
pub fn compute_env_hash(
    zi_version: &str,
    rust_version: &str,
    random_seed: u64,
) -> ZiEnvHash {
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
    ZiEnvHash(*hasher.finalize().as_bytes())
}

#[derive(Clone, Debug)]
pub struct ZiVersion {
    pub id: String,
    pub parent: Option<String>,
    pub created_at: SystemTime,
    pub metadata: Map<String, Value>,
    pub metrics: ZiQualityMetrics,
    pub digest: String,
    pub triple_hash: ZiTripleHash,
}

#[derive(Clone, Debug, Default)]
pub struct ZiVersionMetricsDelta {
    pub total_records_delta: isize,
    pub average_payload_chars_delta: f64,
    pub average_payload_tokens_delta: f64,
    pub toxicity_average_delta: f64,
    pub toxicity_max_delta: f64,
}

#[derive(Clone, Debug, Default)]
pub struct ZiVersionDiff {
    pub left: String,
    pub right: String,
    pub metadata_added: Map<String, Value>,
    pub metadata_removed: Map<String, Value>,
    pub metadata_changed: HashMap<String, (Value, Value)>,
    pub metrics_delta: ZiVersionMetricsDelta,
    pub triple_hash_changed: bool,
    pub data_hash_changed: bool,
    pub code_hash_changed: bool,
    pub env_hash_changed: bool,
}

#[derive(Debug)]
pub struct ZiVersionStore {
    next_id: u64,
    versions: HashMap<String, ZiVersion>,
}

#[derive(Clone, Debug)]
pub struct ZiVersionPersistOptions {
    pub pretty: bool,
    pub atomic: bool,
    pub create_directories: bool,
}

impl Default for ZiVersionPersistOptions {
    fn default() -> Self {
        ZiVersionPersistOptions {
            pretty: true,
            atomic: true,
            create_directories: true,
        }
    }
}

impl ZiVersionStore {
    #[allow(non_snake_case)]
    pub fn new() -> Self {
        ZiVersionStore {
            next_id: 1,
            versions: HashMap::new(),
        }
    }

    #[allow(non_snake_case)]
    pub fn create(
        &mut self,
        parent: Option<&str>,
        metadata: Map<String, Value>,
        metrics: ZiQualityMetrics,
        triple_hash: ZiTripleHash,
    ) -> Result<ZiVersion> {
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

        let version = ZiVersion {
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
    pub fn get(&self, id: &str) -> Option<ZiVersion> {
        self.versions.get(id).cloned()
    }

    #[allow(non_snake_case)]
    pub fn list(&self) -> Vec<ZiVersion> {
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
    pub fn compare(&self, left: &str, right: &str) -> Result<ZiVersionDiff> {
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

        let metrics_delta = ZiVersionMetricsDelta {
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

        Ok(ZiVersionDiff {
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
    pub fn save_to_path(&self, path: impl AsRef<Path>) -> Result<()> {
        self.save_to_path_with_options(path, ZiVersionPersistOptions::default())
    }

    #[allow(non_snake_case)]
    pub fn save_to_path_with_options(
        &self,
        path: impl AsRef<Path>,
        options: ZiVersionPersistOptions,
    ) -> Result<()> {
        let path = path.as_ref();
        if options.create_directories {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent)?;
                }
            }
        }

        let payload = ZiVersionStoreFile::from_store(self);

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
    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self> {
        Self::load_from_path_with_validation(path, true)
    }

    #[allow(non_snake_case)]
    pub fn load_from_path_with_validation(path: impl AsRef<Path>, validate: bool) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let payload: ZiVersionStoreFile = serde_json::from_reader(reader)?;
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
pub fn compute_digest(batch: &[ZiRecord]) -> String {
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

fn _compute_digest_from_triple(triple: &ZiTripleHash) -> String {
    triple.to_compact_string()
}

#[derive(Serialize, Deserialize)]
struct ZiVersionRecord {
    id: String,
    parent: Option<String>,
    created_at_secs: u64,
    created_at_nanos: u32,
    metadata: Map<String, Value>,
    metrics: ZiQualityMetrics,
    digest: String,
    triple_hash: ZiTripleHashRecord,
}

#[derive(Serialize, Deserialize)]
struct ZiTripleHashRecord {
    data: String,
    code: String,
    env: String,
}

impl ZiTripleHashRecord {
    fn from_triple(triple: &ZiTripleHash) -> Self {
        Self {
            data: blake3::Hash::from(triple.data.0).to_hex().to_string(),
            code: blake3::Hash::from(triple.code.0).to_hex().to_string(),
            env: blake3::Hash::from(triple.env.0).to_hex().to_string(),
        }
    }

    fn into_triple(&self) -> Result<ZiTripleHash> {
        let data_hash = blake3::Hash::from_hex(&self.data)
            .map_err(|_| ZiError::validation("invalid data hash in version file"))?;
        let code_hash = blake3::Hash::from_hex(&self.code)
            .map_err(|_| ZiError::validation("invalid code hash in version file"))?;
        let env_hash = blake3::Hash::from_hex(&self.env)
            .map_err(|_| ZiError::validation("invalid env hash in version file"))?;
        Ok(ZiTripleHash {
            data: ZiDataHash(*data_hash.as_bytes()),
            code: ZiCodeHash(*code_hash.as_bytes()),
            env: ZiEnvHash(*env_hash.as_bytes()),
        })
    }
}

#[derive(Serialize, Deserialize)]
struct ZiVersionStoreFile {
    next_id: u64,
    versions: Vec<ZiVersionRecord>,
}

impl ZiVersionStoreFile {
    fn from_store(store: &ZiVersionStore) -> Self {
        let versions = store
            .list()
            .into_iter()
            .map(|version| {
                let duration = version
                    .created_at
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_else(|_| Duration::from_secs(0));
                ZiVersionRecord {
                    id: version.id,
                    parent: version.parent,
                    created_at_secs: duration.as_secs(),
                    created_at_nanos: duration.subsec_nanos(),
                    metadata: version.metadata,
                    metrics: version.metrics,
                    digest: version.digest,
                    triple_hash: ZiTripleHashRecord::from_triple(&version.triple_hash),
                }
            })
            .collect();

        ZiVersionStoreFile {
            next_id: store.next_id,
            versions,
        }
    }

    fn into_store(self) -> ZiVersionStore {
        let mut versions = HashMap::new();
        for record in self.versions {
            let duration = Duration::new(record.created_at_secs, record.created_at_nanos);
            let created_at = UNIX_EPOCH + duration;
            let triple_hash = record.triple_hash.into_triple().unwrap_or_else(|_| ZiTripleHash {
                data: ZiDataHash([0u8; 32]),
                code: ZiCodeHash([0u8; 32]),
                env: ZiEnvHash([0u8; 32]),
            });
            let version = ZiVersion {
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

        ZiVersionStore {
            next_id: self.next_id,
            versions,
        }
    }
}


fn _write_store<W: Write>(
    writer: &mut W,
    payload: &ZiVersionStoreFile,
    pretty: bool,
) -> Result<()> {
    if pretty {
        serde_json::to_writer_pretty(writer, payload)?;
    } else {
        serde_json::to_writer(writer, payload)?;
    }
    Ok(())
}
