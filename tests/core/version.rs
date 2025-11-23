//! Copyright © 2025 Wenze Wei. All Rights Reserved.
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

use serde_json::{json, Map};
use tempfile::NamedTempFile;
use Zi::metrics::ZiCQualityMetrics;
use Zi::record::ZiCRecord;
use Zi::version::{
    ZiCVersionPersistOptions,
    ZiCVersionStore,
    ZiFComputeDigest,
};

#[test]
fn ZiFTVersionCreateAndRetrieve() {
    let mut store = ZiCVersionStore::ZiFNew();
    let mut metadata = Map::new();
    metadata.insert("note".into(), json!("initial"));
    let metrics = ZiCQualityMetrics::default();
    let version = store
        .ZiFCreate(None, metadata.clone(), metrics.clone(), "digest".into())
        .unwrap();

    assert!(store.ZiFGet(&version.id).is_some());
    assert_eq!(store.ZiFList().len(), 1);
}

#[test]
fn ZiFTVersionCompareReportsChanges() {
    let mut store = ZiCVersionStore::ZiFNew();
    let mut metadata_a = Map::new();
    metadata_a.insert("note".into(), json!("a"));
    let metrics_a = ZiCQualityMetrics::default();
    let v1 = store
        .ZiFCreate(None, metadata_a, metrics_a.clone(), "digest-a".into())
        .unwrap();

    let mut metadata_b = Map::new();
    metadata_b.insert("note".into(), json!("b"));
    metadata_b.insert("extra".into(), json!(1));
    let mut metrics_b = metrics_a.clone();
    metrics_b.total_records = 10;
    let v2 = store
        .ZiFCreate(Some(&v1.id), metadata_b, metrics_b, "digest-b".into())
        .unwrap();

    let diff = store.ZiFCompare(&v1.id, &v2.id).unwrap();
    assert!(diff.metadata_removed.is_empty());
    assert!(diff.metadata_added.contains_key("extra"));
    assert!(diff.metadata_changed.contains_key("note"));
    assert_eq!(diff.metrics_delta.total_records_delta, 10);
}

#[test]
fn ZiFTVersionComputeDigestDiffersForDifferentBatches() {
    let a = ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "a"}));
    let b = ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "b"}));
    let digest_a = ZiFComputeDigest(&[a.clone(), b.clone()]);
    let digest_b = ZiFComputeDigest(&[b, a]);
    assert_ne!(digest_a, digest_b);
}

#[test]
fn ZiFTVersionSaveAndLoadRoundtrip() {
    let mut store = ZiCVersionStore::ZiFNew();
    let mut metadata = Map::new();
    metadata.insert("note".into(), json!("persist"));
    let metrics = ZiCQualityMetrics::default();
    let version = store
        .ZiFCreate(None, metadata, metrics, "digest".into())
        .unwrap();

    let file = NamedTempFile::new().unwrap();
    store.ZiFSaveToPath(file.path()).unwrap();

    let loaded = ZiCVersionStore::ZiFLoadFromPath(file.path()).unwrap();
    let restored = loaded.ZiFGet(&version.id).expect("version restored");
    assert_eq!(restored.digest, "digest");
    assert_eq!(loaded.ZiFList().len(), store.ZiFList().len());
}

#[test]
fn ZiFTVersionSaveWithOptionsCreatesDirectories() {
    let mut store = ZiCVersionStore::ZiFNew();
    let version = store
        .ZiFCreate(
            None,
            Map::new(),
            ZiCQualityMetrics::default(),
            "digest".into(),
        )
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested/store.json");
    let options = ZiCVersionPersistOptions {
        pretty: false,
        atomic: false,
        create_directories: true,
    };
    store.ZiFSaveToPathWithOptions(&path, options).unwrap();

    let loaded = ZiCVersionStore::ZiFLoadFromPathWithValidation(&path, true).unwrap();
    let restored = loaded.ZiFGet(&version.id).unwrap();
    assert_eq!(restored.digest, "digest");
}
