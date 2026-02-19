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

use serde_json::json;
use Zi::orbit::plugin_package::ZiFLoadPluginDescriptorFromPath;
use Zi::orbit::runtime::{ZiCPluginDescriptor, ZiCPluginExportKind};
use tempfile::NamedTempFile;

#[test]
fn ZiFTOrbitPluginExportWithoutScript() {
    let value = json!({
        "id": "test.plugin",
        "exports": [
            {"kind": "operator", "name": "op1"}
        ]
    });
    let file: Zi::orbit::plugin_package::ZiCPluginFile = serde_json::from_value(value).unwrap();
    let descriptor = file.into_runtime().unwrap();
    assert_eq!(descriptor.id, "test.plugin");
    assert_eq!(descriptor.exports.len(), 1);
    assert_eq!(descriptor.exports[0].name, "op1");
    assert!(descriptor.exports[0].script.is_none());
}

#[test]
fn ZiFTOrbitPluginExportWithScript() {
    let value = json!({
        "id": "test.plugin",
        "exports": [
            {"kind": "operator", "name": "op1", "script": "ops/op1.zi"}
        ]
    });
    let file: Zi::orbit::plugin_package::ZiCPluginFile = serde_json::from_value(value).unwrap();
    let descriptor = file.into_runtime().unwrap();
    assert_eq!(descriptor.exports.len(), 1);
    assert_eq!(descriptor.exports[0].name, "op1");
    assert_eq!(descriptor.exports[0].script.as_deref(), Some("ops/op1.zi"));
}
