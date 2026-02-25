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

//! # Zi Orbit Plugin Tests
//!
//! This module contains tests for the Orbit plugin system in Zi framework.
//! The Orbit system provides extensibility through dynamic plugin loading.
//!
//! ## Plugin System Architecture
//!
//! - **ZiPluginManifest**: Plugin metadata and configuration
//! - **ZiPluginDependency**: Plugin dependency declarations
//! - **ZiPluginRegistry**: Plugin registration and discovery
//!
//! ## Running Tests
//!
//! ```bash
//! cargo test --test plugin
//! ```

use zix::orbit::plugin::{
    ZiPluginManifest, ZiPluginDependency, ZiPluginRegistry
};

/// Tests plugin manifest serialization and deserialization.
///
/// Verifies that a plugin manifest can be serialized to JSON and
/// correctly deserialized back to a ZiPluginManifest struct.
#[test]
fn test_plugin_manifest_serialization() {
    let manifest = ZiPluginManifest {
        name: "test-plugin".to_string(),
        version: "1.0.0".to_string(),
        description: "A test plugin".to_string(),
        author: "Test Author".to_string(),
        abi_version: 1,
        dependencies: vec![ZiPluginDependency {
            name: "zi-core".to_string(),
            version_range: ">= 1.0".to_string(),
        }],
        operators: vec!["custom.operator".to_string()],
        capabilities: vec!["custom.capability".to_string()],
    };

    let json_str = serde_json::to_string(&manifest).unwrap();
    let deserialized: ZiPluginManifest = serde_json::from_str(&json_str).unwrap();

    assert_eq!(manifest.name, deserialized.name);
    assert_eq!(manifest.version, deserialized.version);
}

#[test]
fn test_plugin_registry() {
    let mut registry = ZiPluginRegistry::new();

    let manifest = ZiPluginManifest {
        name: "test".to_string(),
        version: "1.0.0".to_string(),
        description: "".to_string(),
        author: "".to_string(),
        abi_version: 1,
        dependencies: vec![],
        operators: vec![],
        capabilities: vec![],
    };

    registry.register(manifest);
    assert_eq!(registry.list_plugins().len(), 1);
}
