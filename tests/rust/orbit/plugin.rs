//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
//!
//! This file is part of Zi.
//! The Zi project belongs to the Dunimd project team.

use zix::orbit::plugin::{
    ZiPluginManifest, ZiPluginDependency, ZiPluginApi, ZiPluginRegistry
};

#[test]
fn test_plugin_manifest_serialization() {
    let manifest = ZiPluginManifest {
        name: "test-plugin".to_string(),
        version: "1.0.0".to_string(),
        description: "A test plugin".to_string(),
        author: "Test Author".to_string(),
        abi_version: ZiPluginApi { major: 1, minor: 0 },
        dependencies: vec![ZiPluginDependency {
            name: "zi-core".to_string(),
            version_range: ">=0.1.0".to_string(),
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
        abi_version: ZiPluginApi { major: 1, minor: 0 },
        dependencies: vec![],
        operators: vec![],
        capabilities: vec![],
    };

    registry.register("test".to_string(), manifest);
    assert_eq!(registry.list_plugins().len(), 1);
}
