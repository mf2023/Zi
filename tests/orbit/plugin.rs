//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
//!
//! This file is part of Zi.
//! The Zi project belongs to the Dunimd project team.

use zi::orbit::plugin::*;

#[test]
fn test_plugin_manifest_serialization() {
    let manifest = ZiCPluginManifest {
        name: "test-plugin".to_string(),
        version: "1.0.0".to_string(),
        description: "A test plugin".to_string(),
        author: "Test Author".to_string(),
        abi_version: ZI_PLUGIN_ABI_VERSION,
        dependencies: vec![ZiCPluginDependency {
            name: "zi-core".to_string(),
            version_range: ">=0.1.0".to_string(),
        }],
        operators: vec!["custom.operator".to_string()],
        capabilities: vec!["custom.capability".to_string()],
    };

    let json = serde_json::to_string(&manifest).unwrap();
    let deserialized: ZiCPluginManifest = serde_json::from_str(&json).unwrap();

    assert_eq!(manifest.name, deserialized.name);
    assert_eq!(manifest.version, deserialized.version);
    assert_eq!(manifest.abi_version, deserialized.abi_version);
}

#[test]
fn test_plugin_registry() {
    let mut registry = ZiCPluginRegistry::ZiFNew();

    let manifest = ZiCPluginManifest {
        name: "test".to_string(),
        version: "1.0.0".to_string(),
        description: "".to_string(),
        author: "".to_string(),
        abi_version: ZI_PLUGIN_ABI_VERSION,
        dependencies: vec![],
        operators: vec![],
        capabilities: vec![],
    };

    registry.ZiFRegister(manifest.clone());
    assert_eq!(registry.ZiFListPlugins().len(), 1);
}

#[test]
fn test_lifecycle_manager() {
    let manager = ZiCPluginLifecycleManager::ZiFNew();
    assert!(manager.ZiFGetAllPlugins().is_empty());
}
