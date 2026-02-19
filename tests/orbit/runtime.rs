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

use std::collections::HashMap;
use std::time::SystemTime;

use serde_json::{json, Value};
use Zi::errors::Result;
use Zi::metrics::ZiCQualityMetrics;
use Zi::orbit::runtime::{
    ZiCDataVisibility,
    ZiCExecutionContext,
    ZiCInProcessOrbit,
    ZiCPluginDependency,
    ZiCPluginDescriptor,
    ZiCPluginLifecycleManager,
    ZiCPluginPolicy,
    ZiCPluginState,
    ZiCPluginVersion,
};
use Zi::version::ZiCVersionStore;

fn ZiCTLogCapability(args: &Value, _ctx: &mut ZiCExecutionContext) -> Result<Value> {
    Ok(args.clone())
}

fn ZiCTMakePlugin(id: &str, version: &str) -> ZiCPluginDescriptor {
    ZiCPluginDescriptor {
        id: id.to_string(),
        version: ZiCPluginVersion::parse(version).expect("version"),
        exports: vec![],
        policy: ZiCPluginPolicy::default(),
        dependencies: vec![],
        state: ZiCPluginState::Loaded,
        load_time: SystemTime::now(),
    }
}

#[test]
fn ZiFTOrbitPluginVersionParsing() {
    let v1 = ZiCPluginVersion::parse("1.2.3").expect("v1");
    assert_eq!(v1.major, 1);
    assert_eq!(v1.minor, 2);
    assert_eq!(v1.patch, 3);
    assert!(v1.pre_release.is_none());

    let v2 = ZiCPluginVersion::parse("2.0.0-beta.1").expect("v2");
    assert_eq!(v2.major, 2);
    assert_eq!(v2.minor, 0);
    assert_eq!(v2.patch, 0);
    assert_eq!(v2.pre_release, Some("beta".to_string()));

    let v3 = ZiCPluginVersion::parse("invalid");
    assert!(v3.is_err());
}

#[test]
fn ZiFTOrbitPluginVersionComparison() {
    let v1 = ZiCPluginVersion::parse("1.0.0").expect("v1");
    let v2 = ZiCPluginVersion::parse("2.0.0").expect("v2");
    let v3 = ZiCPluginVersion::parse("1.1.0").expect("v3");
    let v4 = ZiCPluginVersion::parse("1.0.1").expect("v4");

    assert!(v1 < v2);
    assert!(v1 < v3);
    assert!(v1 < v4);
    assert!(v3 < v2);
    assert!(v4 < v3);
}

#[test]
fn ZiFTOrbitPluginRegistrationAndUnload() {
    let mut orbit = ZiCInProcessOrbit::ZiFNew();

    let plugin = ZiCTMakePlugin("test.plugin", "1.0.0");
    orbit.ZiFRegisterPlugin(plugin);

    assert!(orbit.ZiFIsPluginLoaded("test.plugin"));
    assert_eq!(orbit.ZiFGetLoadedPlugins().len(), 1);

    orbit.ZiFUnloadPlugin("test.plugin").expect("unload");
    assert!(!orbit.ZiFIsPluginLoaded("test.plugin"));
    assert_eq!(orbit.ZiFGetLoadedPlugins().len(), 0);
}

#[test]
fn ZiFTOrbitPluginVersionUpgrade() {
    let mut orbit = ZiCInProcessOrbit::ZiFNew();

    let plugin_v1 = ZiCTMakePlugin("test.plugin", "1.0.0");
    orbit.ZiFRegisterPlugin(plugin_v1);

    let plugin_v2 = ZiCTMakePlugin("test.plugin", "2.0.0");
    orbit.ZiFRegisterPlugin(plugin_v2);

    let loaded = orbit.ZiFGetLoadedPlugins();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].version.major, 2);
}

#[test]
fn ZiFTOrbitPluginDependencyManagement() {
    let mut lifecycle = ZiCPluginLifecycleManager::new();

    let base_plugin = ZiCTMakePlugin("base.plugin", "1.0.0");
    let dependent_plugin = ZiCPluginDescriptor {
        id: "dependent.plugin".to_string(),
        version: ZiCPluginVersion::parse("1.0.0").unwrap(),
        exports: vec![],
        policy: ZiCPluginPolicy::default(),
        dependencies: vec![ZiCPluginDependency {
            plugin_id: "base.plugin".to_string(),
            min_version: None,
            max_version: None,
            required: true,
        }],
        state: ZiCPluginState::Loaded,
        load_time: SystemTime::now(),
    };

    lifecycle.register_plugin(&base_plugin).expect("register base");
    lifecycle
        .register_plugin(&dependent_plugin)
        .expect("register dependent");

    let mut plugins = HashMap::new();
    plugins.insert("base.plugin".to_string(), base_plugin);
    plugins.insert("dependent.plugin".to_string(), dependent_plugin);

    assert!(lifecycle
        .can_unload_plugin("base.plugin", &plugins)
        .is_err());
    assert!(lifecycle
        .can_unload_plugin("dependent.plugin", &plugins)
        .is_ok());
}

#[test]
fn ZiFTOrbitCircularDependencyDetection() {
    let mut lifecycle = ZiCPluginLifecycleManager::new();

    let plugin_a = ZiCPluginDescriptor {
        id: "plugin.a".to_string(),
        version: ZiCPluginVersion::parse("1.0.0").unwrap(),
        exports: vec![],
        policy: ZiCPluginPolicy::default(),
        dependencies: vec![ZiCPluginDependency {
            plugin_id: "plugin.b".to_string(),
            min_version: None,
            max_version: None,
            required: true,
        }],
        state: ZiCPluginState::Loaded,
        load_time: SystemTime::now(),
    };

    let plugin_b = ZiCPluginDescriptor {
        id: "plugin.b".to_string(),
        version: ZiCPluginVersion::parse("1.0.0").unwrap(),
        exports: vec![],
        policy: ZiCPluginPolicy::default(),
        dependencies: vec![ZiCPluginDependency {
            plugin_id: "plugin.a".to_string(),
            min_version: None,
            max_version: None,
            required: true,
        }],
        state: ZiCPluginState::Loaded,
        load_time: SystemTime::now(),
    };

    lifecycle.register_plugin(&plugin_a).expect("register a");
    let result = lifecycle.register_plugin(&plugin_b);
    assert!(result.is_ok());
}

#[test]
fn ZiFTOrbitCapabilityPolicyAllowsAndBlocksCalls() {
    let mut orbit = ZiCInProcessOrbit::ZiFNew();
    orbit
        .ZiFCapabilitiesMut()
        .ZiFRegister("log.info", ZiCTLogCapability);

    let descriptor = ZiCPluginDescriptor {
        id: "allowed.plugin".to_string(),
        version: ZiCPluginVersion::parse("1.0.0").unwrap(),
        exports: vec![],
        policy: ZiCPluginPolicy {
            allowed_capabilities: vec!["log.info".to_string()],
            can_access_versions: false,
            default_visibility: ZiCDataVisibility::Full,
        },
        dependencies: vec![],
        state: ZiCPluginState::Loaded,
        load_time: SystemTime::now(),
    };
    orbit.ZiFRegisterPlugin(descriptor);

    let mut metrics = ZiCQualityMetrics::default();
    let capabilities = orbit.capabilities.clone();
    let mut ctx = ZiCExecutionContext {
        metrics: &mut metrics,
        version_store: None,
        capabilities: &capabilities,
        visibility: ZiCDataVisibility::Full,
    };

    let args = json!({"message": "hello"});
    let out = orbit
        .ZiFCallCapability("allowed.plugin", "log.info", &args, &mut ctx)
        .expect("allowed call");
    assert_eq!(out, args);

    let descriptor_blocked = ZiCPluginDescriptor {
        id: "blocked.plugin".to_string(),
        version: ZiCPluginVersion::parse("1.0.0").unwrap(),
        exports: vec![],
        policy: ZiCPluginPolicy {
            allowed_capabilities: vec![],
            can_access_versions: false,
            default_visibility: ZiCDataVisibility::Full,
        },
        dependencies: vec![],
        state: ZiCPluginState::Loaded,
        load_time: SystemTime::now(),
    };
    orbit.ZiFRegisterPlugin(descriptor_blocked);

    let mut ctx_blocked = ZiCExecutionContext {
        metrics: &mut metrics,
        version_store: None,
        capabilities: &capabilities,
        visibility: ZiCDataVisibility::Full,
    };

    let err = orbit
        .ZiFCallCapability("blocked.plugin", "log.info", &args, &mut ctx_blocked)
        .expect_err("blocked call");
    let msg = format!("{}", err);
    assert!(msg.contains("not allowed to call capability"));
}
