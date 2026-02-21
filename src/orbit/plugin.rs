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

//! Zi Plugin System - ABI-stable plugin interface for extensions.
//!
//! This module provides a stable ABI for Zi plugins, allowing third-party
//! developers to create extensions without breaking compatibility across
//! Zi versions.
//!
//! # Example
//!
//! ```ignore
//! use zi_core::orbit::plugin::{ZiCPlugin, ZiCPluginDescriptor};
//!
//! #[no_mangle]
//! pub fn zi_create_plugin() -> *mut dyn ZiCPlugin {
//!     let plugin = MyCustomPlugin;
//!     Box::into_raw(Box::new(plugin))
//! }
//! ```

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::ffi::CString;
use std::os::raw::c_char;

use crate::errors::Result;
use crate::operator::ZiCOperator;
use crate::record::ZiCRecordBatch;

pub const ZI_PLUGIN_ABI_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCPluginManifest {
    pub name: String,
    pub version: String,
    pub description: String,
    pub author: String,
    pub abi_version: u32,
    pub dependencies: Vec<ZiCPluginDependency>,
    pub operators: Vec<String>,
    pub capabilities: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCPluginDependency {
    pub name: String,
    pub version_range: String,
}

#[derive(Clone, Debug)]
pub struct ZiCPluginDescriptor {
    pub manifest: ZiCPluginManifest,
    pub path: String,
    pub loaded_at: std::time::SystemTime,
    pub id: String,
}

#[derive(Debug)]
pub struct ZiCLoadedPluginPackage {
    pub descriptor: ZiCPluginDescriptor,
    pub library: libloading::Library,
    pub instance: Box<dyn ZiCPluginInstance>,
}

pub trait ZiCPluginInstance: Send + Sync + std::fmt::Debug {
    fn name(&self) -> &str;
    fn version(&self) -> &str;
    fn initialize(&mut self, config: &Value) -> Result<()>;
    fn shutdown(&mut self) -> Result<()>;
    fn create_operator(&self, name: &str, config: &Value) -> Result<Box<dyn ZiCOperator>>;
    fn capabilities(&self) -> Vec<&str>;
}

pub trait ZiCPlugin: Send + Sync + std::fmt::Debug {
    fn manifest(&self) -> &ZiCPluginManifest;
    fn initialize(&mut self, config: &Value) -> Result<()>;
    fn shutdown(&mut self) -> Result<()>;
    fn create_operator(&self, name: &str, config: &Value) -> Result<Box<dyn ZiCOperator>>;
    fn capabilities(&self) -> Vec<&str>;
}

#[derive(Default)]
pub struct ZiCPluginLifecycleManager {
    active_plugins: HashMap<String, Box<dyn ZiCPluginInstance>>,
    #[allow(dead_code)]
    state: HashMap<String, serde_json::Map<String, Value>>,
}

impl ZiCPluginLifecycleManager {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        Self {
            active_plugins: HashMap::new(),
            state: HashMap::new(),
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFRegister(&mut self, plugin_id: String, instance: Box<dyn ZiCPluginInstance>) {
        self.active_plugins.insert(plugin_id, instance);
    }

    #[allow(non_snake_case)]
    pub fn ZiFUnregister(&mut self, plugin_id: &str) -> Result<()> {
        if let Some(mut instance) = self.active_plugins.remove(plugin_id) {
            instance.shutdown()?;
        }
        Ok(())
    }

    #[allow(non_snake_case)]
    pub fn ZiFGetPlugin(&self, plugin_id: &str) -> Option<&dyn ZiCPluginInstance> {
        self.active_plugins.get(plugin_id).map(|p| p.as_ref())
    }

    #[allow(non_snake_case)]
    pub fn ZiFGetAllPlugins(&self) -> Vec<&dyn ZiCPluginInstance> {
        self.active_plugins.values().map(|p| p.as_ref()).collect()
    }
}

#[derive(Default)]
pub struct ZiCPluginRegistry {
    manifests: HashMap<String, ZiCPluginManifest>,
    factories: HashMap<String, fn(&Value) -> Result<Box<dyn ZiCOperator>>>,
}

impl ZiCPluginRegistry {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        Self {
            manifests: HashMap::new(),
            factories: HashMap::new(),
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFRegister(&mut self, manifest: ZiCPluginManifest) {
        self.manifests.insert(manifest.name.clone(), manifest);
    }

    #[allow(non_snake_case)]
    pub fn ZiFRegisterOperatorFactory(
        &mut self,
        plugin_name: &str,
        operator_name: &str,
        factory: fn(&Value) -> Result<Box<dyn ZiCOperator>>,
    ) {
        let key = format!("{}::{}", plugin_name, operator_name);
        self.factories.insert(key, factory);
    }

    #[allow(non_snake_case)]
    pub fn ZiFGetFactory(
        &self,
        plugin_name: &str,
        operator_name: &str,
    ) -> Option<fn(&Value) -> Result<Box<dyn ZiCOperator>>> {
        let key = format!("{}::{}", plugin_name, operator_name);
        self.factories.get(&key).copied()
    }

    #[allow(non_snake_case)]
    pub fn ZiFListPlugins(&self) -> Vec<&ZiCPluginManifest> {
        self.manifests.values().collect()
    }
}

#[repr(C)]
pub struct ZiCExternalOperatorVTable {
    pub version: u32,
    pub apply: extern "C" fn(*const ZiCRecordBatch, *mut ZiCRecordBatch) -> i32,
    pub name: extern "C" fn() -> *const c_char,
    pub destroy: extern "C" fn(*mut ZiCExternalOperatorVTable),
}

#[repr(C)]
pub struct ZiCRecordBatchC {
    pub ptr: *mut std::os::raw::c_void,
    pub len: usize,
    pub capacity: usize,
}

#[repr(C)]
pub struct ZiCPluginApi {
    pub register_operator: extern "C" fn(*const ZiCExternalOperatorVTable, *const c_char) -> i32,
    pub get_version: extern "C" fn() -> u32,
    pub log_info: extern "C" fn(*const c_char),
    pub log_error: extern "C" fn(*const c_char),
}

static mut ZI_GLOBAL_API: Option<ZiCPluginApi> = None;

#[no_mangle]
pub extern "C" fn zi_core_register_plugin_api(api: ZiCPluginApi) {
    unsafe {
        ZI_GLOBAL_API = Some(api);
    }
}

#[no_mangle]
pub extern "C" fn zi_core_get_api_version() -> u32 {
    ZI_PLUGIN_ABI_VERSION
}

#[allow(dead_code)]
extern "C" fn default_apply(_input: *const ZiCRecordBatch, _output: *mut ZiCRecordBatch) -> i32 {
    -1
}

#[allow(dead_code)]
extern "C" fn default_name() -> *const c_char {
    CString::new("unknown").unwrap().into_raw()
}

#[allow(dead_code)]
extern "C" fn default_destroy(_vtable: *mut ZiCExternalOperatorVTable) {}

pub fn create_default_vtable() -> ZiCExternalOperatorVTable {
    ZiCExternalOperatorVTable {
        version: ZI_PLUGIN_ABI_VERSION,
        apply: default_apply,
        name: default_name,
        destroy: default_destroy,
    }
}


