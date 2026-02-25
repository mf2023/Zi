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

//! # Plugin Module
//!
//! This module provides the plugin interface and descriptor types for ZiOrbit.
//! Defines how plugins expose their capabilities and metadata to the runtime.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::ffi::CString;
use std::os::raw::c_char;

use crate::errors::Result;
use crate::operator::ZiOperator;
use crate::record::ZiRecordBatch;

pub const ZI_PLUGIN_ABI_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiPluginManifest {
    pub name: String,
    pub version: String,
    pub description: String,
    pub author: String,
    pub abi_version: u32,
    pub dependencies: Vec<ZiPluginDependency>,
    pub operators: Vec<String>,
    pub capabilities: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiPluginDependency {
    pub name: String,
    pub version_range: String,
}

#[derive(Clone, Debug)]
pub struct ZiPluginDescriptor {
    pub manifest: ZiPluginManifest,
    pub path: String,
    pub loaded_at: std::time::SystemTime,
    pub id: String,
}

#[derive(Debug)]
pub struct ZiLoadedPluginPackage {
    pub descriptor: ZiPluginDescriptor,
    pub library: libloading::Library,
    pub instance: Box<dyn ZiPluginInstance>,
}

pub trait ZiPluginInstance: Send + Sync + std::fmt::Debug {
    fn name(&self) -> &str;
    fn version(&self) -> &str;
    fn initialize(&mut self, config: &Value) -> Result<()>;
    fn shutdown(&mut self) -> Result<()>;
    fn create_operator(&self, name: &str, config: &Value) -> Result<Box<dyn ZiOperator>>;
    fn capabilities(&self) -> Vec<&str>;
}

pub trait ZiPlugin: Send + Sync + std::fmt::Debug {
    fn manifest(&self) -> &ZiPluginManifest;
    fn initialize(&mut self, config: &Value) -> Result<()>;
    fn shutdown(&mut self) -> Result<()>;
    fn create_operator(&self, name: &str, config: &Value) -> Result<Box<dyn ZiOperator>>;
    fn capabilities(&self) -> Vec<&str>;
}

#[derive(Default)]
pub struct ZiPluginLifecycleManager {
    active_plugins: HashMap<String, Box<dyn ZiPluginInstance>>,
    #[allow(dead_code)]
    state: HashMap<String, serde_json::Map<String, Value>>,
}

impl ZiPluginLifecycleManager {
    #[allow(non_snake_case)]
    pub fn new() -> Self {
        Self {
            active_plugins: HashMap::new(),
            state: HashMap::new(),
        }
    }

    #[allow(non_snake_case)]
    pub fn register(&mut self, plugin_id: String, instance: Box<dyn ZiPluginInstance>) {
        self.active_plugins.insert(plugin_id, instance);
    }

    #[allow(non_snake_case)]
    pub fn unregister(&mut self, plugin_id: &str) -> Result<()> {
        if let Some(mut instance) = self.active_plugins.remove(plugin_id) {
            instance.shutdown()?;
        }
        Ok(())
    }

    #[allow(non_snake_case)]
    pub fn get_plugin(&self, plugin_id: &str) -> Option<&dyn ZiPluginInstance> {
        self.active_plugins.get(plugin_id).map(|p| p.as_ref())
    }

    #[allow(non_snake_case)]
    pub fn get_all_plugins(&self) -> Vec<&dyn ZiPluginInstance> {
        self.active_plugins.values().map(|p| p.as_ref()).collect()
    }
}

#[derive(Default)]
pub struct ZiPluginRegistry {
    manifests: HashMap<String, ZiPluginManifest>,
    factories: HashMap<String, fn(&Value) -> Result<Box<dyn ZiOperator>>>,
}

impl ZiPluginRegistry {
    #[allow(non_snake_case)]
    pub fn new() -> Self {
        Self {
            manifests: HashMap::new(),
            factories: HashMap::new(),
        }
    }

    #[allow(non_snake_case)]
    pub fn register(&mut self, manifest: ZiPluginManifest) {
        self.manifests.insert(manifest.name.clone(), manifest);
    }

    #[allow(non_snake_case)]
    pub fn register_operator_factory(
        &mut self,
        plugin_name: &str,
        operator_name: &str,
        factory: fn(&Value) -> Result<Box<dyn ZiOperator>>,
    ) {
        let key = format!("{}::{}", plugin_name, operator_name);
        self.factories.insert(key, factory);
    }

    #[allow(non_snake_case)]
    pub fn get_factory(
        &self,
        plugin_name: &str,
        operator_name: &str,
    ) -> Option<fn(&Value) -> Result<Box<dyn ZiOperator>>> {
        let key = format!("{}::{}", plugin_name, operator_name);
        self.factories.get(&key).copied()
    }

    #[allow(non_snake_case)]
    pub fn list_plugins(&self) -> Vec<&ZiPluginManifest> {
        self.manifests.values().collect()
    }
}

#[repr(C)]
pub struct ZiExternalOperatorVTable {
    pub version: u32,
    pub apply: extern "C" fn(*const ZiRecordBatch, *mut ZiRecordBatch) -> i32,
    pub name: extern "C" fn() -> *const c_char,
    pub destroy: extern "C" fn(*mut ZiExternalOperatorVTable),
}

#[repr(C)]
pub struct ZiRecordBatchC {
    pub ptr: *mut std::os::raw::c_void,
    pub len: usize,
    pub capacity: usize,
}

#[repr(C)]
pub struct ZiPluginApi {
    pub register_operator: extern "C" fn(*const ZiExternalOperatorVTable, *const c_char) -> i32,
    pub get_version: extern "C" fn() -> u32,
    pub log_info: extern "C" fn(*const c_char),
    pub log_error: extern "C" fn(*const c_char),
}

static mut ZI_GLOBAL_API: Option<ZiPluginApi> = None;

#[no_mangle]
pub extern "C" fn zix_register_plugin_api(api: ZiPluginApi) {
    unsafe {
        ZI_GLOBAL_API = Some(api);
    }
}

#[no_mangle]
pub extern "C" fn zix_get_api_version() -> u32 {
    ZI_PLUGIN_ABI_VERSION
}

#[allow(dead_code)]
extern "C" fn default_apply(_input: *const ZiRecordBatch, _output: *mut ZiRecordBatch) -> i32 {
    -1
}

#[allow(dead_code)]
extern "C" fn default_name() -> *const c_char {
    CString::new("unknown").unwrap().into_raw()
}

#[allow(dead_code)]
extern "C" fn default_destroy(_vtable: *mut ZiExternalOperatorVTable) {}

pub fn create_default_vtable() -> ZiExternalOperatorVTable {
    ZiExternalOperatorVTable {
        version: ZI_PLUGIN_ABI_VERSION,
        apply: default_apply,
        name: default_name,
        destroy: default_destroy,
    }
}


