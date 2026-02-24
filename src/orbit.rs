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

//! # ZiOrbit Plugin System Module
//!
//! This module provides the plugin runtime system for Zi, enabling dynamic loading
//! and execution of operator plugins at runtime.
//!
//! ## Submodules
//!
//! - `runtime`: Core runtime for plugin execution
//! - `plugin`: Plugin interface and descriptors
//! - `operator_registry`: Registry for operator factories
//! - `plugin_package`: Plugin package loading and validation

pub mod runtime;
pub(crate) mod operator_registry;
pub(crate) mod plugin_package;
pub mod plugin;

pub use runtime::{
    ZiDataVisibility,
    ZiExecutionContext,
    ZiInProcessOrbit,
    ZiOrbit,
    ZiPluginDescriptor,
    ZiPluginExport,
    ZiPluginExportKind,
    ZiPluginPolicy,
    ScriptOperatorFn,
};

pub use operator_registry::{ZiOperatorRegistry, OperatorFactory};
pub use plugin_package::load_plugin_descriptor_from_path;
pub use plugin::{
    ZiPlugin, ZiPluginInstance, ZiPluginManifest, ZiPluginRegistry,
    ZiPluginLifecycleManager, ZiPluginApi, ZI_PLUGIN_ABI_VERSION,
};
