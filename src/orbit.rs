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

pub mod runtime;
pub(crate) mod operator_registry;
pub(crate) mod plugin_package;
pub mod plugin;

pub use runtime::{
    ZiCDataVisibility,
    ZiCExecutionContext,
    ZiCInProcessOrbit,
    ZiCOrbit,
    ZiCPluginDescriptor,
    ZiCPluginExport,
    ZiCPluginExportKind,
    ZiCPluginPolicy,
    ZiFScriptOperatorFn,
};

pub use operator_registry::{ZiCOperatorRegistry, ZiFOperatorFactory};
pub use plugin_package::ZiFLoadPluginDescriptorFromPath;
pub use plugin::{
    ZiCPlugin, ZiCPluginInstance, ZiCPluginManifest, ZiCPluginRegistry,
    ZiCPluginLifecycleManager, ZiCPluginApi, ZI_PLUGIN_ABI_VERSION,
};
