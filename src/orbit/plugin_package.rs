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

use std::fs;
use std::path::Path;

use serde::Deserialize;

use crate::errors::{Result, ZiError};
use crate::orbit::runtime::{
    ZiCDataVisibility,
    ZiCPluginDescriptor,
    ZiCPluginExport,
    ZiCPluginExportKind,
    ZiCPluginPolicy,
};

#[derive(Debug, Deserialize)]
struct ZiCPluginExportFile {
    kind: String,
    name: String,
    #[serde(default)]
    script: Option<String>,
}

impl ZiCPluginExportFile {
    fn into_runtime(self) -> Result<ZiCPluginExport> {
        let kind = match self.kind.to_ascii_lowercase().as_str() {
            "operator" => ZiCPluginExportKind::Operator,
            "capability" => ZiCPluginExportKind::Capability,
            "hook" => ZiCPluginExportKind::Hook,
            other => {
                return Err(ZiError::validation(format!(
                    "unknown export kind: {}",
                    other
                )));
            }
        };
        Ok(ZiCPluginExport {
            kind,
            name: self.name,
            script: self.script,
        })
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct ZiCPluginPolicyFile {
    allowed_capabilities: Vec<String>,
    can_access_versions: bool,
}

impl ZiCPluginPolicyFile {
    fn into_runtime(self) -> ZiCPluginPolicy {
        ZiCPluginPolicy {
            allowed_capabilities: self.allowed_capabilities,
            can_access_versions: self.can_access_versions,
            default_visibility: ZiCDataVisibility::default(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct ZiCPluginDependencyFile {
    plugin_id: String,
    #[serde(default)]
    min_version: Option<String>,
    #[serde(default)]
    max_version: Option<String>,
    #[serde(default = "default_required")]
    required: bool,
}

fn default_required() -> bool {
    true
}

impl ZiCPluginDependencyFile {
    fn into_runtime(self) -> Result<crate::orbit::runtime::ZiCPluginDependency> {
        use crate::orbit::runtime::ZiCPluginVersion;
        
        let min_version = match self.min_version {
            Some(v) => Some(ZiCPluginVersion::parse(&v)?),
            None => None,
        };
        
        let max_version = match self.max_version {
            Some(v) => Some(ZiCPluginVersion::parse(&v)?),
            None => None,
        };
        
        Ok(crate::orbit::runtime::ZiCPluginDependency {
            plugin_id: self.plugin_id,
            min_version,
            max_version,
            required: self.required,
        })
    }
}

#[derive(Debug, Deserialize)]
struct ZiCPluginFile {
    id: String,
    #[serde(default = "default_version")]
    version: String,
    exports: Vec<ZiCPluginExportFile>,
    #[serde(default)]
    policy: ZiCPluginPolicyFile,
    #[serde(default)]
    visibility: Option<String>,
    #[serde(default)]
    dependencies: Vec<ZiCPluginDependencyFile>,
}

fn default_version() -> String {
    "1.0.0".to_string()
}

impl ZiCPluginFile {
    fn into_runtime(self) -> Result<ZiCPluginDescriptor> {
        use crate::orbit::runtime::{ZiCPluginVersion, ZiCPluginState};
        
        let exports = self
            .exports
            .into_iter()
            .map(|e| e.into_runtime())
            .collect::<Result<Vec<_>>>()?;
            
        let dependencies = self
            .dependencies
            .into_iter()
            .map(|d| d.into_runtime())
            .collect::<Result<Vec<_>>>()?;
            
        let mut policy = self.policy.into_runtime();
        if let Some(vis) = self.visibility {
            let parsed = match vis.to_ascii_lowercase().as_str() {
                "full" => ZiCDataVisibility::Full,
                "mask" | "mask_sensitive" => ZiCDataVisibility::MaskSensitive,
                _ => ZiCDataVisibility::default(),
            };
            policy.default_visibility = parsed;
        }
        
        let version = ZiCPluginVersion::parse(&self.version)?;
        
        Ok(ZiCPluginDescriptor {
            id: self.id,
            version,
            exports,
            policy,
            dependencies,
            state: ZiCPluginState::Loaded,
            load_time: std::time::SystemTime::now(),
        })
    }
}

pub fn ZiFLoadPluginDescriptorFromPath(path: &Path) -> Result<ZiCPluginDescriptor> {
    let meta_path = if path.is_dir() {
        path.join("orbit_plugin.json")
    } else {
        path.to_path_buf()
    };

    let text = fs::read_to_string(&meta_path)?;
    let file: ZiCPluginFile = serde_json::from_str(&text)?;
    file.into_runtime()
}
