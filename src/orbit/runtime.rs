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

use std::collections::HashMap;
use std::path::Path;

use serde_json::Value;

use crate::log::{ZiCLogLevel, ZiCLogger};
use crate::orbit::operator_registry::{ZiCOperatorRegistry, ZiFOperatorFactory};
use crate::orbit::plugin_package;
use crate::errors::{Result, ZiError};
use crate::metrics::ZiCQualityMetrics;
use crate::record::{ZiCRecord, ZiCRecordBatch};
use crate::version::ZiCVersionStore;

/// Kinds of symbols a plugin may export into the ZiOrbit runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ZiCPluginExportKind {
    Operator,
    Capability,
    Hook,
}

/// Built-in capability implementation that logs a structured event through
/// the global ZiCLogger. The expected JSON shape is:
/// {
///   "level": "INFO" | "DEBUG" | "WARNING" | "ERROR" | "SUCCESS",
///   "event": "EVENT_NAME",
///   "message": "human readable message",
///   "fields": { ... arbitrary JSON ... }
/// }
fn _orbit_log_event_capability(args: &Value, _ctx: &mut ZiCExecutionContext) -> Result<Value> {
    let obj = args
        .as_object()
        .ok_or_else(|| ZiError::internal("log.event args must be an object"))?;

    let level_str = obj
        .get("level")
        .and_then(Value::as_str)
        .unwrap_or("INFO")
        .to_ascii_uppercase();
    let level = match level_str.as_str() {
        "DEBUG" => ZiCLogLevel::Debug,
        "WARNING" => ZiCLogLevel::Warning,
        "ERROR" => ZiCLogLevel::Error,
        "SUCCESS" => ZiCLogLevel::Success,
        _ => ZiCLogLevel::Info,
    };

    let event = obj
        .get("event")
        .and_then(Value::as_str)
        .unwrap_or("ORBIT_EVENT");
    let message = obj
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("");

    let mut field_pairs = Vec::new();
    if let Some(fields) = obj.get("fields").and_then(Value::as_object) {
        for (k, v) in fields {
            field_pairs.push((k.clone(), v.clone()));
        }
    }

    ZiCLogger::ZiFEvent(level, event, message, field_pairs);
    Ok(args.clone())
}

/// Create a masked view of the given batch by stripping obviously sensitive
/// metadata fields. This is a conservative first step which can be extended in
/// the future once we have more structured sensitivity annotations.
fn _mask_sensitive_view(batch: ZiCRecordBatch) -> ZiCRecordBatch {
    batch
        .into_iter()
        .map(|mut rec: ZiCRecord| {
            if let Some(meta) = rec.metadata.as_mut() {
                // Drop common PII-related metadata keys if present.
                meta.remove("pii");
                meta.remove("pii_matches");
                meta.remove("pii.redact");
            }
            rec
        })
        .collect()
}

/// A single exported symbol from a plugin: an operator, capability, or hook.
#[derive(Clone, Debug)]
pub struct ZiCPluginExport {
    pub kind: ZiCPluginExportKind,
    pub name: String,
    /// Optional script path associated with this export (typically for
    /// operator implementations backed by a DSL or scripting language).
    pub script: Option<String>,
}

/// Simple policy describing what a plugin is allowed to do inside ZiOrbit.
#[derive(Clone, Debug, Default)]
pub struct ZiCPluginPolicy {
    /// Names of capabilities this plugin is allowed to call.
    pub allowed_capabilities: Vec<String>,
    /// Whether the plugin may access the version store.
    pub can_access_versions: bool,
    /// Default data visibility policy applied when creating an execution
    /// context for this plugin.
    pub default_visibility: ZiCDataVisibility,
}

/// Plugin version information
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ZiCPluginVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    pub pre_release: Option<String>,
}

impl ZiCPluginVersion {
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        ZiCPluginVersion {
            major,
            minor,
            patch,
            pre_release: None,
        }
    }

    pub fn parse(version_str: &str) -> Result<Self> {
        let parts: Vec<&str> = version_str.split('.').collect();
        if parts.len() < 3 {
            return Err(ZiError::validation("Invalid version format, expected major.minor.patch".to_string()));
        }

        let major = parts[0].parse::<u32>()
            .map_err(|_| ZiError::validation("Invalid major version".to_string()))?;
        let minor = parts[1].parse::<u32>()
            .map_err(|_| ZiError::validation("Invalid minor version".to_string()))?;
        
        let (patch, pre_release) = if let Some(dash_pos) = parts[2].find('-') {
            let patch_str = &parts[2][..dash_pos];
            let pre_str = &parts[2][dash_pos + 1..];
            let patch = patch_str.parse::<u32>()
                .map_err(|_| ZiError::validation("Invalid patch version".to_string()))?;
            (patch, Some(pre_str.to_string()))
        } else {
            let patch = parts[2].parse::<u32>()
                .map_err(|_| ZiError::validation("Invalid patch version".to_string()))?;
            (patch, None)
        };

        Ok(ZiCPluginVersion {
            major,
            minor,
            patch,
            pre_release,
        })
    }

    pub fn to_string(&self) -> String {
        match &self.pre_release {
            Some(pre) => format!("{}.{}.{}", self.major, self.minor, pre),
            None => format!("{}.{}.{}", self.major, self.minor, self.patch),
        }
    }
}

/// Plugin dependency specification
#[derive(Clone, Debug)]
pub struct ZiCPluginDependency {
    pub plugin_id: String,
    pub min_version: Option<ZiCPluginVersion>,
    pub max_version: Option<ZiCPluginVersion>,
    pub required: bool,
}

/// Plugin lifecycle state
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ZiCPluginState {
    Loaded,
    Active,
    Inactive,
    Unloading,
    Error(String),
}

/// High-level description of a loaded plugin inside ZiOrbit.
#[derive(Clone, Debug)]
pub struct ZiCPluginDescriptor {
    pub id: String,
    pub version: ZiCPluginVersion,
    pub exports: Vec<ZiCPluginExport>,
    pub policy: ZiCPluginPolicy,
    pub dependencies: Vec<ZiCPluginDependency>,
    pub state: ZiCPluginState,
    pub load_time: std::time::SystemTime,
}

/// Controls how much of the underlying data a plugin may see.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ZiCDataVisibility {
    /// Full access to records and metadata.
    Full,
    /// Sensitive content may be masked or removed before being exposed.
    MaskSensitive,
}

impl Default for ZiCDataVisibility {
    fn default() -> Self {
        ZiCDataVisibility::MaskSensitive
    }
}

/// Execution context passed to plugins when they are invoked.
///
/// This gives controlled access to metrics, versions, and registered
/// capabilities without exposing the full host environment.
#[derive(Debug)]
pub struct ZiCExecutionContext<'a> {
    pub metrics: &'a mut ZiCQualityMetrics,
    pub version_store: Option<&'a mut ZiCVersionStore>,
    /// Registry of host capabilities exposed to plugins.
    pub capabilities: &'a ZiCCapabilityRegistry,
    /// Visibility policy for data exposed to the current plugin.
    pub visibility: ZiCDataVisibility,
}

/// Type alias for a host capability implementation.
pub type ZiFCapabilityFn = fn(&Value, &mut ZiCExecutionContext) -> Result<Value>;

/// Type alias for a script-backed operator implementation. The runtime is
/// responsible for interpreting the script at `script_path` and applying it to
/// the provided batch under the given configuration and execution context.
pub type ZiFScriptOperatorFn = fn(
    plugin_id: &str,
    operator_name: &str,
    script_path: &str,
    batch: ZiCRecordBatch,
    config: &Value,
    ctx: &mut ZiCExecutionContext,
) -> Result<ZiCRecordBatch>;

/// Registry holding all capabilities that ZiOrbit exposes to plugins.
#[derive(Clone, Debug, Default)]
pub struct ZiCCapabilityRegistry {
    inner: HashMap<String, ZiFCapabilityFn>,
}

impl ZiCCapabilityRegistry {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        ZiCCapabilityRegistry {
            inner: HashMap::new(),
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFRegister(&mut self, name: &str, f: ZiFCapabilityFn) {
        self.inner.insert(name.to_string(), f);
    }

    #[allow(non_snake_case)]
    pub fn ZiFCall(
        &self,
        name: &str,
        args: &Value,
        ctx: &mut ZiCExecutionContext,
    ) -> Result<Value> {
        let func = self
            .inner
            .get(name)
            .ok_or_else(|| ZiError::internal(format!("unknown capability: {}", name)))?;
        func(args, ctx)
    }
}

/// Core trait implemented by ZiOrbit runtimes.
///
/// Different runtime backends (in-process, script-based, remote, etc.) can
/// implement this trait to support loading plugins and invoking their exports.
pub trait ZiCOrbit {
    /// Load a plugin from a filesystem path and return its descriptor.
    fn ZiFLoadPluginFromPath(&mut self, path: &Path) -> crate::errors::Result<ZiCPluginDescriptor>;

    /// Unload a plugin from the runtime.
    fn ZiFUnloadPlugin(&mut self, plugin_id: &str) -> crate::errors::Result<()>;

    /// Get information about loaded plugins.
    fn ZiFGetLoadedPlugins(&self) -> Vec<&ZiCPluginDescriptor>;

    /// Check if a plugin is loaded.
    fn ZiFIsPluginLoaded(&self, plugin_id: &str) -> bool;

    /// Upgrade a plugin to a new version.
    fn ZiFUpgradePlugin(&mut self, plugin_id: &str, new_path: &Path) -> crate::errors::Result<()>;

    /// Invoke an operator exported by a plugin.
    fn ZiFCallOperator(
        &self,
        plugin_id: &str,
        operator_name: &str,
        batch: ZiCRecordBatch,
        config: &Value,
        ctx: &mut ZiCExecutionContext,
    ) -> crate::errors::Result<ZiCRecordBatch>;

    /// Invoke a capability exported by a plugin and return an arbitrary value.
    fn ZiFCallCapability(
        &mut self,
        plugin_id: &str,
        capability_name: &str,
        args: &Value,
        ctx: &mut ZiCExecutionContext,
    ) -> crate::errors::Result<Value>;
}

/// Plugin lifecycle manager responsible for dependency resolution and version management
#[derive(Debug, Default)]
pub struct ZiCPluginLifecycleManager {
    /// Map of plugin dependencies (plugin_id -> dependent_plugin_ids)
    dependency_graph: HashMap<String, Vec<String>>,
    /// Map of plugin reverse dependencies (plugin_id -> required_by_plugin_ids)
    reverse_deps: HashMap<String, Vec<String>>,
}

impl ZiCPluginLifecycleManager {
    pub fn new() -> Self {
        ZiCPluginLifecycleManager {
            dependency_graph: HashMap::new(),
            reverse_deps: HashMap::new(),
        }
    }

    /// Register a plugin and its dependencies
    pub fn register_plugin(&mut self, descriptor: &ZiCPluginDescriptor) -> Result<()> {
        let plugin_id = descriptor.id.clone();
        
        // Check for circular dependencies
        self.check_circular_dependencies(&plugin_id, &descriptor.dependencies)?;
        
        // Build dependency graph
        let mut deps = Vec::new();
        for dep in &descriptor.dependencies {
            deps.push(dep.plugin_id.clone());
            
            // Add reverse dependency
            self.reverse_deps
                .entry(dep.plugin_id.clone())
                .or_insert_with(Vec::new)
                .push(plugin_id.clone());
        }
        
        self.dependency_graph.insert(plugin_id, deps);
        Ok(())
    }

    /// Check if removing a plugin would break dependencies
    pub fn can_unload_plugin(&self, plugin_id: &str, loaded_plugins: &HashMap<String, ZiCPluginDescriptor>) -> Result<()> {
        if let Some(dependents) = self.reverse_deps.get(plugin_id) {
            for dependent in dependents {
                if loaded_plugins.contains_key(dependent) {
                    let dependent_plugin = loaded_plugins.get(dependent).unwrap();
                    for dep in &dependent_plugin.dependencies {
                        if dep.plugin_id == plugin_id && dep.required {
                            return Err(ZiError::validation(format!(
                                "Cannot unload plugin '{}' because it is required by plugin '{}'",
                                plugin_id, dependent
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Get all plugins that depend on the given plugin
    pub fn get_dependent_plugins(&self, plugin_id: &str) -> Vec<String> {
        self.reverse_deps.get(plugin_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Check for circular dependencies using DFS
    fn check_circular_dependencies(&self, plugin_id: &str, dependencies: &[ZiCPluginDependency]) -> Result<()> {
        let mut visited = HashMap::new();
        let mut recursion_stack = HashMap::new();
        
        for dep in dependencies {
            if self.has_circular_dependency(&dep.plugin_id, &mut visited, &mut recursion_stack)? {
                return Err(ZiError::validation(format!(
                    "Circular dependency detected involving plugin '{}'",
                    plugin_id
                )));
            }
        }
        Ok(())
    }

    fn has_circular_dependency(&self, plugin_id: &str, visited: &mut HashMap<String, bool>, recursion_stack: &mut HashMap<String, bool>) -> Result<bool> {
        visited.insert(plugin_id.to_string(), true);
        recursion_stack.insert(plugin_id.to_string(), true);

        if let Some(deps) = self.dependency_graph.get(plugin_id) {
            for dep in deps {
                if !visited.contains_key(dep) {
                    if self.has_circular_dependency(dep, visited, recursion_stack)? {
                        return Ok(true);
                    }
                } else if recursion_stack.get(dep) == Some(&true) {
                    return Ok(true);
                }
            }
        }

        recursion_stack.insert(plugin_id.to_string(), false);
        Ok(false)
    }

    /// Remove a plugin from the dependency graph
    pub fn unregister_plugin(&mut self, plugin_id: &str) {
        // Remove from dependency graph
        self.dependency_graph.remove(plugin_id);
        
        // Remove from reverse dependencies
        self.reverse_deps.remove(plugin_id);
        
        // Remove from other plugins' reverse dependencies
        for deps in self.reverse_deps.values_mut() {
            deps.retain(|id| id != plugin_id);
        }
    }
}

/// Minimal in-process implementation of ZiOrbit used during the initial
/// refactor. For now this is only a stub that refuses to load or call
/// anything; pipeline integration will be added in later steps.
#[derive(Debug, Default)]
pub struct ZiCInProcessOrbit {
    capabilities: ZiCCapabilityRegistry,
    plugins: HashMap<String, ZiCPluginDescriptor>,
    operators: ZiCOperatorRegistry,
    script_operator: Option<ZiFScriptOperatorFn>,
    lifecycle_manager: ZiCPluginLifecycleManager,
}

impl ZiCInProcessOrbit {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        let mut capabilities = ZiCCapabilityRegistry::ZiFNew();
        // Built-in logging capability available to plugins that opt-in via
        // their ZiCPluginPolicy.allowed_capabilities.
        capabilities.ZiFRegister("log.event", _orbit_log_event_capability);

        ZiCInProcessOrbit {
            capabilities,
            plugins: HashMap::new(),
            operators: ZiCOperatorRegistry::ZiFNew(),
            script_operator: None,
            lifecycle_manager: ZiCPluginLifecycleManager::new(),
        }
    }

    /// Expose the internal capability registry so that callers can register
    /// host-provided capabilities before running any plugins.
    #[allow(non_snake_case)]
    pub fn ZiFCapabilitiesMut(&mut self) -> &mut ZiCCapabilityRegistry {
        &mut self.capabilities
    }

    /// Construct an execution context for the given plugin, enforcing its
    /// policy (such as version-store access and default visibility).
    #[allow(non_snake_case)]
    pub fn ZiFMakeExecutionContext<'a>(
        &'a self,
        plugin_id: &str,
        metrics: &'a mut ZiCQualityMetrics,
        version_store: Option<&'a mut ZiCVersionStore>,
    ) -> Result<ZiCExecutionContext<'a>> {
        let plugin = self.get_plugin(plugin_id)?;
        let vs = if plugin.policy.can_access_versions {
            version_store
        } else {
            None
        };
        Ok(ZiCExecutionContext {
            metrics,
            version_store: vs,
            capabilities: &self.capabilities,
            visibility: plugin.policy.default_visibility.clone(),
        })
    }

    /// Internal helper to look up a plugin descriptor by id.
    fn get_plugin(&self, plugin_id: &str) -> Result<&ZiCPluginDescriptor> {
        self.plugins
            .get(plugin_id)
            .ok_or_else(|| ZiError::internal(format!("unknown plugin: {}", plugin_id)))
    }

    /// Manually register a plugin descriptor with this in-process runtime.
    ///
    /// This is primarily intended for tests and for embedding ZiOrbit inside
    /// other applications before a full plugin loading mechanism is wired.
    #[allow(non_snake_case)]
    pub fn ZiFRegisterPlugin(&mut self, descriptor: ZiCPluginDescriptor) -> Result<()> {
        let id = descriptor.id.clone();
        
        // Check if plugin with same ID already exists
        if let Some(existing) = self.plugins.get(&id) {
            // Check version compatibility
            if descriptor.version <= existing.version {
                let mut fields = Vec::new();
                fields.push(("plugin".to_string(), Value::String(id)));
                fields.push(("existing_version".to_string(), Value::String(existing.version.to_string())));
                fields.push(("new_version".to_string(), Value::String(descriptor.version.to_string())));
                ZiCLogger::ZiFEvent(
                    ZiCLogLevel::Warning,
                    "orbit.plugin.version_conflict",
                    "plugin with same id and lower/equal version already registered; keeping existing descriptor",
                    fields,
                );
                return Ok(());
            }
            
            // Newer version, allow registration (upgrade)
            let mut fields = Vec::new();
            fields.push(("plugin".to_string(), Value::String(id.clone())));
            fields.push(("existing_version".to_string(), Value::String(existing.version.to_string())));
            fields.push(("new_version".to_string(), Value::String(descriptor.version.to_string())));
            ZiCLogger::ZiFEvent(
                ZiCLogLevel::Info,
                "orbit.plugin.upgrade",
                "upgrading plugin to newer version",
                fields,
            );
        }

        // Register with lifecycle manager
        self.lifecycle_manager.register_plugin(&descriptor)?;

        let mut fields = Vec::new();
        fields.push(("plugin".to_string(), Value::String(id.clone())));
        fields.push(("version".to_string(), Value::String(descriptor.version.to_string())));
        fields.push((
            "export_count".to_string(),
            Value::String(descriptor.exports.len().to_string()),
        ));
        ZiCLogger::ZiFEvent(
            ZiCLogLevel::Info,
            "orbit.plugin.register",
            "plugin registered",
            fields,
        );

        self.plugins.insert(id, descriptor);
        Ok(())
    }

    /// Unload a plugin from the runtime.
    #[allow(non_snake_case)]
    pub fn ZiFUnloadPlugin(&mut self, plugin_id: &str) -> Result<()> {
        // Check if plugin exists
        if !self.plugins.contains_key(plugin_id) {
            return Err(ZiError::internal(format!("Plugin '{}' not found", plugin_id)));
        }

        // Check if plugin can be safely unloaded (no dependent plugins)
        self.lifecycle_manager.can_unload_plugin(plugin_id, &self.plugins)?;

        // Get dependent plugins for logging
        let dependents = self.lifecycle_manager.get_dependent_plugins(plugin_id);

        // Unregister from lifecycle manager
        self.lifecycle_manager.unregister_plugin(plugin_id);

        // Remove from plugins map
        let removed_plugin = self.plugins.remove(plugin_id)
            .ok_or_else(|| ZiError::internal(format!("Failed to remove plugin '{}'", plugin_id)))?;

        // Log the operation
        let mut fields = Vec::new();
        fields.push(("plugin".to_string(), Value::String(plugin_id.to_string())));
        fields.push(("version".to_string(), Value::String(removed_plugin.version.to_string())));
        if !dependents.is_empty() {
            fields.push(("dependents".to_string(), Value::Array(
                dependents.into_iter().map(Value::String).collect()
            )));
        }
        ZiCLogger::ZiFEvent(
            ZiCLogLevel::Info,
            "orbit.plugin.unloaded",
            "plugin unloaded successfully",
            fields,
        );

        Ok(())
    }

    /// Get information about all loaded plugins.
    #[allow(non_snake_case)]
    pub fn ZiFGetLoadedPlugins(&self) -> Vec<&ZiCPluginDescriptor> {
        self.plugins.values().collect()
    }

    /// Check if a plugin is loaded.
    #[allow(non_snake_case)]
    pub fn ZiFIsPluginLoaded(&self, plugin_id: &str) -> bool {
        self.plugins.contains_key(plugin_id)
    }

    /// Register an operator factory with this runtime. This is intended to be
    /// used by the core to expose built-in operators and by plugins that wish
    /// to provide additional operators.
    #[allow(non_snake_case)]
    pub fn ZiFRegisterOperator(&mut self, name: &str, factory: ZiFOperatorFactory) {
        self.operators.ZiFRegister(name, factory);
    }

    /// Install a script-backed operator runtime. When a plugin exports an
    /// operator with an associated script but there is no native factory
    /// registered for that operator name, this hook will be invoked to execute
    /// the script.
    #[allow(non_snake_case)]
    pub fn ZiFSetScriptOperatorRuntime(&mut self, runtime: ZiFScriptOperatorFn) {
        self.script_operator = Some(runtime);
    }

    /// Upgrade a plugin to a new version.
    #[allow(non_snake_case)]
    pub fn ZiFUpgradePlugin(&mut self, plugin_id: &str, new_path: &Path) -> Result<()> {
        // Load new plugin descriptor
        let new_descriptor = plugin_package::ZiFLoadPluginDescriptorFromPath(new_path)?;
        
        // Check if plugin exists
        if !self.plugins.contains_key(plugin_id) {
            return Err(ZiError::internal(format!("Plugin '{}' not found", plugin_id)));
        }
        
        // Check if new plugin ID matches
        if new_descriptor.id != plugin_id {
            return Err(ZiError::internal(format!(
                "Plugin ID mismatch: expected '{}', got '{}'",
                plugin_id, new_descriptor.id
            )));
        }
        
        // Get current plugin
        let current_plugin = self.plugins.get(plugin_id).unwrap();
        
        // Check if new version is newer
        if new_descriptor.version <= current_plugin.version {
            return Err(ZiError::internal(format!(
                "New version {} is not newer than current version {}",
                new_descriptor.version.to_string(),
                current_plugin.version.to_string()
            )));
        }
        
        // Check if upgrade would break dependencies
        self.lifecycle_manager.can_unload_plugin(plugin_id, &self.plugins)?;
        
        // Log upgrade operation
        let mut fields = Vec::new();
        fields.push(("plugin".to_string(), Value::String(plugin_id.to_string())));
        fields.push(("old_version".to_string(), Value::String(current_plugin.version.to_string())));
        fields.push(("new_version".to_string(), Value::String(new_descriptor.version.to_string())));
        fields.push(("path".to_string(), Value::String(new_path.to_string_lossy().to_string())));
        ZiCLogger::ZiFEvent(
            ZiCLogLevel::Info,
            "orbit.plugin.upgrade",
            "upgrading plugin",
            fields,
        );
        
        // Unregister old plugin from lifecycle manager
        self.lifecycle_manager.unregister_plugin(plugin_id);
        
        // Register new plugin (this will handle lifecycle management)
        self.ZiFRegisterPlugin(new_descriptor)?;
        
        Ok(())
    }
}

impl ZiCOrbit for ZiCInProcessOrbit {
    fn ZiFLoadPluginFromPath(&mut self, path: &Path) -> Result<ZiCPluginDescriptor> {
        match plugin_package::ZiFLoadPluginDescriptorFromPath(path) {
            Ok(descriptor) => {
                let mut fields = Vec::new();
                fields.push((
                    "path".to_string(),
                    Value::String(path.to_string_lossy().to_string()),
                ));
                fields.push((
                    "plugin".to_string(),
                    Value::String(descriptor.id.clone()),
                ));
                fields.push((
                    "version".to_string(),
                    Value::String(descriptor.version.to_string()),
                ));
                fields.push((
                    "export_count".to_string(),
                    Value::String(descriptor.exports.len().to_string()),
                ));
                if !descriptor.dependencies.is_empty() {
                    fields.push((
                        "dependencies".to_string(),
                        Value::Array(descriptor.dependencies.iter().map(|d| {
                            Value::String(format!("{} (required: {})", d.plugin_id, d.required))
                        }).collect())
                    ));
                }
                ZiCLogger::ZiFEvent(
                    ZiCLogLevel::Info,
                    "orbit.plugin.load",
                    "plugin descriptor loaded",
                    fields,
                );
                self.ZiFRegisterPlugin(descriptor.clone())?;
                Ok(descriptor)
            }
            Err(e) => {
                let mut fields = Vec::new();
                fields.push((
                    "path".to_string(),
                    Value::String(path.to_string_lossy().to_string()),
                ));
                fields.push((
                    "error".to_string(),
                    Value::String(e.to_string()),
                ));
                ZiCLogger::ZiFEvent(
                    ZiCLogLevel::Error,
                    "orbit.plugin.load_failed",
                    "failed to load plugin descriptor",
                    fields,
                );
                Err(e)
            }
        }
    }

    fn ZiFUnloadPlugin(&mut self, plugin_id: &str) -> crate::errors::Result<()> {
        self.ZiFUnloadPlugin(plugin_id)
    }

    fn ZiFGetLoadedPlugins(&self) -> Vec<&ZiCPluginDescriptor> {
        self.ZiFGetLoadedPlugins()
    }

    fn ZiFIsPluginLoaded(&self, plugin_id: &str) -> bool {
        self.ZiFIsPluginLoaded(plugin_id)
    }

    fn ZiFUpgradePlugin(&mut self, plugin_id: &str, new_path: &Path) -> crate::errors::Result<()> {
        self.ZiFUpgradePlugin(plugin_id, new_path)
    }

    fn ZiFCallOperator(
        &self,
        plugin_id: &str,
        operator_name: &str,
        batch: ZiCRecordBatch,
        config: &Value,
        ctx: &mut ZiCExecutionContext,
    ) -> Result<ZiCRecordBatch> {
        let plugin = self.get_plugin(plugin_id)?;
        // Emit a structured log event for observability of operator calls.
        let mut fields = Vec::new();
        fields.push(("plugin".to_string(), Value::String(plugin_id.to_string())));
        fields.push((
            "operator".to_string(),
            Value::String(operator_name.to_string()),
        ));
        fields.push((
            "visibility".to_string(),
            Value::String(match ctx.visibility {
                ZiCDataVisibility::Full => "full".to_string(),
                ZiCDataVisibility::MaskSensitive => "mask_sensitive".to_string(),
            }),
        ));
        ZiCLogger::ZiFEvent(
            ZiCLogLevel::Debug,
            "orbit.operator.call",
            "plugin operator invocation",
            fields,
        );

        let export_script = plugin
            .exports
            .iter()
            .find(|e| e.kind == ZiCPluginExportKind::Operator && e.name == operator_name)
            .and_then(|e| e.script.as_deref());
        let factory_result = self.operators.ZiFGet(operator_name);

        match factory_result {
            Ok(factory) => {
                let op = factory(config);
                // Apply data visibility policy: when MaskSensitive is in effect we
                // provide the operator with a masked view of the batch to avoid
                // leaking sensitive metadata to untrusted plugins.
                let input_batch = match ctx.visibility {
                    ZiCDataVisibility::Full => batch,
                    ZiCDataVisibility::MaskSensitive => _mask_sensitive_view(batch),
                };
                match op {
                    Ok(operator) => operator.apply(input_batch),
                    Err(e) => {
                        let mut err_fields = Vec::new();
                        err_fields.push((
                            "plugin".to_string(),
                            Value::String(plugin_id.to_string()),
                        ));
                        err_fields.push((
                            "operator".to_string(),
                            Value::String(operator_name.to_string()),
                        ));
                        err_fields.push((
                            "error".to_string(),
                            Value::String(e.to_string()),
                        ));
                        ZiCLogger::ZiFEvent(
                            ZiCLogLevel::Error,
                            "orbit.operator.build_failed",
                            "failed to build operator",
                            err_fields,
                        );
                        Err(ZiError::internal(format!(
                            "failed to build operator '{}': {}",
                            operator_name, e
                        )))
                    }
                }
            }
            Err(e) => {
                if let Some(script_path) = export_script {
                    if let Some(runtime) = self.script_operator {
                        let mut script_fields = Vec::new();
                        script_fields.push((
                            "plugin".to_string(),
                            Value::String(plugin_id.to_string()),
                        ));
                        script_fields.push((
                            "operator".to_string(),
                            Value::String(operator_name.to_string()),
                        ));
                        script_fields.push((
                            "script".to_string(),
                            Value::String(script_path.to_string()),
                        ));
                        ZiCLogger::ZiFEvent(
                            ZiCLogLevel::Debug,
                            "orbit.operator.script_call",
                            "script-backed operator invocation",
                            script_fields,
                        );
                        let input_batch = match ctx.visibility {
                            ZiCDataVisibility::Full => batch,
                            ZiCDataVisibility::MaskSensitive => _mask_sensitive_view(batch),
                        };
                        return runtime(
                            plugin_id,
                            operator_name,
                            script_path,
                            input_batch,
                            config,
                            ctx,
                        );
                    }

                    let mut err_fields = Vec::new();
                    err_fields.push((
                        "plugin".to_string(),
                        Value::String(plugin_id.to_string()),
                    ));
                    err_fields.push((
                        "operator".to_string(),
                        Value::String(operator_name.to_string()),
                    ));
                    err_fields.push((
                        "script".to_string(),
                        Value::String(script_path.to_string()),
                    ));
                    ZiCLogger::ZiFEvent(
                        ZiCLogLevel::Warning,
                        "orbit.operator.script_unavailable",
                        "script-backed operator has no runtime configured",
                        err_fields,
                    );
                    return Err(ZiError::internal(format!(
                        "operator '{}' for plugin '{}' is script-backed ('{}') but no script runtime is configured",
                        operator_name, plugin_id, script_path
                    )));
                }

                Err(e)
            }
        }
    }

    fn ZiFCallCapability(
        &mut self,
        plugin_id: &str,
        capability_name: &str,
        args: &Value,
        ctx: &mut ZiCExecutionContext,
    ) -> Result<Value> {
        let plugin = self.get_plugin(plugin_id)?;

        // Policy check: ensure the plugin is allowed to call this capability.
        if !plugin
            .policy
            .allowed_capabilities
            .iter()
            .any(|name| name == capability_name)
        {
            let mut fields = Vec::new();
            fields.push(("plugin".to_string(), Value::String(plugin_id.to_string())));
            fields.push((
                "capability".to_string(),
                Value::String(capability_name.to_string()),
            ));
            ZiCLogger::ZiFEvent(
                ZiCLogLevel::Warning,
                "orbit.capability.denied",
                "plugin not allowed to call capability",
                fields,
            );
            return Err(ZiError::internal(format!(
                "plugin '{}' is not allowed to call capability '{}'",
                plugin_id, capability_name
            )));
        }

        // Delegate to the capability registry while emitting an audit log event.
        let mut fields = Vec::new();
        fields.push(("plugin".to_string(), Value::String(plugin_id.to_string())));
        fields.push((
            "capability".to_string(),
            Value::String(capability_name.to_string()),
        ));
        ZiCLogger::ZiFEvent(
            ZiCLogLevel::Debug,
            "orbit.capability.call",
            "plugin capability invocation",
            fields,
        );

        self.capabilities.ZiFCall(capability_name, args, ctx)
    }
}

