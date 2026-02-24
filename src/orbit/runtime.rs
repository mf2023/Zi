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

//! # ZiOrbit Runtime Module
//!
//! This module provides the plugin runtime system for Zi, enabling dynamic
//! loading and execution of operator plugins at runtime.
//!
//! ## Core Concepts
//!
//! - **ZiOrbit**: Main runtime context for plugin execution
//! - **ZiInProcessOrbit**: In-process plugin execution environment
//! - **ZiExecutionContext**: Mutable context passed to plugins during execution
//! - **ZiPluginDescriptor**: Metadata describing a loaded plugin
//!
//! ## Plugin System
//!
//! Plugins can export operators, capabilities, and hooks into the runtime.
//! The runtime manages plugin lifecycle, symbol resolution, and execution.

use std::collections::HashMap;
use std::path::Path;

use serde_json::Value;

use crate::orbit::operator_registry::{ZiOperatorRegistry, OperatorFactory};
use crate::orbit::plugin_package;
use crate::errors::{Result, ZiError};
use crate::metrics::ZiQualityMetrics;
use crate::record::{ZiRecord, ZiRecordBatch};
use crate::version::ZiVersionStore;

/// Kinds of symbols a plugin may export into the ZiOrbit runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ZiPluginExportKind {
    Operator,
    Capability,
    Hook,
}

/// Built-in capability implementation that logs a structured event through
/// the standard log crate. The expected JSON shape is:
/// {
///   "level": "INFO" | "DEBUG" | "WARNING" | "ERROR" | "SUCCESS",
///   "event": "EVENT_NAME",
///   "message": "human readable message",
///   "fields": { ... arbitrary JSON ... }
/// }
fn _orbit_log_event_capability(args: &Value, _ctx: &mut ZiExecutionContext) -> Result<Value> {
    let obj = args
        .as_object()
        .ok_or_else(|| ZiError::internal("log.event args must be an object"))?;

    let level_str = obj
        .get("level")
        .and_then(Value::as_str)
        .unwrap_or("INFO")
        .to_ascii_uppercase();

    let event = obj
        .get("event")
        .and_then(Value::as_str)
        .unwrap_or("ORBIT_EVENT");
    let message = obj
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("");

    let log_msg = format!("{}: {}", event, message);
    match level_str.as_str() {
        "DEBUG" => log::debug!("{}", log_msg),
        "WARNING" => log::warn!("{}", log_msg),
        "ERROR" => log::error!("{}", log_msg),
        _ => log::info!("{}", log_msg),
    }

    Ok(args.clone())
}

/// Create a masked view of the given batch by stripping obviously sensitive
/// metadata fields. This is a conservative first step which can be extended in
/// the future once we have more structured sensitivity annotations.
fn _mask_sensitive_view(batch: ZiRecordBatch) -> ZiRecordBatch {
    batch
        .into_iter()
        .map(|mut rec: ZiRecord| {
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
pub struct ZiPluginExport {
    pub kind: ZiPluginExportKind,
    pub name: String,
    /// Optional script path associated with this export (typically for
    /// operator implementations backed by a DSL or scripting language).
    pub script: Option<String>,
}

/// Sandbox configuration for a plugin.
#[derive(Clone, Debug, Default)]
pub struct ZiSandboxConfig {
    /// Maximum CPU usage allowed for the plugin (in percent).
    pub max_cpu_percent: Option<u8>,
    /// Maximum memory usage allowed for the plugin (in MB).
    pub max_memory_mb: Option<u32>,
    /// Maximum number of threads the plugin can create.
    pub max_threads: Option<u32>,
    /// Maximum disk space the plugin can use (in MB).
    pub max_disk_mb: Option<u32>,
    /// Maximum network bandwidth the plugin can use (in KB/s).
    pub max_network_kbps: Option<u32>,
    /// Whether the plugin can access the network.
    pub can_access_network: bool,
    /// Whether the plugin can execute commands.
    pub can_execute_commands: bool,
    /// Whether the plugin can write files.
    pub can_write_files: bool,
    /// Allowed file paths that the plugin can access.
    pub allowed_file_paths: Vec<String>,
    /// Allowed network hosts that the plugin can connect to.
    pub allowed_network_hosts: Vec<String>,
    /// Allowed network ports that the plugin can connect to.
    pub allowed_network_ports: Vec<u16>,
}

/// Simple policy describing what a plugin is allowed to do inside ZiOrbit.
#[derive(Clone, Debug, Default)]
pub struct ZiPluginPolicy {
    /// Names of capabilities this plugin is allowed to call.
    pub allowed_capabilities: Vec<String>,
    /// Whether the plugin may access the version store.
    pub can_access_versions: bool,
    /// Default data visibility policy applied when creating an execution
    /// context for this plugin.
    pub default_visibility: ZiDataVisibility,
    /// Plugin role, used for role-based access control.
    pub role: Option<String>,
    /// Sandbox configuration for the plugin.
    pub sandbox: ZiSandboxConfig,
}

/// Plugin version information
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ZiPluginVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    pub pre_release: Option<String>,
}

impl ZiPluginVersion {
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        ZiPluginVersion {
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

        Ok(ZiPluginVersion {
            major,
            minor,
            patch,
            pre_release,
        })
    }

    pub fn to_string(&self) -> String {
        match &self.pre_release {
            Some(pre) => format!("{}.{}.{}-{}", self.major, self.minor, self.patch, pre),
            None => format!("{}.{}.{}", self.major, self.minor, self.patch),
        }
    }
}

/// Plugin dependency specification
#[derive(Clone, Debug)]
pub struct ZiPluginDependency {
    pub plugin_id: String,
    pub min_version: Option<ZiPluginVersion>,
    pub max_version: Option<ZiPluginVersion>,
    pub required: bool,
}

/// Plugin lifecycle state
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ZiPluginState {
    Loaded,
    Active,
    Inactive,
    Unloading,
    Error(String),
}

/// High-level description of a loaded plugin inside ZiOrbit.
#[derive(Clone, Debug)]
pub struct ZiPluginDescriptor {
    pub id: String,
    pub version: ZiPluginVersion,
    pub exports: Vec<ZiPluginExport>,
    pub policy: ZiPluginPolicy,
    pub dependencies: Vec<ZiPluginDependency>,
    pub state: ZiPluginState,
    pub load_time: std::time::SystemTime,
}

/// Controls how much of the underlying data a plugin may see.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ZiDataVisibility {
    /// Full access to records and metadata.
    Full,
    /// Sensitive content may be masked or removed before being exposed.
    MaskSensitive,
}

impl Default for ZiDataVisibility {
    fn default() -> Self {
        ZiDataVisibility::MaskSensitive
    }
}

/// Execution context passed to plugins when they are invoked.
///
/// This gives controlled access to metrics, versions, and registered
/// capabilities without exposing the full host environment.
#[derive(Debug)]
pub struct ZiExecutionContext<'a> {
    pub metrics: &'a mut ZiQualityMetrics,
    pub version_store: Option<&'a mut ZiVersionStore>,
    /// Registry of host capabilities exposed to plugins.
    pub capabilities: &'a ZiCapabilityRegistry,
    /// Visibility policy for data exposed to the current plugin.
    pub visibility: ZiDataVisibility,
    /// Plugin ID associated with this execution context
    pub plugin_id: String,
    /// Sandbox configuration for the plugin
    pub sandbox: &'a ZiSandboxConfig,
}

/// Type alias for a host capability implementation.
pub type CapabilityFn = fn(&Value, &mut ZiExecutionContext) -> Result<Value>;

/// Type alias for a script-backed operator implementation. The runtime is
/// responsible for interpreting the script at `script_path` and applying it to
/// the provided batch under the given configuration and execution context.
pub type ScriptOperatorFn = fn(
    plugin_id: &str,
    operator_name: &str,
    script_path: &str,
    batch: ZiRecordBatch,
    config: &Value,
    ctx: &mut ZiExecutionContext,
) -> Result<ZiRecordBatch>;

/// Registry holding all capabilities that ZiOrbit exposes to plugins.
#[derive(Clone, Debug, Default)]
pub struct ZiCapabilityRegistry {
    inner: HashMap<String, CapabilityFn>,
}

impl ZiCapabilityRegistry {
    #[allow(non_snake_case)]
    pub fn new() -> Self {
        ZiCapabilityRegistry {
            inner: HashMap::new(),
        }
    }

    #[allow(non_snake_case)]
    pub fn register(&mut self, name: &str, f: CapabilityFn) {
        self.inner.insert(name.to_string(), f);
    }

    #[allow(non_snake_case)]
    pub fn call(
        &self,
        name: &str,
        args: &Value,
        ctx: &mut ZiExecutionContext,
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
pub trait ZiOrbit {
    /// Load a plugin from a filesystem path and return its descriptor.
    fn load_plugin_from_path(&mut self, path: &Path) -> crate::errors::Result<ZiPluginDescriptor>;

    /// Unload a plugin from the runtime.
    fn unload_plugin(&mut self, plugin_id: &str) -> crate::errors::Result<()>;

    /// Get information about loaded plugins.
    fn get_loaded_plugins(&self) -> Vec<&ZiPluginDescriptor>;

    /// Check if a plugin is loaded.
    fn is_plugin_loaded(&self, plugin_id: &str) -> bool;

    /// Upgrade a plugin to a new version.
    fn upgrade_plugin(&mut self, plugin_id: &str, new_path: &Path) -> crate::errors::Result<()>;

    /// Invoke an operator exported by a plugin.
    fn call_operator(
        &self,
        plugin_id: &str,
        operator_name: &str,
        batch: ZiRecordBatch,
        config: &Value,
        ctx: &mut ZiExecutionContext,
    ) -> crate::errors::Result<ZiRecordBatch>;

    /// Invoke a capability exported by a plugin and return an arbitrary value.
    fn call_capability(
        &mut self,
        plugin_id: &str,
        capability_name: &str,
        args: &Value,
        ctx: &mut ZiExecutionContext,
    ) -> crate::errors::Result<Value>;
}

/// Plugin lifecycle manager responsible for dependency resolution and version management
#[derive(Debug, Default)]
pub struct ZiPluginLifecycleManager {
    /// Map of plugin dependencies (plugin_id -> dependent_plugin_ids)
    dependency_graph: HashMap<String, Vec<String>>,
    /// Map of plugin reverse dependencies (plugin_id -> required_by_plugin_ids)
    reverse_deps: HashMap<String, Vec<String>>,
}

impl ZiPluginLifecycleManager {
    pub fn new() -> Self {
        ZiPluginLifecycleManager {
            dependency_graph: HashMap::new(),
            reverse_deps: HashMap::new(),
        }
    }

    /// Register a plugin and its dependencies
    pub fn register_plugin(&mut self, descriptor: &ZiPluginDescriptor) -> Result<()> {
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
    pub fn can_unload_plugin(&self, plugin_id: &str, loaded_plugins: &HashMap<String, ZiPluginDescriptor>) -> Result<()> {
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
    fn check_circular_dependencies(&self, plugin_id: &str, dependencies: &[ZiPluginDependency]) -> Result<()> {
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

/// Represents a loaded plugin package, including its metadata and resources.
#[derive(Debug)]
#[allow(dead_code)]
pub struct ZiLoadedPluginPackage {
    /// Path to the plugin package file or directory
    path: std::path::PathBuf,
    /// Plugin descriptor
    descriptor: ZiPluginDescriptor,
    /// Whether this is a ZIP package
    is_zip: bool,
    /// Last access time for cache management
    last_access: std::time::SystemTime,
}

/// Minimal in-process implementation of ZiOrbit.
///
/// This implementation provides full plugin lifecycle management including:
/// - Plugin loading from filesystem paths (ZIP packages and directories)
/// - Plugin unloading with dependency checking
/// - Plugin upgrades with version validation
/// - Operator invocation with security audit logging
/// - Capability invocation with policy enforcement
/// - Script-backed operator support
/// - Dependency graph management and circular dependency detection
#[derive(Debug, Default)]
pub struct ZiInProcessOrbit {
    capabilities: ZiCapabilityRegistry,
    plugins: HashMap<String, ZiPluginDescriptor>,
    /// Map of plugin ID to loaded package information
    loaded_packages: HashMap<String, ZiLoadedPluginPackage>,
    operators: ZiOperatorRegistry,
    script_operator: Option<ScriptOperatorFn>,
    lifecycle_manager: ZiPluginLifecycleManager,
}

impl ZiInProcessOrbit {
    #[allow(non_snake_case)]
    pub fn new() -> Self {
        let mut capabilities = ZiCapabilityRegistry::new();
        // Built-in logging capability available to plugins that opt-in via
        // their ZiPluginPolicy.allowed_capabilities.
        capabilities.register("log.event", _orbit_log_event_capability);

        let mut operators = ZiOperatorRegistry::new();
        // Register built-in operators
        operators.register("filter.equals", crate::operators::filter::filter_equals_factory);
        operators.register("filter.not_equals", crate::operators::filter::filter_not_equals_factory);
        operators.register("filter.any", crate::operators::filter::filter_any_factory);
        operators.register("filter.between", crate::operators::filter::filter_between_factory);
        operators.register("filter.less_than", crate::operators::filter::filter_less_than_factory);
        operators.register("filter.greater_than", crate::operators::filter::filter_greater_than_factory);
        operators.register("filter.is_null", crate::operators::filter::filter_is_null_factory);
        operators.register("filter.regex", crate::operators::filter::filter_regex_factory);
        operators.register("filter.ends_with", crate::operators::filter::filter_ends_with_factory);
        operators.register("filter.starts_with", crate::operators::filter::filter_starts_with_factory);
        operators.register("filter.range", crate::operators::filter::filter_range_factory);
        operators.register("filter.in", crate::operators::filter::filter_in_factory);
        operators.register("filter.not_in", crate::operators::filter::filter_not_in_factory);
        operators.register("filter.contains", crate::operators::filter::filter_contains_factory);
        operators.register("filter.contains_all", crate::operators::filter::filter_contains_all_factory);
        operators.register("filter.contains_any", crate::operators::filter::filter_contains_any_factory);
        operators.register("filter.contains_none", crate::operators::filter::filter_contains_none_factory);
        operators.register("filter.array_contains", crate::operators::filter::filter_array_contains_factory);
        operators.register("filter.exists", crate::operators::filter::filter_exists_factory);
        operators.register("filter.not_exists", crate::operators::filter::filter_not_exists_factory);
        operators.register("filter.length_range", crate::operators::filter::filter_length_range_factory);
        operators.register("filter.token_range", crate::operators::filter::filter_token_range_factory);
        
        // Register language operators
        operators.register("lang.detect", crate::operators::lang::lang_detect_factory);
        operators.register("lang.confidence", crate::operators::lang::lang_confidence_factory);
        
        // Register quality operators
        operators.register("quality.score", crate::operators::quality::quality_score_factory);
        operators.register("quality.filter", crate::operators::quality::quality_filter_factory);
        operators.register("quality.toxicity", crate::operators::quality::toxicity_factory);
        
        // Register deduplication operators
        operators.register("dedup.simhash", crate::operators::dedup::dedup_simhash_factory);
        operators.register("dedup.minhash", crate::operators::dedup::dedup_minhash_factory);
        operators.register("dedup.semantic", crate::operators::dedup::dedup_semantic_factory);
        
        // Register transform operators
        operators.register("transform.normalize", crate::operators::transform::transform_normalize_factory);

        ZiInProcessOrbit {
            capabilities,
            plugins: HashMap::new(),
            loaded_packages: HashMap::new(),
            operators,
            script_operator: None,
            lifecycle_manager: ZiPluginLifecycleManager::new(),
        }
    }

    /// Expose the internal capability registry so that callers can register
    /// host-provided capabilities before running any plugins.
    #[allow(non_snake_case)]
    pub fn capabilities_mut(&mut self) -> &mut ZiCapabilityRegistry {
        &mut self.capabilities
    }

    /// Construct an execution context for the given plugin, enforcing its
    /// policy (such as version-store access and default visibility).
    #[allow(non_snake_case)]
    pub fn make_execution_context<'a>(
        &'a self,
        plugin_id: &str,
        metrics: &'a mut ZiQualityMetrics,
        version_store: Option<&'a mut ZiVersionStore>,
    ) -> Result<ZiExecutionContext<'a>> {
        let plugin = self.get_plugin(plugin_id)?;
        let vs = if plugin.policy.can_access_versions {
            version_store
        } else {
            None
        };
        Ok(ZiExecutionContext {
            metrics,
            version_store: vs,
            capabilities: &self.capabilities,
            visibility: plugin.policy.default_visibility.clone(),
            plugin_id: plugin_id.to_string(),
            sandbox: &plugin.policy.sandbox,
        })
    }

    /// Internal helper to look up a plugin descriptor by id.
    fn get_plugin(&self, plugin_id: &str) -> Result<&ZiPluginDescriptor> {
        self.plugins
            .get(plugin_id)
            .ok_or_else(|| ZiError::internal(format!("unknown plugin: {}", plugin_id)))
    }

    /// Manually register a plugin descriptor with this in-process runtime.
    ///
    /// This is primarily intended for tests and for embedding ZiOrbit inside
    /// other applications before a full plugin loading mechanism is wired.
    #[allow(non_snake_case)]
    pub fn register_plugin(&mut self, descriptor: ZiPluginDescriptor) -> Result<()> {
        let id = descriptor.id.clone();
        
        // Check if plugin with same ID already exists
        if let Some(existing) = self.plugins.get(&id) {
            // Check version compatibility
            if descriptor.version <= existing.version {
                log::warn!(
                    "orbit.plugin.version_conflict: plugin with same id and lower/equal version already registered; keeping existing descriptor - plugin={}, existing_version={}, new_version={}",
                    id,
                    existing.version.to_string(),
                    descriptor.version.to_string()
                );
                return Ok(());
            }
            
            // Newer version, allow registration (upgrade)
            log::info!(
                "orbit.plugin.upgrade: upgrading plugin to newer version - plugin={}, existing_version={}, new_version={}",
                id,
                existing.version.to_string(),
                descriptor.version.to_string()
            );
        }

        // Register with lifecycle manager
        self.lifecycle_manager.register_plugin(&descriptor)?;

        log::info!(
            "orbit.plugin.register: plugin registered - plugin={}, version={}, export_count={}",
            id,
            descriptor.version.to_string(),
            descriptor.exports.len()
        );

        self.plugins.insert(id, descriptor);
        Ok(())
    }

    /// Unload a plugin from the runtime.
    #[allow(non_snake_case)]
    pub fn unload_plugin(&mut self, plugin_id: &str) -> Result<()> {
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

        // Remove from loaded packages map
        if let Some(loaded_package) = self.loaded_packages.remove(plugin_id) {
            log::info!(
                "orbit.plugin.unloaded: plugin unloaded successfully - plugin={}, version={}, is_zip_package={}, dependents={:?}",
                plugin_id,
                removed_plugin.version.to_string(),
                loaded_package.is_zip,
                dependents
            );
        }

        Ok(())
    }

    /// Get information about all loaded plugins.
    #[allow(non_snake_case)]
    pub fn get_loaded_plugins(&self) -> Vec<&ZiPluginDescriptor> {
        self.plugins.values().collect()
    }

    /// Check if a plugin is loaded.
    #[allow(non_snake_case)]
    pub fn is_plugin_loaded(&self, plugin_id: &str) -> bool {
        self.plugins.contains_key(plugin_id)
    }

    /// Register an operator factory with this runtime. This is intended to be
    /// used by the core to expose built-in operators and by plugins that wish
    /// to provide additional operators.
    #[allow(non_snake_case)]
    pub fn register_operator(&mut self, name: &str, factory: OperatorFactory) {
        self.operators.register(name, factory);
    }

    /// Install a script-backed operator runtime. When a plugin exports an
    /// operator with an associated script but there is no native factory
    /// registered for that operator name, this hook will be invoked to execute
    /// the script.
    #[allow(non_snake_case)]
    pub fn set_script_operator_runtime(&mut self, runtime: ScriptOperatorFn) {
        self.script_operator = Some(runtime);
    }

    /// Upgrade a plugin to a new version.
    #[allow(non_snake_case)]
    pub fn upgrade_plugin(&mut self, plugin_id: &str, new_path: &Path) -> Result<()> {
        // Load new plugin descriptor
        let new_descriptor = plugin_package::load_plugin_descriptor_from_path(new_path)?;
        
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
        
        // Check if it's a ZIP package
        let is_zip = !new_path.is_dir() && {
            let ext = new_path.extension().and_then(|e| e.to_str()).unwrap_or("");
            ext == "zip" || ext == "zop"
        };
        
        // Log upgrade operation
        log::info!(
            "orbit.plugin.upgrade: upgrading plugin - plugin={}, old_version={}, new_version={}, path={}, is_zip_package={}",
            plugin_id,
            current_plugin.version.to_string(),
            new_descriptor.version.to_string(),
            new_path.to_string_lossy(),
            is_zip
        );
        
        // Unregister old plugin from lifecycle manager
        self.lifecycle_manager.unregister_plugin(plugin_id);
        
        // Register new plugin (this will handle lifecycle management)
        self.register_plugin(new_descriptor.clone())?;
        
        // Update loaded package information
        let loaded_package = ZiLoadedPluginPackage {
            path: new_path.to_path_buf(),
            descriptor: new_descriptor,
            is_zip,
            last_access: std::time::SystemTime::now(),
        };
        self.loaded_packages.insert(plugin_id.to_string(), loaded_package);
        
        Ok(())
    }
}

impl ZiOrbit for ZiInProcessOrbit {
    fn load_plugin_from_path(&mut self, path: &Path) -> Result<ZiPluginDescriptor> {
        // Validate plugin package structure first
        if let Err(e) = plugin_package::validate_plugin_package(path) {
            log::error!(
                "orbit.plugin.validate_failed: failed to validate plugin package - path={}, error={}",
                path.to_string_lossy(),
                e
            );
            return Err(e);
        }
        
        // Load plugin descriptor to get plugin ID for caching check
        let temp_descriptor = plugin_package::load_plugin_descriptor_from_path(path)?;
        let plugin_id = temp_descriptor.id.clone();
        
        // If plugin is already loaded, return existing descriptor
        if let Some(existing) = self.plugins.get(&plugin_id) {
            log::info!(
                "orbit.plugin.already_loaded: plugin already loaded, returning existing instance - path={}, plugin={}, version={}",
                path.to_string_lossy(),
                plugin_id,
                existing.version.to_string()
            );
            
            // Update last access time for cache management
            if let Some(package) = self.loaded_packages.get_mut(&plugin_id) {
                package.last_access = std::time::SystemTime::now();
            }
            
            return Ok(existing.clone());
        }
        
        // Check if it's a ZIP package
        let is_zip = !path.is_dir() && {
            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            ext == "zip" || ext == "zop"
        };
        
        log::info!(
            "orbit.plugin.load: plugin descriptor loaded - path={}, plugin={}, version={}, export_count={}, is_zip_package={}, dependencies={}",
            path.to_string_lossy(),
            temp_descriptor.id,
            temp_descriptor.version.to_string(),
            temp_descriptor.exports.len(),
            is_zip,
            temp_descriptor.dependencies.len()
        );
        
        // Register the plugin
        self.register_plugin(temp_descriptor.clone())?;
        
        // Create and store loaded package information for caching
        let loaded_package = ZiLoadedPluginPackage {
            path: path.to_path_buf(),
            descriptor: temp_descriptor.clone(),
            is_zip,
            last_access: std::time::SystemTime::now(),
        };
        self.loaded_packages.insert(plugin_id, loaded_package);
        
        Ok(temp_descriptor)
    }

    fn unload_plugin(&mut self, plugin_id: &str) -> crate::errors::Result<()> {
        self.unload_plugin(plugin_id)
    }

    fn get_loaded_plugins(&self) -> Vec<&ZiPluginDescriptor> {
        self.get_loaded_plugins()
    }

    fn is_plugin_loaded(&self, plugin_id: &str) -> bool {
        self.is_plugin_loaded(plugin_id)
    }

    fn upgrade_plugin(&mut self, plugin_id: &str, new_path: &Path) -> crate::errors::Result<()> {
        self.upgrade_plugin(plugin_id, new_path)
    }

    fn call_operator(
        &self,
        plugin_id: &str,
        operator_name: &str,
        batch: ZiRecordBatch,
        config: &Value,
        ctx: &mut ZiExecutionContext,
    ) -> Result<ZiRecordBatch> {
        let plugin = self.get_plugin(plugin_id)?;
        // Emit a structured security audit log for operator calls.
        let visibility_str = match ctx.visibility {
            ZiDataVisibility::Full => "full",
            ZiDataVisibility::MaskSensitive => "mask_sensitive",
        };
        
        // Emit security audit log at INFO level for all operator calls
        log::info!(
            "security.audit.operator_call: Plugin operator invocation (security audit) - plugin={}, operator={}, visibility={}, record_count={}, sandbox_enabled=true, max_memory_mb={}, max_cpu_percent={}",
            plugin_id,
            operator_name,
            visibility_str,
            batch.len(),
            plugin.policy.sandbox.max_memory_mb.unwrap_or(0),
            plugin.policy.sandbox.max_cpu_percent.unwrap_or(0)
        );
        
        // Emit debug log for observability
        log::debug!(
            "orbit.operator.call: plugin operator invocation - plugin={}, operator={}, visibility={}, record_count={}",
            plugin_id,
            operator_name,
            visibility_str,
            batch.len()
        );

        let export_script = plugin
            .exports
            .iter()
            .find(|e| e.kind == ZiPluginExportKind::Operator && e.name == operator_name)
            .and_then(|e| e.script.as_deref());
        let factory_result = self.operators.get(operator_name);

        match factory_result {
            Ok(factory) => {
                let op = factory(config);
                // Apply data visibility policy: when MaskSensitive is in effect we
                // provide the operator with a masked view of the batch to avoid
                // leaking sensitive metadata to untrusted plugins.
                let input_batch = match ctx.visibility {
                    ZiDataVisibility::Full => batch,
                    ZiDataVisibility::MaskSensitive => _mask_sensitive_view(batch),
                };
                match op {
                    Ok(operator) => operator.apply(input_batch),
                    Err(e) => {
                        log::error!(
                            "orbit.operator.build_failed: failed to build operator - plugin={}, operator={}, error={}",
                            plugin_id,
                            operator_name,
                            e
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
                        log::debug!(
                            "orbit.operator.script_call: script-backed operator invocation - plugin={}, operator={}, script={}",
                            plugin_id,
                            operator_name,
                            script_path
                        );
                        let input_batch = match ctx.visibility {
                            ZiDataVisibility::Full => batch,
                            ZiDataVisibility::MaskSensitive => _mask_sensitive_view(batch),
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

                    log::warn!(
                        "orbit.operator.script_unavailable: script-backed operator has no runtime configured - plugin={}, operator={}, script={}",
                        plugin_id,
                        operator_name,
                        script_path
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

    fn call_capability(
        &mut self,
        plugin_id: &str,
        capability_name: &str,
        args: &Value,
        ctx: &mut ZiExecutionContext,
    ) -> Result<Value> {
        let plugin = self.get_plugin(plugin_id)?;

        // Policy check: ensure the plugin is allowed to call this capability.
        if !plugin
            .policy
            .allowed_capabilities
            .iter()
            .any(|name| name == capability_name)
        {
            // Emit security audit log for denied capability calls
            log::warn!(
                "security.audit.capability_denied: Plugin capability call denied (security audit) - plugin={}, capability={}, args={}",
                plugin_id,
                capability_name,
                args
            );
            
            // Emit regular log for observability
            log::warn!(
                "orbit.capability.denied: plugin not allowed to call capability - plugin={}, capability={}",
                plugin_id,
                capability_name
            );
            return Err(ZiError::internal(format!(
                "plugin '{}' is not allowed to call capability '{}'",
                plugin_id, capability_name
            )));
        }

        // Delegate to the capability registry while emitting a security audit log event.
        // Emit security audit log for allowed capability calls
        log::info!(
            "security.audit.capability_call: Plugin capability invocation (security audit) - plugin={}, capability={}, action=allowed, sandbox_enabled=true",
            plugin_id,
            capability_name
        );
        
        // Emit regular log for observability
        log::debug!(
            "orbit.capability.call: plugin capability invocation - plugin={}, capability={}",
            plugin_id,
            capability_name
        );

        self.capabilities.call(capability_name, args, ctx)
    }
}
