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

use std::fs;
use std::io::Read;
use std::path::Path;

use base64::{engine::general_purpose::STANDARD, Engine};
use ring::signature;
use serde::Deserialize;
use zip::ZipArchive;

use crate::errors::{Result, ZiError};
use crate::orbit::runtime::{
    ZiCDataVisibility,
    ZiCPluginDescriptor,
    ZiCPluginExport,
    ZiCPluginExportKind,
    ZiCPluginPolicy,
};

/// Zi Orbit Plugin Package Structure Specification
/// 
/// A valid Zi Orbit plugin package (ZOP) is a ZIP file with the following structure:
/// ```
/// plugin.zop (zip file)
/// ├── orbit_plugin.json    # Required: Plugin metadata and configuration
/// ├── operators/           # Optional: Operator implementations
/// │   ├── operator1.js
/// │   └── operator2.py
/// ├── resources/           # Optional: Additional resources (models, data files, etc.)
/// │   ├── model.bin
/// │   └── config.json
/// └── signatures/          # Optional: Digital signatures for package verification
///     └── signature.json
/// ```
/// 
/// The orbit_plugin.json file must contain:
/// ```json
/// {
///   "id": "plugin_unique_id",
///   "version": "1.0.0",
///   "exports": [
///     {
///       "kind": "operator",
///       "name": "operator_name",
///       "script": "operators/operator1.js"
///     }
///   ],
///   "policy": {
///     "allowed_capabilities": ["log.event"],
///     "can_access_versions": false
///   },
///   "visibility": "mask_sensitive",
///   "dependencies": [
///     {
///       "plugin_id": "dependency_plugin",
///       "min_version": "1.0.0",
///       "required": true
///     }
///   ]
/// }
/// ```
/// 
/// The signature.json file (optional but recommended) contains digital signatures for package verification:
/// ```json
/// {
///   "algorithm": "sha256-rsa",
///   "timestamp": "2025-01-01T00:00:00Z",
///   "signatures": {
///     "orbit_plugin.json": "base64_encoded_signature",
///     "operators/operator1.js": "base64_encoded_signature"
///   },
///   "public_key": "base64_encoded_public_key"
/// }
/// ```

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
    allowed_file_paths: Vec<String>,
    allowed_network_hosts: Vec<String>,
    allowed_network_ports: Vec<u16>,
    can_write_files: bool,
    can_execute_commands: bool,
    can_access_network: bool,
    max_memory_mb: Option<u32>,
    max_cpu_percent: Option<u8>,
    max_threads: Option<u32>,
    max_disk_mb: Option<u32>,
    max_network_kbps: Option<u32>,
    role: Option<String>,
}

impl ZiCPluginPolicyFile {
    fn into_runtime(self) -> ZiCPluginPolicy {
        ZiCPluginPolicy {
            allowed_capabilities: self.allowed_capabilities,
            can_access_versions: self.can_access_versions,
            default_visibility: ZiCDataVisibility::default(),
            role: self.role,
            sandbox: crate::orbit::runtime::ZiCSandboxConfig {
                max_cpu_percent: self.max_cpu_percent,
                max_memory_mb: self.max_memory_mb,
                max_threads: self.max_threads,
                max_disk_mb: self.max_disk_mb,
                max_network_kbps: self.max_network_kbps,
                can_access_network: self.can_access_network,
                can_execute_commands: self.can_execute_commands,
                can_write_files: self.can_write_files,
                allowed_file_paths: self.allowed_file_paths,
                allowed_network_hosts: self.allowed_network_hosts,
                allowed_network_ports: self.allowed_network_ports,
            },
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

/// Plugin signature file structure
#[derive(Debug, Deserialize)]
struct ZiCPluginSignatureFile {
    /// Signature algorithm used (e.g., "sha256-rsa")
    algorithm: String,
    /// Timestamp when the signature was created
    timestamp: String,
    /// Map of file paths to their base64-encoded signatures
    signatures: std::collections::HashMap<String, String>,
    /// Base64-encoded public key used to verify the signatures
    public_key: String,
}

impl ZiCPluginSignatureFile {
    /// Verifies the signature of a single file
    fn verify_file(&self, file_path: &str, file_content: &[u8]) -> Result<bool> {
        // Check if we have a signature for this file
        let signature_b64 = self.signatures.get(file_path)
            .ok_or_else(|| ZiError::validation(format!("No signature found for file: {}", file_path)))?;
        
        // Decode the base64 signature
        let signature = STANDARD.decode(signature_b64)
            .map_err(|e| ZiError::validation(format!("Invalid base64 signature for file {}: {}", file_path, e)))?;
        
        // Decode the public key
        let public_key = STANDARD.decode(&self.public_key)
            .map_err(|e| ZiError::validation(format!("Invalid base64 public key: {}", e)))?;
        
        // Verify the signature based on the algorithm
        match self.algorithm.as_str() {
            "sha256-rsa" => {
                // Verify using RSA-PKCS1 with SHA-256
                let public_key = signature::UnparsedPublicKey::new(&signature::RSA_PKCS1_2048_8192_SHA256, &public_key);
                public_key.verify(file_content, &signature)
                    .map(|_| true)
                    .map_err(|e| ZiError::validation(format!("Signature verification failed for file {}: {}", file_path, e)))
            },
            "sha256-ecdsa" => {
                // Verify using ECDSA with SHA-256
                let public_key = signature::UnparsedPublicKey::new(&signature::ECDSA_P256_SHA256_ASN1, &public_key);
                public_key.verify(file_content, &signature)
                    .map(|_| true)
                    .map_err(|e| ZiError::validation(format!("Signature verification failed for file {}: {}", file_path, e)))
            },
            _ => {
                Err(ZiError::validation(format!("Unsupported signature algorithm: {}", self.algorithm)))
            }
        }
    }
    
    /// Verifies all signatures in the package
    fn verify_all(&self, zip: &mut ZipArchive<std::fs::File>) -> Result<()> {
        // Iterate through all files in the signature map and verify them
        for (file_path, _) in &self.signatures {
            // Read the file content
            let mut file = zip.by_name(file_path)?;
            let mut content = Vec::new();
            file.read_to_end(&mut content)?;
            
            // Verify the signature
            if !self.verify_file(file_path, &content)? {
                return Err(ZiError::validation(format!("Signature verification failed for file: {}", file_path)));
            }
        }
        
        Ok(())
    }
    
    /// Validates the signature file itself
    fn validate(&self) -> Result<()> {
        // Check if the algorithm is supported
        match self.algorithm.as_str() {
            "sha256-rsa" | "sha256-ecdsa" => {},
            _ => {
                return Err(ZiError::validation(format!("Unsupported signature algorithm: {}", self.algorithm)));
            }
        }
        
        // Check if the public key is valid base64
        STANDARD.decode(&self.public_key)
            .map_err(|e| ZiError::validation(format!("Invalid base64 public key: {}", e)))?;
        
        // Check if there are any signatures
        if self.signatures.is_empty() {
            return Err(ZiError::validation("No signatures found in signature file"));
        }
        
        // Check if the timestamp is a valid ISO 8601 format
        chrono::DateTime::parse_from_rfc3339(&self.timestamp)
            .map_err(|e| ZiError::validation(format!("Invalid timestamp format: {}", e)))?;
        
        Ok(())
    }
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

/// Loads a plugin descriptor from a file path, supporting both directory and ZIP/ZOP files.
pub fn ZiFLoadPluginDescriptorFromPath(path: &Path) -> Result<ZiCPluginDescriptor> {
    if path.is_dir() {
        // Load from directory
        let meta_path = path.join("orbit_plugin.json");
        let text = fs::read_to_string(&meta_path)?;
        let file: ZiCPluginFile = serde_json::from_str(&text)?;
        file.into_runtime()
    } else {
        // Check if it's a ZIP file
        let extension = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");
        if extension == "zip" || extension == "zop" {
            // Load from ZIP file
            let file = fs::File::open(path)?;
            let mut zip = ZipArchive::new(file)?;
            
            // Find and read the orbit_plugin.json file
            let mut meta_file = zip.by_name("orbit_plugin.json")?;
            let mut text = String::new();
            meta_file.read_to_string(&mut text)?;
            
            let file: ZiCPluginFile = serde_json::from_str(&text)?;
            file.into_runtime()
        } else {
            // Assume it's a standalone orbit_plugin.json file
            let text = fs::read_to_string(path)?;
            let file: ZiCPluginFile = serde_json::from_str(&text)?;
            file.into_runtime()
        }
    }
}

/// Extracts a file from a ZIP plugin package.
#[allow(dead_code)]
pub fn ZiFExtractFileFromPluginPackage(
    path: &Path,
    file_path: &str
) -> Result<Vec<u8>> {
    let file = fs::File::open(path)?;
    let mut zip = ZipArchive::new(file)?;
    
    let mut entry = zip.by_name(file_path)?;
    let mut buffer = Vec::new();
    entry.read_to_end(&mut buffer)?;
    
    Ok(buffer)
}

/// Validates a plugin package structure, including optional signature verification.
pub fn ZiFValidatePluginPackage(path: &Path) -> Result<()> {
    if path.is_dir() {
        // Validate directory structure
        let meta_path = path.join("orbit_plugin.json");
        if !meta_path.exists() {
            return Err(ZiError::validation(format!(
                "Plugin directory missing required file: orbit_plugin.json"
            )));
        }
        
        // Read and parse the metadata file to validate its content
        let text = fs::read_to_string(&meta_path)?;
        let _file: ZiCPluginFile = serde_json::from_str(&text)?;
        
        // Check for signature file and validate if present
        let signature_path = path.join("signatures/signature.json");
        if signature_path.exists() {
            let signature_text = fs::read_to_string(&signature_path)?;
            let signature: ZiCPluginSignatureFile = serde_json::from_str(&signature_text)?;
            
            // Validate the signature file itself
            signature.validate()?;
            
            // Verify all signatures for directory-based packages
            for (file_path, _) in &signature.signatures {
                let full_path = path.join(file_path);
                if !full_path.exists() {
                    return Err(ZiError::validation(format!(
                        "Signed file not found: {}", file_path
                    )));
                }
                
                let content = fs::read(&full_path)?;
                if !signature.verify_file(file_path, &content)? {
                    return Err(ZiError::validation(format!(
                        "Signature verification failed for file: {}", file_path
                    )));
                }
            }
            
            log::info!(
                "orbit.plugin.signature.verify_success: All plugin signatures verified successfully for directory - path={}, file_count={}",
                path.to_string_lossy(),
                signature.signatures.len()
            );
        }
        
        Ok(())
    } else {
        // Validate ZIP file structure
        let extension = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");
        if extension != "zip" && extension != "zop" {
            return Err(ZiError::validation(format!(
                "Plugin package must be a ZIP file with .zip or .zop extension, got: {}",
                extension
            )));
        }
        
        let file = fs::File::open(path)?;
        let mut zip = ZipArchive::new(file)?;
        
        // Check for required orbit_plugin.json file
        { // Create a scope to ensure meta_file is dropped before checking signature
            let mut meta_file = zip.by_name("orbit_plugin.json")?;
            let mut text = String::new();
            meta_file.read_to_string(&mut text)?;
            let _file: ZiCPluginFile = serde_json::from_str(&text)?;
        }
        
        // Check for signature file and validate if present
        let mut signature_text = String::new();
        let signature_exists = {
            if let Ok(mut signature_file) = zip.by_name("signatures/signature.json") {
                signature_file.read_to_string(&mut signature_text)?;
                true
            } else {
                false
            }
        };
        
        // If signature file exists, verify signatures
        if signature_exists {
            let signature: ZiCPluginSignatureFile = serde_json::from_str(&signature_text)?;
            
            // Validate the signature file itself
            signature.validate()?;
            
            // Log signature file validation success
            log::info!(
                "orbit.plugin.signature.validate_success: Plugin signature file validated successfully - path={}, algorithm={}, timestamp={}, file_count={}",
                path.to_string_lossy(),
                signature.algorithm,
                signature.timestamp,
                signature.signatures.len()
            );
            
            // Reopen the zip file for signature verification
            let file = fs::File::open(path)?;
            let mut zip = ZipArchive::new(file)?;
            
            // Verify all signatures in the package
            if let Err(e) = signature.verify_all(&mut zip) {
                // Log signature verification failure
                log::error!(
                    "orbit.plugin.signature.verify_failed: Plugin signature verification failed - path={}, error={}",
                    path.to_string_lossy(),
                    e
                );
                return Err(e);
            }

            // Log signature verification success
            log::info!(
                "orbit.plugin.signature.verify_success: All plugin signatures verified successfully - path={}, file_count={}",
                path.to_string_lossy(),
                signature.signatures.len()
            );
        }
        
        Ok(())
    }
}
