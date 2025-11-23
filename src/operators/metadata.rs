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

use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::operator::ZiCOperator;
use crate::record::ZiCRecordBatch;

/// Adds metadata key/values to every record in the batch.
#[derive(Debug)]
pub struct ZiCMetadataEnrich {
    entries: Vec<(String, Value)>,
}

impl ZiCMetadataEnrich {
    #[allow(non_snake_case)]
    pub fn ZiFNew(entries: Vec<(String, Value)>) -> Self {
        Self { entries }
    }
}

impl ZiCOperator for ZiCMetadataEnrich {
    fn name(&self) -> &'static str {
        "metadata.enrich"
    }

    fn apply(&self, mut batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        for record in &mut batch {
            let metadata = record.ZiFMetadataMut();
            for (key, value) in &self.entries {
                metadata.insert(key.clone(), value.clone());
            }
        }
        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn ZiFMetadataEnrichFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("metadata.enrich config must be object"))?;

    let entries = obj
        .get("entries")
        .and_then(Value::as_object)
        .ok_or_else(|| ZiError::validation("metadata.enrich requires object 'entries'"))?
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();

    Ok(Box::new(ZiCMetadataEnrich::ZiFNew(entries)))
}

/// Removes metadata keys from every record in the batch.
#[derive(Debug)]
pub struct ZiCMetadataRemove {
    keys: Vec<String>,
}

impl ZiCMetadataRemove {
    #[allow(non_snake_case)]
    pub fn ZiFNew(keys: Vec<String>) -> Self {
        Self { keys }
    }
}

impl ZiCOperator for ZiCMetadataRemove {
    fn name(&self) -> &'static str {
        "metadata.remove"
    }

    fn apply(&self, mut batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        if self.keys.is_empty() {
            return Ok(batch);
        }

        for record in &mut batch {
            let should_clear = if let Some(metadata) = record.metadata.as_mut() {
                for key in &self.keys {
                    metadata.remove(key);
                }
                metadata.is_empty()
            } else {
                false
            };

            if should_clear {
                record.metadata = None;
            }
        }

        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn ZiFMetadataRemoveFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("metadata.remove config must be object"))?;

    let keys_value = obj
        .get("keys")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("metadata.remove requires array 'keys'"))?;

    let keys = keys_value
        .iter()
        .map(|value| {
            value.as_str().map(|s| s.to_string()).ok_or_else(|| {
                ZiError::validation("metadata.remove 'keys' must be array of strings")
            })
        })
        .collect::<Result<Vec<String>>>()?;

    Ok(Box::new(ZiCMetadataRemove::ZiFNew(keys)))
}

/// Keeps only the specified metadata keys, dropping others.
#[derive(Debug)]
pub struct ZiCMetadataKeep {
    keys: Vec<String>,
}

impl ZiCMetadataKeep {
    #[allow(non_snake_case)]
    pub fn ZiFNew(keys: Vec<String>) -> Self {
        Self { keys }
    }
}

impl ZiCOperator for ZiCMetadataKeep {
    fn name(&self) -> &'static str {
        "metadata.keep"
    }

    fn apply(&self, mut batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        if self.keys.is_empty() {
            return Ok(batch);
        }

        for record in &mut batch {
            if let Some(metadata) = record.metadata.as_mut() {
                metadata.retain(|key, _| self.keys.iter().any(|allowed| allowed == key));
                if metadata.is_empty() {
                    record.metadata = None;
                }
            }
        }

        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn ZiFMetadataKeepFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("metadata.keep config must be object"))?;

    let keys_value = obj
        .get("keys")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("metadata.keep requires array 'keys'"))?;

    let keys = keys_value
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| ZiError::validation("metadata.keep 'keys' must be array of strings"))
        })
        .collect::<Result<Vec<String>>>()?;

    if keys.is_empty() {
        return Err(ZiError::validation("metadata.keep 'keys' may not be empty"));
    }

    Ok(Box::new(ZiCMetadataKeep::ZiFNew(keys)))
}

/// Renames metadata keys using a provided mapping.
#[derive(Debug)]
pub struct ZiCMetadataRename {
    mappings: Vec<(String, String)>,
}

impl ZiCMetadataRename {
    #[allow(non_snake_case)]
    pub fn ZiFNew(mappings: Vec<(String, String)>) -> Self {
        Self { mappings }
    }
}

impl ZiCOperator for ZiCMetadataRename {
    fn name(&self) -> &'static str {
        "metadata.rename"
    }

    fn apply(&self, mut batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        if self.mappings.is_empty() {
            return Ok(batch);
        }

        for record in &mut batch {
            if record.metadata.is_none() {
                continue;
            }

            let metadata = record.ZiFMetadataMut();
            let mut additions = Vec::new();

            for (from, to) in &self.mappings {
                if let Some(value) = metadata.remove(from) {
                    additions.push((to.clone(), value));
                }
            }

            for (to, value) in additions {
                metadata.insert(to, value);
            }

            if metadata.is_empty() {
                record.metadata = None;
            }
        }

        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn ZiFMetadataRenameFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("metadata.rename config must be object"))?;

    let keys = obj
        .get("keys")
        .and_then(Value::as_object)
        .ok_or_else(|| ZiError::validation("metadata.rename requires object 'keys'"))?;

    let mappings = keys
        .iter()
        .map(|(from, to)| {
            let to = to
                .as_str()
                .ok_or_else(|| {
                    ZiError::validation("metadata.rename 'keys' values must be strings")
                })?
                .to_string();

            if to.is_empty() {
                return Err(ZiError::validation(
                    "metadata.rename target names may not be empty",
                ));
            }

            Ok((from.clone(), to))
        })
        .collect::<Result<Vec<(String, String)>>>()?;

    Ok(Box::new(ZiCMetadataRename::ZiFNew(mappings)))
}

/// Copies metadata keys to new targets without removing the originals.
#[derive(Debug)]
pub struct ZiCMetadataCopy {
    mappings: Vec<(String, String)>,
}

impl ZiCMetadataCopy {
    #[allow(non_snake_case)]
    pub fn ZiFNew(mappings: Vec<(String, String)>) -> Self {
        Self { mappings }
    }
}

impl ZiCOperator for ZiCMetadataCopy {
    fn name(&self) -> &'static str {
        "metadata.copy"
    }

    fn apply(&self, mut batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        if self.mappings.is_empty() {
            return Ok(batch);
        }

        for record in &mut batch {
            let Some(existing) = record.metadata.as_ref() else {
                continue;
            };

            let mut additions = Vec::new();
            for (from, to) in &self.mappings {
                if let Some(value) = existing.get(from) {
                    additions.push((to.clone(), value.clone()));
                }
            }

            if additions.is_empty() {
                continue;
            }

            let metadata = record.ZiFMetadataMut();
            for (to, value) in additions {
                metadata.insert(to, value);
            }
        }

        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn ZiFMetadataCopyFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("metadata.copy config must be object"))?;

    let keys = obj
        .get("keys")
        .and_then(Value::as_object)
        .ok_or_else(|| ZiError::validation("metadata.copy requires object 'keys'"))?;

    let mappings = keys
        .iter()
        .map(|(from, to)| {
            let to = to
                .as_str()
                .ok_or_else(|| ZiError::validation("metadata.copy 'keys' values must be strings"))?
                .to_string();

            if to.is_empty() {
                return Err(ZiError::validation(
                    "metadata.copy target names may not be empty",
                ));
            }

            Ok((from.clone(), to))
        })
        .collect::<Result<Vec<(String, String)>>>()?;

    Ok(Box::new(ZiCMetadataCopy::ZiFNew(mappings)))
}

/// Validates that specified metadata keys exist on every record.
#[derive(Debug)]
pub struct ZiCMetadataRequire {
    required: Vec<String>,
}

impl ZiCMetadataRequire {
    #[allow(non_snake_case)]
    pub fn ZiFNew(required: Vec<String>) -> Self {
        Self { required }
    }
}

impl ZiCOperator for ZiCMetadataRequire {
    fn name(&self) -> &'static str {
        "metadata.require"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        if self.required.is_empty() {
            return Ok(batch);
        }

        for (index, record) in batch.iter().enumerate() {
            let metadata = record.metadata.as_ref().ok_or_else(|| {
                ZiError::validation(format!(
                    "record #{index} missing metadata required by metadata.require"
                ))
            })?;

            for key in &self.required {
                if !metadata.contains_key(key) {
                    return Err(ZiError::validation(format!(
                        "record #{index} missing metadata key '{key}'"
                    )));
                }
            }
        }

        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn ZiFMetadataRequireFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("metadata.require config must be object"))?;

    let keys = obj
        .get("keys")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("metadata.require requires array 'keys'"))?;

    let required = keys
        .iter()
        .map(|value| {
            value.as_str().map(|s| s.to_string()).ok_or_else(|| {
                ZiError::validation("metadata.require 'keys' must be array of strings")
            })
        })
        .collect::<Result<Vec<String>>>()?;

    if required.is_empty() {
        return Err(ZiError::validation(
            "metadata.require 'keys' may not be empty",
        ));
    }

    Ok(Box::new(ZiCMetadataRequire::ZiFNew(required)))
}

/// Copies values from payload paths into metadata keys.
#[derive(Debug)]
pub struct ZiCMetadataExtract {
    mappings: Vec<ZiCExtractionRule>,
}

/// Represents a single extraction rule for `metadata.extract`.
///
/// This struct is used to configure the `metadata.extract` operator.
#[derive(Debug, Clone)]
pub struct ZiCExtractionRule {
    /// The path segments to extract from the payload.
    ///
    /// The path must start with "payload" and reference a valid field.
    pub path_segments: Vec<String>,
    /// The target metadata key to write the extracted value to.
    pub target_key: String,
    /// The default value to use if the extraction fails.
    pub default_value: Option<Value>,
    /// Whether the extraction is optional.
    ///
    /// If `true`, the operator will not error if the extraction fails.
    pub optional: bool,
    /// The regular expression pattern to apply to the extracted value.
    pub pattern: Option<regex::Regex>,
    /// The capture index to use if the pattern matches.
    pub capture_index: Option<usize>,
}

impl ZiCMetadataExtract {
    #[allow(non_snake_case)]
    pub fn ZiFNew(mappings: Vec<ZiCExtractionRule>) -> Self {
        Self { mappings }
    }

    #[allow(non_snake_case)]
    fn ZiFResolvePath<'a>(value: &'a Value, segments: &[String]) -> Option<&'a Value> {
        let mut current = value;
        for segment in segments {
            match current {
                Value::Object(map) => current = map.get(segment)?,
                _ => return None,
            }
        }
        Some(current)
    }
}

impl ZiCOperator for ZiCMetadataExtract {
    fn name(&self) -> &'static str {
        "metadata.extract"
    }

    fn apply(&self, mut batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        if self.mappings.is_empty() {
            return Ok(batch);
        }

        for (index, record) in batch.iter_mut().enumerate() {
            for rule in &self.mappings {
                let Some(raw_value) =
                    ZiCMetadataExtract::ZiFResolvePath(&record.payload, &rule.path_segments)
                else {
                    if let Some(default) = &rule.default_value {
                        record
                            .ZiFMetadataMut()
                            .insert(rule.target_key.clone(), default.clone());
                        continue;
                    }
                    if rule.optional {
                        continue;
                    }
                    let path_string = rule
                        .path_segments
                        .iter()
                        .map(|segment| segment.as_str())
                        .collect::<Vec<_>>()
                        .join(".");
                    return Err(ZiError::validation(format!(
                        "record #{index} missing payload path '{path_string}' for metadata.extract"
                    )));
                };

                let extracted = match (&rule.pattern, raw_value) {
                    (Some(pattern), Value::String(s)) => {
                        if let Some(caps) = pattern.captures(s) {
                            let idx = rule.capture_index.unwrap_or(0);
                            if let Some(mat) = caps.get(idx) {
                                Value::String(mat.as_str().to_string())
                            } else if let Some(default) = &rule.default_value {
                                default.clone()
                            } else if rule.optional {
                                continue;
                            } else {
                                return Err(ZiError::validation(format!(
                                    "record #{index} capture group {idx} missing for metadata.extract"
                                )));
                            }
                        } else if let Some(default) = &rule.default_value {
                            default.clone()
                        } else if rule.optional {
                            continue;
                        } else {
                            return Err(ZiError::validation(format!(
                                "record #{index} pattern did not match for metadata.extract"
                            )));
                        }
                    }
                    _ => raw_value.clone(),
                };

                record
                    .ZiFMetadataMut()
                    .insert(rule.target_key.clone(), extracted);
            }
        }

        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn ZiFMetadataExtractFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("metadata.extract config must be object"))?;

    let keys = obj
        .get("keys")
        .and_then(Value::as_object)
        .ok_or_else(|| ZiError::validation("metadata.extract requires object 'keys'"))?;

    let mut mappings = Vec::with_capacity(keys.len());
    for (source, target) in keys {
        let target_obj = target.as_object().ok_or_else(|| {
            ZiError::validation("metadata.extract targets must be objects containing 'name'")
        })?;

        let target_key = target_obj
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| ZiError::validation("metadata.extract target requires string 'name'"))?
            .to_string();

        if target_key.is_empty() {
            return Err(ZiError::validation(
                "metadata.extract target 'name' may not be empty",
            ));
        }

        let optional = target_obj
            .get("optional")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let default_value = target_obj.get("default").cloned();

        let pattern = target_obj
            .get("pattern")
            .and_then(Value::as_str)
            .map(|regex_str| {
                regex::Regex::new(regex_str).map_err(|err| {
                    ZiError::validation(format!(
                        "metadata.extract provided invalid regex '{regex_str}': {err}"
                    ))
                })
            })
            .transpose()?;

        let capture_index = target_obj
            .get("capture")
            .and_then(Value::as_u64)
            .map(|idx| idx as usize);

        let segments: Vec<String> = source
            .split('.')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if segments.is_empty() || segments.first().map(|s| s.as_str()) != Some("payload") {
            return Err(ZiError::validation(
                "metadata.extract keys must start with 'payload'",
            ));
        }

        if segments.len() == 1 {
            return Err(ZiError::validation(
                "metadata.extract keys must reference a payload field",
            ));
        }

        let rule = ZiCExtractionRule {
            path_segments: segments[1..].to_vec(),
            target_key,
            default_value,
            optional,
            pattern,
            capture_index,
        };
        mappings.push(rule);
    }

    Ok(Box::new(ZiCMetadataExtract::ZiFNew(mappings)))
}
