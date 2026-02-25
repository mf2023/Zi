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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::operator::ZiOperator;
use crate::record::{ZiRecord, ZiRecordBatch};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiFieldSelectMode {
    Include,
    Exclude,
}

impl Default for ZiFieldSelectMode {
    fn default() -> Self {
        Self::Include
    }
}

#[derive(Debug)]
pub struct ZiFieldSelect {
    fields: Vec<String>,
    mode: ZiFieldSelectMode,
}

impl ZiFieldSelect {
    #[allow(non_snake_case)]
    pub fn new(fields: Vec<String>, mode: ZiFieldSelectMode) -> Self {
        Self { fields, mode }
    }

    fn parse_field_path(&self, path: &str) -> Vec<String> {
        path.split('.').map(|s| s.to_string()).collect()
    }

    fn should_keep_field(&self, field_name: &str) -> bool {
        let field_matches = self.fields.iter().any(|f| {
            let parts = self.parse_field_path(f);
            parts.last().map(|last| last == field_name).unwrap_or(false)
        });

        match self.mode {
            ZiFieldSelectMode::Include => field_matches,
            ZiFieldSelectMode::Exclude => !field_matches,
        }
    }

    fn filter_object(&self, obj: &serde_json::Map<String, Value>) -> serde_json::Map<String, Value> {
        obj.iter()
            .filter(|(key, _)| self.should_keep_field(key))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

impl ZiOperator for ZiFieldSelect {
    fn name(&self) -> &'static str {
        "field.select"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        batch
            .into_iter()
            .map(|mut record| {
                if let Value::Object(map) = &record.payload {
                    record.payload = Value::Object(self.filter_object(map));
                }

                if let Some(meta) = &record.metadata {
                    let filtered_meta = self.filter_object(meta);
                    record.metadata = if filtered_meta.is_empty() {
                        None
                    } else {
                        Some(filtered_meta)
                    };
                }

                Ok(record)
            })
            .collect()
    }
}

#[allow(non_snake_case)]
pub fn field_select_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("field.select config must be object"))?;

    let fields = obj
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("field.select requires array 'fields'"))?
        .iter()
        .map(|v| {
            v.as_str()
                .ok_or_else(|| ZiError::validation("fields must be strings"))
                .map(|s| s.to_string())
        })
        .collect::<Result<Vec<_>>>()?;

    let mode = obj
        .get("mode")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();

    Ok(Box::new(ZiFieldSelect::new(fields, mode)))
}

#[derive(Debug)]
pub struct ZiFieldRename {
    mappings: HashMap<String, String>,
}

impl ZiFieldRename {
    #[allow(non_snake_case)]
    pub fn new(mappings: HashMap<String, String>) -> Self {
        Self { mappings }
    }

    fn rename_in_object(&self, obj: &mut serde_json::Map<String, Value>) {
        for (old_name, new_name) in &self.mappings {
            if let Some(value) = obj.remove(old_name) {
                obj.insert(new_name.clone(), value);
            }
        }
    }
}

impl ZiOperator for ZiFieldRename {
    fn name(&self) -> &'static str {
        "field.rename"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        batch
            .into_iter()
            .map(|mut record| {
                if let Value::Object(ref mut map) = record.payload {
                    self.rename_in_object(map);
                }

                if let Some(ref mut meta) = record.metadata {
                    self.rename_in_object(meta);
                }

                Ok(record)
            })
            .collect()
    }
}

#[allow(non_snake_case)]
pub fn field_rename_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("field.rename config must be object"))?;

    let mappings = obj
        .get("mappings")
        .and_then(Value::as_object)
        .ok_or_else(|| ZiError::validation("field.rename requires object 'mappings'"))?
        .iter()
        .map(|(k, v)| {
            v.as_str()
                .ok_or_else(|| ZiError::validation("mapping values must be strings"))
                .map(|s| (k.clone(), s.to_string()))
        })
        .collect::<Result<HashMap<_, _>>>()?;

    Ok(Box::new(ZiFieldRename::new(mappings)))
}

#[derive(Debug)]
pub struct ZiFieldDrop {
    fields: Vec<String>,
}

impl ZiFieldDrop {
    #[allow(non_snake_case)]
    pub fn new(fields: Vec<String>) -> Self {
        Self { fields }
    }

    fn drop_from_object(&self, obj: &mut serde_json::Map<String, Value>) {
        for field in &self.fields {
            obj.remove(field);
        }
    }
}

impl ZiOperator for ZiFieldDrop {
    fn name(&self) -> &'static str {
        "field.drop"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        batch
            .into_iter()
            .map(|mut record| {
                if let Value::Object(ref mut map) = record.payload {
                    self.drop_from_object(map);
                }

                if let Some(ref mut meta) = record.metadata {
                    self.drop_from_object(meta);
                }

                Ok(record)
            })
            .collect()
    }
}

#[allow(non_snake_case)]
pub fn field_drop_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("field.drop config must be object"))?;

    let fields = obj
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("field.drop requires array 'fields'"))?
        .iter()
        .map(|v| {
            v.as_str()
                .ok_or_else(|| ZiError::validation("fields must be strings"))
                .map(|s| s.to_string())
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Box::new(ZiFieldDrop::new(fields)))
}

#[derive(Debug)]
pub struct ZiFieldCopy {
    source: String,
    target: String,
}

impl ZiFieldCopy {
    #[allow(non_snake_case)]
    pub fn new(source: String, target: String) -> Self {
        Self { source, target }
    }

    fn get_value_at_path(&self, record: &ZiRecord, path: &str) -> Option<Value> {
        let parts: Vec<&str> = path.split('.').collect();
        if parts.len() < 2 {
            return None;
        }

        match parts[0] {
            "payload" => {
                let mut current = &record.payload;
                for part in &parts[1..] {
                    match current {
                        Value::Object(map) => {
                            current = map.get(*part)?;
                        }
                        _ => return None,
                    }
                }
                Some(current.clone())
            }
            "metadata" => {
                let meta = record.metadata.as_ref()?;
                let mut current = meta.get(*parts.get(1)?)?;
                for part in parts.iter().skip(2) {
                    match current {
                        Value::Object(map) => {
                            current = map.get(*part)?;
                        }
                        _ => return None,
                    }
                }
                Some(current.clone())
            }
            _ => None,
        }
    }

    fn set_value_at_path(&self, record: &mut ZiRecord, path: &str, value: Value) {
        let parts: Vec<&str> = path.split('.').collect();
        if parts.len() < 2 {
            return;
        }

        match parts[0] {
            "payload" => {
                if let Value::Object(ref mut map) = record.payload {
                    if parts.len() == 2 {
                        map.insert(parts[1].to_string(), value);
                    }
                }
            }
            "metadata" => {
                let meta = record.metadata_mut();
                if parts.len() == 2 {
                    meta.insert(parts[1].to_string(), value);
                }
            }
            _ => {}
        }
    }
}

impl ZiOperator for ZiFieldCopy {
    fn name(&self) -> &'static str {
        "field.copy"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        batch
            .into_iter()
            .map(|mut record| {
                if let Some(value) = self.get_value_at_path(&record, &self.source) {
                    self.set_value_at_path(&mut record, &self.target, value);
                }
                Ok(record)
            })
            .collect()
    }
}

#[allow(non_snake_case)]
pub fn field_copy_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("field.copy config must be object"))?;

    let source = obj
        .get("source")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("field.copy requires string 'source'"))?
        .to_string();

    let target = obj
        .get("target")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("field.copy requires string 'target'"))?
        .to_string();

    Ok(Box::new(ZiFieldCopy::new(source, target)))
}

#[derive(Debug)]
pub struct ZiFieldMove {
    source: String,
    target: String,
}

impl ZiFieldMove {
    #[allow(non_snake_case)]
    pub fn new(source: String, target: String) -> Self {
        Self { source, target }
    }

    fn get_value_at_path(&self, record: &ZiRecord, path: &str) -> Option<Value> {
        let parts: Vec<&str> = path.split('.').collect();
        if parts.len() < 2 {
            return None;
        }

        match parts[0] {
            "payload" => {
                let mut current = &record.payload;
                for part in &parts[1..] {
                    match current {
                        Value::Object(map) => {
                            current = map.get(*part)?;
                        }
                        _ => return None,
                    }
                }
                Some(current.clone())
            }
            "metadata" => {
                let meta = record.metadata.as_ref()?;
                let mut current = meta.get(*parts.get(1)?)?;
                for part in parts.iter().skip(2) {
                    match current {
                        Value::Object(map) => {
                            current = map.get(*part)?;
                        }
                        _ => return None,
                    }
                }
                Some(current.clone())
            }
            _ => None,
        }
    }

    fn remove_at_path(&self, record: &mut ZiRecord, path: &str) {
        let parts: Vec<&str> = path.split('.').collect();
        if parts.len() < 2 {
            return;
        }

        match parts[0] {
            "payload" => {
                if let Value::Object(ref mut map) = record.payload {
                    if parts.len() == 2 {
                        map.remove(parts[1]);
                    }
                }
            }
            "metadata" => {
                if let Some(ref mut meta) = record.metadata {
                    if parts.len() == 2 {
                        meta.remove(parts[1]);
                    }
                }
            }
            _ => {}
        }
    }

    fn set_value_at_path(&self, record: &mut ZiRecord, path: &str, value: Value) {
        let parts: Vec<&str> = path.split('.').collect();
        if parts.len() < 2 {
            return;
        }

        match parts[0] {
            "payload" => {
                if let Value::Object(ref mut map) = record.payload {
                    if parts.len() == 2 {
                        map.insert(parts[1].to_string(), value);
                    }
                }
            }
            "metadata" => {
                let meta = record.metadata_mut();
                if parts.len() == 2 {
                    meta.insert(parts[1].to_string(), value);
                }
            }
            _ => {}
        }
    }
}

impl ZiOperator for ZiFieldMove {
    fn name(&self) -> &'static str {
        "field.move"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        batch
            .into_iter()
            .map(|mut record| {
                if let Some(value) = self.get_value_at_path(&record, &self.source) {
                    self.remove_at_path(&mut record, &self.source);
                    self.set_value_at_path(&mut record, &self.target, value);
                }
                Ok(record)
            })
            .collect()
    }
}

#[allow(non_snake_case)]
pub fn field_move_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("field.move config must be object"))?;

    let source = obj
        .get("source")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("field.move requires string 'source'"))?
        .to_string();

    let target = obj
        .get("target")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("field.move requires string 'target'"))?
        .to_string();

    Ok(Box::new(ZiFieldMove::new(source, target)))
}

#[derive(Debug)]
pub struct ZiFieldFlatten {
    field: String,
    separator: String,
}

impl ZiFieldFlatten {
    #[allow(non_snake_case)]
    pub fn new(field: String, separator: String) -> Self {
        Self { field, separator }
    }

    fn flatten_object(
        &self,
        obj: &serde_json::Map<String, Value>,
        prefix: &str,
    ) -> serde_json::Map<String, Value> {
        let mut result = serde_json::Map::new();

        for (key, value) in obj {
            let new_key = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{}{}{}", prefix, self.separator, key)
            };

            match value {
                Value::Object(nested) => {
                    let flattened = self.flatten_object(nested, &new_key);
                    result.extend(flattened);
                }
                _ => {
                    result.insert(new_key, value.clone());
                }
            }
        }

        result
    }
}

impl ZiOperator for ZiFieldFlatten {
    fn name(&self) -> &'static str {
        "field.flatten"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        batch
            .into_iter()
            .map(|mut record| {
                let parts: Vec<&str> = self.field.split('.').collect();
                if parts.len() < 2 {
                    return Ok(record);
                }

                match parts[0] {
                    "payload" => {
                        if let Value::Object(ref mut map) = record.payload {
                            if let Some(nested) = map.get(parts[1]).cloned() {
                                if let Value::Object(nested_obj) = nested {
                                    let flattened = self.flatten_object(&nested_obj, parts[1]);
                                    map.remove(parts[1]);
                                    map.extend(flattened);
                                }
                            }
                        }
                    }
                    "metadata" => {
                        if let Some(ref mut meta) = record.metadata {
                            if let Some(nested) = meta.get(parts[1]).cloned() {
                                if let Value::Object(nested_obj) = nested {
                                    let flattened = self.flatten_object(&nested_obj, parts[1]);
                                    meta.remove(parts[1]);
                                    meta.extend(flattened);
                                }
                            }
                        }
                    }
                    _ => {}
                }

                Ok(record)
            })
            .collect()
    }
}

#[allow(non_snake_case)]
pub fn field_flatten_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("field.flatten config must be object"))?;

    let field = obj
        .get("field")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("field.flatten requires string 'field'"))?
        .to_string();

    let separator = obj
        .get("separator")
        .and_then(Value::as_str)
        .unwrap_or("_")
        .to_string();

    Ok(Box::new(ZiFieldFlatten::new(field, separator)))
}

#[derive(Debug)]
pub struct ZiFieldDefault {
    field: String,
    default_value: Value,
}

impl ZiFieldDefault {
    #[allow(non_snake_case)]
    pub fn new(field: String, default_value: Value) -> Self {
        Self { field, default_value }
    }
}

impl ZiOperator for ZiFieldDefault {
    fn name(&self) -> &'static str {
        "field.default"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        batch
            .into_iter()
            .map(|mut record| {
                let parts: Vec<&str> = self.field.split('.').collect();
                if parts.len() < 2 {
                    return Ok(record);
                }

                match parts[0] {
                    "payload" => {
                        if let Value::Object(ref mut map) = record.payload {
                            if !map.contains_key(parts[1]) {
                                map.insert(parts[1].to_string(), self.default_value.clone());
                            }
                        }
                    }
                    "metadata" => {
                        let meta = record.metadata_mut();
                        if !meta.contains_key(parts[1]) {
                            meta.insert(parts[1].to_string(), self.default_value.clone());
                        }
                    }
                    _ => {}
                }

                Ok(record)
            })
            .collect()
    }
}

#[allow(non_snake_case)]
pub fn field_default_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("field.default config must be object"))?;

    let field = obj
        .get("field")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("field.default requires string 'field'"))?
        .to_string();

    let default_value = obj
        .get("default_value")
        .cloned()
        .ok_or_else(|| ZiError::validation("field.default requires 'default_value'"))?;

    Ok(Box::new(ZiFieldDefault::new(field, default_value)))
}

#[derive(Debug)]
pub struct ZiFieldRequire {
    fields: Vec<String>,
}

impl ZiFieldRequire {
    #[allow(non_snake_case)]
    pub fn new(fields: Vec<String>) -> Self {
        Self { fields }
    }

    fn has_field(&self, record: &ZiRecord, field: &str) -> bool {
        let parts: Vec<&str> = field.split('.').collect();
        if parts.len() < 2 {
            return false;
        }

        match parts[0] {
            "payload" => {
                let mut current = &record.payload;
                for part in &parts[1..] {
                    match current {
                        Value::Object(map) => {
                            if !map.contains_key(*part) {
                                return false;
                            }
                            current = map.get(*part).unwrap();
                        }
                        _ => return false,
                    }
                }
                !current.is_null()
            }
            "metadata" => {
                if let Some(ref meta) = record.metadata {
                    if parts.len() < 2 {
                        return false;
                    }
                    let first_key = parts[1];
                    let mut current = meta.get(first_key);
                    for part in parts.iter().skip(2) {
                        match current {
                            Some(Value::Object(map)) => {
                                if !map.contains_key(*part) {
                                    return false;
                                }
                                current = map.get(*part);
                            }
                            _ => return false,
                        }
                    }
                    current.map(|v| !v.is_null()).unwrap_or(false)
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

impl ZiOperator for ZiFieldRequire {
    fn name(&self) -> &'static str {
        "field.require"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| self.fields.iter().all(|field| self.has_field(record, field)))
            .collect())
    }
}

#[allow(non_snake_case)]
pub fn field_require_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("field.require config must be object"))?;

    let fields = obj
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("field.require requires array 'fields'"))?
        .iter()
        .map(|v| {
            v.as_str()
                .ok_or_else(|| ZiError::validation("fields must be strings"))
                .map(|s| s.to_string())
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Box::new(ZiFieldRequire::new(fields)))
}
