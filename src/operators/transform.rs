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

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::operator::ZiOperator;
use crate::operators::filter::ZiFieldPath;
use crate::record::{ZiRecord, ZiRecordBatch};

#[derive(Debug)]
pub struct ZiTransformNormalize {
    path: ZiFieldPath,
    lowercase: bool,
}

impl ZiTransformNormalize {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, lowercase: bool) -> Self {
        Self { path, lowercase }
    }

    #[allow(non_snake_case)]
    fn norm(&self, s: &str) -> String {
        let mut t = s.trim().split_whitespace().collect::<Vec<_>>().join(" ");
        if self.lowercase {
            t = t.to_lowercase();
        }
        t
    }
}

impl ZiOperator for ZiTransformNormalize {
    fn name(&self) -> &'static str {
        "transform.normalize"
    }

    fn apply(&self, mut batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        for record in &mut batch {
            if let Some(Value::String(text)) = self.path.resolve(record) {
                let nt = self.norm(text);
                let _ = self.path.set_value(record, Value::String(nt));
            }
        }
        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn transform_normalize_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("transform.normalize config must be object"))?;
    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("transform.normalize requires string 'path'"))?;
    let lowercase = obj
        .get("lowercase")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let field_path = ZiFieldPath::parse(path)?;
    Ok(Box::new(ZiTransformNormalize::new(field_path, lowercase)))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiTransformFn {
    Lowercase,
    Uppercase,
    Trim,
    Strip,
    Replace { from: String, to: String },
    RegexReplace { pattern: String, replacement: String },
    Prefix { prefix: String },
    Suffix { suffix: String },
    Truncate { max_length: usize },
    Pad { length: usize, char: String },
    JsonStringify,
    JsonParse,
    ToInt,
    ToFloat,
    ToBool,
    ToString,
    Split { separator: String },
    Join { separator: String },
}

impl ZiTransformFn {
    fn apply(&self, value: &Value) -> Value {
        match self {
            ZiTransformFn::Lowercase => {
                if let Value::String(s) = value {
                    Value::String(s.to_lowercase())
                } else {
                    value.clone()
                }
            }
            ZiTransformFn::Uppercase => {
                if let Value::String(s) = value {
                    Value::String(s.to_uppercase())
                } else {
                    value.clone()
                }
            }
            ZiTransformFn::Trim => {
                if let Value::String(s) = value {
                    Value::String(s.trim().to_string())
                } else {
                    value.clone()
                }
            }
            ZiTransformFn::Strip => {
                if let Value::String(s) = value {
                    Value::String(s.trim().to_string())
                } else {
                    value.clone()
                }
            }
            ZiTransformFn::Replace { from, to } => {
                if let Value::String(s) = value {
                    Value::String(s.replace(from, to))
                } else {
                    value.clone()
                }
            }
            ZiTransformFn::RegexReplace { pattern, replacement } => {
                if let Value::String(s) = value {
                    if let Ok(re) = Regex::new(pattern) {
                        Value::String(re.replace_all(s, replacement.as_str()).to_string())
                    } else {
                        value.clone()
                    }
                } else {
                    value.clone()
                }
            }
            ZiTransformFn::Prefix { prefix } => {
                if let Value::String(s) = value {
                    Value::String(format!("{}{}", prefix, s))
                } else {
                    value.clone()
                }
            }
            ZiTransformFn::Suffix { suffix } => {
                if let Value::String(s) = value {
                    Value::String(format!("{}{}", s, suffix))
                } else {
                    value.clone()
                }
            }
            ZiTransformFn::Truncate { max_length } => {
                if let Value::String(s) = value {
                    Value::String(s.chars().take(*max_length).collect())
                } else {
                    value.clone()
                }
            }
            ZiTransformFn::Pad { length, char } => {
                if let Value::String(s) = value {
                    let current_len = s.chars().count();
                    if current_len < *length {
                        let padding = char.repeat(length - current_len);
                        Value::String(format!("{}{}", s, padding))
                    } else {
                        value.clone()
                    }
                } else {
                    value.clone()
                }
            }
            ZiTransformFn::JsonStringify => {
                Value::String(value.to_string())
            }
            ZiTransformFn::JsonParse => {
                if let Value::String(s) = value {
                    serde_json::from_str(s).unwrap_or(value.clone())
                } else {
                    value.clone()
                }
            }
            ZiTransformFn::ToInt => {
                match value {
                    Value::String(s) => s.parse::<i64>().ok()
                        .map(|i| Value::Number(i.into())),
                    Value::Number(n) => n.as_i64().map(|i| Value::Number(i.into())),
                    Value::Bool(b) => Some(Value::Number(if *b { 1i64 } else { 0i64 }.into())),
                    _ => None,
                }.unwrap_or(value.clone())
            }
            ZiTransformFn::ToFloat => {
                match value {
                    Value::String(s) => s.parse::<f64>().ok()
                        .and_then(|f| serde_json::Number::from_f64(f))
                        .map(Value::Number),
                    Value::Number(n) => n.as_f64()
                        .and_then(|f| serde_json::Number::from_f64(f))
                        .map(Value::Number),
                    Value::Bool(b) => {
                        let f = if *b { 1.0f64 } else { 0.0f64 };
                        serde_json::Number::from_f64(f).map(Value::Number)
                    }
                    _ => None,
                }.unwrap_or(value.clone())
            }
            ZiTransformFn::ToBool => {
                match value {
                    Value::String(s) => Value::Bool(!s.is_empty() && s != "false" && s != "0"),
                    Value::Number(n) => Value::Bool(n.as_f64().map(|f| f != 0.0).unwrap_or(false)),
                    Value::Bool(_) => value.clone(),
                    Value::Null => Value::Bool(false),
                    _ => Value::Bool(true),
                }
            }
            ZiTransformFn::ToString => {
                Value::String(value.to_string())
            }
            ZiTransformFn::Split { separator } => {
                if let Value::String(s) = value {
                    Value::Array(s.split(separator).map(|s| Value::String(s.to_string())).collect())
                } else {
                    value.clone()
                }
            }
            ZiTransformFn::Join { separator } => {
                if let Value::Array(arr) = value {
                    let strings: Vec<String> = arr
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect();
                    Value::String(strings.join(separator))
                } else {
                    value.clone()
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct ZiTransformMap {
    path: ZiFieldPath,
    transform: ZiTransformFn,
}

impl ZiTransformMap {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, transform: ZiTransformFn) -> Self {
        Self { path, transform }
    }
}

impl ZiOperator for ZiTransformMap {
    fn name(&self) -> &'static str {
        "transform.map"
    }

    fn apply(&self, mut batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        for record in &mut batch {
            if let Some(value) = self.path.resolve(record) {
                let transformed = self.transform.apply(value);
                let _ = self.path.set_value(record, transformed);
            }
        }
        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn transform_map_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("transform.map config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("transform.map requires string 'path'"))?;

    let transform: ZiTransformFn = serde_json::from_value(obj.get("transform").cloned().unwrap_or(Value::Null))
        .map_err(|e| ZiError::validation(format!("invalid transform function: {}", e)))?;

    let field_path = ZiFieldPath::parse(path)?;
    Ok(Box::new(ZiTransformMap::new(field_path, transform)))
}

#[derive(Debug)]
pub struct ZiTransformTemplate {
    template: String,
    output_field: ZiFieldPath,
}

impl ZiTransformTemplate {
    #[allow(non_snake_case)]
    pub fn new(template: String, output_field: ZiFieldPath) -> Self {
        Self { template, output_field }
    }

    fn render_template(&self, record: &ZiRecord) -> String {
        let mut result = self.template.clone();

        let re = Regex::new(r"\{\{([^}]+)\}\}").unwrap();
        let mut replacements = Vec::new();

        for cap in re.captures_iter(&self.template.clone()) {
            if let Some(match_str) = cap.get(0) {
                if let Some(field_path) = cap.get(1) {
                    let path_str = field_path.as_str().trim();
                    if let Ok(path) = ZiFieldPath::parse(path_str) {
                        if let Some(value) = path.resolve(record) {
                            replacements.push((
                                match_str.as_str().to_string(),
                                match value {
                                    Value::String(s) => s.clone(),
                                    _ => value.to_string(),
                                },
                            ));
                        }
                    }
                }
            }
        }

        for (pattern, replacement) in replacements {
            result = result.replace(&pattern, &replacement);
        }

        result
    }
}

impl ZiOperator for ZiTransformTemplate {
    fn name(&self) -> &'static str {
        "transform.template"
    }

    fn apply(&self, mut batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        for record in &mut batch {
            let rendered = self.render_template(record);
            let _ = self.output_field.set_value(record, Value::String(rendered));
        }
        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn transform_template_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("transform.template config must be object"))?;

    let template = obj
        .get("template")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("transform.template requires string 'template'"))?
        .to_string();

    let output_field = obj
        .get("output_field")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("transform.template requires string 'output_field'"))?;

    let field_path = ZiFieldPath::parse(output_field)?;
    Ok(Box::new(ZiTransformTemplate::new(template, field_path)))
}

#[derive(Debug)]
pub struct ZiTransformChain {
    path: ZiFieldPath,
    transforms: Vec<ZiTransformFn>,
}

impl ZiTransformChain {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, transforms: Vec<ZiTransformFn>) -> Self {
        Self { path, transforms }
    }
}

impl ZiOperator for ZiTransformChain {
    fn name(&self) -> &'static str {
        "transform.chain"
    }

    fn apply(&self, mut batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        for record in &mut batch {
            if let Some(value) = self.path.resolve(record) {
                let mut current = value.clone();
                for transform in &self.transforms {
                    current = transform.apply(&current);
                }
                let _ = self.path.set_value(record, current);
            }
        }
        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn transform_chain_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("transform.chain config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("transform.chain requires string 'path'"))?;

    let transforms: Vec<ZiTransformFn> = obj
        .get("transforms")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("transform.chain requires array 'transforms'"))?
        .iter()
        .map(|v| {
            serde_json::from_value(v.clone())
                .map_err(|e| ZiError::validation(format!("invalid transform: {}", e)))
        })
        .collect::<Result<Vec<_>>>()?;

    let field_path = ZiFieldPath::parse(path)?;
    Ok(Box::new(ZiTransformChain::new(field_path, transforms)))
}

#[derive(Debug)]
pub struct ZiTransformFlatMap {
    path: ZiFieldPath,
    output_field: ZiFieldPath,
}

impl ZiTransformFlatMap {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, output_field: ZiFieldPath) -> Self {
        Self { path, output_field }
    }
}

impl ZiOperator for ZiTransformFlatMap {
    fn name(&self) -> &'static str {
        "transform.flat_map"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        let mut result = Vec::new();

        for record in batch {
            if let Some(Value::Array(arr)) = self.path.resolve(&record) {
                for item in arr {
                    let mut new_record = record.clone();
                    let _ = self.output_field.set_value(&mut new_record, item.clone());
                    result.push(new_record);
                }
            } else {
                result.push(record);
            }
        }

        Ok(result)
    }
}

#[allow(non_snake_case)]
pub fn transform_flat_map_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("transform.flat_map config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("transform.flat_map requires string 'path'"))?;

    let output_field = obj
        .get("output_field")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("transform.flat_map requires string 'output_field'"))?;

    let field_path = ZiFieldPath::parse(path)?;
    let output_path = ZiFieldPath::parse(output_field)?;
    Ok(Box::new(ZiTransformFlatMap::new(field_path, output_path)))
}

#[derive(Debug)]
pub struct ZiTransformCoalesce {
    paths: Vec<ZiFieldPath>,
    output_field: ZiFieldPath,
}

impl ZiTransformCoalesce {
    #[allow(non_snake_case)]
    pub fn new(paths: Vec<ZiFieldPath>, output_field: ZiFieldPath) -> Self {
        Self { paths, output_field }
    }
}

impl ZiOperator for ZiTransformCoalesce {
    fn name(&self) -> &'static str {
        "transform.coalesce"
    }

    fn apply(&self, mut batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        for record in &mut batch {
            for path in &self.paths {
                if let Some(value) = path.resolve(record) {
                    if !value.is_null() {
                        let _ = self.output_field.set_value(record, value.clone());
                        break;
                    }
                }
            }
        }
        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn transform_coalesce_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("transform.coalesce config must be object"))?;

    let paths = obj
        .get("paths")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("transform.coalesce requires array 'paths'"))?
        .iter()
        .map(|v| {
            v.as_str()
                .ok_or_else(|| ZiError::validation("paths must be strings"))
                .and_then(ZiFieldPath::parse)
        })
        .collect::<Result<Vec<_>>>()?;

    let output_field = obj
        .get("output_field")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("transform.coalesce requires string 'output_field'"))?;

    let output_path = ZiFieldPath::parse(output_field)?;
    Ok(Box::new(ZiTransformCoalesce::new(paths, output_path)))
}

#[derive(Debug)]
pub struct ZiTransformConditional {
    condition_path: ZiFieldPath,
    condition_value: Value,
    then_transform: ZiTransformFn,
    else_transform: Option<ZiTransformFn>,
}

impl ZiTransformConditional {
    #[allow(non_snake_case)]
    pub fn new(
        condition_path: ZiFieldPath,
        condition_value: Value,
        then_transform: ZiTransformFn,
        else_transform: Option<ZiTransformFn>,
    ) -> Self {
        Self {
            condition_path,
            condition_value,
            then_transform,
            else_transform,
        }
    }
}

impl ZiOperator for ZiTransformConditional {
    fn name(&self) -> &'static str {
        "transform.conditional"
    }

    fn apply(&self, mut batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        for record in &mut batch {
            let condition_met = self
                .condition_path
                .resolve(record)
                .map(|v| v == &self.condition_value)
                .unwrap_or(false);

            if condition_met {
                if let Some(value) = self.condition_path.resolve(record) {
                    let transformed = self.then_transform.apply(value);
                    let _ = self.condition_path.set_value(record, transformed);
                }
            } else if let Some(else_fn) = &self.else_transform {
                if let Some(value) = self.condition_path.resolve(record) {
                    let transformed = else_fn.apply(value);
                    let _ = self.condition_path.set_value(record, transformed);
                }
            }
        }
        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn transform_conditional_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("transform.conditional config must be object"))?;

    let condition_path = obj
        .get("condition_path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("transform.conditional requires string 'condition_path'"))?;

    let condition_value = obj
        .get("condition_value")
        .cloned()
        .ok_or_else(|| ZiError::validation("transform.conditional requires 'condition_value'"))?;

    let then_transform: ZiTransformFn = serde_json::from_value(
        obj.get("then").cloned().unwrap_or(Value::Null)
    ).map_err(|e| ZiError::validation(format!("invalid then transform: {}", e)))?;

    let else_transform = obj
        .get("else")
        .cloned()
        .map(|v| serde_json::from_value(v))
        .transpose()
        .map_err(|e| ZiError::validation(format!("invalid else transform: {}", e)))?;

    let field_path = ZiFieldPath::parse(condition_path)?;
    Ok(Box::new(ZiTransformConditional::new(
        field_path,
        condition_value,
        then_transform,
        else_transform,
    )))
}
