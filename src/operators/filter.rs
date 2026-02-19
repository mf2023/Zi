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

use regex::Regex;
use serde_json::{json, Value};

use crate::errors::{Result, ZiError};
use crate::operator::ZiCOperator;
use crate::record::{ZiCRecord, ZiCRecordBatch};

/// Keeps records whose field equals a target [`Value`].
#[derive(Debug)]
pub struct ZiCFilterEquals {
    path: ZiCFieldPath,
    equals: Value,
}

impl ZiCFilterEquals {
    /// Creates a new filter operator.
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, equals: Value) -> Self {
        Self { path, equals }
    }
}

impl ZiCOperator for ZiCFilterEquals {
    fn name(&self) -> &'static str {
        "filter.equals"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.ZiFResolve(record) {
                Some(value) => value == &self.equals,
                None => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiCFilterEquals`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterEqualsFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.equals config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.equals requires string 'path'"))?;

    let field_path = ZiCFieldPath::ZiFParse(path)?;
    let equals = obj
        .get("equals")
        .cloned()
        .ok_or_else(|| ZiError::validation("filter.equals requires 'equals' value"))?;

    Ok(Box::new(ZiCFilterEquals::ZiFNew(field_path, equals)))
}

/// Keeps records whose field does not equal a target value.
#[derive(Debug)]
pub struct ZiCFilterNotEquals {
    path: ZiCFieldPath,
    equals: Value,
}

impl ZiCFilterNotEquals {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, equals: Value) -> Self {
        Self { path, equals }
    }
}

impl ZiCOperator for ZiCFilterNotEquals {
    fn name(&self) -> &'static str {
        "filter.not_equals"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.ZiFResolve(record) {
                Some(value) => value != &self.equals,
                None => true,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiCFilterNotEquals`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterNotEqualsFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.not_equals config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.not_equals requires string 'path'"))?;

    let equals = obj
        .get("equals")
        .cloned()
        .ok_or_else(|| ZiError::validation("filter.not_equals requires 'equals' value"))?;

    let field_path = ZiCFieldPath::ZiFParse(path)?;

    Ok(Box::new(ZiCFilterNotEquals::ZiFNew(field_path, equals)))
}

/// Keeps records where any configured field equals a target value.
#[derive(Debug)]
pub struct ZiCFilterAny {
    paths: Vec<ZiCFieldPath>,
    equals: Value,
}

impl ZiCFilterAny {
    #[allow(non_snake_case)]
    pub fn ZiFNew(paths: Vec<ZiCFieldPath>, equals: Value) -> Self {
        Self { paths, equals }
    }
}

impl ZiCOperator for ZiCFilterAny {
    fn name(&self) -> &'static str {
        "filter.any"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| {
                self.paths.iter().any(|path| match path.ZiFResolve(record) {
                    Some(value) => value == &self.equals,
                    None => false,
                })
            })
            .collect())
    }
}

/// Factory that constructs [`ZiCFilterAny`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterAnyFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.any config must be object"))?;

    let paths = obj
        .get("paths")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("filter.any requires array 'paths'"))?;

    if paths.is_empty() {
        return Err(ZiError::validation("filter.any 'paths' may not be empty"));
    }

    let field_paths = paths
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| ZiError::validation("filter.any paths must be strings"))
                .and_then(ZiCFieldPath::ZiFParse)
        })
        .collect::<Result<Vec<_>>>()?;

    let equals = obj
        .get("equals")
        .cloned()
        .ok_or_else(|| ZiError::validation("filter.any requires 'equals' value"))?;

    Ok(Box::new(ZiCFilterAny::ZiFNew(field_paths, equals)))
}

/// Keeps records where a numeric field lies within inclusive bounds.
#[derive(Debug)]
pub struct ZiCFilterBetween {
    path: ZiCFieldPath,
    min: f64,
    max: f64,
}

impl ZiCFilterBetween {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, min: f64, max: f64) -> Self {
        Self { path, min, max }
    }
}

impl ZiCOperator for ZiCFilterBetween {
    fn name(&self) -> &'static str {
        "filter.between"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.ZiFResolve(record) {
                Some(Value::Number(number)) => number
                    .as_f64()
                    .map_or(false, |value| value >= self.min && value <= self.max),
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiCFilterBetween`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterBetweenFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.between config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.between requires string 'path'"))?;

    let min = obj
        .get("min")
        .and_then(Value::as_f64)
        .ok_or_else(|| ZiError::validation("filter.between requires numeric 'min'"))?;

    let max = obj
        .get("max")
        .and_then(Value::as_f64)
        .ok_or_else(|| ZiError::validation("filter.between requires numeric 'max'"))?;

    if min > max {
        return Err(ZiError::validation(
            "filter.between 'min' may not exceed 'max'",
        ));
    }

    let field_path = ZiCFieldPath::ZiFParse(path)?;

    Ok(Box::new(ZiCFilterBetween::ZiFNew(field_path, min, max)))
}

/// Keeps records where a numeric field is less than a threshold.
#[derive(Debug)]
pub struct ZiCFilterLessThan {
    path: ZiCFieldPath,
    threshold: f64,
}

impl ZiCFilterLessThan {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, threshold: f64) -> Self {
        Self { path, threshold }
    }
}

impl ZiCOperator for ZiCFilterLessThan {
    fn name(&self) -> &'static str {
        "filter.less_than"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.ZiFResolve(record) {
                Some(Value::Number(number)) => number
                    .as_f64()
                    .map_or(false, |value| value < self.threshold),
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiCFilterLessThan`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterLessThanFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.less_than config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.less_than requires string 'path'"))?;

    let threshold = obj
        .get("threshold")
        .and_then(Value::as_f64)
        .ok_or_else(|| ZiError::validation("filter.less_than requires numeric 'threshold'"))?;

    let field_path = ZiCFieldPath::ZiFParse(path)?;

    Ok(Box::new(ZiCFilterLessThan::ZiFNew(field_path, threshold)))
}

/// Keeps records where a numeric field is greater than a threshold.
#[derive(Debug)]
pub struct ZiCFilterGreaterThan {
    path: ZiCFieldPath,
    threshold: f64,
}

impl ZiCFilterGreaterThan {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, threshold: f64) -> Self {
        Self { path, threshold }
    }
}

impl ZiCOperator for ZiCFilterGreaterThan {
    fn name(&self) -> &'static str {
        "filter.greater_than"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.ZiFResolve(record) {
                Some(Value::Number(number)) => number
                    .as_f64()
                    .map_or(false, |value| value > self.threshold),
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiCFilterGreaterThan`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterGreaterThanFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.greater_than config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.greater_than requires string 'path'"))?;

    let threshold = obj
        .get("threshold")
        .and_then(Value::as_f64)
        .ok_or_else(|| ZiError::validation("filter.greater_than requires numeric 'threshold'"))?;

    let field_path = ZiCFieldPath::ZiFParse(path)?;

    Ok(Box::new(ZiCFilterGreaterThan::ZiFNew(
        field_path, threshold,
    )))
}

/// Keeps records where a field is explicitly null or missing.
#[derive(Debug)]
pub struct ZiCFilterIsNull {
    path: ZiCFieldPath,
    include_missing: bool,
}

impl ZiCFilterIsNull {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, include_missing: bool) -> Self {
        Self {
            path,
            include_missing,
        }
    }
}

impl ZiCOperator for ZiCFilterIsNull {
    fn name(&self) -> &'static str {
        "filter.is_null"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.ZiFResolve(record) {
                Some(Value::Null) => true,
                None => self.include_missing,
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiCFilterIsNull`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterIsNullFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.is_null config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.is_null requires string 'path'"))?;

    let include_missing = obj
        .get("include_missing")
        .and_then(Value::as_bool)
        .unwrap_or(true);

    let field_path = ZiCFieldPath::ZiFParse(path)?;

    Ok(Box::new(ZiCFilterIsNull::ZiFNew(
        field_path,
        include_missing,
    )))
}

/// Keeps records where a field matches a regular expression.
#[derive(Debug)]
pub struct ZiCFilterRegex {
    path: ZiCFieldPath,
    pattern: Regex,
}

impl ZiCFilterRegex {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, pattern: Regex) -> Self {
        Self { path, pattern }
    }
}

impl ZiCOperator for ZiCFilterRegex {
    fn name(&self) -> &'static str {
        "filter.regex"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.ZiFResolve(record) {
                Some(Value::String(value)) => self.pattern.is_match(value),
                Some(Value::Array(values)) => values.iter().any(
                    |value| matches!(value, Value::String(item) if self.pattern.is_match(item)),
                ),
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiCFilterRegex`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterRegexFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.regex config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.regex requires string 'path'"))?;

    let pattern = obj
        .get("pattern")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.regex requires string 'pattern'"))?;

    let regex = Regex::new(pattern)
        .map_err(|err| ZiError::validation(format!("invalid regex pattern: {err}")))?;

    let field_path = ZiCFieldPath::ZiFParse(path)?;

    Ok(Box::new(ZiCFilterRegex::ZiFNew(field_path, regex)))
}

/// Keeps records where a field ends with a suffix.
#[derive(Debug)]
pub struct ZiCFilterEndsWith {
    path: ZiCFieldPath,
    suffix: String,
}

impl ZiCFilterEndsWith {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, suffix: String) -> Self {
        Self { path, suffix }
    }
}

impl ZiCOperator for ZiCFilterEndsWith {
    fn name(&self) -> &'static str {
        "filter.ends_with"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.ZiFResolve(record) {
                Some(Value::String(value)) => value.ends_with(&self.suffix),
                Some(Value::Array(values)) => values.iter().any(
                    |value| matches!(value, Value::String(item) if item.ends_with(&self.suffix)),
                ),
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiCFilterEndsWith`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterEndsWithFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.ends_with config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.ends_with requires string 'path'"))?;

    let suffix = obj
        .get("suffix")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.ends_with requires string 'suffix'"))?
        .to_string();

    let field_path = ZiCFieldPath::ZiFParse(path)?;

    Ok(Box::new(ZiCFilterEndsWith::ZiFNew(field_path, suffix)))
}

/// Keeps records where a field starts with a prefix.
#[derive(Debug)]
pub struct ZiCFilterStartsWith {
    path: ZiCFieldPath,
    prefix: String,
}

impl ZiCFilterStartsWith {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, prefix: String) -> Self {
        Self { path, prefix }
    }
}

impl ZiCOperator for ZiCFilterStartsWith {
    fn name(&self) -> &'static str {
        "filter.starts_with"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.ZiFResolve(record) {
                Some(Value::String(value)) => value.starts_with(&self.prefix),
                Some(Value::Array(values)) => values.iter().any(
                    |value| matches!(value, Value::String(item) if item.starts_with(&self.prefix)),
                ),
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiCFilterStartsWith`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterStartsWithFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.starts_with config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.starts_with requires string 'path'"))?;

    let prefix = obj
        .get("prefix")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.starts_with requires string 'prefix'"))?
        .to_string();

    let field_path = ZiCFieldPath::ZiFParse(path)?;

    Ok(Box::new(ZiCFilterStartsWith::ZiFNew(field_path, prefix)))
}

/// Keeps records where a numeric field falls within an optional range.
#[derive(Debug)]
pub struct ZiCFilterRange {
    path: ZiCFieldPath,
    min: Option<f64>,
    max: Option<f64>,
}

impl ZiCFilterRange {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, min: Option<f64>, max: Option<f64>) -> Self {
        Self { path, min, max }
    }
}

impl ZiCOperator for ZiCFilterRange {
    fn name(&self) -> &'static str {
        "filter.range"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.ZiFResolve(record) {
                Some(Value::Number(number)) => number.as_f64().map_or(false, |value| {
                    if let Some(min) = self.min {
                        if value < min {
                            return false;
                        }
                    }
                    if let Some(max) = self.max {
                        if value > max {
                            return false;
                        }
                    }
                    true
                }),
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiCFilterRange`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterRangeFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.range config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.range requires string 'path'"))?;

    let min = obj.get("min").and_then(Value::as_f64);
    let max = obj.get("max").and_then(Value::as_f64);

    if min.is_none() && max.is_none() {
        return Err(ZiError::validation(
            "filter.range requires at least one of 'min' or 'max'",
        ));
    }

    if let (Some(min), Some(max)) = (min, max) {
        if min > max {
            return Err(ZiError::validation(
                "filter.range 'min' may not be greater than 'max'",
            ));
        }
    }

    let field_path = ZiCFieldPath::ZiFParse(path)?;

    Ok(Box::new(ZiCFilterRange::ZiFNew(field_path, min, max)))
}

/// Keeps records where a field equals any configured value.
#[derive(Debug)]
pub struct ZiCFilterIn {
    path: ZiCFieldPath,
    allowed: Vec<Value>,
}

impl ZiCFilterIn {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, allowed: Vec<Value>) -> Self {
        Self { path, allowed }
    }
}

impl ZiCOperator for ZiCFilterIn {
    fn name(&self) -> &'static str {
        "filter.in"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.ZiFResolve(record) {
                Some(value) => self.allowed.iter().any(|allowed| allowed == value),
                None => false,
            })
            .collect())
    }
}

/// Keeps records where a field does **not** equal any configured value.
#[derive(Debug)]
pub struct ZiCFilterNotIn {
    path: ZiCFieldPath,
    disallowed: Vec<Value>,
}

impl ZiCFilterNotIn {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, disallowed: Vec<Value>) -> Self {
        Self { path, disallowed }
    }
}

impl ZiCOperator for ZiCFilterNotIn {
    fn name(&self) -> &'static str {
        "filter.not_in"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.ZiFResolve(record) {
                Some(value) => !self.disallowed.iter().any(|blocked| blocked == value),
                None => true,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiCFilterIn`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterInFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.in config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.in requires string 'path'"))?;

    let values = obj
        .get("values")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("filter.in requires array 'values'"))?;

    if values.is_empty() {
        return Err(ZiError::validation("filter.in 'values' may not be empty"));
    }

    let allowed = values.to_vec();
    let field_path = ZiCFieldPath::ZiFParse(path)?;

    Ok(Box::new(ZiCFilterIn::ZiFNew(field_path, allowed)))
}

/// Factory that constructs [`ZiCFilterNotIn`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterNotInFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.not_in config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.not_in requires string 'path'"))?;

    let values = obj
        .get("values")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("filter.not_in requires array 'values'"))?;

    if values.is_empty() {
        return Err(ZiError::validation(
            "filter.not_in 'values' may not be empty",
        ));
    }

    let disallowed = values.to_vec();
    let field_path = ZiCFieldPath::ZiFParse(path)?;

    Ok(Box::new(ZiCFilterNotIn::ZiFNew(field_path, disallowed)))
}

/// Keeps records where a field exists and is not null.
#[derive(Debug)]
pub struct ZiCFilterExists {
    path: ZiCFieldPath,
}

impl ZiCFilterExists {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath) -> Self {
        Self { path }
    }
}

impl ZiCOperator for ZiCFilterExists {
    fn name(&self) -> &'static str {
        "filter.exists"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(
                |record| matches!(self.path.ZiFResolve(record), Some(value) if !value.is_null()),
            )
            .collect())
    }
}

/// Keeps records where a field contains a target substring.
#[derive(Debug)]
pub struct ZiCFilterContains {
    path: ZiCFieldPath,
    needle: String,
    needle_lower: String,
    case_insensitive: bool,
}

impl ZiCFilterContains {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, needle: String, case_insensitive: bool) -> Self {
        let needle_lower = if case_insensitive {
            needle.to_lowercase()
        } else {
            needle.clone()
        };

        Self {
            path,
            needle,
            needle_lower,
            case_insensitive,
        }
    }

    fn matches(&self, candidate: &str) -> bool {
        if self.case_insensitive {
            candidate.to_lowercase().contains(&self.needle_lower)
        } else {
            candidate.contains(&self.needle)
        }
    }
}

impl ZiCOperator for ZiCFilterContains {
    fn name(&self) -> &'static str {
        "filter.contains"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.ZiFResolve(record) {
                Some(Value::String(value)) => self.matches(value),
                Some(Value::Array(values)) => values
                    .iter()
                    .any(|value| matches!(value, Value::String(item) if self.matches(item))),
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiCFilterContains`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterContainsFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.contains config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.contains requires string 'path'"))?;

    let needle = obj
        .get("contains")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.contains requires string 'contains'"))?
        .to_string();
    let case_insensitive = obj
        .get("case_insensitive")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let field_path = ZiCFieldPath::ZiFParse(path)?;

    Ok(Box::new(ZiCFilterContains::ZiFNew(
        field_path,
        needle,
        case_insensitive,
    )))
}

/// Keeps records where a field contains all configured substrings.
#[derive(Debug)]
pub struct ZiCFilterContainsAll {
    path: ZiCFieldPath,
    needles: Vec<String>,
    needles_lower: Vec<String>,
    case_insensitive: bool,
}

impl ZiCFilterContainsAll {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, needles: Vec<String>, case_insensitive: bool) -> Self {
        let needles_lower = if case_insensitive {
            needles.iter().map(|needle| needle.to_lowercase()).collect()
        } else {
            needles.clone()
        };

        Self {
            path,
            needles,
            needles_lower,
            case_insensitive,
        }
    }

    fn string_contains_all(&self, candidate: &str) -> bool {
        if self.case_insensitive {
            let haystack = candidate.to_lowercase();
            self.needles_lower
                .iter()
                .all(|needle| haystack.contains(needle))
        } else {
            self.needles.iter().all(|needle| candidate.contains(needle))
        }
    }

    fn array_contains_all(&self, values: &[Value]) -> bool {
        if self.case_insensitive {
            self.needles_lower.iter().all(|needle| {
                values.iter().any(|value| match value {
                    Value::String(item) => item.to_lowercase().contains(needle),
                    _ => false,
                })
            })
        } else {
            self.needles.iter().all(|needle| {
                values
                    .iter()
                    .any(|value| matches!(value, Value::String(item) if item.contains(needle)))
            })
        }
    }
}

impl ZiCOperator for ZiCFilterContainsAll {
    fn name(&self) -> &'static str {
        "filter.contains_all"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.ZiFResolve(record) {
                Some(Value::String(value)) => self.string_contains_all(value),
                Some(Value::Array(values)) => self.array_contains_all(values),
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiCFilterContainsAll`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterContainsAllFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.contains_all config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.contains_all requires string 'path'"))?;

    let needles = obj
        .get("contains_all")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("filter.contains_all requires array 'contains_all'"))?;

    if needles.is_empty() {
        return Err(ZiError::validation(
            "filter.contains_all 'contains_all' may not be empty",
        ));
    }

    let substrings = needles
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| ZiError::validation("filter.contains_all needles must be strings"))
                .map(|needle| needle.to_string())
        })
        .collect::<Result<Vec<_>>>()?;
    let case_insensitive = obj
        .get("case_insensitive")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let field_path = ZiCFieldPath::ZiFParse(path)?;

    Ok(Box::new(ZiCFilterContainsAll::ZiFNew(
        field_path,
        substrings,
        case_insensitive,
    )))
}

/// Keeps records where a field contains any of multiple substrings.
#[derive(Debug)]
pub struct ZiCFilterContainsAny {
    path: ZiCFieldPath,
    needles: Vec<String>,
    needles_lower: Vec<String>,
    case_insensitive: bool,
}

impl ZiCFilterContainsAny {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, needles: Vec<String>, case_insensitive: bool) -> Self {
        let needles_lower = if case_insensitive {
            needles.iter().map(|needle| needle.to_lowercase()).collect()
        } else {
            needles.clone()
        };

        Self {
            path,
            needles,
            needles_lower,
            case_insensitive,
        }
    }

    fn string_contains_any(&self, candidate: &str) -> bool {
        if self.case_insensitive {
            let haystack = candidate.to_lowercase();
            self.needles_lower
                .iter()
                .any(|needle| haystack.contains(needle))
        } else {
            self.needles.iter().any(|needle| candidate.contains(needle))
        }
    }

    fn array_contains_any(&self, values: &[Value]) -> bool {
        if self.case_insensitive {
            values.iter().any(|value| match value {
                Value::String(item) => {
                    let haystack = item.to_lowercase();
                    self.needles_lower
                        .iter()
                        .any(|needle| haystack.contains(needle))
                }
                _ => false,
            })
        } else {
            values.iter().any(|value| {
                matches!(value, Value::String(item) if self
                    .needles
                    .iter()
                    .any(|needle| item.contains(needle)))
            })
        }
    }
}

impl ZiCOperator for ZiCFilterContainsAny {
    fn name(&self) -> &'static str {
        "filter.contains_any"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.ZiFResolve(record) {
                Some(Value::String(value)) => self.string_contains_any(value),
                Some(Value::Array(values)) => self.array_contains_any(values),
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiCFilterContainsAny`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterContainsAnyFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.contains_any config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.contains_any requires string 'path'"))?;

    let needles = obj
        .get("contains_any")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("filter.contains_any requires array 'contains_any'"))?;

    if needles.is_empty() {
        return Err(ZiError::validation(
            "filter.contains_any 'contains_any' may not be empty",
        ));
    }

    let values = needles
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| ZiError::validation("filter.contains_any needles must be strings"))
                .map(|needle| needle.to_string())
        })
        .collect::<Result<Vec<_>>>()?;
    let case_insensitive = obj
        .get("case_insensitive")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let field_path = ZiCFieldPath::ZiFParse(path)?;

    Ok(Box::new(ZiCFilterContainsAny::ZiFNew(
        field_path,
        values,
        case_insensitive,
    )))
}

/// Keeps records where a field contains none of the configured substrings.
#[derive(Debug)]
pub struct ZiCFilterContainsNone {
    path: ZiCFieldPath,
    needles: Vec<String>,
    needles_lower: Vec<String>,
    case_insensitive: bool,
}

impl ZiCFilterContainsNone {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, needles: Vec<String>, case_insensitive: bool) -> Self {
        let needles_lower = if case_insensitive {
            needles.iter().map(|needle| needle.to_lowercase()).collect()
        } else {
            needles.clone()
        };

        Self {
            path,
            needles,
            needles_lower,
            case_insensitive,
        }
    }

    fn string_contains_any(&self, candidate: &str) -> bool {
        if self.case_insensitive {
            let haystack = candidate.to_lowercase();
            self.needles_lower
                .iter()
                .any(|needle| haystack.contains(needle))
        } else {
            self.needles.iter().any(|needle| candidate.contains(needle))
        }
    }

    fn array_contains_any(&self, values: &[Value]) -> bool {
        values.iter().any(|value| match value {
            Value::String(item) => self.string_contains_any(item),
            _ => false,
        })
    }
}

impl ZiCOperator for ZiCFilterContainsNone {
    fn name(&self) -> &'static str {
        "filter.contains_none"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.ZiFResolve(record) {
                Some(Value::String(value)) => !self.string_contains_any(value),
                Some(Value::Array(values)) => !self.array_contains_any(values),
                Some(Value::Null) | None => true,
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiCFilterContainsNone`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterContainsNoneFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.contains_none config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.contains_none requires string 'path'"))?;

    let needles = obj
        .get("contains_none")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ZiError::validation("filter.contains_none requires array 'contains_none'")
        })?;

    if needles.is_empty() {
        return Err(ZiError::validation(
            "filter.contains_none 'contains_none' may not be empty",
        ));
    }

    let substrings = needles
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| ZiError::validation("filter.contains_none needles must be strings"))
                .map(|needle| needle.to_string())
        })
        .collect::<Result<Vec<_>>>()?;
    let case_insensitive = obj
        .get("case_insensitive")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let field_path = ZiCFieldPath::ZiFParse(path)?;

    Ok(Box::new(ZiCFilterContainsNone::ZiFNew(
        field_path,
        substrings,
        case_insensitive,
    )))
}

/// Keeps records when an array field contains a target value.
#[derive(Debug)]
pub struct ZiCFilterArrayContains {
    path: ZiCFieldPath,
    element: Value,
}

impl ZiCFilterArrayContains {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, element: Value) -> Self {
        Self { path, element }
    }
}

impl ZiCOperator for ZiCFilterArrayContains {
    fn name(&self) -> &'static str {
        "filter.array_contains"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.ZiFResolve(record) {
                Some(Value::Array(values)) => values.iter().any(|value| value == &self.element),
                None => true,
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiCFilterArrayContains`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterArrayContainsFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.array_contains config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.array_contains requires string 'path'"))?;

    let element = obj
        .get("element")
        .cloned()
        .ok_or_else(|| ZiError::validation("filter.array_contains requires 'element' value"))?;

    let field_path = ZiCFieldPath::ZiFParse(path)?;

    Ok(Box::new(ZiCFilterArrayContains::ZiFNew(
        field_path, element,
    )))
}

/// Factory that constructs [`ZiCFilterExists`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterExistsFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.exists config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.exists requires string 'path'"))?;

    let field_path = ZiCFieldPath::ZiFParse(path)?;

    Ok(Box::new(ZiCFilterExists::ZiFNew(field_path)))
}

/// Keeps records where a field is missing or null.
#[derive(Debug)]
pub struct ZiCFilterNotExists {
    path: ZiCFieldPath,
}

impl ZiCFilterNotExists {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath) -> Self {
        Self { path }
    }
}

impl ZiCOperator for ZiCFilterNotExists {
    fn name(&self) -> &'static str {
        "filter.not_exists"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.ZiFResolve(record) {
                Some(value) => value.is_null(),
                None => true,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiCFilterNotExists`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterNotExistsFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.not_exists config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.not_exists requires string 'path'"))?;

    let field_path = ZiCFieldPath::ZiFParse(path)?;

    Ok(Box::new(ZiCFilterNotExists::ZiFNew(field_path)))
}

/// Dot-delimited path referencing either payload or metadata values.
#[derive(Clone, Debug)]
pub struct ZiCFieldPath {
    segments: Vec<String>,
}

impl ZiCFieldPath {
    #[allow(non_snake_case)]
    pub fn ZiFParse(path: &str) -> Result<Self> {
        let segments: Vec<String> = path
            .split('.')
            .map(|segment| segment.trim().to_string())
            .filter(|segment| !segment.is_empty())
            .collect();

        if segments.is_empty() {
            return Err(ZiError::validation("field path may not be empty"));
        }

        let first = segments.first().unwrap();
        if first != "payload" && first != "metadata" {
            return Err(ZiError::validation(
                "field path must start with 'payload' or 'metadata'",
            ));
        }

        if first == "metadata" && segments.len() == 1 {
            return Err(ZiError::validation(
                "metadata paths must include at least one key",
            ));
        }

        Ok(Self { segments })
    }

    #[allow(non_snake_case)]
    pub fn ZiFResolve<'a>(&self, record: &'a ZiCRecord) -> Option<&'a Value> {
        let mut segments = self.segments.iter();
        match segments.next()?.as_str() {
            "payload" => {
                let mut current: &Value = &record.payload;
                for segment in segments {
                    current = match current {
                        Value::Object(map) => map.get(segment)?,
                        _ => return None,
                    };
                }
                Some(current)
            }
            "metadata" => {
                let metadata = record.metadata.as_ref()?;
                let mut current = metadata.get(segments.next()?)?;
                for segment in segments {
                    current = match current {
                        Value::Object(map) => map.get(segment)?,
                        _ => return None,
                    };
                }
                Some(current)
            }
            _ => None,
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFSetValue(&self, record: &mut ZiCRecord, value: Value) -> bool {
        let segments: Vec<String> = self.segments.clone();
        match segments.first().map(|s| s.as_str()) {
            Some("payload") => {
                if !matches!(record.payload, Value::Object(_)) {
                    record.payload = Value::Object(serde_json::Map::new());
                }
                let mut current = match &mut record.payload {
                    Value::Object(map) => map,
                    _ => unreachable!(),
                };
                for seg in &segments[1..segments.len().saturating_sub(1)] {
                    if !current.contains_key(seg) {
                        current.insert(seg.clone(), Value::Object(serde_json::Map::new()));
                    }
                    current = match current.get_mut(seg) {
                        Some(Value::Object(map)) => map,
                        Some(_) => return false,
                        None => return false,
                    };
                }
                if let Some(last) = segments.last() {
                    current.insert(last.clone(), value);
                    true
                } else {
                    false
                }
            }
            Some("metadata") => {
                let metadata = record.ZiFMetadataMut();
                let mut current = metadata;
                for seg in &segments[1..segments.len().saturating_sub(1)] {
                    if !current.contains_key(seg) {
                        current.insert(seg.clone(), Value::Object(serde_json::Map::new()));
                    }
                    current = match current.get_mut(seg) {
                        Some(Value::Object(map)) => map,
                        Some(_) => return false,
                        None => return false,
                    };
                }
                if let Some(last) = segments.last() {
                    current.insert(last.clone(), value);
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

    #[test]
    fn filter_contains_factory_parses_config() {
        let config = json!({"path": "metadata.tags", "contains": "vip"});
        let operator = ZiFFilterContainsFactory(&config).unwrap();

        let mut record = ZiCRecord::ZiFNew(None, json!({"lang": "en"}));
        record
            .ZiFMetadataMut()
            .insert("tags".into(), json!(["vip", "priority"]));
        let batch = vec![record, ZiCRecord::ZiFNew(None, json!({"lang": "en"}))];

        let output = operator.apply(batch).unwrap();
        assert_eq!(output.len(), 1);

        let err = ZiFFilterContainsFactory(&json!({"path": "payload.text"})).unwrap_err();
        match err {
            ZiError::Validation { message } => {
                assert!(message.contains("requires string 'contains'"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn filter_starts_with_matches_prefix() {
        let operator = ZiCFilterStartsWith::ZiFNew(
            ZiCFieldPath::ZiFParse("payload.text").unwrap(),
            "hello".into(),
        );
        let batch = vec![
            ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "hello world"})),
            ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "world"})),
        ];

        let output = operator.apply(batch).unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].id.as_deref(), Some("1"));
    }

    #[test]
    fn filter_starts_with_factory_parses_config() {
        let config = json!({"path": "metadata.tags", "prefix": "vip"});
        let operator = ZiFFilterStartsWithFactory(&config).unwrap();

        let mut record = ZiCRecord::ZiFNew(None, json!({"lang": "en"}));
        record
            .ZiFMetadataMut()
            .insert("tags".into(), json!(["vip:gold", "priority"]));
        let batch = vec![record, ZiCRecord::ZiFNew(None, json!({"lang": "en"}))];

        let output = operator.apply(batch).unwrap();
        assert_eq!(output.len(), 1);

        let err = ZiFFilterStartsWithFactory(&json!({"path": "payload.text"})).unwrap_err();
        match err {
            ZiError::Validation { message } => {
                assert!(message.contains("prefix"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn filter_contains_none_rejects_configured_substrings() {
        let operator = ZiCFilterContainsNone::ZiFNew(
            ZiCFieldPath::ZiFParse("payload.text").unwrap(),
            vec!["bad".into(), "spam".into()],
            false,
        );
        let batch = vec![
            ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "clean text"})),
            ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "bad content"})),
            ZiCRecord::ZiFNew(Some("3".into()), json!({"text": "spam mail"})),
        ];

        let output = operator.apply(batch).unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].id.as_deref(), Some("1"));
    }

    #[test]
    fn filter_contains_none_factory_parses_config() {
        let config = json!({
            "path": "metadata.tags",
            "contains_none": ["blocked", "spam"]
        });
        let operator = ZiFFilterContainsNoneFactory(&config).unwrap();

        let mut record = ZiCRecord::ZiFNew(None, json!({"text": "hello"}));
        record
            .ZiFMetadataMut()
            .insert("tags".into(), json!(["clean", "priority"]));
        let output = operator.apply(vec![record]).unwrap();
        assert_eq!(output.len(), 1);

        let mut record = ZiCRecord::ZiFNew(None, json!({"text": "hello"}));
        record
            .ZiFMetadataMut()
            .insert("tags".into(), json!(["blocked", "beta"]));
        let output = operator.apply(vec![record]).unwrap();
        assert_eq!(output.len(), 0);

        let err = ZiFFilterContainsNoneFactory(&json!({"path": "payload.text"})).unwrap_err();
        match err {
            ZiError::Validation { message } => {
                assert!(message.contains("requires array 'contains_none'"));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let err = ZiFFilterContainsNoneFactory(&json!({
            "path": "payload.text",
            "contains_none": []
        }))
        .unwrap_err();
        match err {
            ZiError::Validation { message } => {
                assert!(message.contains("may not be empty"));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let err = ZiFFilterContainsNoneFactory(&json!({
            "path": "payload.text",
            "contains_none": ["ok", 1]
        }))
        .unwrap_err();
        match err {
            ZiError::Validation { message } => {
                assert!(message.contains("must be strings"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn filter_contains_none_allows_missing_or_null() {
        let operator = ZiCFilterContainsNone::ZiFNew(
            ZiCFieldPath::ZiFParse("metadata.notes").unwrap(),
            vec!["blocked".into()],
            false,
        );

        let mut record = ZiCRecord::ZiFNew(None, json!({}));
        record
            .ZiFMetadataMut()
            .insert("tags".into(), json!(["vip_gold", "priority"]));
        let batch = vec![record, ZiCRecord::ZiFNew(None, json!({"lang": "en"}))];

        let output = operator.apply(batch).unwrap();
        assert_eq!(output.len(), 1);

        let err = ZiFFilterRegexFactory(&json!({"path": "payload.text"})).unwrap_err();
        match err {
            ZiError::Validation { message } => {
                assert!(message.contains("pattern"));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let err =
            ZiFFilterRegexFactory(&json!({"path": "payload.text", "pattern": "["})).unwrap_err();
        match err {
            ZiError::Validation { message } => {
                assert!(message.contains("invalid regex"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn filter_is_null_includes_missing_when_enabled() {
        let operator =
            ZiCFilterIsNull::ZiFNew(ZiCFieldPath::ZiFParse("payload.optional").unwrap(), true);
        let batch = vec![
            ZiCRecord::ZiFNew(Some("1".into()), json!({"optional": null})),
            ZiCRecord::ZiFNew(Some("2".into()), json!({})),
            ZiCRecord::ZiFNew(Some("3".into()), json!({"optional": "value"})),
        ];

        let output = operator.apply(batch).unwrap();
        let ids: Vec<_> = output.iter().map(|record| record.id.as_deref()).collect();
        assert_eq!(ids, vec![Some("1"), Some("2")]);
    }

    #[test]
    fn filter_is_null_excludes_missing_when_disabled() {
        let operator =
            ZiCFilterIsNull::ZiFNew(ZiCFieldPath::ZiFParse("payload.optional").unwrap(), false);
        let batch = vec![
            ZiCRecord::ZiFNew(Some("1".into()), json!({"optional": null})),
            ZiCRecord::ZiFNew(Some("2".into()), json!({})),
        ];

        let output = operator.apply(batch).unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].id.as_deref(), Some("1"));
    }

    #[test]
    fn filter_is_null_factory_parses_config() {
        let config = json!({"path": "metadata.flag", "include_missing": false});
        let operator = ZiFFilterIsNullFactory(&config).unwrap();

        let mut record_null = ZiCRecord::ZiFNew(None, json!({"field": 1}));
        record_null
            .ZiFMetadataMut()
            .insert("flag".into(), Value::Null);
        let batch = vec![record_null, ZiCRecord::ZiFNew(None, json!({}))];

        let output = operator.apply(batch).unwrap();
        assert_eq!(output.len(), 1);

        let err = ZiFFilterIsNullFactory(&json!({"include_missing": true})).unwrap_err();
        match err {
            ZiError::Validation { message } => {
                assert!(message.contains("requires string 'path'"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn filter_greater_than_filters_numeric_values() {
        let operator =
            ZiCFilterGreaterThan::ZiFNew(ZiCFieldPath::ZiFParse("payload.score").unwrap(), 0.5);
        let batch = vec![
            ZiCRecord::ZiFNew(Some("1".into()), json!({"score": 0.6})),
            ZiCRecord::ZiFNew(Some("2".into()), json!({"score": 0.4})),
            ZiCRecord::ZiFNew(Some("3".into()), json!({"score": "0.9"})),
        ];

        let output = operator.apply(batch).unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].id.as_deref(), Some("1"));
    }

    #[test]
    fn filter_greater_than_factory_parses_config() {
        let config = json!({"path": "metadata.score", "threshold": 0.2});
        let operator = ZiFFilterGreaterThanFactory(&config).unwrap();

        let mut record = ZiCRecord::ZiFNew(None, json!({"any": true}));
        record.ZiFMetadataMut().insert("score".into(), json!(0.3));
        let batch = vec![record, ZiCRecord::ZiFNew(None, json!({"any": true}))];

        let output = operator.apply(batch).unwrap();
        assert_eq!(output.len(), 1);

        let err = ZiFFilterGreaterThanFactory(&json!({"path": "payload.score"})).unwrap_err();
        match err {
            ZiError::Validation { message } => {
                assert!(message.contains("threshold"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn filter_less_than_filters_numeric_values() {
        let operator =
            ZiCFilterLessThan::ZiFNew(ZiCFieldPath::ZiFParse("payload.score").unwrap(), 0.5);
        let batch = vec![
            ZiCRecord::ZiFNew(Some("1".into()), json!({"score": 0.4})),
            ZiCRecord::ZiFNew(Some("2".into()), json!({"score": 0.6})),
            ZiCRecord::ZiFNew(Some("3".into()), json!({"score": "0.1"})),
        ];

        let output = operator.apply(batch).unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].id.as_deref(), Some("1"));
    }

    #[test]
    fn filter_less_than_factory_parses_config() {
        let config = json!({"path": "metadata.score", "threshold": 0.8});
        let operator = ZiFFilterLessThanFactory(&config).unwrap();

        let mut record = ZiCRecord::ZiFNew(None, json!({"any": true}));
        record.ZiFMetadataMut().insert("score".into(), json!(0.5));
        let batch = vec![record, ZiCRecord::ZiFNew(None, json!({"any": true}))];

        let output = operator.apply(batch).unwrap();
        assert_eq!(output.len(), 1);

        let err = ZiFFilterLessThanFactory(&json!({"path": "payload.score"})).unwrap_err();
        match err {
            ZiError::Validation { message } => {
                assert!(message.contains("threshold"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn filter_between_filters_values_in_range() {
        let operator =
            ZiCFilterBetween::ZiFNew(ZiCFieldPath::ZiFParse("payload.score").unwrap(), 0.3, 0.7);
        let batch = vec![
            ZiCRecord::ZiFNew(Some("1".into()), json!({"score": 0.5})),
            ZiCRecord::ZiFNew(Some("2".into()), json!({"score": 0.2})),
            ZiCRecord::ZiFNew(Some("3".into()), json!({"score": 0.9})),
        ];

        let output = operator.apply(batch).unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].id.as_deref(), Some("1"));
    }

    #[test]
    fn filter_between_factory_parses_config() {
        let config = json!({"path": "metadata.score", "min": 0.1, "max": 0.6});
        let operator = ZiFFilterBetweenFactory(&config).unwrap();

        let mut record = ZiCRecord::ZiFNew(None, json!({"any": true}));
        record.ZiFMetadataMut().insert("score".into(), json!(0.4));
        let batch = vec![record, ZiCRecord::ZiFNew(None, json!({"any": true}))];

        let output = operator.apply(batch).unwrap();
        assert_eq!(output.len(), 1);

        let err =
            ZiFFilterBetweenFactory(&json!({"path": "payload.score", "min": 0.7})).unwrap_err();
        match err {
            ZiError::Validation { message } => {
                assert!(message.contains("max"));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let err =
            ZiFFilterBetweenFactory(&json!({"path": "payload.score", "min": 0.8, "max": 0.2}))
                .unwrap_err();
        match err {
            ZiError::Validation { message } => {
                assert!(message.contains("may not exceed"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn filter_any_matches_across_paths() {
        let operator = ZiCFilterAny::ZiFNew(
            vec![
                ZiCFieldPath::ZiFParse("payload.primary").unwrap(),
                ZiCFieldPath::ZiFParse("metadata.tag").unwrap(),
            ],
            json!("match"),
        );

        let record1 = ZiCRecord::ZiFNew(Some("1".into()), json!({"primary": "match"}));
        let mut record2 = ZiCRecord::ZiFNew(Some("2".into()), json!({"primary": "other"}));
        record2
            .ZiFMetadataMut()
            .insert("tag".into(), json!("match"));
        let record3 = ZiCRecord::ZiFNew(Some("3".into()), json!({"primary": "nope"}));

        let batch = vec![record1, record2, record3];
        let output = operator.apply(batch).unwrap();

        let ids: Vec<_> = output.iter().map(|r| r.id.as_deref()).collect();
        assert_eq!(ids, vec![Some("1"), Some("2")]);
    }

    #[test]
    fn filter_any_factory_parses_config() {
        let config = json!({
            "paths": ["payload.primary", "metadata.tag"],
            "equals": "match"
        });
        let operator = ZiFFilterAnyFactory(&config).unwrap();

        let record = ZiCRecord::ZiFNew(None, json!({"primary": "match"}));
        let batch = vec![record.clone(), {
            let mut alt = ZiCRecord::ZiFNew(None, json!({"primary": "other"}));
            alt.ZiFMetadataMut().insert("tag".into(), json!("match"));
            alt
        }];

        let output = operator.apply(batch).unwrap();
        assert_eq!(output.len(), 2);

        let err = ZiFFilterAnyFactory(&json!({"equals": "match"})).unwrap_err();
        match err {
            ZiError::Validation { message } => assert!(message.contains("paths")),
            other => panic!("unexpected error: {other:?}"),
        }

        let err = ZiFFilterAnyFactory(&json!({"paths": [], "equals": "match"})).unwrap_err();
        match err {
            ZiError::Validation { message } => assert!(message.contains("may not be empty")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn filter_not_equals_filters_out_matches() {
        let operator = ZiCFilterNotEquals::ZiFNew(
            ZiCFieldPath::ZiFParse("payload.lang").unwrap(),
            json!("en"),
        );
        let batch = vec![
            ZiCRecord::ZiFNew(Some("1".into()), json!({"lang": "en"})),
            ZiCRecord::ZiFNew(Some("2".into()), json!({"lang": "zh"})),
        ];

        let output = operator.apply(batch).unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].id.as_deref(), Some("2"));
    }

    #[test]
    fn filter_not_equals_factory_parses_config() {
        let config = json!({"path": "metadata.category", "equals": "blocked"});
        let operator = ZiFFilterNotEqualsFactory(&config).unwrap();

        let mut record = ZiCRecord::ZiFNew(None, json!({"lang": "en"}));
        record
            .ZiFMetadataMut()
            .insert("category".into(), json!("news"));
        let batch = vec![record];

        let output = operator.apply(batch).unwrap();
        assert_eq!(output.len(), 1);

        let err = ZiFFilterNotEqualsFactory(&json!({"path": "payload.lang"})).unwrap_err();
        match err {
            ZiError::Validation { message } => assert!(message.contains("'equals'")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn filter_not_in_excludes_disallowed_values() {
        let operator = ZiCFilterNotIn::ZiFNew(
            ZiCFieldPath::ZiFParse("payload.lang").unwrap(),
            vec![json!("en"), json!("fr")],
        );
        let batch = vec![
            ZiCRecord::ZiFNew(Some("1".into()), json!({"lang": "en"})),
            ZiCRecord::ZiFNew(Some("2".into()), json!({"lang": "zh"})),
        ];

        let output = operator.apply(batch).unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].id.as_deref(), Some("2"));
    }

    #[test]
    fn filter_not_in_factory_parses_config() {
        let config = json!({
            "path": "metadata.tag",
            "values": ["blocked", "spam"]
        });
        let operator = ZiFFilterNotInFactory(&config).unwrap();

        let mut record = ZiCRecord::ZiFNew(None, json!({"lang": "en"}));
        record.ZiFMetadataMut().insert("tag".into(), json!("clean"));
        let output = operator.apply(vec![record]).unwrap();
        assert_eq!(output.len(), 1);

        let err = ZiFFilterNotInFactory(&json!({"path": "payload.lang"})).unwrap_err();
        match err {
            ZiError::Validation { message } => assert!(message.contains("'values'")),
            other => panic!("unexpected error: {other:?}"),
        }

        let err =
            ZiFFilterNotInFactory(&json!({"path": "payload.lang", "values": []})).unwrap_err();
        match err {
            ZiError::Validation { message } => assert!(message.contains("may not be empty")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn filter_length_range_factory_parses_config() {
        let config = json!({
            "path": "payload.text",
            "min": 3,
            "max": 8
        });
        let operator = ZiFFilterLengthRangeFactory(&config).unwrap();

        let batch = vec![
            ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "ok"})),
            ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "perfect"})),
        ];

        let out = operator.apply(batch).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id.as_deref(), Some("2"));

        let err = ZiFFilterLengthRangeFactory(&json!({"path": "payload.text"})).unwrap_err();
        match err {
            ZiError::Validation { message } => assert!(message.contains("at least one of")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn filter_token_range_filters_by_token_count() {
        let op = ZiCFilterTokenRange::ZiFNew(
            ZiCFieldPath::ZiFParse("payload.text").unwrap(),
            Some(2),
            Some(3),
        );
        let batch = vec![
            ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "one"})),
            ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "two tokens"})),
            ZiCRecord::ZiFNew(Some("3".into()), json!({"text": "this has four tokens"})),
        ];

        let out = op.apply(batch).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id.as_deref(), Some("2"));
    }

    #[test]
    fn filter_token_range_factory_parses_config() {
        let config = json!({
            "path": "metadata.summary",
            "min": 2
        });
        let operator = ZiFFilterTokenRangeFactory(&config).unwrap();

        let mut record = ZiCRecord::ZiFNew(None, json!({}));
        record
            .ZiFMetadataMut()
            .insert("summary".into(), json!("short summary"));
        let out = operator.apply(vec![record]).unwrap();
        assert_eq!(out.len(), 1);

        let err = ZiFFilterTokenRangeFactory(&json!({"path": "payload.text"})).unwrap_err();
        match err {
            ZiError::Validation { message } => assert!(message.contains("at least one")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn filter_equals_factory_parses_config() {
        let config = json!({"path": "payload.tag", "equals": 1});
        let operator = ZiFFilterEqualsFactory(&config).unwrap();

        let batch = vec![
            ZiCRecord::ZiFNew(None, json!({"tag": 1})),
            ZiCRecord::ZiFNew(None, json!({"tag": 2})),
        ];

        let output = operator.apply(batch).unwrap();
        assert_eq!(output.len(), 1);
    }

    #[test]
    fn field_path_errors_on_invalid_prefix() {
        let err = ZiCFieldPath::ZiFParse("data.field").unwrap_err();
        match err {
            ZiError::Validation { message } => {
                assert!(message.contains("payload"));
            }
            _ => panic!("unexpected error kind"),
        }
    }

/// Keeps string fields whose character length falls within optional bounds.
#[derive(Debug)]
pub struct ZiCFilterLengthRange {
    path: ZiCFieldPath,
    min: Option<usize>,
    max: Option<usize>,
}

impl ZiCFilterLengthRange {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, min: Option<usize>, max: Option<usize>) -> Self {
        Self { path, min, max }
    }
}

impl ZiCOperator for ZiCFilterLengthRange {
    fn name(&self) -> &'static str {
        "filter.length_range"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.ZiFResolve(record) {
                Some(Value::String(s)) => {
                    let len = s.chars().count();
                    if let Some(min) = self.min {
                        if len < min {
                            return false;
                        }
                    }
                    if let Some(max) = self.max {
                        if len > max {
                            return false;
                        }
                    }
                    true
                }
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiCFilterLengthRange`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterLengthRangeFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.length_range config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.length_range requires string 'path'"))?;

    let min = obj
        .get("min")
        .and_then(Value::as_u64)
        .map(|value| value as usize);
    let max = obj
        .get("max")
        .and_then(Value::as_u64)
        .map(|value| value as usize);

    if min.is_none() && max.is_none() {
        return Err(ZiError::validation(
            "filter.length_range requires at least one of 'min' or 'max'",
        ));
    }

    let field_path = ZiCFieldPath::ZiFParse(path)?;

    Ok(Box::new(ZiCFilterLengthRange::ZiFNew(field_path, min, max)))
}

/// Keeps string fields whose whitespace token counts fall within optional bounds.
#[derive(Debug)]
pub struct ZiCFilterTokenRange {
    path: ZiCFieldPath,
    min: Option<usize>,
    max: Option<usize>,
}

impl ZiCFilterTokenRange {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, min: Option<usize>, max: Option<usize>) -> Self {
        Self { path, min, max }
    }
}

impl ZiCOperator for ZiCFilterTokenRange {
    fn name(&self) -> &'static str {
        "filter.token_range"
    }

    fn apply(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.ZiFResolve(record) {
                Some(Value::String(s)) => {
                    let len = s.split_whitespace().count();
                    if let Some(min) = self.min {
                        if len < min {
                            return false;
                        }
                    }
                    if let Some(max) = self.max {
                        if len > max {
                            return false;
                        }
                    }
                    true
                }
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiCFilterTokenRange`] from JSON configuration.
#[allow(non_snake_case)]
pub fn ZiFFilterTokenRangeFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.token_range config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.token_range requires string 'path'"))?;

    let min = obj
        .get("min")
        .and_then(Value::as_u64)
        .map(|value| value as usize);
    let max = obj
        .get("max")
        .and_then(Value::as_u64)
        .map(|value| value as usize);

    if min.is_none() && max.is_none() {
        return Err(ZiError::validation(
            "filter.token_range requires at least one of 'min' or 'max'",
        ));
    }

    let field_path = ZiCFieldPath::ZiFParse(path)?;

    Ok(Box::new(ZiCFilterTokenRange::ZiFNew(field_path, min, max)))
}
