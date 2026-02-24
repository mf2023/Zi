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
use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::operator::ZiOperator;
use crate::record::{ZiRecord, ZiRecordBatch};

/// Keeps records whose field equals a target [`Value`].
#[derive(Debug)]
pub struct ZiFilterEquals {
    path: ZiFieldPath,
    equals: Value,
}

impl ZiFilterEquals {
    /// Creates a new filter operator.
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, equals: Value) -> Self {
        Self { path, equals }
    }
}

impl ZiOperator for ZiFilterEquals {
    fn name(&self) -> &'static str {
        "filter.equals"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.resolve(record) {
                Some(value) => value == &self.equals,
                None => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiFilterEquals`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_equals_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.equals config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.equals requires string 'path'"))?;

    let field_path = ZiFieldPath::parse(path)?;
    let equals = obj
        .get("equals")
        .cloned()
        .ok_or_else(|| ZiError::validation("filter.equals requires 'equals' value"))?;

    Ok(Box::new(ZiFilterEquals::new(field_path, equals)))
}

/// Keeps records whose field does not equal a target value.
#[derive(Debug)]
pub struct ZiFilterNotEquals {
    path: ZiFieldPath,
    equals: Value,
}

impl ZiFilterNotEquals {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, equals: Value) -> Self {
        Self { path, equals }
    }
}

impl ZiOperator for ZiFilterNotEquals {
    fn name(&self) -> &'static str {
        "filter.not_equals"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.resolve(record) {
                Some(value) => value != &self.equals,
                None => true,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiFilterNotEquals`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_not_equals_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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

    let field_path = ZiFieldPath::parse(path)?;

    Ok(Box::new(ZiFilterNotEquals::new(field_path, equals)))
}

/// Keeps records where any configured field equals a target value.
#[derive(Debug)]
pub struct ZiFilterAny {
    paths: Vec<ZiFieldPath>,
    equals: Value,
}

impl ZiFilterAny {
    #[allow(non_snake_case)]
    pub fn new(paths: Vec<ZiFieldPath>, equals: Value) -> Self {
        Self { paths, equals }
    }
}

impl ZiOperator for ZiFilterAny {
    fn name(&self) -> &'static str {
        "filter.any"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| {
                self.paths.iter().any(|path| match path.resolve(record) {
                    Some(value) => value == &self.equals,
                    None => false,
                })
            })
            .collect())
    }
}

/// Factory that constructs [`ZiFilterAny`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_any_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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
                .and_then(ZiFieldPath::parse)
        })
        .collect::<Result<Vec<_>>>()?;

    let equals = obj
        .get("equals")
        .cloned()
        .ok_or_else(|| ZiError::validation("filter.any requires 'equals' value"))?;

    Ok(Box::new(ZiFilterAny::new(field_paths, equals)))
}

/// Keeps records where a numeric field lies within inclusive bounds.
#[derive(Debug)]
pub struct ZiFilterBetween {
    path: ZiFieldPath,
    min: f64,
    max: f64,
}

impl ZiFilterBetween {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, min: f64, max: f64) -> Self {
        Self { path, min, max }
    }
}

impl ZiOperator for ZiFilterBetween {
    fn name(&self) -> &'static str {
        "filter.between"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.resolve(record) {
                Some(Value::Number(number)) => number
                    .as_f64()
                    .map_or(false, |value| value >= self.min && value <= self.max),
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiFilterBetween`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_between_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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

    let field_path = ZiFieldPath::parse(path)?;

    Ok(Box::new(ZiFilterBetween::new(field_path, min, max)))
}

/// Keeps records where a numeric field is less than a threshold.
#[derive(Debug)]
pub struct ZiFilterLessThan {
    path: ZiFieldPath,
    threshold: f64,
}

impl ZiFilterLessThan {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, threshold: f64) -> Self {
        Self { path, threshold }
    }
}

impl ZiOperator for ZiFilterLessThan {
    fn name(&self) -> &'static str {
        "filter.less_than"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.resolve(record) {
                Some(Value::Number(number)) => number
                    .as_f64()
                    .map_or(false, |value| value < self.threshold),
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiFilterLessThan`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_less_than_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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

    let field_path = ZiFieldPath::parse(path)?;

    Ok(Box::new(ZiFilterLessThan::new(field_path, threshold)))
}

/// Keeps records where a numeric field is greater than a threshold.
#[derive(Debug)]
pub struct ZiFilterGreaterThan {
    path: ZiFieldPath,
    threshold: f64,
}

impl ZiFilterGreaterThan {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, threshold: f64) -> Self {
        Self { path, threshold }
    }
}

impl ZiOperator for ZiFilterGreaterThan {
    fn name(&self) -> &'static str {
        "filter.greater_than"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.resolve(record) {
                Some(Value::Number(number)) => number
                    .as_f64()
                    .map_or(false, |value| value > self.threshold),
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiFilterGreaterThan`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_greater_than_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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

    let field_path = ZiFieldPath::parse(path)?;

    Ok(Box::new(ZiFilterGreaterThan::new(
        field_path, threshold,
    )))
}

/// Keeps records where a field is explicitly null or missing.
#[derive(Debug)]
pub struct ZiFilterIsNull {
    path: ZiFieldPath,
    include_missing: bool,
}

impl ZiFilterIsNull {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, include_missing: bool) -> Self {
        Self {
            path,
            include_missing,
        }
    }
}

impl ZiOperator for ZiFilterIsNull {
    fn name(&self) -> &'static str {
        "filter.is_null"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.resolve(record) {
                Some(Value::Null) => true,
                None => self.include_missing,
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiFilterIsNull`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_is_null_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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

    let field_path = ZiFieldPath::parse(path)?;

    Ok(Box::new(ZiFilterIsNull::new(
        field_path,
        include_missing,
    )))
}

/// Keeps records where a field matches a regular expression.
#[derive(Debug)]
pub struct ZiFilterRegex {
    path: ZiFieldPath,
    pattern: Regex,
}

impl ZiFilterRegex {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, pattern: Regex) -> Self {
        Self { path, pattern }
    }
}

impl ZiOperator for ZiFilterRegex {
    fn name(&self) -> &'static str {
        "filter.regex"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.resolve(record) {
                Some(Value::String(value)) => self.pattern.is_match(value),
                Some(Value::Array(values)) => values.iter().any(
                    |value| matches!(value, Value::String(item) if self.pattern.is_match(item)),
                ),
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiFilterRegex`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_regex_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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

    let field_path = ZiFieldPath::parse(path)?;

    Ok(Box::new(ZiFilterRegex::new(field_path, regex)))
}

/// Keeps records where a field ends with a suffix.
#[derive(Debug)]
pub struct ZiFilterEndsWith {
    path: ZiFieldPath,
    suffix: String,
}

impl ZiFilterEndsWith {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, suffix: String) -> Self {
        Self { path, suffix }
    }
}

impl ZiOperator for ZiFilterEndsWith {
    fn name(&self) -> &'static str {
        "filter.ends_with"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.resolve(record) {
                Some(Value::String(value)) => value.ends_with(&self.suffix),
                Some(Value::Array(values)) => values.iter().any(
                    |value| matches!(value, Value::String(item) if item.ends_with(&self.suffix)),
                ),
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiFilterEndsWith`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_ends_with_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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

    let field_path = ZiFieldPath::parse(path)?;

    Ok(Box::new(ZiFilterEndsWith::new(field_path, suffix)))
}

/// Keeps records where a field starts with a prefix.
#[derive(Debug)]
pub struct ZiFilterStartsWith {
    path: ZiFieldPath,
    prefix: String,
}

impl ZiFilterStartsWith {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, prefix: String) -> Self {
        Self { path, prefix }
    }
}

impl ZiOperator for ZiFilterStartsWith {
    fn name(&self) -> &'static str {
        "filter.starts_with"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.resolve(record) {
                Some(Value::String(value)) => value.starts_with(&self.prefix),
                Some(Value::Array(values)) => values.iter().any(
                    |value| matches!(value, Value::String(item) if item.starts_with(&self.prefix)),
                ),
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiFilterStartsWith`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_starts_with_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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

    let field_path = ZiFieldPath::parse(path)?;

    Ok(Box::new(ZiFilterStartsWith::new(field_path, prefix)))
}

/// Keeps records where a numeric field falls within an optional range.
#[derive(Debug)]
pub struct ZiFilterRange {
    path: ZiFieldPath,
    min: Option<f64>,
    max: Option<f64>,
}

impl ZiFilterRange {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, min: Option<f64>, max: Option<f64>) -> Self {
        Self { path, min, max }
    }
}

impl ZiOperator for ZiFilterRange {
    fn name(&self) -> &'static str {
        "filter.range"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.resolve(record) {
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

/// Factory that constructs [`ZiFilterRange`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_range_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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

    let field_path = ZiFieldPath::parse(path)?;

    Ok(Box::new(ZiFilterRange::new(field_path, min, max)))
}

/// Keeps records where a field equals any configured value.
#[derive(Debug)]
pub struct ZiFilterIn {
    path: ZiFieldPath,
    allowed: Vec<Value>,
}

impl ZiFilterIn {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, allowed: Vec<Value>) -> Self {
        Self { path, allowed }
    }
}

impl ZiOperator for ZiFilterIn {
    fn name(&self) -> &'static str {
        "filter.in"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.resolve(record) {
                Some(value) => self.allowed.iter().any(|allowed| allowed == value),
                None => false,
            })
            .collect())
    }
}

/// Keeps records where a field does **not** equal any configured value.
#[derive(Debug)]
pub struct ZiFilterNotIn {
    path: ZiFieldPath,
    disallowed: Vec<Value>,
}

impl ZiFilterNotIn {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, disallowed: Vec<Value>) -> Self {
        Self { path, disallowed }
    }
}

impl ZiOperator for ZiFilterNotIn {
    fn name(&self) -> &'static str {
        "filter.not_in"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.resolve(record) {
                Some(value) => !self.disallowed.iter().any(|blocked| blocked == value),
                None => true,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiFilterIn`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_in_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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
    let field_path = ZiFieldPath::parse(path)?;

    Ok(Box::new(ZiFilterIn::new(field_path, allowed)))
}

/// Factory that constructs [`ZiFilterNotIn`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_not_in_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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
    let field_path = ZiFieldPath::parse(path)?;

    Ok(Box::new(ZiFilterNotIn::new(field_path, disallowed)))
}

/// Keeps records where a field exists and is not null.
#[derive(Debug)]
pub struct ZiFilterExists {
    path: ZiFieldPath,
}

impl ZiFilterExists {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath) -> Self {
        Self { path }
    }
}

impl ZiOperator for ZiFilterExists {
    fn name(&self) -> &'static str {
        "filter.exists"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(
                |record| matches!(self.path.resolve(record), Some(value) if !value.is_null()),
            )
            .collect())
    }
}

/// Keeps records where a field contains a target substring.
#[derive(Debug)]
pub struct ZiFilterContains {
    path: ZiFieldPath,
    needle: String,
    needle_lower: String,
    case_insensitive: bool,
}

impl ZiFilterContains {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, needle: String, case_insensitive: bool) -> Self {
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

impl ZiOperator for ZiFilterContains {
    fn name(&self) -> &'static str {
        "filter.contains"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.resolve(record) {
                Some(Value::String(value)) => self.matches(value),
                Some(Value::Array(values)) => values
                    .iter()
                    .any(|value| matches!(value, Value::String(item) if self.matches(item))),
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiFilterContains`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_contains_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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

    let field_path = ZiFieldPath::parse(path)?;

    Ok(Box::new(ZiFilterContains::new(
        field_path,
        needle,
        case_insensitive,
    )))
}

/// Keeps records where a field contains all configured substrings.
#[derive(Debug)]
pub struct ZiFilterContainsAll {
    path: ZiFieldPath,
    needles: Vec<String>,
    needles_lower: Vec<String>,
    case_insensitive: bool,
}

impl ZiFilterContainsAll {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, needles: Vec<String>, case_insensitive: bool) -> Self {
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

impl ZiOperator for ZiFilterContainsAll {
    fn name(&self) -> &'static str {
        "filter.contains_all"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.resolve(record) {
                Some(Value::String(value)) => self.string_contains_all(value),
                Some(Value::Array(values)) => self.array_contains_all(values),
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiFilterContainsAll`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_contains_all_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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

    let field_path = ZiFieldPath::parse(path)?;

    Ok(Box::new(ZiFilterContainsAll::new(
        field_path,
        substrings,
        case_insensitive,
    )))
}

/// Keeps records where a field contains any of multiple substrings.
#[derive(Debug)]
pub struct ZiFilterContainsAny {
    path: ZiFieldPath,
    needles: Vec<String>,
    needles_lower: Vec<String>,
    case_insensitive: bool,
}

impl ZiFilterContainsAny {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, needles: Vec<String>, case_insensitive: bool) -> Self {
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

impl ZiOperator for ZiFilterContainsAny {
    fn name(&self) -> &'static str {
        "filter.contains_any"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.resolve(record) {
                Some(Value::String(value)) => self.string_contains_any(value),
                Some(Value::Array(values)) => self.array_contains_any(values),
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiFilterContainsAny`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_contains_any_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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

    let field_path = ZiFieldPath::parse(path)?;

    Ok(Box::new(ZiFilterContainsAny::new(
        field_path,
        values,
        case_insensitive,
    )))
}

/// Keeps records where a field contains none of the configured substrings.
#[derive(Debug)]
pub struct ZiFilterContainsNone {
    path: ZiFieldPath,
    needles: Vec<String>,
    needles_lower: Vec<String>,
    case_insensitive: bool,
}

impl ZiFilterContainsNone {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, needles: Vec<String>, case_insensitive: bool) -> Self {
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

impl ZiOperator for ZiFilterContainsNone {
    fn name(&self) -> &'static str {
        "filter.contains_none"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.resolve(record) {
                Some(Value::String(value)) => !self.string_contains_any(value),
                Some(Value::Array(values)) => !self.array_contains_any(values),
                Some(Value::Null) | None => true,
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiFilterContainsNone`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_contains_none_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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

    let field_path = ZiFieldPath::parse(path)?;

    Ok(Box::new(ZiFilterContainsNone::new(
        field_path,
        substrings,
        case_insensitive,
    )))
}

/// Keeps records when an array field contains a target value.
#[derive(Debug)]
pub struct ZiFilterArrayContains {
    path: ZiFieldPath,
    element: Value,
}

impl ZiFilterArrayContains {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, element: Value) -> Self {
        Self { path, element }
    }
}

impl ZiOperator for ZiFilterArrayContains {
    fn name(&self) -> &'static str {
        "filter.array_contains"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.resolve(record) {
                Some(Value::Array(values)) => values.iter().any(|value| value == &self.element),
                None => true,
                _ => false,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiFilterArrayContains`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_array_contains_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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

    let field_path = ZiFieldPath::parse(path)?;

    Ok(Box::new(ZiFilterArrayContains::new(
        field_path, element,
    )))
}

/// Factory that constructs [`ZiFilterExists`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_exists_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.exists config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.exists requires string 'path'"))?;

    let field_path = ZiFieldPath::parse(path)?;

    Ok(Box::new(ZiFilterExists::new(field_path)))
}

/// Keeps records where a field is missing or null.
#[derive(Debug)]
pub struct ZiFilterNotExists {
    path: ZiFieldPath,
}

impl ZiFilterNotExists {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath) -> Self {
        Self { path }
    }
}

impl ZiOperator for ZiFilterNotExists {
    fn name(&self) -> &'static str {
        "filter.not_exists"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.resolve(record) {
                Some(value) => value.is_null(),
                None => true,
            })
            .collect())
    }
}

/// Factory that constructs [`ZiFilterNotExists`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_not_exists_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("filter.not_exists config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("filter.not_exists requires string 'path'"))?;

    let field_path = ZiFieldPath::parse(path)?;

    Ok(Box::new(ZiFilterNotExists::new(field_path)))
}

/// Dot-delimited path referencing either payload or metadata values.
#[derive(Clone, Debug)]
pub struct ZiFieldPath {
    segments: Vec<String>,
}

impl ZiFieldPath {
    #[allow(non_snake_case)]
    pub fn parse(path: &str) -> Result<Self> {
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
    pub fn resolve<'a>(&self, record: &'a ZiRecord) -> Option<&'a Value> {
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
    pub fn set_value(&self, record: &mut ZiRecord, value: Value) -> bool {
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
                let metadata = record.metadata_mut();
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

/// Keeps string fields whose character length falls within optional bounds.
#[derive(Debug)]
pub struct ZiFilterLengthRange {
    path: ZiFieldPath,
    min: Option<usize>,
    max: Option<usize>,
}

impl ZiFilterLengthRange {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, min: Option<usize>, max: Option<usize>) -> Self {
        Self { path, min, max }
    }
}

impl ZiOperator for ZiFilterLengthRange {
    fn name(&self) -> &'static str {
        "filter.length_range"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.resolve(record) {
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

/// Factory that constructs [`ZiFilterLengthRange`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_length_range_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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

    let field_path = ZiFieldPath::parse(path)?;

    Ok(Box::new(ZiFilterLengthRange::new(field_path, min, max)))
}

/// Keeps string fields whose whitespace token counts fall within optional bounds.
#[derive(Debug)]
pub struct ZiFilterTokenRange {
    path: ZiFieldPath,
    min: Option<usize>,
    max: Option<usize>,
}

impl ZiFilterTokenRange {
    #[allow(non_snake_case)]
    pub fn new(path: ZiFieldPath, min: Option<usize>, max: Option<usize>) -> Self {
        Self { path, min, max }
    }
}

impl ZiOperator for ZiFilterTokenRange {
    fn name(&self) -> &'static str {
        "filter.token_range"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        Ok(batch
            .into_iter()
            .filter(|record| match self.path.resolve(record) {
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

/// Factory that constructs [`ZiFilterTokenRange`] from JSON configuration.
#[allow(non_snake_case)]
pub fn filter_token_range_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
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

    let field_path = ZiFieldPath::parse(path)?;

    Ok(Box::new(ZiFilterTokenRange::new(field_path, min, max)))
}
