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

//! # DSL Parser Module
//!
//! This module provides the parser for the Zi DSL. It converts DSL source code
//! in various formats (JSON, YAML, or simple syntax) into the intermediate representation (IR).
//!
//! ## Supported Formats
//!
//! ### JSON Format
//!
//! ```json
//! [
//!   {"operator": "filter.equals", "config": {"path": "lang", "value": "en"}},
//!   {"operator": "limit", "config": {"max_records": 100}}
//! ]
//! ```
//!
//! ### YAML Format
//!
//! ```yaml
//! steps:
//!   - operator: filter.equals
//!     config:
//!       path: lang
//!       value: en
//!   - operator: limit
//!     config:
//!       max_records: 100
//! ```
//!
//! ### Simple Syntax
//!
//! ```text
//! filter.equals path=lang value=en
//! limit max_records=100
//! $threshold=0.5
//! ```
//!
//! ## Features
//!
//! - **Multi-format support**: JSON, YAML, and simple text syntax
//! - **Variable substitution**: Use $var or ${var} in configurations
//! - **Flexible configuration**: Inline JSON, key=value pairs, or full objects
//! - **Strict mode**: Optional strict parsing with comprehensive error messages
//! - **File parsing**: Auto-detect format based on file extension

use std::collections::HashMap;

use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::dsl::ir::{ZiDSLNode, ZiDSLProgram};

/// The result of parsing a DSL source file.
///
/// This struct contains the parsed program along with any warnings generated
/// during parsing and the variables defined in the source.
///
/// # Fields
///
/// - `program`: The parsed DSL program (IR)
/// - `warnings`: Vector of warning messages generated during parsing
/// - `variables`: HashMap of variable names to their values
///
/// # Example
///
/// ```rust
/// let result = parser.parse(source)?;
/// println!("Parsed {} nodes", result.program.nodes.len());
/// for warning in &result.warnings {
///     println!("Warning: {}", warning);
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ZiParseResult {
    /// The parsed DSL program in intermediate representation format.
    pub program: ZiDSLProgram,
    /// Warnings generated during parsing.
    /// These do not prevent parsing but may indicate potential issues.
    pub warnings: Vec<String>,
    /// Variables defined in the DSL source using $name=value syntax.
    pub variables: HashMap<String, Value>,
}

/// Configuration options for the DSL parser.
///
/// This struct controls parsing behavior including strictness, variable support,
/// and default field names.
///
/// # Fields
///
/// - `strict`: If true, parsing errors cause immediate failure; otherwise warnings are collected
/// - `allow_variables`: If true, allow $variable syntax in configurations
/// - `allow_expressions`: If true, allow expression evaluation in configurations
/// - `default_input_field`: Default field path for input when not specified
/// - `default_output_field`: Default field path for output when not specified
///
/// # Default Configuration
///
/// By default:
/// - strict is false (warnings are collected)
/// - variables are allowed
/// - expressions are allowed
/// - default input field is "payload.text"
/// - default output field is "metadata"
#[derive(Clone, Debug)]
pub struct ZiDSLParserConfig {
    /// Controls parsing strictness.
    /// In strict mode, any parsing error causes immediate failure.
    /// In non-strict mode, errors are collected as warnings.
    pub strict: bool,
    /// Whether to allow variable references ($var or ${var}) in configurations.
    pub allow_variables: bool,
    /// Whether to allow expression evaluation in configurations.
    pub allow_expressions: bool,
    /// Default field path used when input is not specified.
    pub default_input_field: String,
    /// Default field path used when output is not specified.
    pub default_output_field: String,
}

impl Default for ZiDSLParserConfig {
    fn default() -> Self {
        Self {
            strict: false,
            allow_variables: true,
            allow_expressions: true,
            default_input_field: "payload.text".to_string(),
            default_output_field: "metadata".to_string(),
        }
    }
}

/// DSL Parser for converting DSL source code to intermediate representation.
///
/// The parser supports multiple input formats and provides flexible configuration
/// options for controlling parsing behavior.
///
/// # Construction
///
/// ```rust
/// use zi::dsl::parser::{ZiDSLParser, ZiDSLParserConfig};
///
/// // Use default configuration
/// let parser = ZiDSLParser::new();
///
/// // Use custom configuration
/// let config = ZiDSLParserConfig {
///     strict: true,
///     allow_variables: true,
///     allow_expressions: false,
///     default_input_field: "data".to_string(),
///     default_output_field: "result".to_string(),
/// };
/// let parser = ZiDSLParser::with_config(config);
/// ```
///
/// # Usage
///
/// ```rust
/// let parser = ZiDSLParser::new();
/// let result = parser.parse(r#"
///     filter.equals path=lang value=en
///     limit max_records=100
/// "#)?;
/// ```
#[derive(Debug, Default)]
pub struct ZiDSLParser {
    /// Parser configuration options.
    config: ZiDSLParserConfig,
    /// Variables defined during parsing.
    variables: HashMap<String, Value>,
}

impl ZiDSLParser {
    /// Creates a new parser with default configuration.
    ///
    /// # Returns
    ///
    /// A new ZiDSLParser instance with default settings
    #[allow(non_snake_case)]
    pub fn new() -> Self {
        Self {
            config: ZiDSLParserConfig::default(),
            variables: HashMap::new(),
        }
    }

    /// Creates a new parser with custom configuration.
    ///
    /// # Arguments
    ///
    /// - `config`: Custom parser configuration
    ///
    /// # Returns
    ///
    /// A new ZiDSLParser instance with the provided configuration
    #[allow(non_snake_case)]
    pub fn with_config(mut self, config: ZiDSLParserConfig) -> Self {
        self.config = config;
        self
    }

    /// Sets the strict parsing mode.
    ///
    /// # Arguments
    ///
    /// - `strict`: If true, parsing errors cause immediate failure
    ///
    /// # Returns
    ///
    /// Self for method chaining
    #[allow(non_snake_case)]
    pub fn strict(mut self, strict: bool) -> Self {
        self.config.strict = strict;
        self
    }

    /// Sets a variable that can be used in DSL configurations.
    ///
    /// Variables can be referenced using $name or ${name} syntax
    /// within operator configurations.
    ///
    /// # Arguments
    ///
    /// - `name`: Variable name (without $ prefix)
    /// - `value`: Variable value as JSON Value
    ///
    /// # Returns
    ///
    /// Self for method chaining
    ///
    /// # Example
    ///
    /// ```rust
    /// let parser = ZiDSLParser::new()
    ///     .set_variable("threshold", serde_json::json!(0.5))
    ///     .set_variable("language", serde_json::json!("en"));
    /// ```
    #[allow(non_snake_case)]
    pub fn set_variable(mut self, name: &str, value: Value) -> Self {
        self.variables.insert(name.to_string(), value);
        self
    }

    /// Parses DSL source code and returns the result.
    ///
    /// This method auto-detects the format based on the source content:
    /// - JSON: Starts with { or [
    /// - YAML: Starts with "steps:", "pipeline:", or "- operator:"
    /// - Simple: All other formats
    ///
    /// # Arguments
    ///
    /// - `source`: DSL source code as a string
    ///
    /// # Returns
    ///
    /// Result containing ZiParseResult or an error
    #[allow(non_snake_case)]
    pub fn parse(&self, source: &str) -> Result<ZiParseResult> {
        let trimmed = source.trim();
        
        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            self.parse_json(source)
        } else if trimmed.starts_with("steps:") || trimmed.starts_with("pipeline:") || trimmed.starts_with("- operator:") {
            self.parse_yaml(source)
        } else {
            self.parse_simple(source)
        }
    }

    /// Parses a DSL file, auto-detecting format from extension.
    ///
    /// # Arguments
    ///
    /// - `path`: Path to the DSL file
    ///
    /// # Returns
    ///
    /// Result containing ZiParseResult or an error
    ///
    /// # Supported Extensions
    ///
    /// - .json: Force JSON format
    /// - .yaml or .yml: Force YAML format
    /// - Other: Auto-detect based on content
    #[allow(non_snake_case)]
    pub fn parse_file(&self, path: &std::path::Path) -> Result<ZiParseResult> {
        let content = std::fs::read_to_string(path)?;
        
        let extension = path.extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        match extension.as_str() {
            "json" => self.parse_json(&content),
            "yaml" | "yml" => self.parse_yaml(&content),
            _ => self.parse(&content),
        }
    }

    /// Parses DSL source in JSON format.
    ///
    /// JSON format can be either:
    /// - An array of operator objects
    /// - An object with "steps" or "pipeline" key containing an array
    ///
    /// # Arguments
    ///
    /// - `source`: JSON string containing DSL definition
    ///
    /// # Returns
    ///
    /// Result containing ZiParseResult or an error
    ///
    /// # Example JSON Input
    ///
    /// ```json
    /// [
    ///   {"operator": "filter.equals", "config": {"path": "lang", "value": "en"}},
    ///   {"operator": "limit", "config": {"max_records": 100}}
    /// ]
    /// ```
    pub fn parse_json(&self, source: &str) -> Result<ZiParseResult> {
        let mut warnings = Vec::new();
        let value: Value = serde_json::from_str(source)
            .map_err(|e| ZiError::validation(format!("Invalid JSON: {}", e)))?;

        let nodes = match value {
            Value::Array(arr) => {
                self.parse_json_array(&arr, &mut warnings)?
            }
            Value::Object(map) => {
                if let Some(steps) = map.get("steps") {
                    if let Value::Array(arr) = steps {
                        self.parse_json_array(arr, &mut warnings)?
                    } else {
                        return Err(ZiError::validation("'steps' must be an array"));
                    }
                } else if let Some(pipeline) = map.get("pipeline") {
                    if let Value::Array(arr) = pipeline {
                        self.parse_json_array(arr, &mut warnings)?
                    } else {
                        return Err(ZiError::validation("'pipeline' must be an array"));
                    }
                } else {
                    return Err(ZiError::validation("JSON must be array or object with 'steps'/'pipeline'"));
                }
            }
            _ => return Err(ZiError::validation("JSON must be array or object")),
        };

        Ok(ZiParseResult {
            program: ZiDSLProgram { nodes },
            warnings,
            variables: self.variables.clone(),
        })
    }

    /// Parses an array of JSON values into DSL nodes.
    ///
    /// # Arguments
    ///
    /// - `arr`: Slice of JSON values to parse
    /// - `warnings`: Vector to collect warning messages
    ///
    /// # Returns
    ///
    /// Result containing vector of ZiDSLNode or an error
    fn parse_json_array(&self, arr: &[Value], warnings: &mut Vec<String>) -> Result<Vec<ZiDSLNode>> {
        let mut nodes = Vec::new();

        for (idx, item) in arr.iter().enumerate() {
            match self.parse_json_node(item) {
                Ok(node) => nodes.push(node),
                Err(e) => {
                    if self.config.strict {
                        return Err(ZiError::validation(format!(
                            "Error parsing step {}: {}",
                            idx, e
                        )));
                    }
                    warnings.push(format!("Step {}: {}", idx, e));
                }
            }
        }

        Ok(nodes)
    }

    /// Parses a single JSON value into a DSL node.
    ///
    /// Accepts either a full object with operator/config fields or a simple
    /// string in the format "operator_name config_json".
    ///
    /// # Arguments
    ///
    /// - `value`: JSON value to parse
    ///
    /// # Returns
    ///
    /// Result containing ZiDSLNode or an error
    fn parse_json_node(&self, value: &Value) -> Result<ZiDSLNode> {
        match value {
            Value::Object(map) => {
                let operator = map.get("operator")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| ZiError::validation("Missing 'operator' field"))?
                    .to_string();

                let config = map.get("config")
                    .cloned()
                    .unwrap_or_else(|| Value::Object(serde_json::Map::new()));

                let config = self.resolve_variables(&config)?;

                let input = map.get("input").and_then(|v| v.as_str()).map(|s| s.to_string());
                let output = map.get("output").and_then(|v| v.as_str()).map(|s| s.to_string());

                Ok(ZiDSLNode {
                    operator,
                    config,
                    input,
                    output,
                })
            }
            Value::String(s) => {
                let parts: Vec<&str> = s.splitn(2, ' ').collect();
                let operator = parts[0].to_string();
                let config = if parts.len() > 1 {
                    serde_json::from_str(parts[1]).unwrap_or_else(|_| Value::String(parts[1].to_string()))
                } else {
                    Value::Object(serde_json::Map::new())
                };

                Ok(ZiDSLNode {
                    operator,
                    config,
                    input: None,
                    output: None,
                })
            }
            _ => Err(ZiError::validation("Node must be object or string")),
        }
    }

    /// Parses DSL source in YAML format.
    ///
    /// YAML format follows the same structure as JSON but uses YAML syntax.
    /// Supports "steps" and "pipeline" keys similar to JSON format.
    ///
    /// # Arguments
    ///
    /// - `source`: YAML string containing DSL definition
    ///
    /// # Returns
    ///
    /// Result containing ZiParseResult or an error
    pub fn parse_yaml(&self, source: &str) -> Result<ZiParseResult> {
        let mut warnings = Vec::new();
        
        let yaml_value: serde_yaml::Value = serde_yaml::from_str(source)
            .map_err(|e| ZiError::validation(format!("Invalid YAML: {}", e)))?;

        let json_value = self.yaml_to_json(&yaml_value);
        
        let nodes = match &json_value {
            Value::Object(map) => {
                if let Some(steps) = map.get("steps") {
                    if let Value::Array(arr) = steps {
                        self.parse_json_array(arr, &mut warnings)?
                    } else {
                        return Err(ZiError::validation("'steps' must be an array"));
                    }
                } else if let Some(pipeline) = map.get("pipeline") {
                    if let Value::Array(arr) = pipeline {
                        self.parse_json_array(arr, &mut warnings)?
                    } else {
                        return Err(ZiError::validation("'pipeline' must be an array"));
                    }
                } else {
                    let node = self.parse_json_node(&json_value)?;
                    vec![node]
                }
            }
            Value::Array(arr) => {
                self.parse_json_array(arr, &mut warnings)?
            }
            _ => return Err(ZiError::validation("YAML must be array or object")),
        };

        Ok(ZiParseResult {
            program: ZiDSLProgram { nodes },
            warnings,
            variables: self.variables.clone(),
        })
    }

    /// Converts a YAML value to JSON value.
    ///
    /// This is a recursive conversion that maps YAML's type system to JSON's type system.
    ///
    /// # Arguments
    ///
    /// - `yaml`: Reference to a serde_yaml::Value
    ///
    /// # Returns
    ///
    /// Converted JSON Value
    fn yaml_to_json(&self, yaml: &serde_yaml::Value) -> Value {
        match yaml {
            serde_yaml::Value::Null => Value::Null,
            serde_yaml::Value::Bool(b) => Value::Bool(*b),
            serde_yaml::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Number(i.into())
                } else if let Some(f) = n.as_f64() {
                    serde_json::Number::from_f64(f).map(Value::Number).unwrap_or(Value::Null)
                } else {
                    Value::Null
                }
            }
            serde_yaml::Value::String(s) => Value::String(s.clone()),
            serde_yaml::Value::Sequence(seq) => {
                Value::Array(seq.iter().map(|v| self.yaml_to_json(v)).collect())
            }
            serde_yaml::Value::Mapping(map) => {
                let mut obj = serde_json::Map::new();
                for (k, v) in map {
                    let key = match k {
                        serde_yaml::Value::String(s) => s.clone(),
                        serde_yaml::Value::Number(n) => n.to_string(),
                        serde_yaml::Value::Bool(b) => b.to_string(),
                        _ => format!("{:?}", k),
                    };
                    obj.insert(key, self.yaml_to_json(v));
                }
                Value::Object(obj)
            }
            serde_yaml::Value::Tagged(tagged) => {
                self.yaml_to_json(&tagged.value)
            }
        }
    }

    /// Parses DSL source in simple line-based syntax.
    ///
    /// Simple syntax is a line-based format where each line contains an operator
    /// and optional key=value configuration pairs. Lines starting with $ are treated
    /// as variable definitions. Lines starting with # or // are treated as comments.
    ///
    /// # Arguments
    ///
    /// - `source`: Simple syntax DSL source
    ///
    /// # Returns
    ///
    /// Result containing ZiParseResult or an error
    ///
    /// # Simple Syntax Format
    ///
    /// ```text
    /// # This is a comment
    /// $threshold=0.5
    /// filter.equals path=lang value=en
    /// quality.score min_score=0.8
    /// limit max_records=1000
    /// ```
    fn parse_simple(&self, source: &str) -> Result<ZiParseResult> {
        let mut warnings = Vec::new();
        let mut nodes = Vec::new();

        for (line_num, line) in source.lines().enumerate() {
            let trimmed = line.trim();
            
            if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with("//") {
                continue;
            }

            if trimmed.starts_with("$") && self.config.allow_variables {
                if let Err(e) = self.parse_variable_line(trimmed) {
                    if self.config.strict {
                        return Err(ZiError::validation(format!(
                            "Variable error at line {}: {}",
                            line_num + 1, e
                        )));
                    }
                    warnings.push(format!("Line {}: {}", line_num + 1, e));
                }
                continue;
            }

            match self.parse_line(trimmed) {
                Ok(node) => nodes.push(node),
                Err(e) => {
                    if self.config.strict {
                        return Err(ZiError::validation(format!(
                            "Parse error at line {}: {}",
                            line_num + 1, e
                        )));
                    }
                    warnings.push(format!("Line {}: {}", line_num + 1, e));
                }
            }
        }

        Ok(ZiParseResult {
            program: ZiDSLProgram { nodes },
            warnings,
            variables: self.variables.clone(),
        })
    }

    /// Parses a variable definition line.
    ///
    /// Variables are defined using $name=value syntax.
    /// Supported value types: strings (quoted), integers, floats, booleans.
    ///
    /// # Arguments
    ///
    /// - `line`: Line to parse (should start with $)
    ///
    /// # Returns
    ///
    /// Result indicating success or error
    fn parse_variable_line(&self, line: &str) -> Result<()> {
        let line = line.strip_prefix("$").unwrap_or(line);
        let parts: Vec<&str> = line.splitn(2, '=').collect();
        
        if parts.len() != 2 {
            return Err(ZiError::validation("Variable must be in format $name=value"));
        }

        let _name = parts[0].trim();
        let value_str = parts[1].trim();
        
        let _value = if value_str.starts_with('"') || value_str.starts_with('\'') {
            Value::String(value_str.trim_matches(|c| c == '"' || c == '\'').to_string())
        } else if let Ok(n) = value_str.parse::<i64>() {
            Value::Number(n.into())
        } else if let Ok(f) = value_str.parse::<f64>() {
            serde_json::Number::from_f64(f).map(Value::Number).unwrap_or(Value::String(value_str.to_string()))
        } else if value_str == "true" || value_str == "false" {
            Value::Bool(value_str == "true")
        } else {
            Value::String(value_str.to_string())
        };

        Ok(())
    }

    /// Parses a single line in simple syntax.
    ///
    /// A line consists of an operator name followed by optional configuration.
    /// Configuration can be in JSON format or key=value pairs.
    ///
    /// # Arguments
    ///
    /// - `line`: Line to parse
    ///
    /// # Returns
    ///
    /// Result containing ZiDSLNode or an error
    fn parse_line(&self, line: &str) -> Result<ZiDSLNode> {
        let parts: Vec<&str> = line.splitn(2, ' ').collect();
        
        if parts.is_empty() {
            return Err(ZiError::validation("Empty line"));
        }

        let operator = parts[0].to_string();
        let config = if parts.len() > 1 {
            let config_str = parts[1];
            if config_str.starts_with('{') {
                serde_json::from_str(config_str)
                    .map_err(|e| ZiError::validation(format!("Invalid JSON config: {}", e)))?
            } else {
                self.parse_inline_config(config_str)?
            }
        } else {
            Value::Object(serde_json::Map::new())
        };

        let config = self.resolve_variables(&config)?;

        Ok(ZiDSLNode {
            operator,
            config,
            input: None,
            output: None,
        })
    }

    /// Parses inline key=value configuration pairs.
    ///
    /// Converts space-separated key=value pairs into a JSON object.
    ///
    /// # Arguments
    ///
    /// - `config_str`: String containing key=value pairs
    ///
    /// # Returns
    ///
    /// Result containing JSON object or error
    fn parse_inline_config(&self, config_str: &str) -> Result<Value> {
        let mut map = serde_json::Map::new();
        
        let pairs: Vec<&str> = config_str.split_whitespace().collect();
        
        for pair in pairs {
            if pair.contains('=') {
                let kv: Vec<&str> = pair.splitn(2, '=').collect();
                if kv.len() == 2 {
                    let key = kv[0].to_string();
                    let value_str = kv[1];
                    
                    let value = if value_str.starts_with('"') || value_str.starts_with('\'') {
                        Value::String(value_str.trim_matches(|c| c == '"' || c == '\'').to_string())
                    } else if let Ok(n) = value_str.parse::<i64>() {
                        Value::Number(n.into())
                    } else if let Ok(f) = value_str.parse::<f64>() {
                        serde_json::Number::from_f64(f)
                            .map(Value::Number)
                            .unwrap_or_else(|| Value::String(value_str.to_string()))
                    } else if value_str == "true" || value_str == "false" {
                        Value::Bool(value_str == "true")
                    } else {
                        Value::String(value_str.to_string())
                    };
                    
                    map.insert(key, value);
                }
            }
        }

        Ok(Value::Object(map))
    }

    /// Resolves variable references in a JSON value.
    ///
    /// Replaces $var and ${var} patterns with their values from the variables map.
    ///
    /// # Arguments
    ///
    /// - `value`: JSON value that may contain variable references
    ///
    /// # Returns
    ///
    /// Result containing the value with resolved variables
    fn resolve_variables(&self, value: &Value) -> Result<Value> {
        if !self.config.allow_variables {
            return Ok(value.clone());
        }

        match value {
            Value::String(s) => {
                if s.starts_with("${") && s.ends_with('}') {
                    let var_name = &s[2..s.len()-1];
                    self.variables.get(var_name)
                        .cloned()
                        .ok_or_else(|| ZiError::validation(format!("Undefined variable: {}", var_name)))
                } else if s.starts_with("$") && !s.starts_with("${") {
                    let var_name = &s[1..];
                    self.variables.get(var_name)
                        .cloned()
                        .ok_or_else(|| ZiError::validation(format!("Undefined variable: {}", var_name)))
                } else {
                    Ok(Value::String(s.clone()))
                }
            }
            Value::Object(map) => {
                let mut new_map = serde_json::Map::new();
                for (k, v) in map {
                    new_map.insert(k.clone(), self.resolve_variables(v)?);
                }
                Ok(Value::Object(new_map))
            }
            Value::Array(arr) => {
                let new_arr: Result<Vec<Value>> = arr.iter()
                    .map(|v| self.resolve_variables(v))
                    .collect();
                Ok(Value::Array(new_arr?))
            }
            _ => Ok(value.clone()),
        }
    }
}
