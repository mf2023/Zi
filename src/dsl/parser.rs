//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
//!
//! This file is part of Zi.
//! The Zi project belongs to the Dunimd project team.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
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

use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::dsl::ir::{ZiCDSLNode, ZiCDSLProgram};

#[derive(Clone, Debug)]
pub struct ZiCParseResult {
    pub program: ZiCDSLProgram,
    pub warnings: Vec<String>,
    pub variables: HashMap<String, Value>,
}

#[derive(Clone, Debug)]
pub struct ZiCDSLParserConfig {
    pub strict: bool,
    pub allow_variables: bool,
    pub allow_expressions: bool,
    pub default_input_field: String,
    pub default_output_field: String,
}

impl Default for ZiCDSLParserConfig {
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

#[derive(Debug, Default)]
pub struct ZiCDSLParser {
    config: ZiCDSLParserConfig,
    variables: HashMap<String, Value>,
}

impl ZiCDSLParser {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        Self {
            config: ZiCDSLParserConfig::default(),
            variables: HashMap::new(),
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFWithConfig(mut self, config: ZiCDSLParserConfig) -> Self {
        self.config = config;
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFStrict(mut self, strict: bool) -> Self {
        self.config.strict = strict;
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFSetVariable(mut self, name: &str, value: Value) -> Self {
        self.variables.insert(name.to_string(), value);
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFParse(&self, source: &str) -> Result<ZiCParseResult> {
        let trimmed = source.trim();
        
        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            self.parse_json(source)
        } else if trimmed.starts_with("steps:") || trimmed.starts_with("pipeline:") || trimmed.starts_with("- operator:") {
            self.parse_yaml(source)
        } else {
            self.parse_simple(source)
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFParseJson(&self, source: &str) -> Result<ZiCParseResult> {
        self.parse_json(source)
    }

    #[allow(non_snake_case)]
    pub fn ZiFParseYaml(&self, source: &str) -> Result<ZiCParseResult> {
        self.parse_yaml(source)
    }

    #[allow(non_snake_case)]
    pub fn ZiFParseFile(&self, path: &std::path::Path) -> Result<ZiCParseResult> {
        let content = std::fs::read_to_string(path)?;
        
        let extension = path.extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        match extension.as_str() {
            "json" => self.parse_json(&content),
            "yaml" | "yml" => self.parse_yaml(&content),
            _ => self.ZiFParse(&content),
        }
    }

    fn parse_json(&self, source: &str) -> Result<ZiCParseResult> {
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

        Ok(ZiCParseResult {
            program: ZiCDSLProgram { nodes },
            warnings,
            variables: self.variables.clone(),
        })
    }

    fn parse_json_array(&self, arr: &[Value], warnings: &mut Vec<String>) -> Result<Vec<ZiCDSLNode>> {
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

    fn parse_json_node(&self, value: &Value) -> Result<ZiCDSLNode> {
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

                Ok(ZiCDSLNode {
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

                Ok(ZiCDSLNode {
                    operator,
                    config,
                    input: None,
                    output: None,
                })
            }
            _ => Err(ZiError::validation("Node must be object or string")),
        }
    }

    fn parse_yaml(&self, source: &str) -> Result<ZiCParseResult> {
        let mut warnings = Vec::new();
        
        let yaml_value: serde_yaml::Value = serde_yaml::from_str(source)
            .map_err(|e| ZiError::validation(format!("Invalid YAML: {}", e)))?;

        let json_value = self.yaml_to_json(&yaml_value);
        
        let nodes = match json_value {
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
                self.parse_json_array(&arr, &mut warnings)?
            }
            _ => return Err(ZiError::validation("YAML must be array or object")),
        };

        Ok(ZiCParseResult {
            program: ZiCDSLProgram { nodes },
            warnings,
            variables: self.variables.clone(),
        })
    }

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
                        _ => k.to_string(),
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

    fn parse_simple(&self, source: &str) -> Result<ZiCParseResult> {
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

        Ok(ZiCParseResult {
            program: ZiCDSLProgram { nodes },
            warnings,
            variables: self.variables.clone(),
        })
    }

    fn parse_variable_line(&self, line: &str) -> Result<()> {
        let line = line.strip_prefix("$").unwrap_or(line);
        let parts: Vec<&str> = line.splitn(2, '=').collect();
        
        if parts.len() != 2 {
            return Err(ZiError::validation("Variable must be in format $name=value"));
        }

        let name = parts[0].trim();
        let value_str = parts[1].trim();
        
        let value = if value_str.starts_with('"') || value_str.starts_with('\'') {
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

    fn parse_line(&self, line: &str) -> Result<ZiCDSLNode> {
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

        Ok(ZiCDSLNode {
            operator,
            config,
            input: None,
            output: None,
        })
    }

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
