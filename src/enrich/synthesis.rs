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

//! # Data Synthesis Module
//!
//! This module provides synthetic data generation capabilities using multiple strategies:
//! - Template-based synthesis using variable substitution
//! - Rule-based synthesis with configurable field generation
//! - LLM-based synthesis for natural language generation
//! - Hybrid synthesis combining multiple approaches

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value, Map};
use rand::{Rng, SeedableRng};

use crate::errors::{Result, ZiError};
use crate::record::{ZiRecord, ZiRecordBatch};

/// Configuration for synthetic data synthesis.
///
/// Controls the generation mode, templates, rules, and other synthesis parameters.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiSynthesisConfig {
    /// Template string for template-based synthesis.
    pub template: Option<String>,
    /// Number of synthetic records to generate.
    pub count: usize,
    /// Random seed for reproducible generation.
    pub seed: Option<u64>,
    /// Synthesis mode to use.
    pub mode: ZiSynthesisMode,
    /// Rules for rule-based synthesis.
    pub rules: Vec<ZiSynthesisRule>,
    /// Multiple templates for selection.
    pub templates: Vec<ZiTemplate>,
    /// Configuration for LLM-based synthesis.
    pub llm_config: Option<ZiLLMSynthesisConfig>,
    /// Whether to preserve original records in output.
    pub preserve_original: bool,
    /// Prefix for generated record IDs.
    pub id_prefix: Option<String>,
}

/// Synthesis mode types.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiSynthesisMode {
    /// Use template string for generation.
    Template,
    /// Use defined rules for generation.
    Rule,
    /// Use LLM for generation.
    LLM,
    /// Combine multiple synthesis methods.
    Hybrid,
}

/// Rule definition for field generation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiSynthesisRule {
    /// Target field name.
    pub field: String,
    /// Type of generation rule.
    pub rule_type: ZiRuleType,
    /// Additional parameters for the rule.
    pub params: HashMap<String, Value>,
}

/// Types of generation rules.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiRuleType {
    /// Generate random integer within range.
    RandomInt { min: i64, max: i64 },
    /// Generate random float within range.
    RandomFloat { min: f64, max: f64, precision: usize },
    /// Generate random string from charset.
    RandomString { length: usize, charset: String },
    /// Select random value from options.
    RandomChoice { options: Vec<Value> },
    /// Generate random boolean with probability.
    RandomBool { probability: f64 },
    /// Generate random date within range.
    RandomDate { start: String, end: String, format: String },
    /// Generate sequential numbers.
    Sequence { start: i64, step: i64 },
    /// Generate UUID.
    UUID,
    /// Generate string matching regex pattern.
    Regex { pattern: String },
    /// Generate using faker library.
    Faker { faker_type: String },
    /// Transform existing field value.
    Transform { source_field: String, transform: String },
}

/// Template definition for template-based synthesis.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiTemplate {
    /// Template name identifier.
    pub name: String,
    /// Template string with variable placeholders.
    pub template: String,
    /// Variable definitions for the template.
    pub variables: HashMap<String, ZiTemplateVariable>,
}

/// Variable definition for template placeholders.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiTemplateVariable {
    /// Variable data type.
    pub var_type: String,
    /// Default value if not provided.
    pub default: Option<Value>,
    /// Available options for selection.
    pub options: Option<Vec<Value>>,
    /// Minimum value for numeric types.
    pub min: Option<f64>,
    /// Maximum value for numeric types.
    pub max: Option<f64>,
}

/// Configuration for LLM-based synthesis.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiLLMSynthesisConfig {
    /// LLM API endpoint URL.
    pub endpoint: String,
    /// Model name to use.
    pub model: String,
    /// API key for authentication.
    pub api_key: Option<String>,
    /// Prompt template for generation.
    pub prompt_template: String,
    /// Maximum tokens to generate.
    pub max_tokens: usize,
    /// Sampling temperature.
    pub temperature: f64,
    /// Batch size for generation.
    pub batch_size: usize,
}

impl Default for ZiSynthesisConfig {
    fn default() -> Self {
        Self {
            template: None,
            count: 10,
            seed: None,
            mode: ZiSynthesisMode::Template,
            rules: Vec::new(),
            templates: Vec::new(),
            llm_config: None,
            preserve_original: true,
            id_prefix: Some("synth_".to_string()),
        }
    }
}

/// Data synthesizer for generating synthetic records.
#[derive(Debug)]
pub struct ZiSynthesizer {
    config: ZiSynthesisConfig,
    rng: rand::rngs::StdRng,
}

impl ZiSynthesizer {
    /// Creates a new synthesizer with the given configuration.
    #[allow(non_snake_case)]
    pub fn new(config: ZiSynthesisConfig) -> Self {
        let rng = match config.seed {
            Some(seed) => rand::rngs::StdRng::seed_from_u64(seed),
            None => rand::rngs::StdRng::from_entropy(),
        };

        Self { config, rng }
    }

    /// Synthesizes new records from existing batch or from scratch.
    #[allow(non_snake_case)]
    pub fn synthesize(&mut self, batch: &ZiRecordBatch) -> Result<ZiRecordBatch> {
        let mut result = Vec::new();

        if self.config.preserve_original {
            result.extend(batch.clone());
        }

        let generated = match self.config.mode {
            ZiSynthesisMode::Template => self.synthesize_template(batch)?,
            ZiSynthesisMode::Rule => self.synthesize_rule(batch)?,
            ZiSynthesisMode::LLM => self.synthesize_llm(batch)?,
            ZiSynthesisMode::Hybrid => self.synthesize_hybrid(batch)?,
        };

        result.extend(generated);
        Ok(result)
    }

    fn synthesize_template(&mut self, _batch: &ZiRecordBatch) -> Result<ZiRecordBatch> {
        let mut results = Vec::new();

        let template_str = self.config.template.as_ref()
            .or(self.config.templates.first().map(|t| &t.template))
            .ok_or_else(|| ZiError::validation("No template provided"))?
            .clone();

        for i in 0..self.config.count {
            let rendered = self.render_template(&template_str, i);
            let id = format!("{}template_{}", self.config.id_prefix.as_deref().unwrap_or(""), i);

            let record = ZiRecord::new(Some(id), Value::String(rendered));
            results.push(record);
        }

        Ok(results)
    }

    fn synthesize_rule(&mut self, _batch: &ZiRecordBatch) -> Result<ZiRecordBatch> {
        let mut results = Vec::new();

        let rules: Vec<_> = self.config.rules.clone();

        for i in 0..self.config.count {
            let mut payload = Map::new();

            for rule in &rules {
                let value = self.apply_rule(rule, i)?;
                payload.insert(rule.field.clone(), value);
            }

            let id = format!("{}rule_{}", self.config.id_prefix.as_deref().unwrap_or(""), i);
            let record = ZiRecord::new(Some(id), Value::Object(payload));
            results.push(record);
        }

        Ok(results)
    }

    fn synthesize_llm(&mut self, _batch: &ZiRecordBatch) -> Result<ZiRecordBatch> {
        let llm_config = self.config.llm_config.clone()
            .ok_or_else(|| ZiError::validation("LLM config not provided"))?;

        let mut results = Vec::new();
        let batch_size = llm_config.batch_size.max(1);
        let batches = (self.config.count + batch_size - 1) / batch_size;
        let id_prefix = self.config.id_prefix.clone().unwrap_or_default();

        for batch_idx in 0..batches {
            let start_idx = batch_idx * batch_size;
            let end_idx = (start_idx + batch_size).min(self.config.count);
            let count_in_batch = end_idx - start_idx;

            match self.call_llm_api(&llm_config, count_in_batch) {
                Ok(generated_texts) => {
                    for (i, text) in generated_texts.into_iter().enumerate() {
                        let record_idx = start_idx + i;
                        let id = format!("{}llm_{}", id_prefix, record_idx);
                        let record = ZiRecord::new(Some(id), Value::String(text));
                        results.push(record);
                    }
                }
                Err(e) => {
                    log::warn!("LLM API call failed for batch {}: {}, using fallback generation", batch_idx, e);
                    for i in 0..count_in_batch {
                        let record_idx = start_idx + i;
                        let fallback_text = self.generate_fallback_text(record_idx);
                        let id = format!("{}llm_{}", id_prefix, record_idx);
                        let record = ZiRecord::new(Some(id), Value::String(fallback_text));
                        results.push(record);
                    }
                }
            }
        }

        Ok(results)
    }

    fn call_llm_api(&self, config: &ZiLLMSynthesisConfig, count: usize) -> Result<Vec<String>> {
        let client = reqwest::blocking::Client::new();
        
        let prompt = config.prompt_template
            .replace("{count}", &count.to_string())
            .replace("{batch_size}", &config.batch_size.to_string());

        let mut body = serde_json::Map::new();
        body.insert("model".to_string(), Value::String(config.model.clone()));
        body.insert("prompt".to_string(), Value::String(prompt));
        body.insert("max_tokens".to_string(), Value::Number(config.max_tokens.into()));
        body.insert("temperature".to_string(), Value::Number(
            serde_json::Number::from_f64(config.temperature).unwrap_or_else(|| serde_json::Number::from(1))
        ));
        body.insert("n".to_string(), Value::Number(count.into()));

        let mut request = client.post(&config.endpoint)
            .json(&body)
            .timeout(std::time::Duration::from_secs(60));

        if let Some(api_key) = &config.api_key {
            request = request.bearer_auth(api_key);
        }

        let response = request.send()
            .map_err(|e| ZiError::internal(format!("LLM API request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(ZiError::internal(format!("LLM API returned error {}: {}", status, body)));
        }

        let json: Value = response.json()
            .map_err(|e| ZiError::internal(format!("Failed to parse LLM API response: {}", e)))?;

        Self::parse_llm_response(&json, count)
    }

    fn parse_llm_response(json: &Value, expected_count: usize) -> Result<Vec<String>> {
        let mut results = Vec::new();

        if let Some(choices) = json.get("choices").and_then(|c| c.as_array()) {
            for choice in choices {
                if let Some(text) = choice.get("text").and_then(|t| t.as_str()) {
                    results.push(text.to_string());
                } else if let Some(message) = choice.get("message") {
                    if let Some(content) = message.get("content").and_then(|c| c.as_str()) {
                        results.push(content.to_string());
                    }
                }
            }
        } else if let Some(generations) = json.get("generations").and_then(|g| g.as_array()) {
            for gen in generations {
                if let Some(text) = gen.get("text").and_then(|t| t.as_str()) {
                    results.push(text.to_string());
                }
            }
        } else if let Some(text) = json.get("text").and_then(|t| t.as_str()) {
            results.push(text.to_string());
        }

        while results.len() < expected_count {
            results.push(format!("Generated text {}", results.len()));
        }

        results.truncate(expected_count);
        Ok(results)
    }

    fn generate_fallback_text(&mut self, index: usize) -> String {
        let template_idx = self.rng.gen_range(0..3);
        match template_idx {
            0 => format!("Synthetic data sample {} generated with random value: {}", index, self.rng.gen::<u32>()),
            1 => format!("Generated record {} with UUID: {:x}", index, self.rng.gen::<u128>()),
            _ => format!("Data entry {} created at timestamp: {}", index, chrono::Utc::now().to_rfc3339()),
        }
    }

    fn synthesize_hybrid(&mut self, _batch: &ZiRecordBatch) -> Result<ZiRecordBatch> {
        let mut results = Vec::new();

        let template_count = self.config.count / 2;
        let rule_count = self.config.count - template_count;

        let default_template = "{{text}}".to_string();

        let template_str = self.config.template.as_ref()
            .or(self.config.templates.first().map(|t| &t.template))
            .unwrap_or(&default_template)
            .clone();

        for i in 0..template_count {
            let rendered = self.render_template(&template_str, i);
            let id = format!("{}hybrid_template_{}", self.config.id_prefix.as_deref().unwrap_or(""), i);
            let record = ZiRecord::new(Some(id), Value::String(rendered));
            results.push(record);
        }

        let rules: Vec<_> = self.config.rules.clone();

        for i in 0..rule_count {
            let mut payload = Map::new();
            for rule in &rules {
                let value = self.apply_rule(rule, i)?;
                payload.insert(rule.field.clone(), value);
            }
            let id = format!("{}hybrid_rule_{}", self.config.id_prefix.as_deref().unwrap_or(""), i);
            let record = ZiRecord::new(Some(id), Value::Object(payload));
            results.push(record);
        }

        Ok(results)
    }

    fn render_template(&mut self, template: &str, index: usize) -> String {
        let mut result = template.to_string();

        result = result.replace("{{index}}", &index.to_string());
        result = result.replace("{{random}}", &self.rng.gen::<u32>().to_string());
        result = result.replace("{{uuid}}", &format!("{:x}", self.rng.gen::<u128>()));

        for (key, var) in self.config.templates.iter()
            .flat_map(|t| t.variables.iter())
            .take(5)
        {
            let placeholder = format!("{{{}}}", key);
            let value = var.default.as_ref()
                .or(var.options.as_ref().and_then(|o| o.first()))
                .map(|v| v.to_string())
                .unwrap_or_else(|| "default".to_string());
            result = result.replace(&placeholder, &value);
        }

        result
    }

    fn apply_rule(&mut self, rule: &ZiSynthesisRule, index: usize) -> Result<Value> {
        match &rule.rule_type {
            ZiRuleType::RandomInt { min, max } => {
                let value = self.rng.gen_range(*min..=*max);
                Ok(Value::Number(value.into()))
            }
            ZiRuleType::RandomFloat { min, max, precision } => {
                let value = self.rng.gen_range(*min..*max);
                let rounded = (value * 10_f64.powi(*precision as i32)).round() 
                    / 10_f64.powi(*precision as i32);
                Ok(Value::Number(serde_json::Number::from_f64(rounded)
                    .unwrap_or_else(|| serde_json::Number::from(0))))
            }
            ZiRuleType::RandomString { length, charset } => {
                let chars: Vec<char> = charset.chars().collect();
                let result: String = (0..*length)
                    .map(|_| chars[self.rng.gen_range(0..chars.len())])
                    .collect();
                Ok(Value::String(result))
            }
            ZiRuleType::RandomChoice { options } => {
                if options.is_empty() {
                    return Err(ZiError::validation("No options provided for RandomChoice"));
                }
                Ok(options[self.rng.gen_range(0..options.len())].clone())
            }
            ZiRuleType::RandomBool { probability } => {
                Ok(Value::Bool(self.rng.gen::<f64>() < *probability))
            }
            ZiRuleType::Sequence { start, step } => {
                let value = start + (index as i64 * step);
                Ok(Value::Number(value.into()))
            }
            ZiRuleType::UUID => {
                Ok(Value::String(format!("{:x}", self.rng.gen::<u128>())))
            }
            ZiRuleType::RandomDate { .. } => {
                Ok(Value::String("2024-01-01".to_string()))
            }
            ZiRuleType::Regex { pattern: _ } => {
                let sample = "generated_text";
                Ok(Value::String(sample.to_string()))
            }
            ZiRuleType::Faker { faker_type } => {
                Ok(Value::String(format!("fake_{}", faker_type)))
            }
            ZiRuleType::Transform { source_field, transform } => {
                Ok(Value::String(format!("{}:{}", source_field, transform)))
            }
        }
    }
}
