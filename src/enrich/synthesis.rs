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

use serde::{Deserialize, Serialize};
use serde_json::{Value, Map};
use rand::Rng;

use crate::errors::{Result, ZiError};
use crate::record::{ZiCRecord, ZiCRecordBatch};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCSynthesisConfig {
    pub template: Option<String>,
    pub count: usize,
    pub seed: Option<u64>,
    pub mode: ZiCSynthesisMode,
    pub rules: Vec<ZiCSynthesisRule>,
    pub templates: Vec<ZiCTemplate>,
    pub llm_config: Option<ZiCLLMSynthesisConfig>,
    pub preserve_original: bool,
    pub id_prefix: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiCSynthesisMode {
    Template,
    Rule,
    LLM,
    Hybrid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCSynthesisRule {
    pub field: String,
    pub rule_type: ZiCRuleType,
    pub params: HashMap<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiCRuleType {
    RandomInt { min: i64, max: i64 },
    RandomFloat { min: f64, max: f64, precision: usize },
    RandomString { length: usize, charset: String },
    RandomChoice { options: Vec<Value> },
    RandomBool { probability: f64 },
    RandomDate { start: String, end: String, format: String },
    Sequence { start: i64, step: i64 },
    UUID,
    Regex { pattern: String },
    Faker { faker_type: String },
    Transform { source_field: String, transform: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCTemplate {
    pub name: String,
    pub template: String,
    pub variables: HashMap<String, ZiCTemplateVariable>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCTemplateVariable {
    pub var_type: String,
    pub default: Option<Value>,
    pub options: Option<Vec<Value>>,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCLLMSynthesisConfig {
    pub endpoint: String,
    pub model: String,
    pub api_key: Option<String>,
    pub prompt_template: String,
    pub max_tokens: usize,
    pub temperature: f64,
    pub batch_size: usize,
}

impl Default for ZiCSynthesisConfig {
    fn default() -> Self {
        Self {
            template: None,
            count: 1,
            seed: None,
            mode: ZiCSynthesisMode::Template,
            rules: Vec::new(),
            templates: Vec::new(),
            llm_config: None,
            preserve_original: true,
            id_prefix: None,
        }
    }
}

#[derive(Debug)]
pub struct ZiCSynthesizer {
    config: ZiCSynthesisConfig,
    rng: rand::rngs::StdRng,
}

impl ZiCSynthesizer {
    #[allow(non_snake_case)]
    pub fn ZiFNew(config: ZiCSynthesisConfig) -> Self {
        use rand::SeedableRng;
        
        let rng = match config.seed {
            Some(seed) => rand::rngs::StdRng::seed_from_u64(seed),
            None => rand::rngs::StdRng::from_entropy(),
        };

        Self { config, rng }
    }

    #[allow(non_snake_case)]
    pub fn ZiFSynthesize(&mut self, batch: &ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        let mut synthesized = Vec::new();

        if self.config.preserve_original {
            synthesized.extend(batch.iter().cloned());
        }

        match &self.config.mode {
            ZiCSynthesisMode::Template => {
                let generated = self.synthesize_template(batch)?;
                synthesized.extend(generated);
            }
            ZiCSynthesisMode::Rule => {
                let generated = self.synthesize_rules(batch)?;
                synthesized.extend(generated);
            }
            ZiCSynthesisMode::LLM => {
                let generated = self.synthesize_llm(batch)?;
                synthesized.extend(generated);
            }
            ZiCSynthesisMode::Hybrid => {
                let generated = self.synthesize_hybrid(batch)?;
                synthesized.extend(generated);
            }
        }

        Ok(synthesized)
    }

    fn synthesize_template(&mut self, batch: &ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        let mut result = Vec::new();

        for record in batch {
            for i in 0..self.config.count {
                let new_record = self.apply_template(record, i)?;
                result.push(new_record);
            }
        }

        Ok(result)
    }

    fn apply_template(&mut self, record: &ZiCRecord, index: usize) -> Result<ZiCRecord> {
        let mut new_record = record.clone();
        
        if let Some(id) = &record.id {
            let prefix = self.config.id_prefix.as_ref().map(|p| format!("{}_", p)).unwrap_or_default();
            new_record.id = Some(format!("{}{}synth_{}", prefix, id, index));
        }

        if let Some(template_str) = self.config.template.clone() {
            let rendered = self.render_template(&template_str, record)?;
            if let Value::Object(ref mut map) = new_record.payload {
                map.insert("_synthesized".to_string(), Value::String(rendered));
            }
        }

        let templates = self.config.templates.clone();
        for template in templates {
            let rendered = self.render_template(&template.template, record)?;
            if let Value::Object(ref mut map) = new_record.payload {
                map.insert(template.name.clone(), Value::String(rendered));
            }
        }

        new_record.ZiFMetadataMut()
            .insert("synthesized".to_string(), Value::Bool(true));
        new_record.ZiFMetadataMut()
            .insert("synthesis_index".to_string(), Value::Number(index.into()));
        new_record.ZiFMetadataMut()
            .insert("synthesis_mode".to_string(), Value::String("template".to_string()));

        Ok(new_record)
    }

    fn render_template(&mut self, template: &str, record: &ZiCRecord) -> Result<String> {
        let mut result = template.to_string();

        let var_pattern = regex::Regex::new(r"\{\{(\w+(?:\.\w+)*)\}\}").unwrap();
        
        for cap in var_pattern.captures_iter(template) {
            let var_path = &cap[1];
            let value = self.resolve_variable(var_path, record)?;
            result = result.replace(&cap[0], &value);
        }

        let func_pattern = regex::Regex::new(r"\$\{(\w+)\(([^)]*)\)\}").unwrap();
        
        for cap in func_pattern.captures_iter(template) {
            let func_name = &cap[1];
            let args = &cap[2];
            let value = self.evaluate_function(func_name, args)?;
            result = result.replace(&cap[0], &value);
        }

        Ok(result)
    }

    fn resolve_variable(&self, path: &str, record: &ZiCRecord) -> Result<String> {
        let parts: Vec<&str> = path.split('.').collect();
        
        if parts.is_empty() {
            return Ok(String::new());
        }

        let value = match parts[0] {
            "id" => {
                Value::String(record.id.clone().unwrap_or_default())
            }
            "payload" => {
                if parts.len() > 1 {
                    self.get_nested_value(&record.payload, &parts[1..])?
                } else {
                    record.payload.clone()
                }
            }
            "metadata" => {
                if parts.len() > 1 {
                    if let Some(meta) = &record.metadata {
                        self.get_nested_value(&Value::Object(meta.clone()), &parts[1..])?
                    } else {
                        Value::Null
                    }
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        };

        Ok(self.value_to_string(&value))
    }

    fn get_nested_value(&self, value: &Value, path: &[&str]) -> Result<Value> {
        let mut current = value;
        
        for part in path {
            match current {
                Value::Object(map) => {
                    current = map.get(*part).unwrap_or(&Value::Null);
                }
                Value::Array(arr) => {
                    if let Ok(idx) = part.parse::<usize>() {
                        current = arr.get(idx).unwrap_or(&Value::Null);
                    } else {
                        return Ok(Value::Null);
                    }
                }
                _ => return Ok(Value::Null),
            }
        }

        Ok(current.clone())
    }

    fn value_to_string(&self, value: &Value) -> String {
        match value {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => String::new(),
            _ => value.to_string(),
        }
    }

    fn evaluate_function(&mut self, func_name: &str, args: &str) -> Result<String> {
        use rand::Rng;

        match func_name {
            "random_int" => {
                let parts: Vec<&str> = args.split(',').collect();
                let min: i64 = parts.get(0).and_then(|s| s.trim().parse().ok()).unwrap_or(0);
                let max: i64 = parts.get(1).and_then(|s| s.trim().parse().ok()).unwrap_or(100);
                Ok(self.rng.gen_range(min..=max).to_string())
            }
            "random_float" => {
                let parts: Vec<&str> = args.split(',').collect();
                let min: f64 = parts.get(0).and_then(|s| s.trim().parse().ok()).unwrap_or(0.0);
                let max: f64 = parts.get(1).and_then(|s| s.trim().parse().ok()).unwrap_or(1.0);
                Ok(self.rng.gen_range(min..=max).to_string())
            }
            "random_string" => {
                let length: usize = args.trim().parse().unwrap_or(10);
                const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
                let s: String = (0..length)
                    .map(|_| {
                        let idx = self.rng.gen_range(0..CHARSET.len());
                        CHARSET[idx] as char
                    })
                    .collect();
                Ok(s)
            }
            "uuid" => {
                use rand::Rng;
                let rng = &mut self.rng;
                let uuid = format!(
                    "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                    rng.gen::<u32>(),
                    rng.gen::<u16>(),
                    (rng.gen::<u16>() & 0x0fff) | 0x4000,
                    (rng.gen::<u16>() & 0x3fff) | 0x8000,
                    rng.gen::<u64>() & 0xffffffffffff
                );
                Ok(uuid)
            }
            "timestamp" => {
                use std::time::{SystemTime, UNIX_EPOCH};
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                Ok(timestamp.to_string())
            }
            "increment" => {
                let start: i64 = args.trim().parse().unwrap_or(0);
                static COUNTER: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
                let val = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + start;
                Ok(val.to_string())
            }
            _ => Err(ZiError::validation(format!("Unknown function: {}", func_name))),
        }
    }

    fn synthesize_rules(&mut self, batch: &ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        let mut result = Vec::new();

        for record in batch {
            for i in 0..self.config.count {
                let new_record = self.apply_rules(record, i)?;
                result.push(new_record);
            }
        }

        Ok(result)
    }

    fn apply_rules(&mut self, record: &ZiCRecord, index: usize) -> Result<ZiCRecord> {
        let mut new_record = record.clone();
        
        if let Some(id) = &record.id {
            let prefix = self.config.id_prefix.as_ref().map(|p| format!("{}_", p)).unwrap_or_default();
            new_record.id = Some(format!("{}{}rule_{}", prefix, id, index));
        }

        let rules = self.config.rules.clone();
        for rule in rules {
            let value = self.apply_rule(&rule)?;
            self.set_field(&mut new_record, &rule.field, value)?;
        }

        new_record.ZiFMetadataMut()
            .insert("synthesized".to_string(), Value::Bool(true));
        new_record.ZiFMetadataMut()
            .insert("synthesis_mode".to_string(), Value::String("rule".to_string()));

        Ok(new_record)
    }

    fn apply_rule(&mut self, rule: &ZiCSynthesisRule) -> Result<Value> {
        use rand::Rng;

        match &rule.rule_type {
            ZiCRuleType::RandomInt { min, max } => {
                let val: i64 = self.rng.gen_range(*min..=*max);
                Ok(Value::Number(val.into()))
            }
            ZiCRuleType::RandomFloat { min, max, precision } => {
                let val = self.rng.gen_range(*min..=*max);
                let rounded = (val * 10f64.powi(*precision as i32)).round() / 10f64.powi(*precision as i32);
                serde_json::Number::from_f64(rounded)
                    .map(Value::Number)
                    .ok_or_else(|| ZiError::validation("Invalid float value"))
            }
            ZiCRuleType::RandomString { length, charset } => {
                let chars: Vec<char> = charset.chars().collect();
                if chars.is_empty() {
                    return Ok(Value::String(String::new()));
                }
                let s: String = (0..*length)
                    .map(|_| chars[self.rng.gen_range(0..chars.len())])
                    .collect();
                Ok(Value::String(s))
            }
            ZiCRuleType::RandomChoice { options } => {
                if options.is_empty() {
                    return Ok(Value::Null);
                }
                Ok(options[self.rng.gen_range(0..options.len())].clone())
            }
            ZiCRuleType::RandomBool { probability } => {
                Ok(Value::Bool(self.rng.gen::<f64>() < *probability))
            }
            ZiCRuleType::RandomDate { start, end, format } => {
                self.generate_random_date(start, end, format)
            }
            ZiCRuleType::Sequence { start, step } => {
                static COUNTER: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
                let val = COUNTER.fetch_add(*step, std::sync::atomic::Ordering::SeqCst) + start;
                Ok(Value::Number(val.into()))
            }
            ZiCRuleType::UUID => {
                let uuid = format!(
                    "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                    self.rng.gen::<u32>(),
                    self.rng.gen::<u16>(),
                    (self.rng.gen::<u16>() & 0x0fff) | 0x4000,
                    (self.rng.gen::<u16>() & 0x3fff) | 0x8000,
                    self.rng.gen::<u64>() & 0xffffffffffff
                );
                Ok(Value::String(uuid))
            }
            ZiCRuleType::Regex { pattern } => {
                self.generate_from_regex(pattern)
            }
            ZiCRuleType::Faker { faker_type } => {
                self.generate_faker_value(faker_type)
            }
            ZiCRuleType::Transform { source_field, transform } => {
                Ok(Value::String(format!("transformed_{}_{}", source_field, transform)))
            }
        }
    }

    fn generate_random_date(&self, start: &str, end: &str, format: &str) -> Result<Value> {
        use chrono::{DateTime, NaiveDateTime};

        let start_dt = NaiveDateTime::parse_from_str(start, format)
            .map_err(|e| ZiError::validation(format!("Invalid start date: {}", e)))?;
        let end_dt = NaiveDateTime::parse_from_str(end, format)
            .map_err(|e| ZiError::validation(format!("Invalid end date: {}", e)))?;

        let start_ts = start_dt.and_utc().timestamp();
        let end_ts = end_dt.and_utc().timestamp();

        let random_ts = start_ts + (rand::random::<i64>() % (end_ts - start_ts).abs());
        let random_dt = DateTime::from_timestamp(random_ts, 0)
            .ok_or_else(|| ZiError::validation("Invalid timestamp"))?;

        Ok(Value::String(random_dt.format(format).to_string()))
    }

    fn generate_from_regex(&mut self, pattern: &str) -> Result<Value> {
        let mut result = String::new();
        let chars: Vec<char> = pattern.chars().collect();
        let mut i = 0;

        while i < chars.len() {
            if chars[i] == '\\' && i + 1 < chars.len() {
                i += 1;
                match chars[i] {
                    'd' => result.push((self.rng.gen_range(0..10) + '0' as u8) as char),
                    'w' => {
                        const WORD_CHARS: &[char] = &[
                            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                            'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
                            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
                            'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
                            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '_',
                        ];
                        result.push(WORD_CHARS[self.rng.gen_range(0..WORD_CHARS.len())]);
                    }
                    's' => {
                        const SPACE_CHARS: &[char] = &[' ', '\t', '\n'];
                        result.push(SPACE_CHARS[self.rng.gen_range(0..SPACE_CHARS.len())]);
                    }
                    c => result.push(c),
                }
            } else if chars[i] == '.' {
                result.push((self.rng.gen_range(32..127) as u8) as char);
            } else if chars[i] == '[' && i + 1 < chars.len() {
                let mut char_class = String::new();
                i += 1;
                while i < chars.len() && chars[i] != ']' {
                    char_class.push(chars[i]);
                    i += 1;
                }
                if !char_class.is_empty() {
                    let class_chars: Vec<char> = char_class.chars().collect();
                    result.push(class_chars[self.rng.gen_range(0..class_chars.len())]);
                }
            } else if chars[i] == '{' {
                let mut count_str = String::new();
                i += 1;
                while i < chars.len() && chars[i] != '}' {
                    count_str.push(chars[i]);
                    i += 1;
                }
                if let Ok(count) = count_str.parse::<usize>() {
                    if !result.is_empty() {
                        let last_char = result.chars().last().unwrap();
                        for _ in 1..count {
                            result.push(last_char);
                        }
                    }
                }
            } else if chars[i] == '*' || chars[i] == '+' || chars[i] == '?' {
            } else {
                result.push(chars[i]);
            }
            i += 1;
        }

        Ok(Value::String(result))
    }

    fn generate_faker_value(&mut self, faker_type: &str) -> Result<Value> {
        use rand::Rng;
        use rand::seq::SliceRandom;

        match faker_type {
            "name" | "first_name" => {
                const FIRST_NAMES: &[&str] = &[
                    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
                    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
                    "Thomas", "Sarah", "Charles", "Karen", "Wei", "Ming", "Xiaoming", "Lihua",
                ];
                Ok(Value::String(FIRST_NAMES[self.rng.gen_range(0..FIRST_NAMES.len())].to_string()))
            }
            "last_name" => {
                const LAST_NAMES: &[&str] = &[
                    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
                    "Rodriguez", "Martinez", "Wang", "Li", "Zhang", "Liu", "Chen", "Yang",
                ];
                Ok(Value::String(LAST_NAMES[self.rng.gen_range(0..LAST_NAMES.len())].to_string()))
            }
            "email" => {
                const DOMAINS: &[&str] = &["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "example.com"];
                let user: String = (0..8)
                    .map(|_| (self.rng.gen_range(97..123) as u8) as char)
                    .collect();
                let domain = DOMAINS[self.rng.gen_range(0..DOMAINS.len())];
                Ok(Value::String(format!("{}@{}", user, domain)))
            }
            "phone" => {
                Ok(Value::String(format!(
                    "+1-{}-{}-{}",
                    self.rng.gen_range(200..999),
                    self.rng.gen_range(100..999),
                    self.rng.gen_range(1000..9999)
                )))
            }
            "address" => {
                Ok(Value::String(format!(
                    "{} {} Street, City, State {}",
                    self.rng.gen_range(100..9999),
                    ["Main", "Oak", "Pine", "Maple", "Cedar", "Elm"][self.rng.gen_range(0..6)],
                    self.rng.gen_range(10000..99999)
                )))
            }
            "company" => {
                const COMPANIES: &[&str] = &[
                    "TechCorp", "DataSystems", "CloudNet", "AI Solutions", "Digital Innovations",
                    "Smart Analytics", "Future Tech", "Global Systems", "NextGen", "Quantum Labs",
                ];
                Ok(Value::String(COMPANIES[self.rng.gen_range(0..COMPANIES.len())].to_string()))
            }
            "sentence" => {
                const WORDS: &[&str] = &[
                    "the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
                    "data", "processing", "machine", "learning", "artificial", "intelligence",
                ];
                let count = self.rng.gen_range(5..10);
                let sentence: String = WORDS
                    .choose_multiple(&mut self.rng, count)
                    .map(|w| w.to_string())
                    .collect::<Vec<_>>()
                    .join(" ");
                Ok(Value::String(format!("{}{}.", sentence.chars().next().unwrap().to_uppercase(), &sentence[1..])))
            }
            "paragraph" => {
                let mut paragraphs = Vec::new();
                for _ in 0..self.rng.gen_range(2..5) {
                    const WORDS: &[&str] = &[
                        "the", "quick", "brown", "fox", "data", "processing", "analysis",
                        "machine", "learning", "model", "training", "inference", "optimization",
                    ];
                    let count = self.rng.gen_range(8..15);
                    let sentence: String = WORDS
                        .choose_multiple(&mut self.rng, count)
                        .map(|w| w.to_string())
                        .collect::<Vec<_>>()
                        .join(" ");
                    paragraphs.push(format!("{}{}.", sentence.chars().next().unwrap().to_uppercase(), &sentence[1..]));
                }
                Ok(Value::String(paragraphs.join(" ")))
            }
            _ => Err(ZiError::validation(format!("Unknown faker type: {}", faker_type))),
        }
    }

    fn set_field(&self, record: &mut ZiCRecord, path: &str, value: Value) -> Result<()> {
        let parts: Vec<&str> = path.split('.').collect();
        
        if parts.is_empty() {
            return Ok(());
        }

        match parts[0] {
            "payload" => {
                if parts.len() == 1 {
                    record.payload = value;
                } else if let Value::Object(ref mut map) = record.payload {
                    self.set_nested_value(map, &parts[1..], value)?;
                }
            }
            "metadata" => {
                if parts.len() == 1 {
                    if let Value::Object(map) = value {
                        record.metadata = Some(map);
                    }
                } else {
                    let meta = record.ZiFMetadataMut();
                    self.set_nested_value(meta, &parts[1..], value)?;
                }
            }
            "id" => {
                if let Value::String(s) = value {
                    record.id = Some(s);
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn set_nested_value(&self, map: &mut Map<String, Value>, path: &[&str], value: Value) -> Result<()> {
        if path.is_empty() {
            return Ok(());
        }

        if path.len() == 1 {
            map.insert(path[0].to_string(), value);
            return Ok(());
        }

        let entry = map.entry(path[0].to_string()).or_insert_with(|| Value::Object(Map::new()));
        
        if let Value::Object(inner_map) = entry {
            self.set_nested_value(inner_map, &path[1..], value)?;
        }

        Ok(())
    }

    fn synthesize_llm(&self, batch: &ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        let config = match &self.config.llm_config {
            Some(c) => c,
            None => return Err(ZiError::validation("LLM config not provided")),
        };

        let mut result = Vec::new();

        for record in batch {
            for i in 0..self.config.count {
                let new_record = self.create_llm_record(record, i, config)?;
                result.push(new_record);
            }
        }

        Ok(result)
    }

    fn create_llm_record(&self, record: &ZiCRecord, index: usize, config: &ZiCLLMSynthesisConfig) -> Result<ZiCRecord> {
        let mut new_record = record.clone();
        
        if let Some(id) = &record.id {
            let prefix = self.config.id_prefix.as_ref().map(|p| format!("{}_", p)).unwrap_or_default();
            new_record.id = Some(format!("{}{}llm_{}", prefix, id, index));
        }

        let prompt = self.render_llm_prompt(record, &config.prompt_template)?;
        
        if let Value::Object(ref mut map) = new_record.payload {
            map.insert("_llm_prompt".to_string(), Value::String(prompt));
            map.insert("_llm_model".to_string(), Value::String(config.model.clone()));
            map.insert("_llm_endpoint".to_string(), Value::String(config.endpoint.clone()));
        }

        new_record.ZiFMetadataMut()
            .insert("synthesized".to_string(), Value::Bool(true));
        new_record.ZiFMetadataMut()
            .insert("synthesis_mode".to_string(), Value::String("llm".to_string()));
        new_record.ZiFMetadataMut()
            .insert("llm_pending".to_string(), Value::Bool(true));

        Ok(new_record)
    }

    fn render_llm_prompt(&self, record: &ZiCRecord, template: &str) -> Result<String> {
        let mut result = template.to_string();

        if let Some(id) = &record.id {
            result = result.replace("{{id}}", id);
        }

        if let Value::Object(ref map) = record.payload {
            for (key, value) in map {
                let placeholder = format!("{{{{payload.{}}}}}", key);
                result = result.replace(&placeholder, &self.value_to_string(value));
            }
        }

        Ok(result)
    }

    fn synthesize_hybrid(&mut self, batch: &ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        let mut result = Vec::new();
        let rules = self.config.rules.clone();

        for record in batch {
            for i in 0..self.config.count {
                let mut new_record = self.apply_template(record, i)?;
                
                for rule in &rules {
                    let value = self.apply_rule(rule)?;
                    self.set_field(&mut new_record, &rule.field, value)?;
                }

                new_record.ZiFMetadataMut()
                    .insert("synthesis_mode".to_string(), Value::String("hybrid".to_string()));
                
                result.push(new_record);
            }
        }

        Ok(result)
    }
}
