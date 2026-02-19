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

use std::collections::{hash_map::DefaultHasher, HashSet};
use std::hash::{Hash, Hasher};

use regex::{Captures, Regex};
use serde_json::{Map, Value};

use crate::errors::{Result, ZiError};
use crate::operator::ZiCOperator;
use crate::operators::filter::ZiCFieldPath;
use crate::record::ZiCRecordBatch;

#[derive(Debug, Clone)]
pub struct ZiCPiiRule {
    tag: String,
    pattern: Regex,
    strategy: ZiCPiiStrategy,
    context_window: usize,
    store_original: bool,
}

#[derive(Debug, Clone)]
pub enum ZiCPiiStrategy {
    Placeholder(String),
    Mask {
        mask_char: char,
        prefix: usize,
        suffix: usize,
    },
    Hash {
        salt: u64,
        prefix: usize,
        suffix: usize,
    },
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ZiCPiiMatch {
    pub tag: String,
    pub original: Option<String>,
    pub redacted: String,
    pub strategy: String,
    pub context: Option<String>,
}

#[derive(Debug)]
pub struct ZiCPiiRedact {
    path: ZiCFieldPath,
    rules: Vec<ZiCPiiRule>,
    store_map_key: Option<String>,
    allowlist: HashSet<String>,
}

impl ZiCPiiRedact {
    #[allow(non_snake_case)]
    pub fn ZiFNew(
        path: ZiCFieldPath,
        rules: Vec<ZiCPiiRule>,
        store_map_key: Option<String>,
        allowlist: HashSet<String>,
    ) -> Self {
        Self {
            path,
            rules,
            store_map_key,
            allowlist,
        }
    }

    fn redact_text(&self, text: &str) -> (String, Vec<ZiCPiiMatch>) {
        let mut output = text.to_string();
        let mut matches = Vec::new();
        for rule in &self.rules {
            let mut replacements = Vec::new();
            for caps in rule.pattern.captures_iter(&output) {
                if let Some(full) = caps.get(0) {
                    let original = full.as_str();
                    let normalized = original.trim().to_ascii_lowercase();
                    if self.allowlist.contains(&normalized)
                        || normalized
                            .split_whitespace()
                            .any(|token| self.allowlist.contains(token))
                    {
                        continue;
                    }
                    let redacted = match &rule.strategy {
                        ZiCPiiStrategy::Placeholder(token) => token.clone(),
                        ZiCPiiStrategy::Mask {
                            mask_char,
                            prefix,
                            suffix,
                        } => _mask_value(original, *mask_char, *prefix, *suffix),
                        ZiCPiiStrategy::Hash {
                            salt,
                            prefix,
                            suffix,
                        } => _hash_value(original, *salt, *prefix, *suffix),
                    };
                    let context = if rule.context_window > 0 {
                        Some(_extract_window(&caps, text, rule.context_window))
                    } else {
                        None
                    };
                    matches.push(ZiCPiiMatch {
                        tag: rule.tag.clone(),
                        original: rule.store_original.then(|| original.to_string()),
                        redacted: redacted.clone(),
                        strategy: _strategy_name(&rule.strategy),
                        context,
                    });
                    replacements.push((original.to_string(), redacted));
                }
            }
            for (from, to) in replacements {
                output = output.replace(&from, &to);
            }
        }
        (output, matches)
    }
}

impl ZiCOperator for ZiCPiiRedact {
    fn name(&self) -> &'static str {
        "pii.redact"
    }

    fn apply(&self, mut batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        for record in &mut batch {
            let Some(Value::String(text)) = self.path.ZiFResolve(record) else {
                continue;
            };
            let (redacted, matches) = self.redact_text(text);
            let _ = self
                .path
                .ZiFSetValue(record, Value::String(redacted.clone()));
            if let Some(key) = &self.store_map_key {
                if !matches.is_empty() {
                    let entries: Vec<Value> = matches
                        .iter()
                        .map(|m| {
                            let mut obj = Map::new();
                            obj.insert("tag".into(), Value::String(m.tag.clone()));
                            obj.insert("redacted".into(), Value::String(m.redacted.clone()));
                            obj.insert("strategy".into(), Value::String(m.strategy.clone()));
                            if let Some(context) = &m.context {
                                obj.insert("context".into(), Value::String(context.clone()));
                            }
                            if let Some(original) = &m.original {
                                obj.insert("original".into(), Value::String(original.clone()));
                            }
                            Value::Object(obj)
                        })
                        .collect();
                    let metadata = record.ZiFMetadataMut();
                    let target = metadata
                        .entry(key.clone())
                        .or_insert_with(|| Value::Array(Vec::new()));
                    if let Some(array) = target.as_array_mut() {
                        array.extend(entries);
                    } else {
                        *target = Value::Array(entries);
                    }
                }
            }
        }
        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn ZiFPiiRedactFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("pii.redact config must be object"))?;
    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("pii.redact requires string 'path'"))?;
    let store_key = obj
        .get("store_key")
        .and_then(Value::as_str)
        .map(str::to_string);
    let mut rules = _default_rules();
    if let Some(arr) = obj.get("custom").and_then(Value::as_array) {
        for entry in arr {
            rules.push(_parse_rule(entry)?);
        }
    }
    let allowlist = obj
        .get("allowlist")
        .and_then(Value::as_array)
        .map(|arr| _parse_allowlist(arr))
        .unwrap_or_default();
    let field_path = ZiCFieldPath::ZiFParse(path)?;
    Ok(Box::new(ZiCPiiRedact::ZiFNew(
        field_path, rules, store_key, allowlist,
    )))
}

fn _mask_value(input: &str, mask_char: char, prefix: usize, suffix: usize) -> String {
    if input.len() <= prefix + suffix {
        return mask_char.to_string().repeat(input.len());
    }
    let mut out = String::with_capacity(input.len());
    out.push_str(&input[..prefix.min(input.len())]);
    let mask_len = input.len().saturating_sub(prefix + suffix).min(4).max(1);
    out.push_str(&mask_char.to_string().repeat(mask_len));
    if suffix > 0 {
        out.push_str(&input[input.len() - suffix..]);
    }
    out
}

fn _hash_value(input: &str, salt: u64, prefix: usize, suffix: usize) -> String {
    let mut hasher = DefaultHasher::new();
    salt.hash(&mut hasher);
    input.hash(&mut hasher);
    let digest = format!("{:016x}", hasher.finish());
    let prefix_len = prefix.min(input.len());
    let suffix_len = suffix.min(input.len().saturating_sub(prefix_len));
    let prefix_part = &input[..prefix_len];
    let suffix_part = &input[input.len().saturating_sub(suffix_len)..];
    let middle_start = prefix_len;
    let middle_end = input.len().saturating_sub(suffix_len);
    let middle = if middle_end > middle_start {
        &input[middle_start..middle_end]
    } else {
        ""
    };

    let middle_chars: Vec<char> = middle.chars().collect();
    let mut preserved_prefix = String::new();
    let mut preserved_suffix = String::new();

    let mut start = 0usize;
    let mut end = middle_chars.len();

    while start < end && !middle_chars[start].is_alphanumeric() {
        preserved_prefix.push(middle_chars[start]);
        start += 1;
    }

    while end > start && !middle_chars[end - 1].is_alphanumeric() {
        preserved_suffix.insert(0, middle_chars[end - 1]);
        end -= 1;
    }

    let mut out = String::with_capacity(
        prefix_len
            + suffix_len
            + digest.len()
            + 2
            + preserved_prefix.len()
            + preserved_suffix.len(),
    );
    out.push_str(prefix_part);
    out.push_str(&preserved_prefix);
    out.push('<');
    out.push_str(&digest);
    out.push('>');
    out.push_str(&preserved_suffix);
    out.push_str(suffix_part);
    out
}

fn _extract_window(capture: &Captures, text: &str, window: usize) -> String {
    if let Some(mat) = capture.get(0) {
        let start = mat.start().saturating_sub(window);
        let end = (mat.end() + window).min(text.len());
        text[start..end].to_string()
    } else {
        String::new()
    }
}

fn _strategy_name(strategy: &ZiCPiiStrategy) -> String {
    match strategy {
        ZiCPiiStrategy::Placeholder(_) => "placeholder".into(),
        ZiCPiiStrategy::Mask { .. } => "mask".into(),
        ZiCPiiStrategy::Hash { .. } => "hash".into(),
    }
}

fn _default_rules() -> Vec<ZiCPiiRule> {
    vec![
        ZiCPiiRule {
            tag: "email".into(),
            pattern: Regex::new(r"(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}").unwrap(),
            strategy: ZiCPiiStrategy::Placeholder("<EMAIL>".into()),
            context_window: 12,
            store_original: true,
        },
        ZiCPiiRule {
            tag: "phone".into(),
            pattern: Regex::new(r"(?i)\b(?:\+?\d{1,3}[ -]?)?(?:\(?\d{2,4}\)?[ -]?)?\d{7,12}\b")
                .unwrap(),
            strategy: ZiCPiiStrategy::Mask {
                mask_char: '*',
                prefix: 2,
                suffix: 2,
            },
            context_window: 6,
            store_original: false,
        },
        ZiCPiiRule {
            tag: "card".into(),
            pattern: Regex::new(r"(?i)\b(?:\d[ -]?){13,19}\b").unwrap(),
            strategy: ZiCPiiStrategy::Mask {
                mask_char: '#',
                prefix: 4,
                suffix: 2,
            },
            context_window: 4,
            store_original: false,
        },
        ZiCPiiRule {
            tag: "url".into(),
            pattern: Regex::new(r"(?i)https?://[\w.-/?#%=&]+").unwrap(),
            strategy: ZiCPiiStrategy::Placeholder("<URL>".into()),
            context_window: 16,
            store_original: false,
        },
    ]
}

fn _parse_rule(value: &Value) -> Result<ZiCPiiRule> {
    let obj = value
        .as_object()
        .ok_or_else(|| ZiError::validation("pii rule must be object"))?;
    let tag = obj
        .get("tag")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("pii rule requires 'tag'"))?
        .to_string();
    let pattern = obj
        .get("pattern")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("pii rule requires 'pattern'"))?;
    let strategy = obj
        .get("strategy")
        .and_then(Value::as_str)
        .unwrap_or("placeholder");
    let placeholder = obj.get("placeholder").and_then(Value::as_str);
    let mask_char = obj
        .get("mask_char")
        .and_then(Value::as_str)
        .and_then(|s| s.chars().next())
        .unwrap_or('*');
    let prefix = obj.get("prefix").and_then(Value::as_u64).unwrap_or(0) as usize;
    let suffix = obj.get("suffix").and_then(Value::as_u64).unwrap_or(0) as usize;
    let salt = obj.get("salt").and_then(Value::as_u64).unwrap_or(0);
    let context_window = obj
        .get("context_window")
        .and_then(Value::as_u64)
        .unwrap_or(0) as usize;
    let store_original = obj
        .get("store_original")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let regex = Regex::new(pattern)
        .map_err(|err| ZiError::validation(format!("invalid pii pattern '{pattern}': {err}")))?;
    let strategy = match strategy {
        "placeholder" => ZiCPiiStrategy::Placeholder(
            placeholder
                .map(str::to_string)
                .unwrap_or_else(|| format!("<{}>", tag.to_ascii_uppercase())),
        ),
        "mask" => ZiCPiiStrategy::Mask {
            mask_char,
            prefix,
            suffix,
        },
        "hash" => ZiCPiiStrategy::Hash {
            salt,
            prefix,
            suffix,
        },
        other => {
            return Err(ZiError::validation(format!(
                "unknown pii strategy '{other}', expected placeholder|mask|hash"
            )))
        }
    };
    Ok(ZiCPiiRule {
        tag,
        pattern: regex,
        strategy,
        context_window,
        store_original,
    })
}

fn _parse_allowlist(values: &[Value]) -> HashSet<String> {
    let mut allowlist = HashSet::new();
    for value in values {
        if let Some(raw) = value.as_str() {
            let normalized = raw.trim().to_ascii_lowercase();
            if normalized.is_empty() {
                continue;
            }
            allowlist.insert(normalized.clone());
            for token in normalized.split_whitespace() {
                allowlist.insert(token.to_string());
            }
        }
    }
    allowlist
}

