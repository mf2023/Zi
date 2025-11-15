//! Copyright © 2025 Dunimd Team. All Rights Reserved.
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
use crate::operator::ZiCOperator;
use crate::operators::filter::ZiCFieldPath;
use crate::record::ZiCRecordBatch;

#[derive(Debug)]
pub struct ZiCPiiRedact {
    path: ZiCFieldPath,
    rules: Vec<(Regex, String)>,
    store_map_key: Option<String>,
}

impl ZiCPiiRedact {
    #[allow(non_snake_case)]
    pub fn ZiFNew(
        path: ZiCFieldPath,
        rules: Vec<(Regex, String)>,
        store_map_key: Option<String>,
    ) -> Self {
        Self {
            path,
            rules,
            store_map_key,
        }
    }

    fn redact_text(&self, text: &str) -> (String, Vec<(String, String)>) {
        let mut out = text.to_string();
        let mut mappings = Vec::new();
        for (re, placeholder) in &self.rules {
            let mut caps = re
                .captures_iter(&out)
                .map(|c| c.get(0).map(|m| m.as_str().to_string()))
                .flatten()
                .collect::<Vec<_>>();
            caps.dedup();
            for m in caps {
                mappings.push((m.clone(), placeholder.clone()));
                out = out.replace(&m, placeholder);
            }
        }
        (out, mappings)
    }
}

impl ZiCOperator for ZiCPiiRedact {
    fn name(&self) -> &'static str {
        "pii.redact"
    }

    fn apply(&self, mut batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        for record in &mut batch {
            if let Some(Value::String(text)) = self.path.ZiFResolve(record) {
                let (redacted, map) = self.redact_text(text);
                let _ = self
                    .path
                    .ZiFSetValue(record, Value::String(redacted.clone()));
                if let Some(key) = &self.store_map_key {
                    let value = Value::Array(
                        map.into_iter()
                            .map(|(from, to)| {
                                let mut obj = serde_json::Map::new();
                                obj.insert("from".into(), Value::String(from));
                                obj.insert("to".into(), Value::String(to));
                                Value::Object(obj)
                            })
                            .collect(),
                    );
                    record.ZiFMetadataMut().insert(key.clone(), value);
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
        .map(|s| s.to_string());
    // default rules
    let mut rules: Vec<(Regex, String)> = vec![
        (
            Regex::new(r"(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}").unwrap(),
            "<EMAIL>".into(),
        ),
        (
            Regex::new(r"(?i)\b(?:\+?\d{1,3}[ -]?)?(?:\(?\d{2,4}\)?[ -]?)?\d{7,12}\b").unwrap(),
            "<PHONE>".into(),
        ),
        (
            Regex::new(r"(?i)\b(?:\d[ -]?){13,19}\b").unwrap(),
            "<CARD>".into(),
        ),
        (
            Regex::new(r"(?i)https?://[\w.-/?#%=&]+").unwrap(),
            "<URL>".into(),
        ),
    ];
    // additional custom regexes
    if let Some(arr) = obj.get("custom").and_then(Value::as_array) {
        for v in arr {
            if let (Some(pat), Some(tag)) = (
                v.get("pattern").and_then(Value::as_str),
                v.get("tag").and_then(Value::as_str),
            ) {
                let re = Regex::new(pat)
                    .map_err(|e| ZiError::validation(format!("invalid pii pattern: {e}")))?;
                rules.push((re, format!("<{}>", tag.to_uppercase())));
            }
        }
    }
    let field_path = ZiCFieldPath::ZiFParse(path)?;
    Ok(Box::new(ZiCPiiRedact::ZiFNew(field_path, rules, store_key)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::ZiCRecord;
    use serde_json::json;

    #[test]
    fn redact_email_and_phone() {
        let op = ZiCPiiRedact::ZiFNew(
            ZiCFieldPath::ZiFParse("payload.text").unwrap(),
            vec![
                (
                    Regex::new(r"[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}").unwrap(),
                    "<EMAIL>".into(),
                ),
                (Regex::new(r"\b\d{11}\b").unwrap(), "<PHONE>".into()),
            ],
            Some("pii".into()),
        );
        let batch = vec![ZiCRecord::ZiFNew(
            None,
            json!({"text": "mail me at a@b.com, phone 13800138000"}),
        )];
        let out = op.apply(batch).unwrap();
        let text = out[0].payload["text"].as_str().unwrap();
        assert!(text.contains("<EMAIL>"));
        assert!(text.contains("<PHONE>"));
        assert!(out[0].metadata.as_ref().unwrap().get("pii").is_some());
    }
}
