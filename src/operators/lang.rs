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

use serde_json::{Number, Value};

use crate::errors::{Result, ZiError};
use crate::operator::ZiCOperator;
use crate::operators::filter::ZiCFieldPath;
use crate::record::ZiCRecordBatch;

#[derive(Debug)]
pub struct ZiCLangDetect {
    path: ZiCFieldPath,
    target_key: String,
}

impl ZiCLangDetect {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, target_key: String) -> Self {
        Self { path, target_key }
    }

    fn detect_iso(text: &str) -> &'static str {
        let (latin, cjk, arabic, cyrillic, total) = script_counts(text);
        if total == 0 {
            return "und";
        }
        let max = latin.max(cjk).max(arabic).max(cyrillic);
        if max == cjk {
            "zh"
        } else if max == arabic {
            "ar"
        } else if max == cyrillic {
            "ru"
        } else {
            "en"
        }
    }
}

fn script_counts(text: &str) -> (usize, usize, usize, usize, usize) {
    let mut latin = 0usize;
    let mut cjk = 0usize;
    let mut arabic = 0usize;
    let mut cyrillic = 0usize;
    let mut total = 0usize;
    for ch in text.chars() {
        total += 1;
        let code = ch as u32;
        if code <= 0x007F {
            latin += 1;
        } else if (0x4E00..=0x9FFF).contains(&code)
            || (0x3400..=0x4DBF).contains(&code)
            || (0xF900..=0xFAFF).contains(&code)
        {
            cjk += 1;
        } else if (0x0600..=0x06FF).contains(&code)
            || (0x0750..=0x077F).contains(&code)
            || (0x08A0..=0x08FF).contains(&code)
        {
            arabic += 1;
        } else if (0x0400..=0x04FF).contains(&code) || (0x0500..=0x052F).contains(&code) {
            cyrillic += 1;
        }
    }
    (latin, cjk, arabic, cyrillic, total)
}

impl ZiCOperator for ZiCLangDetect {
    fn name(&self) -> &'static str {
        "lang.detect"
    }

    fn apply(&self, mut batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        for record in &mut batch {
            if let Some(Value::String(text)) = self.path.ZiFResolve(record) {
                let iso = Self::detect_iso(text);
                record
                    .ZiFMetadataMut()
                    .insert(self.target_key.clone(), Value::String(iso.to_string()));
            }
        }
        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn ZiFLangDetectFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("lang.detect config must be object"))?;
    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("lang.detect requires string 'path'"))?;
    let key = obj
        .get("key")
        .and_then(Value::as_str)
        .unwrap_or("lang")
        .to_string();
    let field_path = ZiCFieldPath::ZiFParse(path)?;
    Ok(Box::new(ZiCLangDetect::ZiFNew(field_path, key)))
}

#[derive(Debug)]
pub struct ZiCLangConfidence {
    path: ZiCFieldPath,
    target_key: String,
}

impl ZiCLangConfidence {
    #[allow(non_snake_case)]
    pub fn ZiFNew(path: ZiCFieldPath, target_key: String) -> Self {
        Self { path, target_key }
    }
}

impl ZiCOperator for ZiCLangConfidence {
    fn name(&self) -> &'static str {
        "lang.confidence"
    }

    fn apply(&self, mut batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        for record in &mut batch {
            if let Some(Value::String(text)) = self.path.ZiFResolve(record) {
                let (latin, cjk, arabic, cyrillic, total) = script_counts(text);
                if total == 0 {
                    continue;
                }
                let dominant = latin.max(cjk).max(arabic).max(cyrillic) as f64;
                let confidence = (dominant / total as f64).clamp(0.0, 1.0);
                let number = Number::from_f64(confidence).unwrap_or_else(|| Number::from(0));
                record
                    .ZiFMetadataMut()
                    .insert(self.target_key.clone(), Value::Number(number));
            }
        }
        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn ZiFLangConfidenceFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("lang.confidence config must be object"))?;

    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("lang.confidence requires string 'path'"))?;
    let key = obj
        .get("key")
        .and_then(Value::as_str)
        .unwrap_or("lang_confidence")
        .to_string();

    let field_path = ZiCFieldPath::ZiFParse(path)?;
    Ok(Box::new(ZiCLangConfidence::ZiFNew(field_path, key)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::ZiCRecord;
    use serde_json::json;

    #[test]
    fn detect_lang_adds_iso_code() {
        let op = ZiCLangDetect::ZiFNew(
            ZiCFieldPath::ZiFParse("payload.text").unwrap(),
            "lang".into(),
        );
        let batch = vec![
            ZiCRecord::ZiFNew(None, json!({"text": "hello world"})),
            ZiCRecord::ZiFNew(None, json!({"text": "你好世界"})),
        ];
        let out = op.apply(batch).unwrap();
        assert_eq!(out[0].metadata.as_ref().unwrap()["lang"], json!("en"));
        assert_eq!(out[1].metadata.as_ref().unwrap()["lang"], json!("zh"));
    }

    #[test]
    fn confidence_scores_dominant_script() {
        let op = ZiCLangConfidence::ZiFNew(
            ZiCFieldPath::ZiFParse("payload.text").unwrap(),
            "conf".into(),
        );
        let batch = vec![ZiCRecord::ZiFNew(
            None,
            json!({"text": "hello world привет"}),
        )];
        let out = op.apply(batch).unwrap();
        let confidence = out[0].metadata.as_ref().unwrap()["conf"].as_f64().unwrap();
        assert!(confidence > 0.5);
        assert!(confidence <= 1.0);
    }
}
