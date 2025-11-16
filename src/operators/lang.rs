//! Copyright © 2025 Wenze Wei. All Rights Reserved.
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
use std::collections::HashMap;

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
        let cleaned = text.trim();
        if cleaned.is_empty() {
            return "und";
        }

        let (latin, cjk, arabic, cyrillic, devanagari, total) = script_counts(cleaned);
        if total == 0 {
            return "und";
        }

        let script_hint = ScriptScores {
            latin,
            cjk,
            arabic,
            cyrillic,
            devanagari,
            total,
        };

        let trigram_scores = trigram_scores(cleaned, &script_hint);
        let ascii_ratio = latin as f64 / total as f64;
        trigram_scores.best_iso(ascii_ratio)
    }
}

struct ScriptScores {
    latin: usize,
    cjk: usize,
    arabic: usize,
    cyrillic: usize,
    devanagari: usize,
    total: usize,
}

impl ScriptScores {
    fn normalized(&self, value: usize) -> f64 {
        value as f64 / self.total.max(1) as f64
    }
}

#[derive(Clone)]
struct LanguageProfile {
    iso: &'static str,
    trigrams: &'static [(&'static str, f64)],
    script_weight: fn(&ScriptScores) -> f64,
}

fn trigram_profiles() -> &'static [LanguageProfile] {
    #[allow(dead_code)]
    static EN: &[(&str, f64)] = &[
        (" the", 1.0),
        ("and", 1.0),
        ("ing", 1.0),
        ("ion", 1.0),
        ("ent", 1.0),
        ("tha", 1.0),
    ];
    static ZH: &[(&str, f64)] = &[
        ("的", 1.2),
        ("是", 1.2),
        ("在", 1.2),
        ("人", 1.2),
        ("和", 1.2),
        ("有", 1.2),
    ];
    static AR: &[(&str, f64)] = &[
        (" ال", 1.1),
        ("من", 1.1),
        ("في", 1.1),
        ("على", 1.1),
        ("ال", 1.1),
        ("لا", 1.1),
    ];
    static RU: &[(&str, f64)] = &[
        (" про", 1.0),
        ("ост", 1.0),
        ("ени", 1.0),
        ("ого", 1.0),
        ("ать", 1.0),
        ("ове", 1.0),
    ];
    static ES: &[(&str, f64)] = &[
        (" de", 1.0),
        ("que", 1.0),
        ("ción", 1.0),
        ("los", 1.0),
        (" por", 1.0),
        ("las", 1.0),
    ];
    static FR: &[(&str, f64)] = &[
        (" de", 1.0),
        ("ent", 1.0),
        ("ion", 1.0),
        ("que", 1.0),
        (" les", 1.0),
        (" pour", 1.0),
    ];
    static HI: &[(&str, f64)] = &[
        (" के", 1.1),
        ("यह", 1.1),
        ("में", 1.1),
        ("और", 1.1),
        ("है", 1.1),
        ("से", 1.1),
    ];

    static PROFILES: &[LanguageProfile] = &[
        LanguageProfile {
            iso: "en",
            trigrams: EN,
            script_weight: |scores| scores.normalized(scores.latin),
        },
        LanguageProfile {
            iso: "zh",
            trigrams: ZH,
            script_weight: |scores| scores.normalized(scores.cjk),
        },
        LanguageProfile {
            iso: "ar",
            trigrams: AR,
            script_weight: |scores| scores.normalized(scores.arabic),
        },
        LanguageProfile {
            iso: "ru",
            trigrams: RU,
            script_weight: |scores| scores.normalized(scores.cyrillic),
        },
        LanguageProfile {
            iso: "es",
            trigrams: ES,
            script_weight: |scores| scores.normalized(scores.latin),
        },
        LanguageProfile {
            iso: "fr",
            trigrams: FR,
            script_weight: |scores| scores.normalized(scores.latin),
        },
        LanguageProfile {
            iso: "hi",
            trigrams: HI,
            script_weight: |scores| scores.normalized(scores.devanagari),
        },
    ];
    PROFILES
}

struct ScoreBoard {
    entries: Vec<(&'static str, f64)>,
}

impl ScoreBoard {
    fn best_iso(&self, ascii_ratio: f64) -> &'static str {
        let mut best_iso = "und";
        let mut best_score = f64::MIN;

        for (iso, score) in &self.entries {
            if score > &best_score {
                best_score = *score;
                best_iso = *iso;
            }
        }

        if best_score <= 0.15 {
            return if ascii_ratio > 0.9 { "en" } else { "und" };
        }

        if ascii_ratio > 0.9 {
            if let Some(english_score) = self
                .entries
                .iter()
                .find(|(iso, _)| *iso == "en")
                .map(|(_, score)| *score)
            {
                if english_score + 0.1 >= best_score {
                    return "en";
                }
            }

            if best_iso != "en" && best_score <= 0.25 {
                return "en";
            }
        }

        best_iso
    }
}

fn trigram_scores(text: &str, scripts: &ScriptScores) -> ScoreBoard {
    let lowered = text.to_lowercase();
    let mut counts: HashMap<String, usize> = HashMap::new();
    let chars: Vec<char> = lowered.chars().collect();
    for window in chars.windows(3) {
        let trigram: String = window.iter().collect();
        *counts.entry(trigram).or_insert(0) += 1;
    }

    let total = counts.values().sum::<usize>().max(1) as f64;
    let entries: Vec<(&'static str, f64)> = trigram_profiles()
        .iter()
        .map(|profile| {
            let mut score = 0.0f64;
            for (gram, weight) in profile.trigrams.iter() {
                if let Some(count) = counts.get(*gram) {
                    score += (*count as f64 / total) * weight;
                }
            }
            score += (profile.script_weight)(scripts) * 0.8;
            (profile.iso, score)
        })
        .collect();

    ScoreBoard { entries }
}

fn script_counts(text: &str) -> (usize, usize, usize, usize, usize, usize) {
    let mut latin = 0usize;
    let mut cjk = 0usize;
    let mut arabic = 0usize;
    let mut cyrillic = 0usize;
    let mut devanagari = 0usize;
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
        } else if (0x0900..=0x097F).contains(&code) {
            devanagari += 1;
        }
    }
    (latin, cjk, arabic, cyrillic, devanagari, total)
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
                let (latin, cjk, arabic, cyrillic, devanagari, total) = script_counts(text);
                if total == 0 {
                    continue;
                }
                let dominant = latin.max(cjk).max(arabic).max(cyrillic).max(devanagari) as f64;
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
