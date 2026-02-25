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

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::errors::{Result, ZiError};
use crate::record::{ZiRecord, ZiRecordBatch};
use crate::operator::ZiOperator;

const CHARS_PER_CHINESE_TOKEN: f64 = 0.6;
const CHARS_PER_ENGLISH_TOKEN: f64 = 1.3;

pub fn estimate_tokens(text: &str) -> usize {
    let word_count = text.split_whitespace().count();
    let chinese_chars = text.chars().filter(|c| '\u{4e00}' <= *c && *c <= '\u{9fff}').count();
    let english_words = word_count.saturating_sub(chinese_chars / 2);
    let estimated = (chinese_chars as f64 * CHARS_PER_CHINESE_TOKEN + english_words as f64 * CHARS_PER_ENGLISH_TOKEN).ceil() as usize;
    estimated.max(1)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiTokenCountConfig {
    pub text_field: String,
    pub output_field: String,
    pub model: Option<String>,
}

impl Default for ZiTokenCountConfig {
    fn default() -> Self {
        Self {
            text_field: "payload.text".to_string(),
            output_field: "metadata.token_count".to_string(),
            model: Some("cl100k_base".to_string()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiConversationConfig {
    pub input_field: String,
    pub output_field: String,
    pub format: ZiConversationFormat,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiConversationFormat {
    ChatML,
    ShareGPT,
    Alpaca,
    OpenAI,
    Custom { system_key: String, user_key: String, assistant_key: String },
}

impl Default for ZiConversationConfig {
    fn default() -> Self {
        Self {
            input_field: "payload.conversation".to_string(),
            output_field: "payload.messages".to_string(),
            format: ZiConversationFormat::ChatML,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiContextLengthConfig {
    pub text_field: String,
    pub min_tokens: usize,
    pub max_tokens: usize,
    pub action: ZiContextLengthAction,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiContextLengthAction {
    Filter,
    Truncate { max_tokens: usize },
    Split { max_tokens: usize, overlap: usize },
}

impl Default for ZiContextLengthConfig {
    fn default() -> Self {
        Self {
            text_field: "payload.text".to_string(),
            min_tokens: 0,
            max_tokens: 8192,
            action: ZiContextLengthAction::Filter,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiQAExtractConfig {
    pub text_field: String,
    pub output_field: String,
    pub pattern: ZiQAPattern,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiQAPattern {
    Auto,
    MarkdownQA,
    NumberedQA,
    Custom { question_pattern: String, answer_pattern: String },
}

impl Default for ZiQAExtractConfig {
    fn default() -> Self {
        Self {
            text_field: "payload.text".to_string(),
            output_field: "payload.qa_pairs".to_string(),
            pattern: ZiQAPattern::Auto,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiInstructionFormatConfig {
    pub instruction_field: String,
    pub input_field: Option<String>,
    pub output_field: String,
    pub format: ZiInstructionFormat,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiInstructionFormat {
    Alpaca,
    Vicuna,
    Llama2,
    ChatML,
    Custom { template: String },
}

impl Default for ZiInstructionFormatConfig {
    fn default() -> Self {
        Self {
            instruction_field: "payload.instruction".to_string(),
            input_field: Some("payload.input".to_string()),
            output_field: "payload.formatted".to_string(),
            format: ZiInstructionFormat::Alpaca,
        }
    }
}

#[derive(Debug)]
pub struct ZiTokenCounter {
    config: ZiTokenCountConfig,
}

impl ZiTokenCounter {
    #[allow(non_snake_case)]
    pub fn new(config: ZiTokenCountConfig) -> Self {
        Self { config }
    }
}

impl ZiOperator for ZiTokenCounter {
    fn name(&self) -> &'static str {
        "llm.token_count"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        batch.into_iter().map(|record| {
            let text = self.extract_text(&record)?;
            let token_count = estimate_tokens(&text);
            self.set_token_count(record, token_count)
        }).collect()
    }
}

impl ZiTokenCounter {
    fn extract_text(&self, record: &ZiRecord) -> Result<String> {
        let parts: Vec<&str> = self.config.text_field.split('.').collect();
        
        let mut current = &record.payload;
        for part in &parts[1..] {
            match current {
                Value::Object(map) => {
                    current = map.get(*part).unwrap_or(&Value::Null);
                }
                _ => return Ok(String::new()),
            }
        }

        match current {
            Value::String(s) => Ok(s.clone()),
            Value::Object(map) => {
                Ok(map.values()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(" "))
            }
            _ => Ok(String::new()),
        }
    }

    fn set_token_count(&self, mut record: ZiRecord, count: usize) -> Result<ZiRecord> {
        let parts: Vec<&str> = self.config.output_field.split('.').collect();
        
        if parts.len() < 2 {
            return Err(ZiError::validation("Invalid output field path"));
        }

        if parts[0] == "metadata" {
            record.metadata_mut()
                .insert(parts[1].to_string(), Value::Number(count.into()));
        }

        Ok(record)
    }
}

#[allow(non_snake_case)]
pub fn token_count_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let config: ZiTokenCountConfig = serde_json::from_value(config.clone())
        .map_err(|e| ZiError::validation(format!("Invalid token count config: {}", e)))?;
    Ok(Box::new(ZiTokenCounter::new(config)))
}

#[derive(Debug)]
pub struct ZiConversationFormatter {
    config: ZiConversationConfig,
}

impl ZiConversationFormatter {
    #[allow(non_snake_case)]
    pub fn new(config: ZiConversationConfig) -> Self {
        Self { config }
    }

    fn format_conversation(&self, conv: &Value) -> Result<Vec<Value>> {
        match &self.config.format {
            ZiConversationFormat::ChatML => self.to_chatml(conv),
            ZiConversationFormat::ShareGPT => self.to_sharegpt(conv),
            ZiConversationFormat::Alpaca => self.to_alpaca(conv),
            ZiConversationFormat::OpenAI => self.to_openai(conv),
            ZiConversationFormat::Custom { system_key, user_key, assistant_key } => {
                self.to_custom(conv, system_key, user_key, assistant_key)
            }
        }
    }

    fn to_chatml(&self, conv: &Value) -> Result<Vec<Value>> {
        let mut messages = Vec::new();

        if let Value::Object(map) = conv {
            if let Some(system) = map.get("system").and_then(|s| s.as_str()) {
                messages.push(json!({
                    "role": "system",
                    "content": system
                }));
            }

            if let Some(turns) = map.get("turns").and_then(|t| t.as_array()) {
                for (i, turn) in turns.iter().enumerate() {
                    let role = if i % 2 == 0 { "user" } else { "assistant" };
                    if let Some(content) = turn.as_str() {
                        messages.push(json!({
                            "role": role,
                            "content": content
                        }));
                    }
                }
            }
        }

        Ok(messages)
    }

    fn to_sharegpt(&self, conv: &Value) -> Result<Vec<Value>> {
        let mut messages = Vec::new();

        if let Value::Object(map) = conv {
            if let Some(conversations) = map.get("conversations").and_then(|c| c.as_array()) {
                for msg in conversations {
                    if let Value::Object(msg_map) = msg {
                        let from = msg_map.get("from").and_then(|f| f.as_str()).unwrap_or("user");
                        let value = msg_map.get("value").and_then(|v| v.as_str()).unwrap_or("");
                        
                        messages.push(json!({
                            "role": from,
                            "content": value
                        }));
                    }
                }
            }
        }

        Ok(messages)
    }

    fn to_alpaca(&self, conv: &Value) -> Result<Vec<Value>> {
        let mut messages = Vec::new();

        if let Value::Object(map) = conv {
            if let Some(instruction) = map.get("instruction").and_then(|i| i.as_str()) {
                messages.push(json!({
                    "role": "user",
                    "content": instruction
                }));
            }

            if let Some(output) = map.get("output").and_then(|o| o.as_str()) {
                messages.push(json!({
                    "role": "assistant",
                    "content": output
                }));
            }
        }

        Ok(messages)
    }

    fn to_openai(&self, conv: &Value) -> Result<Vec<Value>> {
        if let Value::Array(arr) = conv {
            return Ok(arr.clone());
        }
        Ok(Vec::new())
    }

    fn to_custom(&self, conv: &Value, system_key: &str, user_key: &str, assistant_key: &str) -> Result<Vec<Value>> {
        let mut messages = Vec::new();

        if let Value::Object(map) = conv {
            if let Some(system) = map.get(system_key).and_then(|s| s.as_str()) {
                messages.push(json!({
                    "role": "system",
                    "content": system
                }));
            }

            if let Some(user) = map.get(user_key).and_then(|u| u.as_str()) {
                messages.push(json!({
                    "role": "user",
                    "content": user
                }));
            }

            if let Some(assistant) = map.get(assistant_key).and_then(|a| a.as_str()) {
                messages.push(json!({
                    "role": "assistant",
                    "content": assistant
                }));
            }
        }

        Ok(messages)
    }
}

impl ZiOperator for ZiConversationFormatter {
    fn name(&self) -> &'static str {
        "llm.conversation_format"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        batch.into_iter().map(|mut record| {
            let conv = self.extract_conversation(&record)?;
            let messages = self.format_conversation(&conv)?;
            self.set_messages(&mut record, messages)?;
            Ok(record)
        }).collect()
    }
}

impl ZiConversationFormatter {
    fn extract_conversation(&self, record: &ZiRecord) -> Result<Value> {
        let parts: Vec<&str> = self.config.input_field.split('.').collect();
        
        let mut current = &record.payload;
        for part in &parts[1..] {
            match current {
                Value::Object(map) => {
                    current = map.get(*part).unwrap_or(&Value::Null);
                }
                _ => return Ok(Value::Null),
            }
        }

        Ok(current.clone())
    }

    fn set_messages(&self, record: &mut ZiRecord, messages: Vec<Value>) -> Result<()> {
        let parts: Vec<&str> = self.config.output_field.split('.').collect();
        
        if parts.len() < 2 {
            return Err(ZiError::validation("Invalid output field path"));
        }

        if let Value::Object(ref mut map) = record.payload {
            map.insert(parts[1].to_string(), Value::Array(messages));
        }

        Ok(())
    }
}

#[allow(non_snake_case)]
pub fn conversation_format_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let config: ZiConversationConfig = serde_json::from_value(config.clone())
        .map_err(|e| ZiError::validation(format!("Invalid conversation config: {}", e)))?;
    Ok(Box::new(ZiConversationFormatter::new(config)))
}

#[derive(Debug)]
pub struct ZiContextLengthFilter {
    config: ZiContextLengthConfig,
}

impl ZiContextLengthFilter {
    #[allow(non_snake_case)]
    pub fn new(config: ZiContextLengthConfig) -> Self {
        Self { config }
    }

    fn extract_text(&self, record: &ZiRecord) -> String {
        let parts: Vec<&str> = self.config.text_field.split('.').collect();
        
        let mut current = &record.payload;
        for part in &parts[1..] {
            match current {
                Value::Object(map) => {
                    current = map.get(*part).unwrap_or(&Value::Null);
                }
                _ => return String::new(),
            }
        }

        match current {
            Value::String(s) => s.clone(),
            _ => String::new(),
        }
    }
}

impl ZiOperator for ZiContextLengthFilter {
    fn name(&self) -> &'static str {
        "llm.context_length"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        match &self.config.action {
            ZiContextLengthAction::Filter => {
                Ok(batch.into_iter()
                    .filter(|record| {
                        let text = self.extract_text(record);
                        let tokens = estimate_tokens(&text);
                        tokens >= self.config.min_tokens && tokens <= self.config.max_tokens
                    })
                    .collect())
            }
            ZiContextLengthAction::Truncate { max_tokens } => {
                batch.into_iter()
                    .map(|mut record| {
                        let text = self.extract_text(&record);
                        let tokens = estimate_tokens(&text);
                        if tokens > *max_tokens {
                            let truncated = self.truncate_text(&text, *max_tokens);
                            self.set_text(&mut record, truncated)?;
                        }
                        Ok(record)
                    })
                    .collect()
            }
            ZiContextLengthAction::Split { max_tokens, overlap } => {
                let mut results = Vec::new();
                for record in batch {
                    let text = self.extract_text(&record);
                    let tokens = estimate_tokens(&text);
                    
                    if tokens <= *max_tokens {
                        results.push(record);
                    } else {
                        let chunks = self.split_text(&text, *max_tokens, *overlap);
                        for (i, chunk) in chunks.into_iter().enumerate() {
                            let mut new_record = record.clone();
                            self.set_text(&mut new_record, chunk)?;
                            if let Some(ref mut id) = new_record.id {
                                *id = format!("{}_chunk_{}", id, i);
                            }
                            results.push(new_record);
                        }
                    }
                }
                Ok(results)
            }
        }
    }
}

impl ZiContextLengthFilter {
    fn truncate_text(&self, text: &str, max_tokens: usize) -> String {
        let chars_per_token = text.chars().count() as f64 / estimate_tokens(text) as f64;
        let max_chars = (max_tokens as f64 * chars_per_token) as usize;
        text.chars().take(max_chars).collect()
    }

    fn split_text(&self, text: &str, max_tokens: usize, overlap: usize) -> Vec<String> {
        let chars_per_token = text.chars().count() as f64 / estimate_tokens(text) as f64;
        let chunk_size = (max_tokens as f64 * chars_per_token) as usize;
        let overlap_chars = (overlap as f64 * chars_per_token) as usize;
        
        let mut chunks = Vec::new();
        let chars: Vec<char> = text.chars().collect();
        let mut start = 0;

        while start < chars.len() {
            let end = (start + chunk_size).min(chars.len());
            let chunk: String = chars[start..end].iter().collect();
            chunks.push(chunk);
            
            start = if end >= chars.len() {
                chars.len()
            } else {
                end - overlap_chars
            };
        }

        chunks
    }

    fn set_text(&self, record: &mut ZiRecord, text: String) -> Result<()> {
        let parts: Vec<&str> = self.config.text_field.split('.').collect();
        
        if parts.len() >= 2 {
            if let Value::Object(ref mut map) = record.payload {
                map.insert(parts[1].to_string(), Value::String(text));
            }
        }

        Ok(())
    }
}

#[allow(non_snake_case)]
pub fn context_length_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let config: ZiContextLengthConfig = serde_json::from_value(config.clone())
        .map_err(|e| ZiError::validation(format!("Invalid context length config: {}", e)))?;
    Ok(Box::new(ZiContextLengthFilter::new(config)))
}

#[derive(Debug)]
pub struct ZiQAExtractor {
    config: ZiQAExtractConfig,
}

impl ZiQAExtractor {
    #[allow(non_snake_case)]
    pub fn new(config: ZiQAExtractConfig) -> Self {
        Self { config }
    }

    fn extract_qa_pairs(&self, text: &str) -> Vec<Value> {
        match &self.config.pattern {
            ZiQAPattern::Auto => {
                if self.looks_like_markdown_qa(text) {
                    self.extract_markdown_qa(text)
                } else if self.looks_like_numbered_qa(text) {
                    self.extract_numbered_qa(text)
                } else {
                    self.extract_heuristic_qa(text)
                }
            }
            ZiQAPattern::MarkdownQA => self.extract_markdown_qa(text),
            ZiQAPattern::NumberedQA => self.extract_numbered_qa(text),
            ZiQAPattern::Custom { question_pattern, answer_pattern } => {
                self.extract_custom_qa(text, question_pattern, answer_pattern)
            }
        }
    }

    fn looks_like_markdown_qa(&self, text: &str) -> bool {
        text.contains("## Q") || text.contains("## Question") || text.contains("**Q:**")
    }

    fn looks_like_numbered_qa(&self, text: &str) -> bool {
        text.contains("Q1:") || text.contains("Question 1:") || text.contains("1. ")
    }

    fn extract_markdown_qa(&self, text: &str) -> Vec<Value> {
        let mut pairs = Vec::new();
        let re = regex::Regex::new(r"##\s*[Qq]uestion[:\s]+(.*?)(?=##\s*[Aa]nswer|$)")
            .ok();
        let re_ans = regex::Regex::new(r"##\s*[Aa]nswer[:\s]+(.*?)(?=##\s*[Qq]uestion|$)")
            .ok();

        if let (Some(q_re), Some(a_re)) = (re, re_ans) {
            let questions: Vec<&str> = q_re.find_iter(text).map(|m| m.as_str()).collect();
            let answers: Vec<&str> = a_re.find_iter(text).map(|m| m.as_str()).collect();

            for (q, a) in questions.iter().zip(answers.iter()) {
                pairs.push(json!({
                    "question": q.trim(),
                    "answer": a.trim()
                }));
            }
        }

        pairs
    }

    fn extract_numbered_qa(&self, text: &str) -> Vec<Value> {
        let mut pairs = Vec::new();
        let re = regex::Regex::new(r"[Qq](\d+)[:\s]+(.*?)(?=[Aa]\d+[:\s]|$)")
            .ok();
        let re_ans = regex::Regex::new(r"[Aa](\d+)[:\s]+(.*?)(?=[Qq]\d+[:\s]|$)")
            .ok();

        if let (Some(q_re), Some(a_re)) = (re, re_ans) {
            let questions: Vec<(usize, &str)> = q_re.captures_iter(text)
                .filter_map(|c| {
                    let num: usize = c.get(1)?.as_str().parse().ok()?;
                    let text = c.get(2)?.as_str();
                    Some((num, text))
                })
                .collect();

            let answers: Vec<(usize, &str)> = a_re.captures_iter(text)
                .filter_map(|c| {
                    let num: usize = c.get(1)?.as_str().parse().ok()?;
                    let text = c.get(2)?.as_str();
                    Some((num, text))
                })
                .collect();

            for (q_num, q_text) in questions {
                if let Some((_, a_text)) = answers.iter().find(|(a_num, _)| *a_num == q_num) {
                    pairs.push(json!({
                        "question": q_text.trim(),
                        "answer": a_text.trim()
                    }));
                }
            }
        }

        pairs
    }

    fn extract_heuristic_qa(&self, text: &str) -> Vec<Value> {
        let mut pairs = Vec::new();
        let lines: Vec<&str> = text.lines().collect();
        let mut current_q = String::new();
        let mut current_a = String::new();
        let mut in_answer = false;

        for line in lines {
            let trimmed = line.trim();
            if trimmed.starts_with("Q:") || trimmed.starts_with("Question:") {
                if !current_q.is_empty() && !current_a.is_empty() {
                    pairs.push(json!({
                        "question": current_q.trim(),
                        "answer": current_a.trim()
                    }));
                }
                current_q = trimmed[trimmed.find(':').map(|i| i + 1).unwrap_or(0)..].to_string();
                current_a = String::new();
                in_answer = false;
            } else if trimmed.starts_with("A:") || trimmed.starts_with("Answer:") {
                current_a = trimmed[trimmed.find(':').map(|i| i + 1).unwrap_or(0)..].to_string();
                in_answer = true;
            } else if in_answer {
                current_a.push_str(" ");
                current_a.push_str(trimmed);
            } else {
                current_q.push_str(" ");
                current_q.push_str(trimmed);
            }
        }

        if !current_q.is_empty() && !current_a.is_empty() {
            pairs.push(json!({
                "question": current_q.trim(),
                "answer": current_a.trim()
            }));
        }

        pairs
    }

    fn extract_custom_qa(&self, text: &str, q_pattern: &str, a_pattern: &str) -> Vec<Value> {
        let mut pairs = Vec::new();
        
        let q_re = regex::Regex::new(q_pattern).ok();
        let a_re = regex::Regex::new(a_pattern).ok();

        if let (Some(qr), Some(ar)) = (q_re, a_re) {
            let questions: Vec<&str> = qr.find_iter(text).map(|m| m.as_str()).collect();
            let answers: Vec<&str> = ar.find_iter(text).map(|m| m.as_str()).collect();

            for (q, a) in questions.iter().zip(answers.iter()) {
                pairs.push(json!({
                    "question": q.trim(),
                    "answer": a.trim()
                }));
            }
        }

        pairs
    }
}

impl ZiOperator for ZiQAExtractor {
    fn name(&self) -> &'static str {
        "llm.qa_extract"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        batch.into_iter().map(|mut record| {
            let text = self.extract_text(&record);
            let qa_pairs = self.extract_qa_pairs(&text);
            self.set_qa_pairs(&mut record, qa_pairs)?;
            Ok(record)
        }).collect()
    }
}

impl ZiQAExtractor {
    fn extract_text(&self, record: &ZiRecord) -> String {
        let parts: Vec<&str> = self.config.text_field.split('.').collect();
        
        let mut current = &record.payload;
        for part in &parts[1..] {
            match current {
                Value::Object(map) => {
                    current = map.get(*part).unwrap_or(&Value::Null);
                }
                _ => return String::new(),
            }
        }

        match current {
            Value::String(s) => s.clone(),
            _ => String::new(),
        }
    }

    fn set_qa_pairs(&self, record: &mut ZiRecord, pairs: Vec<Value>) -> Result<()> {
        let parts: Vec<&str> = self.config.output_field.split('.').collect();
        
        if parts.len() >= 2 {
            if let Value::Object(ref mut map) = record.payload {
                map.insert(parts[1].to_string(), Value::Array(pairs));
            }
        }

        Ok(())
    }
}

#[allow(non_snake_case)]
pub fn q_a_extract_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let config: ZiQAExtractConfig = serde_json::from_value(config.clone())
        .map_err(|e| ZiError::validation(format!("Invalid QA extract config: {}", e)))?;
    Ok(Box::new(ZiQAExtractor::new(config)))
}

#[derive(Debug)]
pub struct ZiInstructionFormatter {
    config: ZiInstructionFormatConfig,
}

impl ZiInstructionFormatter {
    #[allow(non_snake_case)]
    pub fn new(config: ZiInstructionFormatConfig) -> Self {
        Self { config }
    }

    fn format_instruction(&self, record: &ZiRecord) -> Result<String> {
        let instruction = self.extract_field(record, &self.config.instruction_field);
        let input = self.config.input_field.as_ref()
            .map(|f| self.extract_field(record, f))
            .unwrap_or_default();

        match &self.config.format {
            ZiInstructionFormat::Alpaca => {
                if input.is_empty() {
                    Ok(format!(
                        "### Instruction:\n{}\n\n### Response:\n",
                        instruction
                    ))
                } else {
                    Ok(format!(
                        "### Instruction:\n{}\n\n### Input:\n{}\n\n### Response:\n",
                        instruction, input
                    ))
                }
            }
            ZiInstructionFormat::Vicuna => {
                Ok(format!(
                    "A chat between a curious user and an artificial intelligence assistant. The assistant gives helpful, detailed, and polite answers to the user's questions.\n\nUSER: {}\nASSISTANT:",
                    instruction
                ))
            }
            ZiInstructionFormat::Llama2 => {
                Ok(format!(
                    "<s>[INST] {} [/INST]",
                    instruction
                ))
            }
            ZiInstructionFormat::ChatML => {
                if input.is_empty() {
                    Ok(format!(
                        "<|im_start|>user\n{}<|im_end|>\n<|im_start|>assistant\n",
                        instruction
                    ))
                } else {
                    Ok(format!(
                        "<|im_start|>user\n{}\n\n{}<|im_end|>\n<|im_start|>assistant\n",
                        instruction, input
                    ))
                }
            }
            ZiInstructionFormat::Custom { template } => {
                let mut result = template.clone();
                result = result.replace("{{instruction}}", &instruction);
                result = result.replace("{{input}}", &input);
                Ok(result)
            }
        }
    }

    fn extract_field(&self, record: &ZiRecord, field: &str) -> String {
        let parts: Vec<&str> = field.split('.').collect();
        
        let mut current = &record.payload;
        for part in &parts[1..] {
            match current {
                Value::Object(map) => {
                    current = map.get(*part).unwrap_or(&Value::Null);
                }
                _ => return String::new(),
            }
        }

        match current {
            Value::String(s) => s.clone(),
            _ => String::new(),
        }
    }
}

impl ZiOperator for ZiInstructionFormatter {
    fn name(&self) -> &'static str {
        "llm.instruction_format"
    }

    fn apply(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        batch.into_iter().map(|mut record| {
            let formatted = self.format_instruction(&record)?;
            self.set_formatted(&mut record, formatted)?;
            Ok(record)
        }).collect()
    }
}

impl ZiInstructionFormatter {
    fn set_formatted(&self, record: &mut ZiRecord, text: String) -> Result<()> {
        let parts: Vec<&str> = self.config.output_field.split('.').collect();
        
        if parts.len() >= 2 {
            if let Value::Object(ref mut map) = record.payload {
                map.insert(parts[1].to_string(), Value::String(text));
            }
        }

        Ok(())
    }
}

#[allow(non_snake_case)]
pub fn instruction_format_factory(config: &Value) -> Result<Box<dyn ZiOperator + Send + Sync>> {
    let config: ZiInstructionFormatConfig = serde_json::from_value(config.clone())
        .map_err(|e| ZiError::validation(format!("Invalid instruction format config: {}", e)))?;
    Ok(Box::new(ZiInstructionFormatter::new(config)))
}
