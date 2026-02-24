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

//! # Python Bindings Module
//!
//! This module provides Python bindings for Zi using PyO3, enabling:
//! - Core data structures (ZiRecord)
//! - High-level text processing APIs
//! - Operator execution from Python
//! - Runtime metrics collection
//!
//! ## Usage Example
//!
//! ```python
//! import zi
//!
//! # Create records
//! record = zi.ZiRecord(id="1", payload='{"text": "Hello"}')
//!
//! # Process text
//! processor = zi.ZiTextProcessor()
//! lang, confidence = processor.detect_language("Hello world")
//!
//! # Execute operators
//! op = zi.ZiOperator("filter.equals", '{"field": "status", "value": "active"}')
//! results = op.apply([record])
//! ```

#[cfg(feature = "pyo3")]
pub mod py {
    use pyo3::prelude::*;
    use pyo3::types::PyList;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    use crate::record::ZiRecord;
    use crate::operator::ZiOperator;

    static GLOBAL_START_TIME: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    static GLOBAL_TOTAL_RECORDS: AtomicU64 = AtomicU64::new(0);
    static GLOBAL_TOTAL_BATCHES: AtomicU64 = AtomicU64::new(0);
    static GLOBAL_TOTAL_ERRORS: AtomicU64 = AtomicU64::new(0);
    static GLOBAL_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
    static GLOBAL_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);

    fn get_start_time() -> Instant {
        *GLOBAL_START_TIME.get_or_init(Instant::now)
    }

    fn record_batch_processed(record_count: u64, error_count: u64) {
        GLOBAL_TOTAL_BATCHES.fetch_add(1, Ordering::Relaxed);
        GLOBAL_TOTAL_RECORDS.fetch_add(record_count, Ordering::Relaxed);
        GLOBAL_TOTAL_ERRORS.fetch_add(error_count, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    fn record_cache_hit() {
        GLOBAL_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    fn record_cache_miss() {
        GLOBAL_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
    }

    fn get_metrics_snapshot() -> (u64, u64, u64, u64, f64, f64) {
        let uptime = get_start_time().elapsed().as_secs();
        let total_records = GLOBAL_TOTAL_RECORDS.load(Ordering::Relaxed);
        let total_batches = GLOBAL_TOTAL_BATCHES.load(Ordering::Relaxed);
        let total_errors = GLOBAL_TOTAL_ERRORS.load(Ordering::Relaxed);
        
        let hits = GLOBAL_CACHE_HITS.load(Ordering::Relaxed);
        let misses = GLOBAL_CACHE_MISSES.load(Ordering::Relaxed);
        let cache_hit_rate = if hits + misses > 0 {
            hits as f64 / (hits + misses) as f64
        } else {
            0.0
        };

        let throughput = if uptime > 0 {
            total_records as f64 / uptime as f64
        } else {
            0.0
        };

        (uptime, total_records, total_batches, total_errors, cache_hit_rate, throughput)
    }

    // =============================================================================
    // ZiRecord - Core record class
    // =============================================================================
    
    /// Python wrapper for ZiRecord.
    ///
    /// Represents a single data record with id, payload, and optional metadata.
    /// This is the fundamental data unit in Zi pipelines.
    #[pyclass(name = "ZiRecord")]
    #[derive(Clone)]
    pub struct ZiRecordPy {
        /// Internal ZiRecord instance
        pub inner: ZiRecord,
    }

    #[pymethods]
    impl ZiRecordPy {
        #[new]
        #[pyo3(signature = (id=None, payload="{}"))]
        fn new(id: Option<String>, payload: &str) -> PyResult<Self> {
            let payload_value: serde_json::Value = serde_json::from_str(payload)
                .unwrap_or_else(|_| serde_json::json!({}));
            Ok(Self {
                inner: ZiRecord::new(id, payload_value),
            })
        }

        #[getter(id)]
        fn get_id(&self) -> Option<String> {
            self.inner.id.clone()
        }

        #[setter(id)]
        fn set_id(&mut self, value: Option<String>) {
            self.inner.id = value;
        }

        #[getter(payload)]
        fn get_payload(&self) -> String {
            serde_json::to_string(&self.inner.payload).unwrap_or_default()
        }

        #[setter(payload)]
        fn set_payload(&mut self, value: &str) -> PyResult<()> {
            self.inner.payload = serde_json::from_str(value)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            Ok(())
        }

        #[getter(metadata)]
        fn get_metadata(&self) -> Option<String> {
            self.inner.metadata.as_ref()
                .map(|m| serde_json::to_string(m).unwrap_or_default())
        }

        #[setter(metadata)]
        fn set_metadata(&mut self, value: Option<String>) -> PyResult<()> {
            match value {
                Some(s) => {
                    let meta: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&s)
                        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                    self.inner.metadata = Some(meta);
                }
                None => self.inner.metadata = None,
            }
            Ok(())
        }

        fn with_metadata(&mut self, metadata: &str) -> PyResult<()> {
            let meta: serde_json::Map<String, serde_json::Value> = serde_json::from_str(metadata)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            self.inner = self.inner.clone().with_metadata(meta);
            Ok(())
        }

        fn __repr__(&self) -> String {
            format!("ZiRecord(id={:?})", self.inner.id)
        }
    }

    // =============================================================================
    // ZiMetrics - Runtime metrics
    // =============================================================================

    /// Python wrapper for runtime metrics.
    ///
    /// Collects and exposes pipeline execution metrics including:
    /// - Record and batch counts
    /// - Error tracking
    /// - Cache performance
    /// - Throughput measurements
    #[pyclass]
    pub struct ZiMetrics {
        uptime_seconds: u64,
        total_records: u64,
        total_batches: u64,
        total_errors: u64,
        cache_hit_rate: f64,
        throughput: f64,
    }

    #[pymethods]
    impl ZiMetrics {
        #[new]
        fn new() -> Self {
            let (uptime, records, batches, errors, hit_rate, throughput) = get_metrics_snapshot();
            Self {
                uptime_seconds: uptime,
                total_records: records,
                total_batches: batches,
                total_errors: errors,
                cache_hit_rate: hit_rate,
                throughput,
            }
        }

        fn refresh(&mut self) {
            let (uptime, records, batches, errors, hit_rate, throughput) = get_metrics_snapshot();
            self.uptime_seconds = uptime;
            self.total_records = records;
            self.total_batches = batches;
            self.total_errors = errors;
            self.cache_hit_rate = hit_rate;
            self.throughput = throughput;
        }

        fn reset(&mut self) {
            GLOBAL_TOTAL_RECORDS.store(0, Ordering::Relaxed);
            GLOBAL_TOTAL_BATCHES.store(0, Ordering::Relaxed);
            GLOBAL_TOTAL_ERRORS.store(0, Ordering::Relaxed);
            GLOBAL_CACHE_HITS.store(0, Ordering::Relaxed);
            GLOBAL_CACHE_MISSES.store(0, Ordering::Relaxed);
            self.uptime_seconds = 0;
            self.total_records = 0;
            self.total_batches = 0;
            self.total_errors = 0;
            self.cache_hit_rate = 0.0;
            self.throughput = 0.0;
        }

        #[getter(uptime_seconds)]
        fn uptime_seconds(&self) -> u64 { self.uptime_seconds }

        #[getter(total_records)]
        fn total_records(&self) -> u64 { self.total_records }

        #[getter(total_batches)]
        fn total_batches(&self) -> u64 { self.total_batches }

        #[getter(total_errors)]
        fn total_errors(&self) -> u64 { self.total_errors }

        #[getter(cache_hit_rate)]
        fn cache_hit_rate(&self) -> f64 { self.cache_hit_rate }

        #[getter(throughput)]
        fn throughput(&self) -> f64 { self.throughput }

        fn __repr__(&self) -> String {
            format!("ZiMetrics(records={}, throughput={:.2}/s)", self.total_records, self.throughput)
        }
    }

    // =============================================================================
    // ZiTextProcessor - High-level text processing API
    // =============================================================================

    /// High-level Python API for common text processing operations.
    ///
    /// Provides simplified access to text processing capabilities including:
    /// - SimHash computation for deduplication
    /// - Language detection
    /// - PII redaction
    /// - Text normalization
    /// - Quality and toxicity scoring
    /// - Token counting
    #[pyclass]
    pub struct ZiTextProcessor {
        _phantom: std::marker::PhantomData<()>,
    }

    #[pymethods]
    impl ZiTextProcessor {
        #[new]
        fn new() -> Self {
            Self { _phantom: std::marker::PhantomData }
        }

        fn compute_simhash(&self, text: &str) -> u64 {
            let config = serde_json::json!({});
            if let Ok(operator) = crate::operators::dedup::dedup_simhash_factory(&config) {
                let batch = vec![ZiRecord::new(None, serde_json::json!({"text": text}))];
                if let Ok(result) = operator.apply(batch) {
                    return result.first()
                        .and_then(|r| r.metadata.as_ref())
                        .and_then(|m| m.get("simhash"))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                }
            }
            0
        }

        fn detect_language(&self, text: &str) -> (String, f64) {
            let config = serde_json::json!({"path": "payload.text"});
            if let Ok(operator) = crate::operators::lang::lang_detect_factory(&config) {
                let batch = vec![ZiRecord::new(None, serde_json::json!({"text": text}))];
                if let Ok(result) = operator.apply(batch) {
                    if let Some(record) = result.first() {
                        let lang = record.metadata.as_ref()
                            .and_then(|m| m.get("lang"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                            .to_string();
                        let confidence = record.metadata.as_ref()
                            .and_then(|m| m.get("lang_confidence"))
                            .and_then(|v| v.as_f64())
                            .unwrap_or(0.0);
                        return (lang, confidence);
                    }
                }
            }
            ("unknown".to_string(), 0.0)
        }

        fn redact_pii(&self, text: &str) -> String {
            let config = serde_json::json!({});
            if let Ok(operator) = crate::operators::pii::pii_redact_factory(&config) {
                let batch = vec![ZiRecord::new(None, serde_json::json!({"text": text}))];
                if let Ok(result) = operator.apply(batch) {
                    return result.first()
                        .and_then(|r| r.payload.get("text"))
                        .and_then(|v| v.as_str())
                        .unwrap_or(text)
                        .to_string();
                }
            }
            text.to_string()
        }

        fn normalize_text(&self, text: &str) -> String {
            let config = serde_json::json!({});
            if let Ok(operator) = crate::operators::transform::transform_normalize_factory(&config) {
                let batch = vec![ZiRecord::new(None, serde_json::json!({"text": text}))];
                if let Ok(result) = operator.apply(batch) {
                    return result.first()
                        .and_then(|r| r.payload.get("text"))
                        .and_then(|v| v.as_str())
                        .unwrap_or(text)
                        .to_string();
                }
            }
            text.to_string()
        }

        fn quality_score(&self, text: &str) -> f64 {
            let config = serde_json::json!({"path": "payload.text"});
            if let Ok(operator) = crate::operators::quality::quality_score_factory(&config) {
                let batch = vec![ZiRecord::new(None, serde_json::json!({"text": text}))];
                if let Ok(result) = operator.apply(batch) {
                    return result.first()
                        .and_then(|r| r.metadata.as_ref())
                        .and_then(|m| m.get("quality"))
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                }
            }
            0.0
        }

        fn toxicity_score(&self, text: &str) -> f64 {
            let config = serde_json::json!({"path": "payload.text"});
            if let Ok(operator) = crate::operators::quality::toxicity_factory(&config) {
                let batch = vec![ZiRecord::new(None, serde_json::json!({"text": text}))];
                if let Ok(result) = operator.apply(batch) {
                    return result.first()
                        .and_then(|r| r.metadata.as_ref())
                        .and_then(|m| m.get("toxicity"))
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                }
            }
            0.0
        }

        fn count_tokens(&self, text: &str, tokenizer: Option<String>) -> usize {
            let tokenizer_type = tokenizer.unwrap_or_else(|| "cl100k_base".to_string());
            let config = serde_json::json!({
                "text_field": "payload.text",
                "output_field": "metadata.token_count",
                "tokenizer_type": tokenizer_type
            });
            if let Ok(operator) = crate::operators::token::token_count_factory(&config) {
                let batch = vec![ZiRecord::new(None, serde_json::json!({"text": text}))];
                if let Ok(result) = operator.apply(batch) {
                    return result.first()
                        .and_then(|r| r.metadata.as_ref())
                        .and_then(|m| m.get("token_count"))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0) as usize;
                }
            }
            0
        }

        fn language_confidence(&self, text: &str) -> f64 {
            let config = serde_json::json!({"path": "payload.text"});
            if let Ok(operator) = crate::operators::lang::lang_confidence_factory(&config) {
                let batch = vec![ZiRecord::new(None, serde_json::json!({"text": text}))];
                if let Ok(result) = operator.apply(batch) {
                    return result.first()
                        .and_then(|r| r.metadata.as_ref())
                        .and_then(|m| m.get("lang_confidence"))
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                }
            }
            0.0
        }
    }

    // =============================================================================
    // ZiOperator - Unified operator interface
    // =============================================================================

    /// Python wrapper for Zi operators.
    ///
    /// Provides a unified interface to create and apply any Zi operator from Python.
    /// Operators are identified by name (e.g., "filter.equals", "dedup.simhash")
    /// and configured via JSON strings.
    #[pyclass(name = "ZiOperator")]
    pub struct ZiOperatorPy {
        /// Name of the operator to create
        operator_name: String,
        /// JSON configuration for the operator
        config: serde_json::Value,
    }

    #[pymethods]
    impl ZiOperatorPy {
        #[new]
        #[pyo3(signature = (operator_name, config="{}"))]
        fn new(operator_name: &str, config: &str) -> PyResult<Self> {
            let config_value: serde_json::Value = serde_json::from_str(config)
                .unwrap_or_else(|_| serde_json::json!({}));
            Ok(Self {
                operator_name: operator_name.to_string(),
                config: config_value,
            })
        }

        fn apply(&self, records: &Bound<'_, PyAny>) -> PyResult<Vec<ZiRecordPy>> {
            let batch: Vec<ZiRecord> = extract_records(records)?;
            let operator = self.create_operator()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            let result = operator.apply(batch)
                .map_err(|e| {
                    record_batch_processed(0, 1);
                    pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
                })?;
            let output_count = result.len() as u64;
            record_batch_processed(output_count, 0);
            Ok(result.into_iter().map(|r| ZiRecordPy { inner: r }).collect())
        }

        fn name(&self) -> &str {
            &self.operator_name
        }

        fn __repr__(&self) -> String {
            format!("ZiOperator(name={})", self.operator_name)
        }

        #[staticmethod]
        fn new_token_count(text_field: Option<String>, tokenizer_type: Option<String>) -> PyResult<Self> {
            let config = serde_json::json!({
                "text_field": text_field.unwrap_or_else(|| "payload.text".to_string()),
                "output_field": "metadata.token_count",
                "tokenizer_type": tokenizer_type.unwrap_or_else(|| "cl100k_base".to_string())
            });
            Ok(Self {
                operator_name: "token.count".to_string(),
                config,
            })
        }

        #[staticmethod]
        fn new_token_stats(text_field: Option<String>, tokenizer_type: Option<String>) -> PyResult<Self> {
            let config = serde_json::json!({
                "text_field": text_field.unwrap_or_else(|| "payload.text".to_string()),
                "tokenizer_type": tokenizer_type.unwrap_or_else(|| "cl100k_base".to_string())
            });
            Ok(Self {
                operator_name: "token.stats".to_string(),
                config,
            })
        }

        #[staticmethod]
        fn new_token_filter(min_tokens: usize, max_tokens: usize, text_field: Option<String>, tokenizer_type: Option<String>) -> PyResult<Self> {
            let config = serde_json::json!({
                "text_field": text_field.unwrap_or_else(|| "payload.text".to_string()),
                "min_tokens": min_tokens,
                "max_tokens": max_tokens,
                "tokenizer_type": tokenizer_type.unwrap_or_else(|| "cl100k_base".to_string())
            });
            Ok(Self {
                operator_name: "token.filter".to_string(),
                config,
            })
        }

        #[staticmethod]
        fn new_pii_redact(text_field: Option<String>) -> PyResult<Self> {
            let config = serde_json::json!({
                "text_field": text_field.unwrap_or_else(|| "payload.text".to_string())
            });
            Ok(Self {
                operator_name: "pii.redact".to_string(),
                config,
            })
        }

        #[staticmethod]
        fn new_lang_detect(text_field: Option<String>) -> PyResult<Self> {
            let config = serde_json::json!({
                "path": text_field.unwrap_or_else(|| "payload.text".to_string())
            });
            Ok(Self {
                operator_name: "lang.detect".to_string(),
                config,
            })
        }

        #[staticmethod]
        fn new_lang_confidence(text_field: Option<String>) -> PyResult<Self> {
            let config = serde_json::json!({
                "path": text_field.unwrap_or_else(|| "payload.text".to_string())
            });
            Ok(Self {
                operator_name: "lang.confidence".to_string(),
                config,
            })
        }

        #[staticmethod]
        fn new_augment_synonym(text_field: Option<String>, probability: Option<f64>) -> PyResult<Self> {
            let config = serde_json::json!({
                "text_field": text_field.unwrap_or_else(|| "payload.text".to_string()),
                "probability": probability.unwrap_or(0.3)
            });
            Ok(Self {
                operator_name: "augment.synonym".to_string(),
                config,
            })
        }

        #[staticmethod]
        fn new_augment_noise(text_field: Option<String>, probability: Option<f64>) -> PyResult<Self> {
            let config = serde_json::json!({
                "text_field": text_field.unwrap_or_else(|| "payload.text".to_string()),
                "probability": probability.unwrap_or(0.1)
            });
            Ok(Self {
                operator_name: "augment.noise".to_string(),
                config,
            })
        }
    }

    impl ZiOperatorPy {
        fn create_operator(&self) -> Result<Box<dyn ZiOperator + Send + Sync>, crate::errors::ZiError> {
            match self.operator_name.as_str() {
                // Augment operators
                "augment.synonym" => crate::operators::augment::augment_synonym_factory(&self.config),
                "augment.noise" => crate::operators::augment::augment_noise_factory(&self.config),
                
                // Dedup operators
                "dedup.simhash" => crate::operators::dedup::dedup_simhash_factory(&self.config),
                "dedup.minhash" => crate::operators::dedup::dedup_minhash_factory(&self.config),
                "dedup.semantic" => crate::operators::dedup::dedup_semantic_factory(&self.config),
                
                // Field operators
                "field.select" => crate::operators::field::field_select_factory(&self.config),
                "field.rename" => crate::operators::field::field_rename_factory(&self.config),
                "field.drop" => crate::operators::field::field_drop_factory(&self.config),
                "field.copy" => crate::operators::field::field_copy_factory(&self.config),
                "field.move" => crate::operators::field::field_move_factory(&self.config),
                "field.flatten" => crate::operators::field::field_flatten_factory(&self.config),
                "field.default" => crate::operators::field::field_default_factory(&self.config),
                "field.require" => crate::operators::field::field_require_factory(&self.config),
                
                // Filter operators
                "filter.equals" => crate::operators::filter::filter_equals_factory(&self.config),
                "filter.not_equals" => crate::operators::filter::filter_not_equals_factory(&self.config),
                "filter.any" => crate::operators::filter::filter_any_factory(&self.config),
                "filter.between" => crate::operators::filter::filter_between_factory(&self.config),
                "filter.less_than" => crate::operators::filter::filter_less_than_factory(&self.config),
                "filter.greater_than" => crate::operators::filter::filter_greater_than_factory(&self.config),
                "filter.is_null" => crate::operators::filter::filter_is_null_factory(&self.config),
                "filter.regex" => crate::operators::filter::filter_regex_factory(&self.config),
                "filter.ends_with" => crate::operators::filter::filter_ends_with_factory(&self.config),
                "filter.starts_with" => crate::operators::filter::filter_starts_with_factory(&self.config),
                "filter.range" => crate::operators::filter::filter_range_factory(&self.config),
                "filter.in" => crate::operators::filter::filter_in_factory(&self.config),
                "filter.not_in" => crate::operators::filter::filter_not_in_factory(&self.config),
                "filter.contains" => crate::operators::filter::filter_contains_factory(&self.config),
                "filter.contains_all" => crate::operators::filter::filter_contains_all_factory(&self.config),
                "filter.contains_any" => crate::operators::filter::filter_contains_any_factory(&self.config),
                "filter.contains_none" => crate::operators::filter::filter_contains_none_factory(&self.config),
                "filter.array_contains" => crate::operators::filter::filter_array_contains_factory(&self.config),
                "filter.exists" => crate::operators::filter::filter_exists_factory(&self.config),
                "filter.not_exists" => crate::operators::filter::filter_not_exists_factory(&self.config),
                "filter.length_range" => crate::operators::filter::filter_length_range_factory(&self.config),
                "filter.token_range" => crate::operators::filter::filter_token_range_factory(&self.config),
                
                // Language operators
                "lang.detect" => crate::operators::lang::lang_detect_factory(&self.config),
                "lang.confidence" => crate::operators::lang::lang_confidence_factory(&self.config),
                
                // Limit operator
                "limit" => crate::operators::limit::limit_factory(&self.config),
                
                // LLM operators
                "llm.token_count" => crate::operators::llm::token_count_factory(&self.config),
                "llm.conversation_format" => crate::operators::llm::conversation_format_factory(&self.config),
                "llm.context_length" => crate::operators::llm::context_length_factory(&self.config),
                "llm.qa_extract" => crate::operators::llm::q_a_extract_factory(&self.config),
                "llm.instruction_format" => crate::operators::llm::instruction_format_factory(&self.config),
                
                // Merge operators
                "merge.concat" => crate::operators::merge::merge_concat_factory(&self.config),
                "merge.batch" => crate::operators::merge::merge_batch_factory(&self.config),
                "merge.union" => crate::operators::merge::merge_union_factory(&self.config),
                "merge.intersect" => crate::operators::merge::merge_intersect_factory(&self.config),
                "merge.difference" => crate::operators::merge::merge_difference_factory(&self.config),
                "merge.zip" => crate::operators::merge::merge_zip_factory(&self.config),
                
                // Metadata operators
                "metadata.enrich" => crate::operators::metadata::metadata_enrich_factory(&self.config),
                "metadata.remove" => crate::operators::metadata::metadata_remove_factory(&self.config),
                "metadata.keep" => crate::operators::metadata::metadata_keep_factory(&self.config),
                "metadata.rename" => crate::operators::metadata::metadata_rename_factory(&self.config),
                "metadata.copy" => crate::operators::metadata::metadata_copy_factory(&self.config),
                "metadata.require" => crate::operators::metadata::metadata_require_factory(&self.config),
                "metadata.extract" => crate::operators::metadata::metadata_extract_factory(&self.config),
                
                // PII operator
                "pii.redact" => crate::operators::pii::pii_redact_factory(&self.config),
                
                // Quality operators
                "quality.score" => crate::operators::quality::quality_score_factory(&self.config),
                "quality.filter" => crate::operators::quality::quality_filter_factory(&self.config),
                "quality.toxicity" => crate::operators::quality::toxicity_factory(&self.config),
                
                // Sample operators
                "sample.random" => crate::operators::sample::sample_random_factory(&self.config),
                "sample.top" => crate::operators::sample::sample_top_factory(&self.config),
                "sample.balanced" => crate::operators::sample::sample_balanced_factory(&self.config),
                "sample.by_distribution" => crate::operators::sample::sample_by_distribution_factory(&self.config),
                "sample.by_length" => crate::operators::sample::sample_by_length_factory(&self.config),
                "sample.stratified" => crate::operators::sample::sample_stratified_factory(&self.config),
                
                // Shuffle operators
                "shuffle" => crate::operators::shuffle::shuffle_factory(&self.config),
                "shuffle.deterministic" => crate::operators::shuffle::shuffle_deterministic_factory(&self.config),
                "shuffle.block" => crate::operators::shuffle::shuffle_block_factory(&self.config),
                "shuffle.stratified" => crate::operators::shuffle::shuffle_stratified_factory(&self.config),
                "shuffle.window" => crate::operators::shuffle::shuffle_window_factory(&self.config),
                
                // Split operators
                "split.random" => crate::operators::split::split_random_factory(&self.config),
                "split.stratified" => crate::operators::split::split_stratified_factory(&self.config),
                "split.sequential" => crate::operators::split::split_sequential_factory(&self.config),
                "split.k_fold" => crate::operators::split::split_k_fold_factory(&self.config),
                "split.chunk" => crate::operators::split::split_chunk_factory(&self.config),
                
                // Token operators
                "token.count" => crate::operators::token::token_count_factory(&self.config),
                "token.stats" => crate::operators::token::token_stats_factory(&self.config),
                "token.filter" => crate::operators::token::token_filter_factory(&self.config),
                "token.histogram" => crate::operators::token::token_histogram_factory(&self.config),
                
                // Transform operators
                "transform.normalize" => crate::operators::transform::transform_normalize_factory(&self.config),
                "transform.map" => crate::operators::transform::transform_map_factory(&self.config),
                "transform.template" => crate::operators::transform::transform_template_factory(&self.config),
                "transform.chain" => crate::operators::transform::transform_chain_factory(&self.config),
                "transform.flat_map" => crate::operators::transform::transform_flat_map_factory(&self.config),
                "transform.coalesce" => crate::operators::transform::transform_coalesce_factory(&self.config),
                "transform.conditional" => crate::operators::transform::transform_conditional_factory(&self.config),
                
                _ => Err(crate::errors::ZiError::validation(format!("Unknown operator: {}", self.operator_name))),
            }
        }
    }

    // =============================================================================
    // ZiVersionInfo - Version information
    // =============================================================================

    /// Python wrapper for Zi version information.
    ///
    /// Exposes package metadata including version, name, and ABI compatibility.
    #[pyclass]
    pub struct ZiVersionInfo {
        version: String,
        name: String,
        abi_version: String,
    }

    #[pymethods]
    impl ZiVersionInfo {
        #[new]
        fn new() -> Self {
            Self {
                version: env!("CARGO_PKG_VERSION").to_string(),
                name: "Zi".to_string(),
                abi_version: "1".to_string(),
            }
        }

        #[getter(version)]
        fn version(&self) -> String {
            self.version.clone()
        }

        #[getter(name)]
        fn name(&self) -> String {
            self.name.clone()
        }

        #[getter(abi_version)]
        fn abi_version(&self) -> String {
            self.abi_version.clone()
        }

        fn to_dict(&self) -> HashMap<String, String> {
            let mut info = HashMap::new();
            info.insert("version".to_string(), self.version.clone());
            info.insert("name".to_string(), self.name.clone());
            info.insert("abi_version".to_string(), self.abi_version.clone());
            info
        }

        fn __repr__(&self) -> String {
            format!("ZiVersionInfo(version={}, name={})", self.version, self.name)
        }
    }

    // =============================================================================
    // Helper functions
    // =============================================================================

    fn extract_records(obj: &Bound<'_, PyAny>) -> PyResult<Vec<ZiRecord>> {
        let list = obj.cast::<PyList>()?;
        let mut records = Vec::with_capacity(list.len());
        for item in list.iter() {
            let record_py: ZiRecordPy = item.extract()?;
            records.push(record_py.inner);
        }
        Ok(records)
    }

    // =============================================================================
    // ZiPipelineBuilder - Pipeline builder class
    // =============================================================================

    #[pyclass(name = "ZiPipelineBuilder")]
    pub struct ZiPipelineBuilderPy {
        inner: crate::pipeline::ZiPipelineBuilder,
    }

    #[pymethods]
    impl ZiPipelineBuilderPy {
        #[new]
        fn new() -> PyResult<Self> {
            Ok(Self {
                inner: crate::pipeline::ZiPipelineBuilder::new(),
            })
        }

        #[staticmethod]
        fn with_defaults() -> PyResult<Self> {
            Ok(Self {
                inner: crate::pipeline::ZiPipelineBuilder::with_defaults(),
            })
        }

        fn build_from_config(&self, steps: &str) -> PyResult<ZiPipelinePy> {
            let steps_value: serde_json::Value = serde_json::from_str(steps)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            let steps_array = steps_value.as_array()
                .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("config must be an array"))?;
            let pipeline = self.inner.build_from_config(steps_array)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(ZiPipelinePy { inner: pipeline })
        }

        fn __repr__(&self) -> String {
            "ZiPipelineBuilder()".to_string()
        }
    }

    // =============================================================================
    // ZiPipeline - Pipeline execution class
    // =============================================================================

    #[pyclass(name = "ZiPipeline")]
    pub struct ZiPipelinePy {
        inner: crate::pipeline::ZiPipeline,
    }

    #[pymethods]
    impl ZiPipelinePy {
        fn run(&self, records: &Bound<'_, PyAny>) -> PyResult<Vec<ZiRecordPy>> {
            let batch: Vec<ZiRecord> = extract_records(records)?;
            let result = self.inner.run(batch)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(result.into_iter().map(|r| ZiRecordPy { inner: r }).collect())
        }

        fn run_chunked(&self, records: &Bound<'_, PyAny>, chunk_size: usize) -> PyResult<Vec<ZiRecordPy>> {
            let batch: Vec<ZiRecord> = extract_records(records)?;
            let result = self.inner.run_chunked(batch, chunk_size)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(result.into_iter().map(|r| ZiRecordPy { inner: r }).collect())
        }

        fn validate(&self) -> PyResult<()> {
            self.inner.validate()
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            Ok(())
        }

        fn __repr__(&self) -> String {
            "ZiPipeline()".to_string()
        }
    }

    // =============================================================================
    // ZiSynthesizer - Data synthesis class
    // =============================================================================

    #[pyclass(name = "ZiSynthesizer")]
    pub struct ZiSynthesizerPy {
        inner: crate::enrich::ZiSynthesizer,
    }

    #[pymethods]
    impl ZiSynthesizerPy {
        #[new]
        fn new(config_json: &str) -> PyResult<Self> {
            let config: crate::enrich::ZiSynthesisConfig = serde_json::from_str(config_json)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            Ok(Self {
                inner: crate::enrich::ZiSynthesizer::new(config),
            })
        }

        fn synthesize(&mut self, records: &Bound<'_, PyAny>) -> PyResult<Vec<ZiRecordPy>> {
            let batch: Vec<ZiRecord> = extract_records(records)?;
            let result = self.inner.synthesize(&batch)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(result.into_iter().map(|r| ZiRecordPy { inner: r }).collect())
        }

        fn __repr__(&self) -> String {
            "ZiSynthesizer()".to_string()
        }
    }

    // =============================================================================
    // ZiAnnotator - Data annotation class
    // =============================================================================

    #[pyclass(name = "ZiAnnotator")]
    pub struct ZiAnnotatorPy {
        inner: crate::enrich::ZiAnnotator,
    }

    #[pymethods]
    impl ZiAnnotatorPy {
        #[new]
        fn new(config_json: &str) -> PyResult<Self> {
            let config: crate::enrich::ZiAnnotationConfig = serde_json::from_str(config_json)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            Ok(Self {
                inner: crate::enrich::ZiAnnotator::new(config),
            })
        }

        fn annotate(&self, records: &Bound<'_, PyAny>) -> PyResult<Vec<ZiRecordPy>> {
            let batch: Vec<ZiRecord> = extract_records(records)?;
            let result = self.inner.annotate(batch)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(result.into_iter().map(|r| ZiRecordPy { inner: r }).collect())
        }

        fn __repr__(&self) -> String {
            "ZiAnnotator()".to_string()
        }
    }

    // =============================================================================
    // ZiAugmenter - Data augmentation class
    // =============================================================================

    #[pyclass(name = "ZiAugmenter")]
    pub struct ZiAugmenterPy {
        inner: crate::enrich::ZiAugmenter,
    }

    #[pymethods]
    impl ZiAugmenterPy {
        #[new]
        fn new(config_json: &str) -> PyResult<Self> {
            let config: crate::enrich::ZiAugmentationConfig = serde_json::from_str(config_json)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            Ok(Self {
                inner: crate::enrich::ZiAugmenter::new(config),
            })
        }

        fn augment(&self, records: &Bound<'_, PyAny>) -> PyResult<Vec<ZiRecordPy>> {
            let batch: Vec<ZiRecord> = extract_records(records)?;
            let result = self.inner.augment(batch)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(result.into_iter().map(|r| ZiRecordPy { inner: r }).collect())
        }

        fn __repr__(&self) -> String {
            "ZiAugmenter()".to_string()
        }
    }

    // =============================================================================
    // ZiDAG - Directed Acyclic Graph class
    // =============================================================================

    #[pyclass(name = "ZiDAG")]
    pub struct ZiDAGPy {
        inner: crate::dag::ZiDAG,
    }

    #[pymethods]
    impl ZiDAGPy {
        #[new]
        fn new() -> Self {
            Self {
                inner: crate::dag::ZiDAG::new(),
            }
        }

        fn add_node(&mut self, id: &str, name: &str, operator: &str, config_json: &str) -> PyResult<()> {
            let config: serde_json::Value = serde_json::from_str(config_json)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            let node_config = crate::dag::ZiGraphNodeConfig {
                name: name.to_string(),
                operator: operator.to_string(),
                config,
                parallel: false,
                cache: false,
            };
            let node = crate::dag::ZiGraphNode::new(crate::dag::ZiNodeId::from(id), node_config);
            self.inner.add_node(node)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            Ok(())
        }

        fn add_edge(&mut self, from: &str, to: &str) -> PyResult<()> {
            self.inner.add_edge(crate::dag::ZiNodeId::from(from), crate::dag::ZiNodeId::from(to))
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            Ok(())
        }

        fn topological_sort(&self) -> PyResult<Vec<String>> {
            let sorted = self.inner.topological_sort()
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            Ok(sorted.iter().map(|id| id.0.clone()).collect())
        }

        fn detect_cycles(&self) -> bool {
            self.inner.detect_cycles()
        }

        fn __repr__(&self) -> String {
            format!("ZiDAG(nodes={})", self.inner.nodes.len())
        }
    }

    // =============================================================================
    // ZiProfiler - Data profiling class
    // =============================================================================

    #[pyclass(name = "ZiProfiler")]
    pub struct ZiProfilerPy {
        config: crate::inspect::ZiProfilerConfig,
    }

    #[pymethods]
    impl ZiProfilerPy {
        #[new]
        fn new(config_json: Option<&str>) -> PyResult<Self> {
            let config: crate::inspect::ZiProfilerConfig = if let Some(json) = config_json {
                serde_json::from_str(json)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?
            } else {
                crate::inspect::ZiProfilerConfig::default()
            };
            Ok(Self { config })
        }

        fn profile(&self, records: &Bound<'_, PyAny>) -> PyResult<String> {
            let batch: Vec<ZiRecord> = extract_records(records)?;
            let profiler = crate::inspect::ZiProfiler::new().with_config(self.config.clone());
            let report = profiler.profile(&batch);
            serde_json::to_string(&report)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        }

        fn __repr__(&self) -> String {
            "ZiProfiler()".to_string()
        }
    }

    // =============================================================================
    // ZiStatistics - Statistics calculation class
    // =============================================================================

    #[pyclass(name = "ZiStatistics")]
    pub struct ZiStatisticsPy;

    #[pymethods]
    impl ZiStatisticsPy {
        #[new]
        fn new() -> Self {
            Self
        }

        fn compute(&self, records: &Bound<'_, PyAny>) -> PyResult<String> {
            let batch: Vec<ZiRecord> = extract_records(records)?;
            let stats = crate::inspect::ZiStatistics::compute(&batch);
            serde_json::to_string(&stats)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        }

        fn __repr__(&self) -> String {
            "ZiStatistics()".to_string()
        }
    }

    // =============================================================================
    // ZiStreamReader - Stream reader class
    // =============================================================================

    #[pyclass(name = "ZiStreamReader")]
    pub struct ZiStreamReaderPy {
        config: crate::ingest::ZiReaderConfig,
    }

    #[pymethods]
    impl ZiStreamReaderPy {
        #[new]
        fn new(config_json: Option<&str>) -> PyResult<Self> {
            let config: crate::ingest::ZiReaderConfig = if let Some(json) = config_json {
                serde_json::from_str(json)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?
            } else {
                crate::ingest::ZiReaderConfig::default()
            };
            Ok(Self { config })
        }

        fn read_path(&self, path: &str) -> PyResult<Vec<ZiRecordPy>> {
            let reader = crate::ingest::ZiStreamReader::new().with_config(self.config.clone());
            let result = reader.read_path(std::path::Path::new(path))
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(result.into_iter().map(|r| ZiRecordPy { inner: r }).collect())
        }

        fn __repr__(&self) -> String {
            "ZiStreamReader()".to_string()
        }
    }

    // =============================================================================
    // ZiStreamWriter - Stream writer class
    // =============================================================================

    #[pyclass(name = "ZiStreamWriter")]
    pub struct ZiStreamWriterPy {
        config: crate::export::ZiWriterConfig,
    }

    #[pymethods]
    impl ZiStreamWriterPy {
        #[new]
        fn new(config_json: Option<&str>) -> PyResult<Self> {
            let config: crate::export::ZiWriterConfig = if let Some(json) = config_json {
                serde_json::from_str(json)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?
            } else {
                crate::export::ZiWriterConfig::default()
            };
            Ok(Self { config })
        }

        fn write(&self, records: &Bound<'_, PyAny>, path: &str) -> PyResult<String> {
            let batch: Vec<ZiRecord> = extract_records(records)?;
            let mut writer = crate::export::ZiStreamWriter::new().with_config(self.config.clone());
            let stats = writer.write(&batch, std::path::Path::new(path))
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            serde_json::to_string(&stats)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        }

        fn __repr__(&self) -> String {
            "ZiStreamWriter()".to_string()
        }
    }

    // =============================================================================
    // Python Module
    // =============================================================================

    #[pymodule]
    pub fn zix(m: &Bound<'_, PyModule>) -> PyResult<()> {
        // Core classes
        m.add_class::<ZiRecordPy>()?;
        m.add_class::<ZiMetrics>()?;
        m.add_class::<ZiTextProcessor>()?;
        m.add_class::<ZiVersionInfo>()?;
        
        // Operator class
        m.add_class::<ZiOperatorPy>()?;

        // Pipeline classes
        m.add_class::<ZiPipelineBuilderPy>()?;
        m.add_class::<ZiPipelinePy>()?;

        // Enrich classes
        m.add_class::<ZiSynthesizerPy>()?;
        m.add_class::<ZiAnnotatorPy>()?;
        m.add_class::<ZiAugmenterPy>()?;

        // DAG class
        m.add_class::<ZiDAGPy>()?;

        // Inspect classes
        m.add_class::<ZiProfilerPy>()?;
        m.add_class::<ZiStatisticsPy>()?;

        // IO classes
        m.add_class::<ZiStreamReaderPy>()?;
        m.add_class::<ZiStreamWriterPy>()?;

        m.add("__version__", "0.1.0")?;
        m.add("__author__", "Dunimd Team")?;
        m.add("__license__", "Apache-2.0")?;

        Ok(())
    }
}
