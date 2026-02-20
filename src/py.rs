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

//! Python bindings for Zi Core using PyO3.

#[cfg(feature = "pyo3")]
use pyo3::prelude::*;

#[cfg(feature = "pyo3")]
#[pyclass]
struct PyRecord {
    id: Option<String>,
    payload: String,
    metadata: Option<String>,
}

#[cfg(feature = "pyo3")]
#[pymethods]
impl PyRecord {
    #[new]
    #[pyo3(signature = (id, payload, metadata))]
    fn new(id: Option<String>, payload: String, metadata: Option<String>) -> Self {
        Self { id, payload, metadata }
    }

    #[getter(id)]
    fn get_id(&self) -> Option<String> {
        self.id.clone()
    }

    #[getter(payload)]
    fn get_payload(&self) -> String {
        self.payload.clone()
    }

    #[getter(metadata)]
    fn get_metadata(&self) -> Option<String> {
        self.metadata.clone()
    }

    fn __repr__(&self) -> String {
        format!("PyRecord(id={:?}, payload={})", self.id, self.payload)
    }
}

#[cfg(feature = "pyo3")]
#[pyclass]
struct PyMetrics {
    uptime_seconds: u64,
    total_records: u64,
    total_batches: u64,
    total_errors: u64,
    cache_hit_rate: f64,
    throughput: f64,
}

#[cfg(feature = "pyo3")]
#[pymethods]
impl PyMetrics {
    #[new]
    fn new() -> Self {
        Self {
            uptime_seconds: 0,
            total_records: 0,
            total_batches: 0,
            total_errors: 0,
            cache_hit_rate: 0.0,
            throughput: 0.0,
        }
    }

    #[getter(uptime_seconds)]
    fn uptime_seconds(&self) -> u64 {
        self.uptime_seconds
    }

    #[getter(total_records)]
    fn total_records(&self) -> u64 {
        self.total_records
    }

    #[getter(total_batches)]
    fn total_batches(&self) -> u64 {
        self.total_batches
    }

    #[getter(total_errors)]
    fn total_errors(&self) -> u64 {
        self.total_errors
    }

    #[getter(cache_hit_rate)]
    fn cache_hit_rate(&self) -> f64 {
        self.cache_hit_rate
    }

    #[getter(throughput)]
    fn throughput(&self) -> f64 {
        self.throughput
    }

    fn __repr__(&self) -> String {
        format!(
            "PyMetrics(records={}, throughput={:.2}/s)",
            self.total_records, self.throughput
        )
    }
}

#[cfg(feature = "pyo3")]
#[pyfunction]
fn compute_simhash(text: &str) -> u64 {
    let config = serde_json::json!({});
    let operator = crate::operators::dedup::ZiFDedupSimhashFactory(&config).unwrap();
    let batch = vec![crate::record::ZiCRecord {
        id: None,
        payload: serde_json::json!({"text": text}),
        metadata: None,
    }];
    let result = operator.apply(batch).unwrap();
    result
        .first()
        .and_then(|r| r.metadata.as_ref())
        .and_then(|m| m.get("simhash"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0)
}

#[cfg(feature = "pyo3")]
#[pyfunction]
fn detect_language(text: &str) -> (String, f64) {
    let config = serde_json::json!({"path": "payload.text"});
    let operator = crate::operators::lang::ZiFLangDetectFactory(&config).unwrap();
    let batch = vec![crate::record::ZiCRecord {
        id: None,
        payload: serde_json::json!({"text": text}),
        metadata: None,
    }];
    let result = operator.apply(batch).unwrap();
    if let Some(record) = result.first() {
        let lang = record
            .metadata
            .as_ref()
            .and_then(|m| m.get("lang"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let confidence = record
            .metadata
            .as_ref()
            .and_then(|m| m.get("lang_confidence"))
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        return (lang, confidence);
    }
    ("unknown".to_string(), 0.0)
}

#[cfg(feature = "pyo3")]
#[pyfunction]
fn redact_pii(text: &str) -> String {
    let config = serde_json::json!({});
    let operator = crate::operators::pii::ZiFPiiRedactFactory(&config).unwrap();
    let batch = vec![crate::record::ZiCRecord {
        id: None,
        payload: serde_json::json!({"text": text}),
        metadata: None,
    }];
    let result = operator.apply(batch).unwrap();
    result
        .first()
        .and_then(|r| r.payload.get("text"))
        .and_then(|v| v.as_str())
        .unwrap_or(text)
        .to_string()
}

#[cfg(feature = "pyo3")]
#[pyfunction]
fn normalize_text(text: &str) -> String {
    let config = serde_json::json!({});
    let operator = crate::operators::transform::ZiFTransformNormalizeFactory(&config).unwrap();
    let batch = vec![crate::record::ZiCRecord {
        id: None,
        payload: serde_json::json!({"text": text}),
        metadata: None,
    }];
    let result = operator.apply(batch).unwrap();
    result
        .first()
        .and_then(|r| r.payload.get("text"))
        .and_then(|v| v.as_str())
        .unwrap_or(text)
        .to_string()
}

#[cfg(feature = "pyo3")]
#[pyfunction]
fn quality_score(text: &str) -> f64 {
    let config = serde_json::json!({"path": "payload.text"});
    let operator = crate::operators::quality::ZiFQualityScoreFactory(&config).unwrap();
    let batch = vec![crate::record::ZiCRecord {
        id: None,
        payload: serde_json::json!({"text": text}),
        metadata: None,
    }];
    let result = operator.apply(batch).unwrap();
    result
        .first()
        .and_then(|r| r.metadata.as_ref())
        .and_then(|m| m.get("quality"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0)
}

#[cfg(feature = "pyo3")]
#[pyfunction]
fn toxicity_score(text: &str) -> f64 {
    let config = serde_json::json!({"path": "payload.text"});
    let operator = crate::operators::quality::ZiFToxicityFactory(&config).unwrap();
    let batch = vec![crate::record::ZiCRecord {
        id: None,
        payload: serde_json::json!({"text": text}),
        metadata: None,
    }];
    let result = operator.apply(batch).unwrap();
    result
        .first()
        .and_then(|r| r.metadata.as_ref())
        .and_then(|m| m.get("toxicity"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0)
}

#[cfg(feature = "pyo3")]
#[pyfunction]
fn generate_prometheus_metrics() -> String {
    let metrics = std::sync::Arc::new(crate::monitor::ZiCRuntimeMetrics::ZiFNew());
    crate::monitor::ZiFGeneratePrometheusMetrics(&metrics)
}

#[cfg(feature = "pyo3")]
#[pyfunction]
fn version_info() -> HashMap<String, String> {
    let mut info = HashMap::new();
    info.insert("version".to_string(), "0.1.0".to_string());
    info.insert("name".to_string(), "Zi".to_string());
    info.insert("abi_version".to_string(), "1".to_string());
    info
}

#[cfg(feature = "pyo3")]
#[pymodule]
fn zi_core(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyRecord>()?;
    m.add_class::<PyMetrics>()?;

    m.add_function(wrap_pyfunction!(compute_simhash, m)?)?;
    m.add_function(wrap_pyfunction!(detect_language, m)?)?;
    m.add_function(wrap_pyfunction!(redact_pii, m)?)?;
    m.add_function(wrap_pyfunction!(normalize_text, m)?)?;
    m.add_function(wrap_pyfunction!(quality_score, m)?)?;
    m.add_function(wrap_pyfunction!(toxicity_score, m)?)?;
    m.add_function(wrap_pyfunction!(generate_prometheus_metrics, m)?)?;
    m.add_function(wrap_pyfunction!(version_info, m)?)?;

    m.add("__version__", "0.1.0")?;
    m.add("__author__", "Wenze Wei")?;
    m.add("__license__", "Apache-2.0")?;

    Ok(())
}
