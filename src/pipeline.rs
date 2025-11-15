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

use std::collections::HashMap;
use std::ffi::{c_void, CStr};
use std::os::raw::c_char;
use std::path::Path;

use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::metrics::ZiCQualityMetrics;
use crate::operator::{ZiCOperator, ZiFExecuteOperator};
use crate::record::ZiCRecordBatch;
use libloading::Library;

type OperatorFactory = fn(&Value) -> Result<Box<dyn ZiCOperator + Send + Sync>>;

/// Simple linear pipeline composed of sequential operators.
pub struct ZiCPipeline {
    stages: Vec<Box<dyn ZiCOperator + Send + Sync>>,
    cache: std::collections::HashMap<String, Vec<crate::record::ZiCRecord>>,
}

impl ZiCPipeline {
    /// Constructs a pipeline from a list of operators.
    pub fn new(stages: Vec<Box<dyn ZiCOperator + Send + Sync>>) -> Self {
        ZiCPipeline {
            stages,
            cache: std::collections::HashMap::new(),
        }
    }

    /// Runs the pipeline, passing batches through each operator sequentially.
    pub fn run(&self, mut batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        for stage in &self.stages {
            batch = ZiFExecuteOperator(stage.as_ref(), batch)?;
        }
        Ok(batch)
    }

    pub fn run_chunked(&self, batch: ZiCRecordBatch, chunk_size: usize) -> Result<ZiCRecordBatch> {
        let mut out = Vec::new();
        let mut idx = 0;
        while idx < batch.len() {
            let end = (idx + chunk_size).min(batch.len());
            let chunk = batch[idx..end].to_vec();
            out.extend(self.run(chunk)?);
            idx = end;
        }
        Ok(out)
    }

    pub fn run_with_progress(
        &self,
        mut batch: ZiCRecordBatch,
        progress: impl Fn(&str, usize, usize),
    ) -> Result<ZiCRecordBatch> {
        for stage in &self.stages {
            let before = batch.len();
            batch = ZiFExecuteOperator(stage.as_ref(), batch)?;
            let after = batch.len();
            progress(stage.name(), before, after);
        }
        Ok(batch)
    }

    pub fn run_cached(&mut self, mut batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        fn hash_batch(batch: &ZiCRecordBatch) -> String {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let s = serde_json::to_string(batch).unwrap_or_default();
            let mut h = DefaultHasher::new();
            s.hash(&mut h);
            format!("{:x}", h.finish())
        }
        for stage in &self.stages {
            let key = format!("{}:{}", stage.name(), hash_batch(&batch));
            if let Some(cached) = self.cache.get(&key) {
                batch = cached.clone();
                continue;
            }
            let out = ZiFExecuteOperator(stage.as_ref(), batch)?;
            self.cache.insert(key, out.clone());
            batch = out;
        }
        Ok(batch)
    }

    /// Ensures the pipeline contains at least one stage.
    pub fn validate(&self) -> Result<()> {
        if self.stages.is_empty() {
            return Err(ZiError::pipeline("pipeline", "no stages configured"));
        }
        Ok(())
    }

    /// Executes the pipeline and returns both processed records and quality metrics.
    pub fn run_with_metrics(
        &self,
        batch: ZiCRecordBatch,
    ) -> Result<(ZiCRecordBatch, ZiCQualityMetrics)> {
        let processed = self.run(batch)?;
        let metrics = ZiCQualityMetrics::ZiFCompute(&processed);
        Ok((processed, metrics))
    }
}

/// Builder that knows how to instantiate operators from configuration.
pub struct ZiCPipelineBuilder {
    factories: HashMap<String, OperatorFactory>,
    plugins: Vec<Library>,
}

impl ZiCPipelineBuilder {
    /// Creates an empty builder.
    pub fn new() -> Self {
        ZiCPipelineBuilder {
            factories: HashMap::new(),
            plugins: Vec::new(),
        }
    }

    /// Creates a builder pre-loaded with bundled Zi operators.
    pub fn with_defaults() -> Self {
        let mut builder = Self::new();
        builder.register_defaults();
        builder
    }

    /// Registers a factory for the given operator name.
    pub fn register(&mut self, name: impl Into<String>, factory: OperatorFactory) {
        self.factories.insert(name.into(), factory);
    }

    fn register_defaults(&mut self) {
        self.register(
            "filter.equals",
            crate::operators::filter::ZiFFilterEqualsFactory as OperatorFactory,
        );
        self.register(
            "filter.not_equals",
            crate::operators::filter::ZiFFilterNotEqualsFactory as OperatorFactory,
        );
        self.register(
            "filter.any",
            crate::operators::filter::ZiFFilterAnyFactory as OperatorFactory,
        );
        self.register(
            "filter.in",
            crate::operators::filter::ZiFFilterInFactory as OperatorFactory,
        );
        self.register(
            "filter.not_in",
            crate::operators::filter::ZiFFilterNotInFactory as OperatorFactory,
        );
        self.register(
            "filter.exists",
            crate::operators::filter::ZiFFilterExistsFactory as OperatorFactory,
        );
        self.register(
            "filter.not_exists",
            crate::operators::filter::ZiFFilterNotExistsFactory as OperatorFactory,
        );
        self.register(
            "filter.contains",
            crate::operators::filter::ZiFFilterContainsFactory as OperatorFactory,
        );
        self.register(
            "filter.contains_all",
            crate::operators::filter::ZiFFilterContainsAllFactory as OperatorFactory,
        );
        self.register(
            "filter.contains_any",
            crate::operators::filter::ZiFFilterContainsAnyFactory as OperatorFactory,
        );
        self.register(
            "filter.contains_none",
            crate::operators::filter::ZiFFilterContainsNoneFactory as OperatorFactory,
        );
        self.register(
            "filter.length_range",
            crate::operators::filter::ZiFFilterLengthRangeFactory as OperatorFactory,
        );
        self.register(
            "filter.token_range",
            crate::operators::filter::ZiFFilterTokenRangeFactory as OperatorFactory,
        );
        self.register(
            "filter.array_contains",
            crate::operators::filter::ZiFFilterArrayContainsFactory as OperatorFactory,
        );
        self.register(
            "filter.starts_with",
            crate::operators::filter::ZiFFilterStartsWithFactory as OperatorFactory,
        );
        self.register(
            "filter.ends_with",
            crate::operators::filter::ZiFFilterEndsWithFactory as OperatorFactory,
        );
        self.register(
            "filter.regex",
            crate::operators::filter::ZiFFilterRegexFactory as OperatorFactory,
        );
        self.register(
            "filter.is_null",
            crate::operators::filter::ZiFFilterIsNullFactory as OperatorFactory,
        );
        self.register(
            "filter.greater_than",
            crate::operators::filter::ZiFFilterGreaterThanFactory as OperatorFactory,
        );
        self.register(
            "filter.less_than",
            crate::operators::filter::ZiFFilterLessThanFactory as OperatorFactory,
        );
        self.register(
            "filter.between",
            crate::operators::filter::ZiFFilterBetweenFactory as OperatorFactory,
        );
        self.register(
            "filter.range",
            crate::operators::filter::ZiFFilterRangeFactory as OperatorFactory,
        );
        self.register(
            "metadata.enrich",
            crate::operators::metadata::ZiFMetadataEnrichFactory as OperatorFactory,
        );
        self.register(
            "metadata.rename",
            crate::operators::metadata::ZiFMetadataRenameFactory as OperatorFactory,
        );
        self.register(
            "metadata.remove",
            crate::operators::metadata::ZiFMetadataRemoveFactory as OperatorFactory,
        );
        self.register(
            "metadata.copy",
            crate::operators::metadata::ZiFMetadataCopyFactory as OperatorFactory,
        );
        self.register(
            "metadata.require",
            crate::operators::metadata::ZiFMetadataRequireFactory as OperatorFactory,
        );
        self.register(
            "metadata.extract",
            crate::operators::metadata::ZiFMetadataExtractFactory as OperatorFactory,
        );
        self.register(
            "metadata.keep",
            crate::operators::metadata::ZiFMetadataKeepFactory as OperatorFactory,
        );
        self.register(
            "limit",
            crate::operators::limit::ZiFLimitFactory as OperatorFactory,
        );

        // language
        self.register(
            "lang.detect",
            crate::operators::lang::ZiFLangDetectFactory as OperatorFactory,
        );
        self.register(
            "lang.confidence",
            crate::operators::lang::ZiFLangConfidenceFactory as OperatorFactory,
        );

        // pii
        self.register(
            "pii.redact",
            crate::operators::pii::ZiFPiiRedactFactory as OperatorFactory,
        );

        // dedup
        self.register(
            "dedup.simhash",
            crate::operators::dedup::ZiFDedupSimhashFactory as OperatorFactory,
        );
        self.register(
            "dedup.minhash",
            crate::operators::dedup::ZiFDedupMinhashFactory as OperatorFactory,
        );
        self.register(
            "dedup.semantic",
            crate::operators::dedup::ZiFDedupSemanticFactory as OperatorFactory,
        );

        // quality
        self.register(
            "quality.score",
            crate::operators::quality::ZiFQualityScoreFactory as OperatorFactory,
        );
        self.register(
            "quality.filter",
            crate::operators::quality::ZiFQualityFilterFactory as OperatorFactory,
        );
        self.register(
            "quality.toxicity",
            crate::operators::quality::ZiFToxicityFactory as OperatorFactory,
        );

        // transform
        self.register(
            "transform.normalize",
            crate::operators::transform::ZiFTransformNormalizeFactory as OperatorFactory,
        );

        // augment
        self.register(
            "augment.synonym",
            crate::operators::augment::ZiFAugmentSynonymFactory as OperatorFactory,
        );
        self.register(
            "augment.noise",
            crate::operators::augment::ZiFAugmentNoiseFactory as OperatorFactory,
        );

        // sampling
        self.register(
            "sample.random",
            crate::operators::sample::ZiFSampleRandomFactory as OperatorFactory,
        );
        self.register(
            "sample.top",
            crate::operators::sample::ZiFSampleTopFactory as OperatorFactory,
        );
    }

    /// Builds a pipeline from a sequence of configuration steps.
    pub fn build_from_config(&self, steps: &[Value]) -> Result<ZiCPipeline> {
        let mut stages = Vec::with_capacity(steps.len());
        for (index, step) in steps.iter().enumerate() {
            let object = step.as_object().ok_or_else(|| {
                ZiError::validation(format!("pipeline step #{index} must be an object"))
            })?;

            let operator_name =
                object
                    .get("operator")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        ZiError::validation(format!(
                            "pipeline step #{index} missing string 'operator'"
                        ))
                    })?;

            let factory = self.factories.get(operator_name).ok_or_else(|| {
                ZiError::validation(format!("unknown operator '{operator_name}'"))
            })?;

            let config_value = object.get("config").cloned().unwrap_or(Value::Null);
            let operator = factory(&config_value)?;
            stages.push(operator);
        }

        let pipeline = ZiCPipeline::new(stages);
        pipeline.validate()?;
        Ok(pipeline)
    }

    /// Loads a dynamic plugin library and registers its operators.
    pub fn load_plugin(&mut self, path: impl AsRef<Path>) -> Result<()> {
        unsafe {
            let library = Library::new(path.as_ref())
                .map_err(|err| ZiError::internal(format!("failed to load plugin: {err}")))?;

            let register_symbol: libloading::Symbol<PluginRegisterFn> =
                library.get(b"zi_register_operators\0").map_err(|err| {
                    ZiError::internal(format!(
                        "plugin missing symbol 'zi_register_operators': {err}"
                    ))
                })?;

            let mut ctx = PluginContext {
                builder: self as *mut ZiCPipelineBuilder,
                error: None,
            };

            let success = register_symbol(
                register_operator_callback,
                &mut ctx as *mut _ as *mut c_void,
            );

            if let Some(err) = ctx.error {
                return Err(err);
            }

            if !success {
                return Err(ZiError::internal(
                    "plugin registration reported failure".to_string(),
                ));
            }

            self.plugins.push(library);
        }

        Ok(())
    }
}

#[allow(improper_ctypes_definitions)]
type PluginRegisterFn = unsafe extern "C" fn(RegisterOperatorFn, *mut c_void) -> bool;
#[allow(improper_ctypes_definitions)]
type RegisterOperatorFn = unsafe extern "C" fn(*const c_char, OperatorFactory, *mut c_void);

struct PluginContext {
    builder: *mut ZiCPipelineBuilder,
    error: Option<ZiError>,
}

#[allow(improper_ctypes_definitions)]
unsafe extern "C" fn register_operator_callback(
    name: *const c_char,
    factory: OperatorFactory,
    user_data: *mut c_void,
) {
    let ctx = &mut *(user_data as *mut PluginContext);

    if name.is_null() {
        ctx.error = Some(ZiError::validation("plugin registered null operator name"));
        return;
    }

    let c_str = match CStr::from_ptr(name).to_str() {
        Ok(value) => value,
        Err(err) => {
            ctx.error = Some(ZiError::validation(format!(
                "plugin provided invalid UTF-8 operator name: {err}"
            )));
            return;
        }
    };

    if let Some(existing) = (*ctx.builder).factories.get(c_str) {
        // Avoid replacing existing operators; signal validation error.
        let _ = existing;
        ctx.error = Some(ZiError::validation(format!(
            "operator '{c_str}' already registered"
        )));
        return;
    }

    (*ctx.builder).register(c_str.to_string(), factory);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::ZiCRecord;
    use serde_json::json;

    #[test]
    fn run_applies_stages_in_order() {
        #[derive(Debug)]
        struct Append(String);

        impl ZiCOperator for Append {
            fn name(&self) -> &'static str {
                "append"
            }

            fn apply(&self, mut batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
                for record in &mut batch {
                    let mut payload = record
                        .payload
                        .as_str()
                        .map(|s| s.to_string())
                        .ok_or_else(|| ZiError::validation("payload must be string"))?;
                    payload.push_str(&self.0);
                    record.payload = json!(payload);
                }
                Ok(batch)
            }
        }

        let pipeline = ZiCPipeline::new(vec![
            Box::new(Append("-one".into())),
            Box::new(Append("-two".into())),
        ]);

        let batch = vec![ZiCRecord::ZiFNew(None, json!("start"))];
        let output = pipeline.run(batch).unwrap();

        assert_eq!(output[0].payload, json!("start-one-two"));
    }

    #[test]
    fn validate_fails_when_empty() {
        let pipeline = ZiCPipeline::new(Vec::new());
        let err = pipeline.validate().unwrap_err();

        match err {
            ZiError::Pipeline { stage, message } => {
                assert_eq!(stage, "pipeline");
                assert_eq!(message, "no stages configured");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn builder_with_defaults_builds_pipeline() {
        let builder = ZiCPipelineBuilder::with_defaults();
        let config = json!([
            {
                "operator": "metadata.enrich",
                "config": {"entries": {"source": "test", "temp": true, "optional": null, "tags": ["primary", "beta"], "description": "primary dataset"}}
            },
            {
                "operator": "metadata.rename",
                "config": {"keys": {"source": "origin"}}
            },
            {
                "operator": "metadata.remove",
                "config": {"keys": ["temp"]}
            },
            {
                "operator": "metadata.copy",
                "config": {"keys": {"origin": "origin_backup"}}
            },
            {
                "operator": "metadata.require",
                "config": {"keys": ["origin", "origin_backup"]}
            },
            {
                "operator": "metadata.extract",
                "config": {"keys": {"payload.keep": "keep_flag"}}
            },
            {
                "operator": "metadata.keep",
                "config": {"keys": ["origin", "origin_backup", "keep_flag", "optional", "tags", "description"]}
            },
            {
                "operator": "filter.length_range",
                "config": {"path": "payload.text", "min": 5, "max": 20}
            },
            {
                "operator": "filter.range",
                "config": {"path": "payload.score", "min": 0.4, "max": 1.0}
            },
            {
                "operator": "filter.in",
                "config": {"path": "metadata.keep_flag", "values": [true]}
            },
            {
                "operator": "filter.array_contains",
                "config": {"path": "metadata.tags", "element": "primary"}
            },
            {
                "operator": "filter.not_in",
                "config": {"path": "metadata.origin", "values": ["blocked", "spam"]}
            },
            {
                "operator": "filter.starts_with",
                "config": {"path": "payload.text", "prefix": "hell"}
            },
            {
                "operator": "filter.ends_with",
                "config": {"path": "payload.text", "suffix": "ld"}
            },
            {
                "operator": "filter.regex",
                "config": {"path": "payload.text", "pattern": "^hell"}
            },
            {
                "operator": "filter.greater_than",
                "config": {"path": "payload.score", "threshold": 0.6}
            },
            {
                "operator": "filter.token_range",
                "config": {"path": "payload.text", "min": 2, "max": 4}
            },
            {
                "operator": "filter.any",
                "config": {"paths": ["payload.text", "metadata.origin"], "equals": "hello world"}
            },
            {
                "operator": "filter.not_equals",
                "config": {"path": "metadata.origin", "equals": "blocked"}
            },
            {
                "operator": "filter.is_null",
                "config": {"path": "metadata.optional", "include_missing": false}
            },
            {
                "operator": "filter.not_exists",
                "config": {"path": "metadata.optional"}
            },
            {
                "operator": "filter.exists",
                "config": {"path": "metadata.keep_flag"}
            },
            {
                "operator": "filter.contains",
                "config": {"path": "metadata.origin", "contains": "te"}
            },
            {
                "operator": "filter.contains_all",
                "config": {"path": "metadata.tags", "contains_all": ["primary", "beta"]}
            },
            {
                "operator": "filter.contains_any",
                "config": {"path": "metadata.tags", "contains_any": ["primary", "seed"]}
            },
            {
                "operator": "filter.contains_none",
                "config": {"path": "metadata.description", "contains_none": ["blocked", "deprecated"]}
            },
            {
                "operator": "filter.equals",
                "config": {"path": "payload.keep", "equals": true}
            },
            {"operator": "limit", "config": {"count": 1}}
        ]);

        let pipeline = builder
            .build_from_config(config.as_array().unwrap())
            .unwrap();
        let batch = vec![
            ZiCRecord::ZiFNew(
                Some("1".into()),
                json!({
                    "keep": true,
                    "score": 0.7,
                    "text": "hello world",
                    "lang": "en"
                }),
            ),
            ZiCRecord::ZiFNew(
                Some("2".into()),
                json!({
                    "keep": false,
                    "score": 0.8,
                    "text": "short",
                    "lang": "en"
                }),
            ),
            ZiCRecord::ZiFNew(
                Some("3".into()),
                json!({
                    "keep": true,
                    "score": 0.9,
                    "text": "hello universe",
                    "lang": "en"
                }),
            ),
        ];
        let output = pipeline.run(batch).unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].id.as_deref(), Some("1"));
        let metadata = output[0].metadata.as_ref().unwrap();
        assert_eq!(metadata["origin"], json!("test"));
        assert!(metadata.get("source").is_none());
        assert!(metadata.get("temp").is_none());
        assert_eq!(metadata["origin_backup"], json!("test"));
        assert_eq!(metadata["keep_flag"], json!(true));
        assert!(metadata["optional"].is_null());
        assert_eq!(metadata["tags"], json!(["primary", "beta"]));
        assert_eq!(metadata["description"], json!("primary dataset"));
        assert!(metadata.get("extra").is_none());
    }
}
