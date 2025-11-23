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

use std::collections::HashMap;
use std::ffi::{c_void, CStr};
use std::os::raw::c_char;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Map, Value};

use crate::errors::{Result, ZiError};
use crate::metrics::{ZiCQualityMetrics, ZiCStatisticSummary};
use crate::operator::{ZiCOperator, ZiFExecuteOperator};
use crate::orbit::{ZiCInProcessOrbit, ZiCOrbit, ZiFOperatorFactory};
use crate::record::ZiCRecordBatch;
use crate::version::{ZiCVersion, ZiCVersionStore, ZiFComputeDigest};
use libloading::Library;

type OperatorFactory = ZiFOperatorFactory;

/// Simple linear pipeline composed of sequential operators.
pub struct ZiCPipeline {
    stages: Vec<Box<dyn ZiCOperator + Send + Sync>>,
    cache: std::collections::HashMap<String, Vec<crate::record::ZiCRecord>>,
    instrumentation: bool,
    stage_metrics: Option<Arc<Mutex<Vec<ZiCPipelineStageMetrics>>>>,
}

impl ZiCPipeline {
    /// Constructs a pipeline from a list of operators.
    pub fn new(stages: Vec<Box<dyn ZiCOperator + Send + Sync>>) -> Self {
        ZiCPipeline {
            stages,
            cache: std::collections::HashMap::new(),
            instrumentation: false,
            stage_metrics: None,
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFWithInstrumentation(mut self, enabled: bool) -> Self {
        self.instrumentation = enabled;
        if enabled {
            self.stage_metrics = Some(Arc::new(Mutex::new(Vec::new())));
        } else {
            self.stage_metrics = None;
        }
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFStageMetrics(&self) -> Option<Vec<ZiCPipelineStageMetrics>> {
        self.stage_metrics
            .as_ref()
            .and_then(|metrics| metrics.lock().ok().map(|guard| guard.clone()))
    }

    /// Runs the pipeline, passing batches through each operator sequentially.
    pub fn run(&self, mut batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        if self.instrumentation {
            self.reset_stage_metrics();
        }

        for stage in &self.stages {
            let before = batch.len();
            let start = Instant::now();
            batch = ZiFExecuteOperator(stage.as_ref(), batch)?;
            if self.instrumentation {
                let duration = start.elapsed();
                let after = batch.len();
                self.record_stage_metric(ZiCPipelineStageMetrics::new(
                    stage.name().to_string(),
                    before,
                    after,
                    duration,
                ));
            }
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
        if self.instrumentation {
            self.reset_stage_metrics();
        }
        for stage in &self.stages {
            let before = batch.len();
            let start = Instant::now();
            batch = ZiFExecuteOperator(stage.as_ref(), batch)?;
            let after = batch.len();
            let duration = start.elapsed();
            progress(stage.name(), before, after);
            if self.instrumentation {
                self.record_stage_metric(ZiCPipelineStageMetrics::new(
                    stage.name().to_string(),
                    before,
                    after,
                    duration,
                ));
            }
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

    /// Executes the pipeline on multiple chunks concurrently.
    #[allow(non_snake_case)]
    pub fn ZiFRunParallel(
        &self,
        batch: ZiCRecordBatch,
        num_workers: usize,
    ) -> Result<ZiCRecordBatch> {
        if num_workers == 0 {
            return Err(ZiError::validation(
                "parallel execution requires at least one worker",
            ));
        }
        if batch.len() <= 1 || num_workers == 1 || self.stages.len() <= 1 {
            return self.run(batch);
        }

        let chunk_size = (batch.len().max(1) + num_workers - 1) / num_workers;
        let mut chunks: Vec<ZiCRecordBatch> = Vec::new();
        let mut current = Vec::with_capacity(chunk_size);
        for record in batch {
            current.push(record);
            if current.len() == chunk_size {
                chunks.push(std::mem::take(&mut current));
                current = Vec::with_capacity(chunk_size);
            }
        }
        if !current.is_empty() {
            chunks.push(current);
        }

        if chunks.len() == 1 {
            return self.run(chunks.pop().unwrap());
        }

        let mut results = Vec::with_capacity(chunks.len());
        thread::scope(|scope| -> Result<()> {
            let stages = &self.stages;
            let mut handles = Vec::with_capacity(chunks.len());
            for (idx, chunk) in chunks.into_iter().enumerate() {
                let stage_refs = stages;
                handles.push(scope.spawn(move || -> Result<(usize, ZiCRecordBatch)> {
                    let mut local = chunk;
                    for stage in stage_refs {
                        local = ZiFExecuteOperator(stage.as_ref(), local)?;
                    }
                    Ok((idx, local))
                }));
            }

            for handle in handles {
                let pair = handle
                    .join()
                    .map_err(|_| ZiError::internal("parallel execution worker panicked"))?;
                match pair {
                    Ok(pair) => results.push(pair),
                    Err(err) => return Err(err),
                }
            }

            Ok(())
        })?;

        results.sort_by_key(|(idx, _)| *idx);
        let mut merged = Vec::new();
        for (_, chunk) in results {
            merged.extend(chunk);
        }
        Ok(merged)
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

    /// Runs the pipeline and records a version snapshot in the provided store.
    #[allow(non_snake_case)]
    pub fn ZiFRunWithVersion(
        &self,
        batch: ZiCRecordBatch,
        store: &mut ZiCVersionStore,
        parent: Option<&str>,
        mut metadata: Map<String, Value>,
    ) -> Result<(ZiCRecordBatch, ZiCVersion)> {
        let processed = if self.instrumentation {
            let (records, stage_metrics) = self.run_with_stage_metrics(batch)?;
            let stage_values: Vec<Value> = stage_metrics.iter().map(|m| m.to_value()).collect();
            let durations: Vec<f64> = stage_metrics
                .iter()
                .map(|m| m.duration.as_secs_f64() * 1000.0)
                .collect();
            let summary = ZiCStatisticSummary::from_slice(&durations);
            metadata.insert("stage_metrics".into(), Value::Array(stage_values));
            metadata.insert(
                "stage_timing_ms".into(),
                serde_json::to_value(summary).unwrap_or(Value::Null),
            );
            records
        } else {
            self.run(batch)?
        };
        let metrics = ZiCQualityMetrics::ZiFCompute(&processed);
        let digest = ZiFComputeDigest(&processed);

        if !metadata.contains_key("stages") {
            let stage_names: Vec<Value> = self
                .stages
                .iter()
                .map(|stage| Value::String(stage.name().to_string()))
                .collect();
            metadata.insert("stages".into(), Value::Array(stage_names));
        }

        metadata
            .entry("record_count".to_string())
            .or_insert_with(|| Value::from(processed.len() as u64));
        metadata
            .entry("digest".to_string())
            .or_insert_with(|| Value::String(digest.clone()));

        let version = store.ZiFCreate(parent, metadata, metrics, digest)?;
        Ok((processed, version))
    }

    fn run_with_stage_metrics(
        &self,
        mut batch: ZiCRecordBatch,
    ) -> Result<(ZiCRecordBatch, Vec<ZiCPipelineStageMetrics>)> {
        let mut stage_metrics = Vec::with_capacity(self.stages.len());
        for stage in &self.stages {
            let before = batch.len();
            let start = Instant::now();
            batch = ZiFExecuteOperator(stage.as_ref(), batch)?;
            let duration = start.elapsed();
            let after = batch.len();
            let metric =
                ZiCPipelineStageMetrics::new(stage.name().to_string(), before, after, duration);
            self.record_stage_metric(metric.clone());
            stage_metrics.push(metric);
        }
        Ok((batch, stage_metrics))
    }

    fn reset_stage_metrics(&self) {
        if let Some(metrics) = &self.stage_metrics {
            if let Ok(mut guard) = metrics.lock() {
                guard.clear();
            }
        }
    }

    fn record_stage_metric(&self, metric: ZiCPipelineStageMetrics) {
        if let Some(metrics) = &self.stage_metrics {
            if let Ok(mut guard) = metrics.lock() {
                guard.push(metric);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ZiCPipelineStageMetrics {
    pub stage_name: String,
    pub input_records: usize,
    pub output_records: usize,
    pub duration: Duration,
}

impl ZiCPipelineStageMetrics {
    pub fn new(
        stage_name: String,
        input_records: usize,
        output_records: usize,
        duration: Duration,
    ) -> Self {
        Self {
            stage_name,
            input_records,
            output_records,
            duration,
        }
    }

    pub fn to_value(&self) -> Value {
        Value::Object(Map::from_iter([
            ("stage".to_string(), Value::String(self.stage_name.clone())),
            (
                "input".to_string(),
                Value::Number(self.input_records.into()),
            ),
            (
                "output".to_string(),
                Value::Number(self.output_records.into()),
            ),
            (
                "duration_millis".to_string(),
                Value::Number(
                    serde_json::Number::from_f64(self.duration.as_secs_f64() * 1000.0)
                        .unwrap_or_else(|| serde_json::Number::from(0)),
                ),
            ),
        ]))
    }
}

pub struct ZiCOrbitPipelineStep {
    operator_name: String,
    config: Value,
}

pub struct ZiCOrbitPipeline {
    plugin_id: String,
    steps: Vec<ZiCOrbitPipelineStep>,
}

impl ZiCOrbitPipeline {
    pub fn run(
        &self,
        orbit: &mut ZiCInProcessOrbit,
        metrics: &mut ZiCQualityMetrics,
        version_store: Option<&mut ZiCVersionStore>,
        mut batch: ZiCRecordBatch,
    ) -> Result<ZiCRecordBatch> {
        let mut ctx = orbit.ZiFMakeExecutionContext(&self.plugin_id, metrics, version_store)?;
        for step in &self.steps {
            batch = orbit.ZiFCallOperator(
                &self.plugin_id,
                &step.operator_name,
                batch,
                &step.config,
                &mut ctx,
            )?;
        }
        Ok(batch)
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

    /// Register all known operator factories into the provided ZiOrbit
    /// runtime. This allows the in-process VM and the direct pipeline builder
    /// to share a single source of truth for available operators.
    #[allow(non_snake_case)]
    pub fn ZiFRegisterOperatorsIntoOrbit(&self, orbit: &mut ZiCInProcessOrbit) {
        for (name, factory) in &self.factories {
            orbit.ZiFRegisterOperator(name, *factory);
        }
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

    #[allow(non_snake_case)]
    pub fn ZiFBuildOrbitPipeline(
        &self,
        plugin_id: impl Into<String>,
        steps: &[Value],
    ) -> Result<ZiCOrbitPipeline> {
        let mut orbit_steps = Vec::with_capacity(steps.len());
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

            if !self.factories.contains_key(operator_name) {
                return Err(ZiError::validation(format!(
                    "unknown operator '{operator_name}'"
                )));
            }

            let config_value = object.get("config").cloned().unwrap_or(Value::Null);
            orbit_steps.push(ZiCOrbitPipelineStep {
                operator_name: operator_name.to_string(),
                config: config_value,
            });
        }

        if orbit_steps.is_empty() {
            return Err(ZiError::pipeline("orbit_pipeline", "no stages configured"));
        }

        Ok(ZiCOrbitPipeline {
            plugin_id: plugin_id.into(),
            steps: orbit_steps,
        })
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
