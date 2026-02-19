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

use serde_json::{json, Value};
use Zi::errors::ZiError;
use Zi::orbit::runtime::{
    ZiCDataVisibility,
    ZiCInProcessOrbit,
    ZiCPluginDescriptor,
    ZiCPluginPolicy,
    ZiCPluginState,
    ZiCPluginVersion,
};
use Zi::orbit::ZiCPluginDescriptor as PubPluginDescriptor;
use Zi::pipeline::{
    OperatorFactory,
    ZiCOperator,
    ZiCOperatorRegistry,
    ZiCPipeline,
    ZiCPipelineBuilder,
    ZiCRecordBatch,
    ZiCVersionStore,
};
use Zi::record::ZiCRecord;
use Zi::metrics::ZiCQualityMetrics;
use serde_json::Map;

#[derive(Debug)]
struct ZiCTAppend(String);

impl ZiCOperator for ZiCTAppend {
    fn name(&self) -> &'static str {
        "append"
    }

    fn apply(&self, mut batch: ZiCRecordBatch) -> Zi::errors::Result<ZiCRecordBatch> {
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

fn ZiCTAppendFactory(config: &Value) -> Zi::errors::Result<Box<dyn ZiCOperator + Send + Sync>> {
    let suffix = config
        .get("suffix")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    Ok(Box::new(ZiCTAppend(suffix)))
}

#[test]
fn ZiFTPipelineRunAppliesStagesInOrder() {
    let pipeline = ZiCPipeline::new(vec![
        Box::new(ZiCTAppend("-one".into())),
        Box::new(ZiCTAppend("-two".into())),
    ]);

    let batch = vec![ZiCRecord::ZiFNew(None, json!("start"))];
    let output = pipeline.run(batch).unwrap();

    assert_eq!(output[0].payload, json!("start-one-two"));
}

#[test]
fn ZiFTPipelineValidateFailsWhenEmpty() {
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
fn ZiFTPipelineRunParallelAppliesAllStages() {
    #[derive(Debug)]
    struct ZiCTTagger;

    impl ZiCOperator for ZiCTTagger {
        fn name(&self) -> &'static str {
            "tagger"
        }

        fn apply(&self, mut batch: ZiCRecordBatch) -> Zi::errors::Result<ZiCRecordBatch> {
            for (index, record) in batch.iter_mut().enumerate() {
                record
                    .ZiFMetadataMut()
                    .insert("worker".into(), json!(index as i64));
            }
            Ok(batch)
        }
    }

    let pipeline = ZiCPipeline::new(vec![Box::new(ZiCTTagger)]);
    let mut input = Vec::new();
    for i in 0..16 {
        input.push(ZiCRecord::ZiFNew(Some(format!("{i}")), json!({"value": i})));
    }

    let output = pipeline.ZiFRunParallel(input.clone(), 4).unwrap();
    assert_eq!(output.len(), input.len());
    for record in output {
        assert!(record.metadata.as_ref().unwrap().contains_key("worker"));
    }
}

#[test]
fn ZiFTPipelineRunParallelRejectsZeroWorkers() {
    let pipeline = ZiCPipeline::new(Vec::new());
    let result = pipeline.ZiFRunParallel(Vec::new(), 0);
    assert!(result.is_err());
}

#[test]
fn ZiFTPipelineRunWithVersionCreatesSnapshot() {
    #[derive(Debug)]
    struct ZiCTIdentity;

    impl ZiCOperator for ZiCTIdentity {
        fn name(&self) -> &'static str {
            "identity"
        }

        fn apply(&self, batch: ZiCRecordBatch) -> Zi::errors::Result<ZiCRecordBatch> {
            Ok(batch)
        }
    }

    let pipeline = ZiCPipeline::new(vec![Box::new(ZiCTIdentity)]);
    let mut store = ZiCVersionStore::ZiFNew();
    let batch = vec![ZiCRecord::ZiFNew(Some("id".into()), json!({"text": "hello"}))];
    let mut metadata = Map::new();
    metadata.insert("source".into(), json!("test"));

    let (processed, version) = pipeline
        .ZiFRunWithVersion(batch, &mut store, None, metadata)
        .unwrap();

    assert_eq!(processed.len(), 1);
    let stored = store.ZiFGet(&version.id).expect("version stored");
    assert!(stored.metadata.contains_key("stages"));
    assert_eq!(stored.metadata.get("record_count").unwrap(), &json!(1));
}

#[test]
fn ZiFTOrbitPipelineExecutesStepsViaOrbitRuntime() {
    let mut builder = ZiCPipelineBuilder::new();
    builder.register("append", ZiCTAppendFactory as OperatorFactory);

    let steps = vec![
        json!({"operator": "append", "config": {"suffix": "-one"}}),
        json!({"operator": "append", "config": {"suffix": "-two"}}),
    ];

    let orbit_pipeline = builder
        .ZiFBuildOrbitPipeline("test.plugin", &steps)
        .expect("build orbit pipeline");

    let mut orbit = ZiCInProcessOrbit::ZiFNew();
    builder.ZiFRegisterOperatorsIntoOrbit(&mut orbit);

    let descriptor = ZiCPluginDescriptor {
        id: "test.plugin".to_string(),
        version: ZiCPluginVersion::parse("1.0.0").unwrap(),
        exports: Vec::new(),
        policy: ZiCPluginPolicy {
            allowed_capabilities: Vec::new(),
            can_access_versions: false,
            default_visibility: ZiCDataVisibility::Full,
        },
        dependencies: vec![],
        state: ZiCPluginState::Loaded,
        load_time: std::time::SystemTime::now(),
    };
    orbit.ZiFRegisterPlugin(descriptor);

    let mut metrics = ZiCQualityMetrics::default();
    let batch = vec![ZiCRecord::ZiFNew(None, json!("start"))];

    let output = orbit_pipeline
        .run(&mut orbit, &mut metrics, None, batch)
        .expect("orbit pipeline run");
    assert_eq!(output[0].payload, json!("start-one-two"));
}
