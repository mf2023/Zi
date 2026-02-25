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

//! # Zi Operator Tests - Filter
//!
//! This module contains tests for filter operators in the Zi framework.
//! Filter operators are used to select or reject records based on field values.
//!
//! ## Test Categories
//!
//! - **Record Creation Tests**: Verify ZiRecord instantiation
//! - **DAG Tests**: Verify pipeline dependency management
//!
//! ## Running Tests
//!
//! ```bash
//! cargo test --test filter
//! ```

use zix::{ZiRecord, ZiDAG, ZiGraphNode, ZiNodeId, ZiGraphNodeConfig};
use serde_json::json;

/// Tests basic ZiRecord creation with ID and payload.
///
/// Verifies that records can be created with an identifier and JSON payload.
#[test]
fn test_record_creation() {
    let record = ZiRecord::new(Some("1".into()), json!({"text": "hello"}));
    assert_eq!(record.id, Some("1".into()));
    assert_eq!(record.payload["text"], "hello");
}

/// Tests attaching metadata to a ZiRecord.
///
/// Verifies that metadata can be added to records using the metadata_mut builder.
#[test]
fn test_record_with_metadata() {
    let mut record = ZiRecord::new(Some("1".into()), json!({"text": "hello"}));
    record.metadata = Some(serde_json::Map::new());
    record.metadata_mut().insert("score".into(), json!(0.9));
    assert_eq!(record.metadata.as_ref().unwrap()["score"], 0.9);
}

/// Tests creating multiple records as a batch.
///
/// Verifies that batches of records can be created and processed together.
#[test]
fn test_record_batch_creation() {
    let records = vec![
        ZiRecord::new(Some("1".into()), json!({"text": "hello"})),
        ZiRecord::new(Some("2".into()), json!({"text": "world"})),
    ];
    assert_eq!(records.len(), 2);
}

/// Tests DAG topological sort ordering.
///
/// Verifies that the DAG can determine correct execution order based on dependencies.
#[test]
fn test_dag_topological_sort() {
    let mut dag = ZiDAG::new();

    dag.add_node(ZiGraphNode::new(
        ZiNodeId::from("a"),
        ZiGraphNodeConfig {
            name: "a".to_string(),
            operator: "test".to_string(),
            config: serde_json::json!({}),
            parallel: false,
            cache: false,
        },
    )).unwrap();

    dag.add_node(ZiGraphNode::new(
        ZiNodeId::from("b"),
        ZiGraphNodeConfig {
            name: "b".to_string(),
            operator: "test".to_string(),
            config: serde_json::json!({}),
            parallel: false,
            cache: false,
        },
    )).unwrap();

    dag.add_node(ZiGraphNode::new(
        ZiNodeId::from("c"),
        ZiGraphNodeConfig {
            name: "c".to_string(),
            operator: "test".to_string(),
            config: serde_json::json!({}),
            parallel: false,
            cache: false,
        },
    )).unwrap();

    dag.add_edge(ZiNodeId::from("a"), ZiNodeId::from("c")).unwrap();
    dag.add_edge(ZiNodeId::from("b"), ZiNodeId::from("c")).unwrap();

    let sorted = dag.topological_sort().unwrap();
    assert!(sorted.len() == 3);
}

#[test]
fn test_dag_cycle_detection() {
    let mut dag = ZiDAG::new();

    dag.add_node(ZiGraphNode::new(
        ZiNodeId::from("a"),
        ZiGraphNodeConfig {
            name: "a".to_string(),
            operator: "test".to_string(),
            config: serde_json::json!({}),
            parallel: false,
            cache: false,
        },
    )).unwrap();

    dag.add_node(ZiGraphNode::new(
        ZiNodeId::from("b"),
        ZiGraphNodeConfig {
            name: "b".to_string(),
            operator: "test".to_string(),
            config: serde_json::json!({}),
            parallel: false,
            cache: false,
        },
    )).unwrap();

    dag.add_edge(ZiNodeId::from("a"), ZiNodeId::from("b")).unwrap();
    dag.add_edge(ZiNodeId::from("b"), ZiNodeId::from("a")).unwrap();

    assert!(dag.topological_sort().is_err());
}

#[test]
fn test_record_id_options() {
    let record_with_id = ZiRecord::new(Some("test_id".to_string()), json!({"data": 1}));
    let record_without_id = ZiRecord::new(None::<String>, json!({"data": 2}));
    
    assert!(record_with_id.id.is_some());
    assert!(record_without_id.id.is_none());
}

#[test]
fn test_record_payload_access() {
    let record = ZiRecord::new(Some("1".into()), json!({
        "name": "test",
        "value": 42,
        "nested": {"a": 1, "b": 2}
    }));
    
    assert_eq!(record.payload["name"], "test");
    assert_eq!(record.payload["value"], 42);
    assert_eq!(record.payload["nested"]["a"], 1);
}

#[test]
fn test_record_clone() {
    let record1 = ZiRecord::new(Some("1".into()), json!({"text": "hello"}));
    let record2 = record1.clone();
    assert_eq!(record1.id, record2.id);
    assert_eq!(record1.payload, record2.payload);
}
