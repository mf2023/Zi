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

use zix::{ZiDAG, ZiGraphNode, ZiNodeId, ZiGraphNodeConfig};

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
    ))
    .unwrap();

    dag.add_node(ZiGraphNode::new(
        ZiNodeId::from("b"),
        ZiGraphNodeConfig {
            name: "b".to_string(),
            operator: "test".to_string(),
            config: serde_json::json!({}),
            parallel: false,
            cache: false,
        },
    ))
    .unwrap();

    dag.add_node(ZiGraphNode::new(
        ZiNodeId::from("c"),
        ZiGraphNodeConfig {
            name: "c".to_string(),
            operator: "test".to_string(),
            config: serde_json::json!({}),
            parallel: false,
            cache: false,
        },
    ))
    .unwrap();

    dag.add_edge(ZiNodeId::from("a"), ZiNodeId::from("c")).unwrap();
    dag.add_edge(ZiNodeId::from("b"), ZiNodeId::from("c")).unwrap();

    let sorted = dag.topological_sort().unwrap();

    assert!(sorted.iter().position(|id| id == &ZiNodeId::from("a")).unwrap()
        < sorted.iter().position(|id| id == &ZiNodeId::from("c")).unwrap());
    assert!(sorted.iter().position(|id| id == &ZiNodeId::from("b")).unwrap()
        < sorted.iter().position(|id| id == &ZiNodeId::from("c")).unwrap());
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
    ))
    .unwrap();

    dag.add_node(ZiGraphNode::new(
        ZiNodeId::from("b"),
        ZiGraphNodeConfig {
            name: "b".to_string(),
            operator: "test".to_string(),
            config: serde_json::json!({}),
            parallel: false,
            cache: false,
        },
    ))
    .unwrap();

    dag.add_edge(ZiNodeId::from("a"), ZiNodeId::from("b")).unwrap();
    dag.add_edge(ZiNodeId::from("b"), ZiNodeId::from("a")).unwrap();

    assert!(dag.topological_sort().is_err());
}
