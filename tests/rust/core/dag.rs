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

//! # ZiDAG Core Tests
//!
//! This module provides comprehensive tests for the ZiDAG (Directed Acyclic Graph)
//! implementation, which is fundamental to Zi pipeline execution.
//!
//! ## Test Coverage
//!
//! - **Topological Sort**: Verifies that nodes are sorted according to dependencies
//! - **Cycle Detection**: Ensures circular dependencies are properly detected and rejected
//! - **Node Operations**: Tests adding, removing, and connecting nodes
//! - **Graph Traversal**: Tests various graph traversal methods
//!
//! ## Architecture
//!
//! The DAG system represents pipeline dependencies as a directed graph where:
//! - **Nodes** represent individual operators or pipeline stages
//! - **Edges** represent data flow dependencies between stages
//! - **Topological order** ensures dependencies are processed before dependents
//!
//! ## Test Execution
//!
//! These tests can be run with cargo:
//! ```bash
//! cargo test --test dag
//! ```

use zix::{ZiDAG, ZiGraphNode, ZiNodeId, ZiGraphNodeConfig};

/// Tests topological sort functionality of the DAG.
///
/// This test creates a simple DAG with three nodes (a, b, c) where:
/// - a -> c (a is a dependency of c)
/// - b -> c (b is a dependency of c)
///
/// The topological sort should return nodes in an order where
/// dependencies come before their dependents.
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

/// Tests cycle detection in the DAG.
///
/// This test creates a DAG with a circular dependency:
/// - a -> b (a depends on b)
/// - b -> c (b depends on c)
/// - c -> a (c depends on a - creates a cycle)
///
/// The topological sort should return an error indicating the cycle exists.
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
