//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
//!
//! This file is part of Zi.
//! The Zi project belongs to the Dunimd project team.

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
