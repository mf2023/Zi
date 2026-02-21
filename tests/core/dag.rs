//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
//!
//! This file is part of Zi.
//! The Zi project belongs to the Dunimd project team.

use zi::dag::*;

#[test]
fn test_dag_topological_sort() {
    let mut dag = ZiCDAG::ZiFNew();

    dag.ZiFAddNode(ZiCGraphNode::ZiFNew(
        ZiCNodeId::from("a"),
        ZiCGraphNodeConfig {
            name: "a".to_string(),
            operator: "test".to_string(),
            config: serde_json::json!({}),
            parallel: false,
            cache: false,
        },
    ))
    .unwrap();

    dag.ZiFAddNode(ZiCGraphNode::ZiFNew(
        ZiCNodeId::from("b"),
        ZiCGraphNodeConfig {
            name: "b".to_string(),
            operator: "test".to_string(),
            config: serde_json::json!({}),
            parallel: false,
            cache: false,
        },
    ))
    .unwrap();

    dag.ZiFAddNode(ZiCGraphNode::ZiFNew(
        ZiCNodeId::from("c"),
        ZiCGraphNodeConfig {
            name: "c".to_string(),
            operator: "test".to_string(),
            config: serde_json::json!({}),
            parallel: false,
            cache: false,
        },
    ))
    .unwrap();

    dag.ZiFAddEdge(ZiCNodeId::from("a"), ZiCNodeId::from("c")).unwrap();
    dag.ZiFAddEdge(ZiCNodeId::from("b"), ZiCNodeId::from("c")).unwrap();

    let sorted = dag.ZiFTopologicalSort().unwrap();

    assert!(sorted.iter().position(|id| id == &ZiCNodeId::from("a")).unwrap()
        < sorted.iter().position(|id| id == &ZiCNodeId::from("c")).unwrap());
    assert!(sorted.iter().position(|id| id == &ZiCNodeId::from("b")).unwrap()
        < sorted.iter().position(|id| id == &ZiCNodeId::from("c")).unwrap());
}

#[test]
fn test_dag_cycle_detection() {
    let mut dag = ZiCDAG::ZiFNew();

    dag.ZiFAddNode(ZiCGraphNode::ZiFNew(
        ZiCNodeId::from("a"),
        ZiCGraphNodeConfig {
            name: "a".to_string(),
            operator: "test".to_string(),
            config: serde_json::json!({}),
            parallel: false,
            cache: false,
        },
    ))
    .unwrap();

    dag.ZiFAddNode(ZiCGraphNode::ZiFNew(
        ZiCNodeId::from("b"),
        ZiCGraphNodeConfig {
            name: "b".to_string(),
            operator: "test".to_string(),
            config: serde_json::json!({}),
            parallel: false,
            cache: false,
        },
    ))
    .unwrap();

    dag.ZiFAddEdge(ZiCNodeId::from("a"), ZiCNodeId::from("b")).unwrap();
    dag.ZiFAddEdge(ZiCNodeId::from("b"), ZiCNodeId::from("a")).unwrap();

    assert!(dag.ZiFTopologicalSort().is_err());
}
