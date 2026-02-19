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

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::errors::{Result, ZiError};
use crate::operator::ZiCOperator;
use crate::record::ZiCRecord;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct ZiCNodeId(pub String);

impl fmt::Display for ZiCNodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for ZiCNodeId {
    fn from(s: &str) -> Self {
        ZiCNodeId(s.to_string())
    }
}

impl From<String> for ZiCNodeId {
    fn from(s: String) -> Self {
        ZiCNodeId(s)
    }
}

#[derive(Clone, Debug)]
pub struct ZiCGraphNodeConfig {
    pub name: String,
    pub operator: String,
    pub config: serde_json::Value,
    pub parallel: bool,
    pub cache: bool,
}

#[derive(Clone, Debug)]
pub struct ZiCGraphNode {
    pub id: ZiCNodeId,
    pub config: ZiCGraphNodeConfig,
    pub dependencies: Vec<ZiCNodeId>,
}

impl ZiCGraphNode {
    #[allow(non_snake_case)]
    pub fn ZiFNew(id: ZiCNodeId, config: ZiCGraphNodeConfig) -> Self {
        Self {
            id,
            config,
            dependencies: Vec::new(),
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFAddDependency(&mut self, dep: ZiCNodeId) {
        if !self.dependencies.contains(&dep) {
            self.dependencies.push(dep);
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ZiCDAG {
    pub nodes: HashMap<ZiCNodeId, ZiCGraphNode>,
    pub entry_points: Vec<ZiCNodeId>,
    pub exit_points: Vec<ZiCNodeId>,
}

impl ZiCDAG {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        Self::default()
    }

    #[allow(non_snake_case)]
    pub fn ZiFAddNode(&mut self, node: ZiCGraphNode) -> Result<()> {
        if self.nodes.contains_key(&node.id) {
            return Err(ZiError::validation(format!(
                "node '{}' already exists",
                node.id
            )));
        }
        self.nodes.insert(node.id.clone(), node);
        Ok(())
    }

    #[allow(non_snake_case)]
    pub fn ZiFAddEdge(&mut self, from: ZiCNodeId, to: ZiCNodeId) -> Result<()> {
        if !self.nodes.contains_key(&from) {
            return Err(ZiError::validation(format!(
                "source node '{}' not found",
                from
            )));
        }
        if !self.nodes.contains_key(&to) {
            return Err(ZiError::validation(format!(
                "target node '{}' not found",
                to
            )));
        }

        self.nodes
            .get_mut(&to)
            .ok_or_else(|| ZiError::validation("node not found".to_string()))?
            .ZiFAddDependency(from.clone());

        if !self.entry_points.contains(&to) {
            self.entry_points.push(to);
        }

        if self.nodes.get(&from).map(|n| n.dependencies.is_empty()).unwrap_or(false)
            && !self.entry_points.contains(&from)
        {
            self.entry_points.push(from.clone());
        }

        Ok(())
    }

    #[allow(non_snake_case)]
    pub fn ZiFTopologicalSort(&self) -> Result<Vec<ZiCNodeId>> {
        let mut in_degree: HashMap<ZiCNodeId, usize> = self
            .nodes
            .keys()
            .map(|id| (id.clone(), 0))
            .collect();

        for node in self.nodes.values() {
            for dep in &node.dependencies {
                if let Some(count) = in_degree.get_mut(dep) {
                    *count += 1;
                }
            }
        }

        let mut queue: VecDeque<ZiCNodeId> = in_degree
            .iter()
            .filter(|(_, &count)| count == 0)
            .map(|(id, _)| id.clone())
            .collect();

        let mut sorted: Vec<ZiCNodeId> = Vec::new();

        while let Some(node_id) = queue.pop_front() {
            sorted.push(node_id.clone());

            for (target_id, node) in &self.nodes {
                if node.dependencies.contains(&node_id) {
                    if let Some(degree) = in_degree.get_mut(target_id) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push_back(target_id.clone());
                        }
                    }
                }
            }
        }

        if sorted.len() != self.nodes.len() {
            let cycle_nodes: Vec<_> = self
                .nodes
                .keys()
                .filter(|id| !sorted.contains(id))
                .collect();
            return Err(ZiError::validation(format!(
                "cycle detected involving nodes: {:?}",
                cycle_nodes
            )));
        }

        Ok(sorted)
    }

    #[allow(non_snake_case)]
    pub fn ZiFDetectCycles(&self) -> bool {
        self.ZiFTopologicalSort().is_err()
    }

    #[allow(non_snake_case)]
    pub fn ZiFGetParallelGroups(&self, sorted: &[ZiCNodeId]) -> Vec<Vec<ZiCNodeId>> {
        let mut groups: Vec<Vec<ZiCNodeId>> = Vec::new();
        let mut current_group: Vec<ZiCNodeId> = Vec::new();
        let mut processed_deps: HashSet<ZiCNodeId> = HashSet::new();

        for node_id in sorted {
            let node = match self.nodes.get(node_id) {
                Some(n) => n,
                None => continue,
            };

            let all_deps_processed = node
                .dependencies
                .iter()
                .all(|dep| processed_deps.contains(dep));

            if all_deps_processed {
                current_group.push(node_id.clone());
            } else {
                if !current_group.is_empty() {
                    groups.push(current_group);
                    current_group = Vec::new();
                }
                current_group.push(node_id.clone());
            }

            processed_deps.insert(node_id.clone());
        }

        if !current_group.is_empty() {
            groups.push(current_group);
        }

        groups
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCCheckpointState {
    pub node_id: String,
    pub record_count: usize,
    pub data_hash: String,
    pub created_at: u64,
}

#[derive(Clone, Debug, Default)]
pub struct ZiCCheckpointStore {
    states: HashMap<String, ZiCCheckpointState>,
}

impl ZiCCheckpointStore {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        Self::default()
    }

    #[allow(non_snake_case)]
    pub fn ZiFSave(&mut self, node_id: &str, record_count: usize, data_hash: String) {
        self.states.insert(
            node_id.to_string(),
            ZiCCheckpointState {
                node_id: node_id.to_string(),
                record_count,
                data_hash,
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            },
        );
    }

    #[allow(non_snake_case)]
    pub fn ZiFLoad(&self, node_id: &str) -> Option<&ZiCCheckpointState> {
        self.states.get(node_id)
    }

    #[allow(non_snake_case)]
    pub fn ZiFGetNodeStates(&self) -> HashMap<String, ZiCCheckpointState> {
        self.states.clone()
    }
}

pub struct ZiCSchedulerConfig {
    pub max_parallelism: usize,
    pub cache_enabled: bool,
    pub checkpoint_enabled: bool,
    pub retry_count: usize,
}

impl Default for ZiCSchedulerConfig {
    fn default() -> Self {
        Self {
            max_parallelism: num_cpus::get(),
            cache_enabled: true,
            checkpoint_enabled: false,
            retry_count: 3,
        }
    }
}

pub trait ZiCOperatorFactoryTrait: Send + Sync {
    fn create(&self, name: &str, config: &serde_json::Value) -> Result<Box<dyn ZiCOperator + Send + Sync>>;
}

#[allow(dead_code)]
pub struct ZiCScheduler {
    config: ZiCSchedulerConfig,
    checkpoint_store: Arc<Mutex<ZiCCheckpointStore>>,
    operator_factory: Arc<dyn ZiCOperatorFactoryTrait>,
}

impl ZiCScheduler {
    #[allow(non_snake_case)]
    pub fn ZiFNew(
        operator_factory: Arc<dyn ZiCOperatorFactoryTrait>,
    ) -> Self {
        Self {
            config: ZiCSchedulerConfig::default(),
            checkpoint_store: Arc::new(Mutex::new(ZiCCheckpointStore::ZiFNew())),
            operator_factory,
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFWithConfig(
        operator_factory: Arc<dyn ZiCOperatorFactoryTrait>,
        config: ZiCSchedulerConfig,
    ) -> Self {
        Self {
            config,
            checkpoint_store: Arc::new(Mutex::new(ZiCCheckpointStore::ZiFNew())),
            operator_factory,
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFExecute(&self, dag: &mut ZiCDAG, input: &[ZiCRecord]) -> Result<Vec<ZiCRecord>> {
        let sorted_nodes = dag.ZiFTopologicalSort()?;
        let mut node_outputs: HashMap<ZiCNodeId, Vec<ZiCRecord>> = HashMap::new();
        node_outputs.insert(ZiCNodeId::from("__input__"), input.to_vec());

        for node_id in sorted_nodes {
            let node = match dag.nodes.get(&node_id) {
                Some(n) => n.clone(),
                None => continue,
            };

            let inputs: Vec<ZiCRecord> = node
                .dependencies
                .iter()
                .flat_map(|dep| {
                    node_outputs
                        .get(dep)
                        .cloned()
                        .unwrap_or_else(Vec::new)
                })
                .collect();

            let operator = self.operator_factory.create(
                &node.config.operator,
                &node.config.config,
            )?;

            let output = operator.apply(inputs)?;
            node_outputs.insert(node_id, output);
        }

        let final_outputs: Vec<ZiCRecord> = dag
            .exit_points
            .iter()
            .flat_map(|id| node_outputs.get(id).cloned().unwrap_or_default())
            .collect();

        Ok(final_outputs)
    }

    #[allow(non_snake_case)]
    pub fn ZiFGetCheckpointStore(&self) -> Arc<Mutex<ZiCCheckpointStore>> {
        self.checkpoint_store.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
