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

//! # Zi DAG Module
//!
//! This module provides Directed Acyclic Graph (DAG) representation for managing
//! operator dependencies and execution ordering in Zi pipelines.
//!
//! ## DAG Architecture
//!
//! The DAG module enables:
//! - **Dependency Tracking**: Operators can declare dependencies on other operators
//! - **Topological Sorting**: Automatic determination of optimal execution order
//! - **Parallel Execution**: Independent operators can run concurrently
//! - **Cycle Detection**: Prevents invalid pipeline configurations
//! - **Checkpointing**: State preservation for recovery
//!
//! ## Usage
//!
//! ```rust
//! use zi::dag::{ZiDAG, ZiNodeId};
//!
//! // Build a DAG
//! let mut dag = ZiDAG::new();
//! dag.add_node("read", operator_a)?;
//! dag.add_node("transform", operator_b)?;
//! dag.add_dependency("read", "transform")?;
//!
//! // Execute in topological order
//! let order = dag.topological_sort()?;
//! ```

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::errors::{Result, ZiError};
use crate::operator::ZiOperator;
use crate::record::ZiRecord;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct ZiNodeId(pub String);

impl fmt::Display for ZiNodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for ZiNodeId {
    fn from(s: &str) -> Self {
        ZiNodeId(s.to_string())
    }
}

impl From<String> for ZiNodeId {
    fn from(s: String) -> Self {
        ZiNodeId(s)
    }
}

#[derive(Clone, Debug)]
pub struct ZiGraphNodeConfig {
    pub name: String,
    pub operator: String,
    pub config: serde_json::Value,
    pub parallel: bool,
    pub cache: bool,
}

#[derive(Clone, Debug)]
pub struct ZiGraphNode {
    pub id: ZiNodeId,
    pub config: ZiGraphNodeConfig,
    pub dependencies: Vec<ZiNodeId>,
}

impl ZiGraphNode {
    #[allow(non_snake_case)]
    pub fn new(id: ZiNodeId, config: ZiGraphNodeConfig) -> Self {
        Self {
            id,
            config,
            dependencies: Vec::new(),
        }
    }

    #[allow(non_snake_case)]
    pub fn add_dependency(&mut self, dep: ZiNodeId) {
        if !self.dependencies.contains(&dep) {
            self.dependencies.push(dep);
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ZiDAG {
    pub nodes: HashMap<ZiNodeId, ZiGraphNode>,
    pub entry_points: Vec<ZiNodeId>,
    pub exit_points: Vec<ZiNodeId>,
}

impl ZiDAG {
    #[allow(non_snake_case)]
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(non_snake_case)]
    pub fn add_node(&mut self, node: ZiGraphNode) -> Result<()> {
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
    pub fn add_edge(&mut self, from: ZiNodeId, to: ZiNodeId) -> Result<()> {
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
            .add_dependency(from.clone());

        if self.nodes.get(&from).map(|n| n.dependencies.is_empty()).unwrap_or(false)
            && !self.entry_points.contains(&from)
        {
            self.entry_points.push(from.clone());
        }

        self.recompute_exit_points();

        Ok(())
    }

    fn recompute_exit_points(&mut self) {
        let mut has_dependents: HashSet<ZiNodeId> = HashSet::new();
        for node in self.nodes.values() {
            for dep in &node.dependencies {
                has_dependents.insert(dep.clone());
            }
        }

        self.exit_points = self
            .nodes
            .keys()
            .filter(|id| !has_dependents.contains(id))
            .cloned()
            .collect();
    }

    #[allow(non_snake_case)]
    pub fn topological_sort(&self) -> Result<Vec<ZiNodeId>> {
        let mut in_degree: HashMap<ZiNodeId, usize> = self
            .nodes
            .keys()
            .map(|id| (id.clone(), 0))
            .collect();

        for node in self.nodes.values() {
            let dep_count = node.dependencies.len();
            if dep_count > 0 {
                if let Some(count) = in_degree.get_mut(&node.id) {
                    *count = dep_count;
                }
            }
        }

        let mut queue: VecDeque<ZiNodeId> = in_degree
            .iter()
            .filter(|(_, &count)| count == 0)
            .map(|(id, _)| id.clone())
            .collect();

        let mut sorted: Vec<ZiNodeId> = Vec::new();

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
    pub fn detect_cycles(&self) -> bool {
        self.topological_sort().is_err()
    }

    #[allow(non_snake_case)]
    pub fn get_parallel_groups(&self, sorted: &[ZiNodeId]) -> Vec<Vec<ZiNodeId>> {
        let mut groups: Vec<Vec<ZiNodeId>> = Vec::new();
        let mut current_group: Vec<ZiNodeId> = Vec::new();
        let mut processed_deps: HashSet<ZiNodeId> = HashSet::new();

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
pub struct ZiCheckpointState {
    pub node_id: String,
    pub record_count: usize,
    pub data_hash: String,
    pub created_at: u64,
}

#[derive(Clone, Debug, Default)]
pub struct ZiCheckpointStore {
    states: HashMap<String, ZiCheckpointState>,
}

impl ZiCheckpointStore {
    #[allow(non_snake_case)]
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(non_snake_case)]
    pub fn save(&mut self, node_id: &str, record_count: usize, data_hash: String) {
        self.states.insert(
            node_id.to_string(),
            ZiCheckpointState {
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
    pub fn load(&self, node_id: &str) -> Option<&ZiCheckpointState> {
        self.states.get(node_id)
    }

    #[allow(non_snake_case)]
    pub fn get_node_states(&self) -> HashMap<String, ZiCheckpointState> {
        self.states.clone()
    }
}

pub struct ZiSchedulerConfig {
    pub max_parallelism: usize,
    pub cache_enabled: bool,
    pub checkpoint_enabled: bool,
    pub retry_count: usize,
}

impl Default for ZiSchedulerConfig {
    fn default() -> Self {
        Self {
            max_parallelism: num_cpus::get(),
            cache_enabled: true,
            checkpoint_enabled: false,
            retry_count: 3,
        }
    }
}

pub trait ZiOperatorFactoryTrait: Send + Sync {
    fn create(&self, name: &str, config: &serde_json::Value) -> Result<Box<dyn ZiOperator + Send + Sync>>;
}

#[allow(dead_code)]
pub struct ZiScheduler {
    config: ZiSchedulerConfig,
    checkpoint_store: Arc<Mutex<ZiCheckpointStore>>,
    operator_factory: Arc<dyn ZiOperatorFactoryTrait>,
}

impl ZiScheduler {
    #[allow(non_snake_case)]
    pub fn new(
        operator_factory: Arc<dyn ZiOperatorFactoryTrait>,
    ) -> Self {
        Self {
            config: ZiSchedulerConfig::default(),
            checkpoint_store: Arc::new(Mutex::new(ZiCheckpointStore::new())),
            operator_factory,
        }
    }

    #[allow(non_snake_case)]
    pub fn with_config(
        operator_factory: Arc<dyn ZiOperatorFactoryTrait>,
        config: ZiSchedulerConfig,
    ) -> Self {
        Self {
            config,
            checkpoint_store: Arc::new(Mutex::new(ZiCheckpointStore::new())),
            operator_factory,
        }
    }

    #[allow(non_snake_case)]
    pub fn execute(&self, dag: &mut ZiDAG, input: &[ZiRecord]) -> Result<Vec<ZiRecord>> {
        let sorted_nodes = dag.topological_sort()?;
        let mut node_outputs: HashMap<ZiNodeId, Vec<ZiRecord>> = HashMap::new();
        node_outputs.insert(ZiNodeId::from("__input__"), input.to_vec());

        for node_id in sorted_nodes {
            let node = match dag.nodes.get(&node_id) {
                Some(n) => n.clone(),
                None => continue,
            };

            let inputs: Vec<ZiRecord> = node
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

        let final_outputs: Vec<ZiRecord> = dag
            .exit_points
            .iter()
            .flat_map(|id| node_outputs.get(id).cloned().unwrap_or_default())
            .collect();

        Ok(final_outputs)
    }

    #[allow(non_snake_case)]
    pub fn get_checkpoint_store(&self) -> Arc<Mutex<ZiCheckpointStore>> {
        self.checkpoint_store.clone()
    }
}


