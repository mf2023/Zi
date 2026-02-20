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

use std::collections::HashMap;
use std::ffi::{c_void, CStr};
use std::io::Write;
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
use crate::version::{
    ZiCCodeHash, ZiCEnvHash, ZiCTripleHash, ZiCVersion, ZiCVersionStore, ZiFComputeDataHash,
};
use libloading::Library;

type OperatorFactory = ZiFOperatorFactory;

/// Pipeline node types for supporting complex pipeline topologies.
pub enum ZiCPipelineNode {
    /// A single operator stage
    Operator(Box<dyn ZiCOperator + Send + Sync>),
    /// A sequence of nodes executed in order
    Sequence(Vec<ZiCPipelineNode>),
    /// A conditional branch that executes one of two branches based on a predicate
    Conditional {
        predicate: Box<dyn ZiCOperator + Send + Sync>,
        then_branch: Box<ZiCPipelineNode>,
        else_branch: Box<ZiCPipelineNode>,
    },
    /// Parallel branches that execute concurrently and merge results
    Parallel {
        branches: Vec<ZiCPipelineNode>,
        num_workers: usize,
    },
}

// Remove manual Clone implementation for now
// We'll use a different approach to handle pipeline execution

/// Cache entry with expiration time
#[derive(Debug, Clone)]
struct ZiCCacheEntry {
    /// Cached records
    records: ZiCRecordBatch,
    /// Expiration time (None means never expires)
    expires_at: Option<std::time::Instant>,
    /// Last access time for LRU eviction
    last_access: std::time::Instant,
    /// Size of the cached records in bytes (approximate)
    size: usize,
}

/// Execution mode for parallel processing.
pub enum ExecutionMode {
    /// Use multi-threading within a single process.
    Threaded,
    /// Use multiple processes.
    MultiProcess,
}

/// Enhanced pipeline supporting complex topologies.
pub struct ZiCPipeline {
    root: ZiCPipelineNode,
    /// Cache with expiration and size limits
    cache: std::collections::HashMap<String, ZiCCacheEntry>,
    /// Cache configuration
    cache_config: ZiCCacheConfig,
    /// Current cache size in bytes
    cache_size: usize,
    instrumentation: bool,
    stage_metrics: Option<Arc<Mutex<Vec<ZiCPipelineStageMetrics>>>>,
}

/// Cache configuration
#[derive(Debug, Clone)]
pub struct ZiCCacheConfig {
    /// Maximum cache size in bytes (0 means unlimited)
    pub max_size: usize,
    /// Default cache expiration time (None means never expires)
    pub default_ttl: Option<std::time::Duration>,
    /// Whether to enable cache compression
    pub compression: bool,
}

impl Default for ZiCCacheConfig {
    fn default() -> Self {
        Self {
            max_size: 100 * 1024 * 1024, // 100 MB default
            default_ttl: Some(std::time::Duration::from_secs(600)), // 10 minutes default (600 seconds)
            compression: false,
        }
    }
}

impl ZiCPipeline {
    /// Constructs a pipeline from a list of operators.
    pub fn new(stages: Vec<Box<dyn ZiCOperator + Send + Sync>>) -> Self {
        // Convert linear stages to a sequence node
        let nodes: Vec<ZiCPipelineNode> = stages
            .into_iter()
            .map(ZiCPipelineNode::Operator)
            .collect();
        
        ZiCPipeline {
            root: ZiCPipelineNode::Sequence(nodes),
            cache: std::collections::HashMap::new(),
            cache_config: ZiCCacheConfig::default(),
            cache_size: 0,
            instrumentation: false,
            stage_metrics: None,
        }
    }
    
    /// Constructs a pipeline from a root node.
    pub fn from_node(root: ZiCPipelineNode) -> Self {
        ZiCPipeline {
            root,
            cache: std::collections::HashMap::new(),
            cache_config: ZiCCacheConfig::default(),
            cache_size: 0,
            instrumentation: false,
            stage_metrics: None,
        }
    }

    /// Returns a reference to the root node.
    pub fn root(&self) -> &ZiCPipelineNode {
        &self.root
    }
    
    /// Sets the cache configuration for the pipeline.
    #[allow(non_snake_case)]
    pub fn ZiFWithCacheConfig(mut self, config: ZiCCacheConfig) -> Self {
        self.cache_config = config;
        self
    }
    
    /// Clears all expired cache entries.
    fn cleanup_expired_cache(&mut self) {
        let now = std::time::Instant::now();
        let mut to_remove = Vec::new();
        
        // Find all expired entries
        for (key, entry) in &self.cache {
            if let Some(expires_at) = entry.expires_at {
                if now > expires_at {
                    to_remove.push(key.clone());
                }
            }
        }
        
        // Remove expired entries and update cache size
        for key in to_remove {
            if let Some(entry) = self.cache.remove(&key) {
                self.cache_size = self.cache_size.saturating_sub(entry.size);
            }
        }
    }
    
    /// Evicts entries using LRU policy to make room for new entries.
    fn evict_lru(&mut self, needed_space: usize) {
        let mut entries: Vec<(std::time::Instant, String, usize)> = self.cache
            .iter()
            .map(|(key, entry)| (entry.last_access, key.clone(), entry.size))
            .collect();
        
        // Sort by last access time (oldest first)
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        
        // Evict entries until we have enough space or no more entries
        for (_, key, _size) in entries {
            if self.cache_size <= needed_space {
                break;
            }
            
            if let Some(entry) = self.cache.remove(&key) {
                self.cache_size = self.cache_size.saturating_sub(entry.size);
            }
        }
    }
    
    /// Calculates the approximate size of a record batch in bytes.
    fn calculate_batch_size(batch: &ZiCRecordBatch) -> usize {
        // This is an approximation - in a real implementation, we'd use a more accurate method
        batch.iter().map(|record| {
            let id_size = record.id.as_ref().map(|id| id.len()).unwrap_or(0);
            let payload_size = serde_json::to_string(&record.payload).unwrap_or_default().len();
            let metadata_size = record.metadata.as_ref().map(|md| {
                serde_json::to_string(md).unwrap_or_default().len()
            }).unwrap_or(0);
            id_size + payload_size + metadata_size
        }).sum()
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
}

impl ZiCPipelineNode {
    /// Executes the pipeline node and returns the processed batch.
    fn execute(&self, batch: ZiCRecordBatch, instrumentation: bool, metrics: &Option<Arc<Mutex<Vec<ZiCPipelineStageMetrics>>>>) -> Result<ZiCRecordBatch> {
        match self {
            ZiCPipelineNode::Operator(op) => {
                let before = batch.len();
                let start = Instant::now();
                let result = ZiFExecuteOperator(op.as_ref(), batch)?;
                if instrumentation {
                    let duration = start.elapsed();
                    let after = result.len();
                    let stage_metric = ZiCPipelineStageMetrics::new(
                        op.name().to_string(),
                        before,
                        after,
                        duration,
                    );
                    if let Some(metrics) = metrics {
                        if let Ok(mut guard) = metrics.lock() {
                            guard.push(stage_metric);
                        }
                    }
                }
                Ok(result)
            }
            
            ZiCPipelineNode::Sequence(nodes) => {
                let mut result = batch;
                for node in nodes {
                    result = node.execute(result, instrumentation, metrics)?;
                }
                Ok(result)
            }
            
            ZiCPipelineNode::Conditional { predicate, then_branch, else_branch } => {
                // Execute predicate to determine which branch to take
                let before = batch.len();
                let start = Instant::now();
                let predicate_result = ZiFExecuteOperator(predicate.as_ref(), batch.clone())?;
                
                if instrumentation {
                    let duration = start.elapsed();
                    let after = predicate_result.len();
                    let stage_metric = ZiCPipelineStageMetrics::new(
                        predicate.name().to_string(),
                        before,
                        after,
                        duration,
                    );
                    if let Some(metrics) = metrics {
                        if let Ok(mut guard) = metrics.lock() {
                            guard.push(stage_metric);
                        }
                    }
                }
                
                // If predicate returns any records, execute then_branch, else execute else_branch
                if !predicate_result.is_empty() {
                    then_branch.execute(batch, instrumentation, metrics)
                } else {
                    else_branch.execute(batch, instrumentation, metrics)
                }
            }
            
            ZiCPipelineNode::Parallel { branches, num_workers: _ } => {
                if branches.is_empty() {
                    return Ok(batch);
                }
                
                if branches.len() == 1 {
                    return branches[0].execute(batch, instrumentation, metrics);
                }
                
                // For parallel execution, we'll process the batch through each branch concurrently
                let mut results = Vec::with_capacity(branches.len());
                
                thread::scope(|scope| -> Result<()> {
                    let mut handles = Vec::with_capacity(branches.len());
                    
                    // Process each branch in parallel
                    for branch in branches {
                        // Clone the batch for this thread
                        let batch_clone = batch.clone();
                        let instrumentation = instrumentation;
                        let metrics = metrics;
                        
                        handles.push(scope.spawn(move || -> Result<ZiCRecordBatch> {
                            // Execute the branch on the cloned batch
                            branch.execute(batch_clone, instrumentation, metrics)
                        }));
                    }
                    
                    // Wait for all branches to complete and collect results
                    for handle in handles {
                        let result = handle
                            .join()
                            .map_err(|_| ZiError::internal("parallel execution worker panicked"))?;
                        results.push(result?);
                    }
                    
                    Ok(())
                })?;
                
                // Merge results from all branches
                let mut merged = Vec::new();
                for result in results {
                    merged.extend(result);
                }
                
                Ok(merged)
            }
        }
    }
}

impl ZiCPipeline {
    /// Runs the pipeline, supporting complex topologies.
    pub fn run(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        if self.instrumentation {
            self.reset_stage_metrics();
        }
        
        self.root.execute(batch, self.instrumentation, &self.stage_metrics)
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
        batch: ZiCRecordBatch,
        progress: impl Fn(&str, usize, usize),
    ) -> Result<ZiCRecordBatch> {
        // For now, we'll use the existing run method and then report progress
        // based on the stage metrics if instrumentation is enabled
        let result = self.run(batch)?;
        
        // If instrumentation is enabled, we can report progress for each stage
        if self.instrumentation {
            if let Some(metrics) = self.ZiFStageMetrics() {
                for metric in metrics {
                    progress(&metric.stage_name, metric.input_records, metric.output_records);
                }
            }
        }
        
        Ok(result)
    }

    pub fn run_cached(&mut self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        fn hash_batch(batch: &ZiCRecordBatch) -> String {
            let s = serde_json::to_string(batch).unwrap_or_default();
            blake3::hash(s.as_bytes()).to_hex().to_string()
        }
        
        // Cleanup expired cache entries first
        self.cleanup_expired_cache();
        
        let key = format!("pipeline:{}:cached", hash_batch(&batch));
        let now = std::time::Instant::now();
        
        // Check if we have a valid cached entry
        if let Some(mut entry) = self.cache.remove(&key) {
            // Update last access time
            entry.last_access = now;
            
            // Check if the entry is still valid
            if entry.expires_at.map(|exp| now <= exp).unwrap_or(true) {
                // Put the entry back with updated access time
                self.cache.insert(key.clone(), entry.clone());
                return Ok(entry.records.clone());
            }
            
            // Entry is expired, remove it from cache size
            self.cache_size = self.cache_size.saturating_sub(entry.size);
        }
        
        // Execute the pipeline
        let out = self.run(batch)?;
        
        // Calculate the size of the result batch
        let batch_size = Self::calculate_batch_size(&out);
        
        // Check if we need to make room for the new entry
        if self.cache_config.max_size > 0 {
            let needed_space = if self.cache_size + batch_size > self.cache_config.max_size {
                (self.cache_size + batch_size) - self.cache_config.max_size
            } else {
                0
            };
            
            if needed_space > 0 {
                self.evict_lru(needed_space);
            }
        }
        
        // Create and store the new cache entry
        let expires_at = self.cache_config.default_ttl.map(|ttl| now + ttl);
        let entry = ZiCCacheEntry {
            records: out.clone(),
            expires_at,
            last_access: now,
            size: batch_size,
        };
        
        self.cache.insert(key, entry);
        self.cache_size = self.cache_size.saturating_add(batch_size);
        
        Ok(out)
    }

    /// Executes the pipeline on multiple chunks concurrently using threads.
    #[allow(non_snake_case)]
    pub fn ZiFRunParallel(
        &self,
        batch: ZiCRecordBatch,
        num_workers: usize,
    ) -> Result<ZiCRecordBatch> {
        self.run_parallel_impl(batch, num_workers, ExecutionMode::Threaded)
    }
    
    /// Executes the pipeline on multiple processes.
    #[allow(non_snake_case)]
    pub fn ZiFRunMultiProcess(
        &self,
        batch: ZiCRecordBatch,
        num_processes: usize,
    ) -> Result<ZiCRecordBatch> {
        self.run_parallel_impl(batch, num_processes, ExecutionMode::MultiProcess)
    }
    
    /// Internal implementation for parallel execution, supporting both threaded and multi-process modes.
    fn run_parallel_impl(
        &self,
        batch: ZiCRecordBatch,
        num_workers: usize,
        mode: ExecutionMode,
    ) -> Result<ZiCRecordBatch> {
        if num_workers == 0 {
            return Err(ZiError::validation(
                "parallel execution requires at least one worker",
            ));
        }
        if batch.len() <= 1 || num_workers == 1 {
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

        match mode {
            ExecutionMode::Threaded => {
                // Existing threaded implementation
                let mut results = Vec::with_capacity(chunks.len());
                thread::scope(|scope| -> Result<()> {
                    let pipeline = self;
                    let mut handles = Vec::with_capacity(chunks.len());
                    for (idx, chunk) in chunks.into_iter().enumerate() {
                        handles.push(scope.spawn(move || -> Result<(usize, ZiCRecordBatch)> {
                            let result = pipeline.run(chunk)?;
                            Ok((idx, result))
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
            
            ExecutionMode::MultiProcess => {
                // Multi-process implementation using standard library
                let mut results = Vec::with_capacity(chunks.len());
                
                #[cfg(unix)]
                {
                    // Unix-specific implementation using fork
                    use std::os::unix::process::fork;
                    
                    let mut child_pids = Vec::new();
                    
                    // Fork a child process for each chunk
                    for (idx, chunk) in chunks.into_iter().enumerate() {
                        match unsafe { fork() } {
                            Ok(Some(child)) => {
                                // Parent process: track child PID
                                child_pids.push((idx, child));
                            }
                            Ok(None) => {
                                // Child process: execute pipeline and exit with result
                                match self.run(chunk) {
                                    Ok(result) => {
                                        // Serialize result and write to stdout
                                        let serialized = serde_json::to_string(&result).unwrap();
                                        println!("{}", serialized);
                                        std::process::exit(0);
                                    }
                                    Err(err) => {
                                        // Serialize error and write to stderr
                                        let serialized = serde_json::to_string(&err).unwrap();
                                        eprintln!("{}", serialized);
                                        std::process::exit(1);
                                    }
                                }
                            }
                            Err(err) => {
                                return Err(ZiError::internal(format!("failed to fork process: {err}")));
                            }
                        }
                    }
                    
                    // Parent process: collect results from children
                    for (idx, pid) in child_pids {
                        let mut exit_status = std::process::waitpid(pid, None)?;
                        if exit_status.success() {
                            // Read result from stdout
                            let output = std::process::Command::new("cat")
                                .stdin(std::process::Stdio::inherit())
                                .stdout(std::process::Stdio::piped())
                                .output()?;
                            let result_str = String::from_utf8_lossy(&output.stdout);
                            let result: ZiCRecordBatch = serde_json::from_str(&result_str)?;
                            results.push((idx, result));
                        } else {
                            // Read error from stderr
                            let output = std::process::Command::new("cat")
                                .stdin(std::process::Stdio::inherit())
                                .stderr(std::process::Stdio::piped())
                                .output()?;
                            let err_str = String::from_utf8_lossy(&output.stderr);
                            let err: ZiError = serde_json::from_str(&err_str)?;
                            return Err(err);
                        }
                    }
                }
                
                #[cfg(windows)]
                {
                    // Windows-specific implementation using child processes
                    use std::process::{Command, Stdio};
                    
                    let mut child_processes = Vec::new();
                    
                    // Create a child process for each chunk
                    for (idx, chunk) in chunks.into_iter().enumerate() {
                        // Serialize the chunk to pass to the child process
                        let chunk_str = serde_json::to_string(&chunk)?;
                        
                        // Start a child process
                        let mut child = Command::new(std::env::current_exe()?)
                            .arg("--pipeline-worker")
                            .arg(idx.to_string())
                            .stdin(Stdio::piped())
                            .stdout(Stdio::piped())
                            .stderr(Stdio::piped())
                            .spawn()?;
                        
                        // Write the chunk to the child's stdin before pushing to the vector
                        if let Some(mut stdin) = child.stdin.take() {
                            stdin.write_all(chunk_str.as_bytes())?;
                        }
                        
                        child_processes.push((idx, child));
                    }
                    
                    // Collect results from child processes
                    for (idx, child) in child_processes {
                        let output = child.wait_with_output()?;
                        if output.status.success() {
                            let result_str = String::from_utf8_lossy(&output.stdout);
                            let result: ZiCRecordBatch = serde_json::from_str(&result_str)?;
                            results.push((idx, result));
                        } else {
                            let err_str = String::from_utf8_lossy(&output.stderr);
                            return Err(ZiError::internal(format!("child process failed: {}", err_str)));
                        }
                    }
                }
                
                // Sort results by index and merge
                results.sort_by_key(|(idx, _)| *idx);
                let mut merged = Vec::new();
                for (_, chunk) in results {
                    merged.extend(chunk);
                }
                
                Ok(merged)
            }
        }
    }

    /// Validates the pipeline topology and configuration.
    pub fn validate(&self) -> Result<()> {
        // Validate the root node and its children
        self.validate_node(&self.root, &mut Vec::new())
    }
    
    /// Validates a pipeline node and its children recursively.
    fn validate_node(&self, node: &ZiCPipelineNode, visited: &mut Vec<*const ZiCPipelineNode>) -> Result<()> {
        // Check for cycles in the pipeline topology
        let node_ptr = node as *const ZiCPipelineNode;
        if visited.contains(&node_ptr) {
            return Err(ZiError::validation("pipeline contains a cycle in its topology"));
        }
        visited.push(node_ptr);
        
        // Validate based on node type
        match node {
            ZiCPipelineNode::Operator(op) => {
                // Validate that the operator is properly configured
                self.validate_operator(op.as_ref())?;
            }
            
            ZiCPipelineNode::Sequence(nodes) => {
                // Validate that sequence contains at least one node
                if nodes.is_empty() {
                    return Err(ZiError::validation("sequence node must contain at least one child node"));
                }
                
                // Validate each node in the sequence
                for child in nodes {
                    self.validate_node(child, visited)?;
                }
            }
            
            ZiCPipelineNode::Conditional { predicate, then_branch, else_branch } => {
                // Validate predicate operator
                self.validate_operator(predicate.as_ref())?;
                
                // Validate both branches
                self.validate_node(then_branch, visited)?;
                self.validate_node(else_branch, visited)?;
            }
            
            ZiCPipelineNode::Parallel { branches, num_workers } => {
                // Validate that parallel node contains at least one branch
                if branches.is_empty() {
                    return Err(ZiError::validation("parallel node must contain at least one branch"));
                }
                
                // Validate num_workers is positive
                if *num_workers == 0 {
                    return Err(ZiError::validation("parallel node must have at least one worker"));
                }
                
                // Validate each branch
                for branch in branches {
                    self.validate_node(branch, visited)?;
                }
            }
        }
        
        // Remove current node from visited list before returning
        visited.pop();
        Ok(())
    }
    
    /// Validates that an operator is properly configured.
    fn validate_operator(&self, op: &dyn ZiCOperator) -> Result<()> {
        // In a real implementation, we'd have more detailed validation
        // For now, we'll just ensure the operator has a valid name
        let name = op.name();
        if name.is_empty() {
            return Err(ZiError::validation("operator must have a non-empty name"));
        }
        
        // Check that the operator name is valid (contains no whitespace or special characters)
        if name.contains(|c: char| !c.is_ascii_alphanumeric() && c != '.' && c != '-') {
            return Err(ZiError::validation(format!("operator name '{}' contains invalid characters", name)));
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
        let data_hash = ZiFComputeDataHash(&processed);

        if !metadata.contains_key("stages") {
            // For now, we'll use a placeholder for stage names
            // In the future, we could extract stage names from the root node
            metadata.insert("stages".into(), Value::Array(Vec::new()));
        }

        metadata
            .entry("record_count".to_string())
            .or_insert_with(|| Value::from(processed.len() as u64));

        let triple_hash = ZiCTripleHash {
            data: data_hash,
            code: ZiCCodeHash([0u8; 32]),
            env: ZiCEnvHash([0u8; 32]),
        };

        let version = store.ZiFCreate(parent, metadata, metrics, triple_hash)?;
        Ok((processed, version))
    }

    fn run_with_stage_metrics(
        &self,
        batch: ZiCRecordBatch,
    ) -> Result<(ZiCRecordBatch, Vec<ZiCPipelineStageMetrics>)> {
        // Create a new metrics vector to store stage metrics
        let stage_metrics = Arc::new(Mutex::new(Vec::new()));
        
        // Clone the Arc to avoid moving the original
        let stage_metrics_clone = stage_metrics.clone();
        
        // Execute the root node directly with instrumentation enabled
        let records = self.root.execute(batch, true, &Some(stage_metrics_clone))?;
        
        // Get the collected stage metrics from the original Arc
        let metrics = stage_metrics.lock().map_err(|_| {
            ZiError::internal("failed to acquire stage metrics mutex")
        })?;
        
        Ok((records, metrics.clone()))
    }

    fn reset_stage_metrics(&self) {
        if let Some(metrics) = &self.stage_metrics {
            if let Ok(mut guard) = metrics.lock() {
                guard.clear();
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

        // llm operators
        self.register(
            "llm.token_count",
            crate::operators::llm::ZiFTokenCountFactory as OperatorFactory,
        );
        self.register(
            "llm.conversation_format",
            crate::operators::llm::ZiFConversationFormatFactory as OperatorFactory,
        );
        self.register(
            "llm.context_length",
            crate::operators::llm::ZiFContextLengthFactory as OperatorFactory,
        );
        self.register(
            "llm.qa_extract",
            crate::operators::llm::ZiFQAExtractFactory as OperatorFactory,
        );
        self.register(
            "llm.instruction_format",
            crate::operators::llm::ZiFInstructionFormatFactory as OperatorFactory,
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
