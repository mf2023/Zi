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

//! # Distributed Execution Module
//!
//! This module provides distributed computing capabilities for Zi pipelines,
//! supporting master-worker architecture with TCP-based communication.
//!
//! ## Architecture
//!
//! - **Master Node**: Coordinates task distribution and result aggregation
//! - **Worker Nodes**: Execute pipeline stages on data chunks
//!
//! ## Features
//!
//! - TCP-based communication between master and workers
//! - Automatic worker registration and health monitoring
//! - Task distribution with round-robin scheduling
//! - Pipeline serialization for remote execution
//!
//! ## Usage Example
//!
//! ```rust,no_run
//! use zi::distributed::{ZiDistributedCluster, ZiDistributedNodeConfig, ZiDistributedNodeRole};
//!
//! // Create master node configuration
//! let master_config = ZiDistributedNodeConfig {
//!     id: "master_1".to_string(),
//!     address: "127.0.0.1:8000".to_string(),
//!     role: ZiDistributedNodeRole::Master,
//!     known_nodes: vec![],
//!     max_memory_mb: None,
//!     num_cpus: None,
//! };
//!
//! // Start cluster
//! let mut cluster = ZiDistributedCluster::new(master_config);
//! cluster.start().unwrap();
//! ```

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream, Shutdown};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::errors::{Result, ZiError};
use crate::pipeline::{ZiPipeline, ZiPipelineNode, ZiPipelineBuilder};
use crate::record::ZiRecordBatch;

/// Connection timeout for TCP communications (30 seconds).
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// Configuration for a distributed node in the cluster.
///
/// Defines node identity, network address, role, and resource constraints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZiDistributedNodeConfig {
    /// Unique identifier for this node
    pub id: String,
    /// Network address for TCP communication (format: "host:port")
    pub address: String,
    /// Role in the cluster (Master or Worker)
    pub role: ZiDistributedNodeRole,
    /// List of known nodes in the cluster for discovery
    pub known_nodes: Vec<ZiDistributedNodeConfig>,
    /// Maximum memory usage in megabytes (None = unlimited)
    pub max_memory_mb: Option<usize>,
    /// Number of CPUs available (None = auto-detect)
    pub num_cpus: Option<usize>,
}

/// Role types for distributed nodes.
///
/// Master nodes coordinate task distribution while worker nodes execute tasks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ZiDistributedNodeRole {
    /// Coordinator node that distributes work and aggregates results
    Master,
    /// Execution node that processes data chunks
    Worker,
}

/// Request to execute a pipeline on a data chunk in distributed mode.
///
/// Contains all information needed for distributed task execution including
/// pipeline configuration, input data, and execution constraints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZiDistributedExecutionRequest {
    /// Unique identifier for tracking this request
    pub request_id: String,
    /// Serialized pipeline configuration
    pub pipeline: Value,
    /// Input data to process
    pub data_chunk: ZiRecordBatch,
    /// Maximum execution time in milliseconds
    pub timeout_ms: u64,
    /// Priority level (higher values execute first)
    pub priority: u8,
}

/// Response from distributed execution containing results or error information.
///
/// Returned by worker nodes after processing a task chunk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZiDistributedExecutionResponse {
    /// Request ID for correlation
    pub request_id: String,
    /// Processed records if successful
    pub result: Option<ZiRecordBatch>,
    /// Error message if execution failed
    pub error: Option<String>,
    /// Actual execution time in milliseconds
    pub execution_time_ms: u64,
    /// Number of records processed
    pub records_processed: usize,
}

/// Task representation for distributed execution queue.
///
/// Represents a unit of work that can be scheduled across worker nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZiDistributedTask {
    /// Unique task identifier
    pub task_id: String,
    /// Index of this chunk in the total batch
    pub chunk_index: usize,
    /// Total number of chunks the batch is divided into
    pub total_chunks: usize,
    /// Records to process
    pub chunk: ZiRecordBatch,
    /// Pipeline configuration for execution
    pub pipeline_config: Value,
    /// Task priority for scheduling
    pub priority: u8,
    /// Creation timestamp (Unix epoch seconds)
    pub created_at: u64,
    /// Number of retry attempts
    pub retry_count: u32,
}

/// Runtime status information for a worker node.
///
/// Used by master to monitor cluster health and load balance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZiWorkerStatus {
    /// Worker node identifier
    pub node_id: String,
    /// Network address for communication
    pub address: String,
    /// Last heartbeat timestamp (Unix epoch seconds)
    pub last_heartbeat: u64,
    /// Whether the worker is currently processing a task
    pub is_busy: bool,
    /// Cumulative records processed since start
    pub records_processed: u64,
    /// Average task latency in milliseconds
    pub avg_latency_ms: f64,
    /// Current memory usage in megabytes
    pub memory_usage_mb: usize,
}

/// Distributed cluster managing master-worker coordination.
///
/// This is the main entry point for distributed execution. It handles:
/// - Worker node registration and health monitoring
/// - Task distribution and result aggregation
/// - Pipeline serialization for remote execution
pub struct ZiDistributedCluster {
    /// Node configuration (role, address, resources)
    config: ZiDistributedNodeConfig,
    /// Worker node status registry
    workers: Arc<Mutex<HashMap<String, Arc<Mutex<ZiWorkerStatus>>>>>,
    /// Cached pipelines for execution
    pipelines: Arc<Mutex<HashMap<String, ZiPipeline>>>,
    /// Pending tasks in the execution queue
    tasks: Arc<Mutex<Vec<ZiDistributedTask>>>,
    /// Completed results indexed by request ID
    results: Arc<Mutex<HashMap<String, ZiDistributedExecutionResponse>>>,
}

impl ZiDistributedCluster {
    /// Creates a new distributed cluster from configuration.
    ///
    /// Initializes the cluster with the given node configuration.
    /// For worker nodes, also pre-registers all known worker nodes from config.
    #[allow(non_snake_case)]
    pub fn new(config: ZiDistributedNodeConfig) -> Self {
        let mut workers = HashMap::new();
        for node in &config.known_nodes {
            if node.role == ZiDistributedNodeRole::Worker {
                workers.insert(node.id.clone(), Arc::new(Mutex::new(ZiWorkerStatus {
                    node_id: node.id.clone(),
                    address: node.address.clone(),
                    last_heartbeat: 0,
                    is_busy: false,
                    records_processed: 0,
                    avg_latency_ms: 0.0,
                    memory_usage_mb: 0,
                })));
            }
        }

        ZiDistributedCluster {
            config,
            workers: Arc::new(Mutex::new(workers)),
            pipelines: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(Vec::new())),
            results: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Starts the cluster in the appropriate mode based on node role.
    ///
    /// For Master nodes: starts TCP listener for worker connections
    /// For Worker nodes: registers with master and starts TCP listener for tasks
    #[allow(non_snake_case)]
    pub fn start(&mut self) -> Result<()> {
        match self.config.role {
            ZiDistributedNodeRole::Master => {
                self.start_master()?;
            }
            ZiDistributedNodeRole::Worker => {
                self.start_worker()?;
            }
        }
        Ok(())
    }

    fn start_master(&mut self) -> Result<()> {
        let listener = TcpListener::bind(&self.config.address)?;
        println!("Master node started on {}", self.config.address);

        let workers = self.workers.clone();
        let pipelines = self.pipelines.clone();
        let tasks = self.tasks.clone();
        let results = self.results.clone();

        thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        stream.set_read_timeout(Some(CONNECTION_TIMEOUT)).ok();
                        let workers = workers.clone();
                        let pipelines = pipelines.clone();
                        let tasks = tasks.clone();
                        let results = results.clone();

                        thread::spawn(move || {
                            Self::handle_master_connection(stream, workers, pipelines, tasks, results);
                        });
                    }
                    Err(err) => {
                        eprintln!("Error accepting connection: {}", err);
                    }
                }
            }
        });

        Ok(())
    }

    fn start_worker(&mut self) -> Result<()> {
        self.register_with_master()?;

        let listener = TcpListener::bind(&self.config.address)?;
        println!("Worker node started on {}", self.config.address);

        thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        stream.set_read_timeout(Some(CONNECTION_TIMEOUT)).ok();
                        thread::spawn(move || {
                            Self::handle_worker_connection(stream);
                        });
                    }
                    Err(err) => {
                        eprintln!("Error accepting connection: {}", err);
                    }
                }
            }
        });

        Ok(())
    }

    fn register_with_master(&self) -> Result<()> {
        let master = self.config.known_nodes
            .iter()
            .find(|node| node.role == ZiDistributedNodeRole::Master)
            .ok_or_else(|| {
                ZiError::internal("no master node found in known nodes")
            })?;

        let mut stream = TcpStream::connect(&master.address)?;
        stream.set_write_timeout(Some(CONNECTION_TIMEOUT)).ok();

        let request = json!({
            "type": "register_worker",
            "node_id": self.config.id,
            "address": self.config.address,
            "max_memory_mb": self.config.max_memory_mb,
            "num_cpus": self.config.num_cpus,
        });

        serde_json::to_writer(&mut stream, &request)?;
        stream.flush()?;

        let mut reader = BufReader::new(&stream);
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let response: Value = serde_json::from_str(&line.trim())?;

        if response.get("status") != Some(&json!("ok")) {
            return Err(ZiError::internal("worker registration failed"));
        }

        Ok(())
    }

    fn handle_master_connection(
        mut stream: TcpStream,
        workers: Arc<Mutex<HashMap<String, Arc<Mutex<ZiWorkerStatus>>>>>,
        _pipelines: Arc<Mutex<HashMap<String, ZiPipeline>>>,
        _tasks: Arc<Mutex<Vec<ZiDistributedTask>>>,
        _results: Arc<Mutex<HashMap<String, ZiDistributedExecutionResponse>>>,
    ) {
        let mut reader = BufReader::new(&stream);
        let mut line = String::new();

        if reader.read_line(&mut line).is_err() {
            return;
        }

        let request: Value = match serde_json::from_str(&line.trim()) {
            Ok(v) => v,
            Err(_) => return,
        };

        let msg_type = request.get("type").and_then(Value::as_str).unwrap_or("unknown");

        match msg_type {
            "register_worker" => {
                if let Some(node_id) = request.get("node_id").and_then(Value::as_str) {
                    if let Some(address) = request.get("address").and_then(Value::as_str) {
                        let mut workers_guard = workers.lock().unwrap();
                        if !workers_guard.contains_key(node_id) {
                            workers_guard.insert(node_id.to_string(), Arc::new(Mutex::new(ZiWorkerStatus {
                                node_id: node_id.to_string(),
                                address: address.to_string(),
                                last_heartbeat: 0,
                                is_busy: false,
                                records_processed: 0,
                                avg_latency_ms: 0.0,
                                memory_usage_mb: 0,
                            })));
                        }
                    }
                }

                let response = json!({"status": "ok", "message": "worker registered"});
                if serde_json::to_writer(&stream, &response).is_ok() {
                    let _ = stream.write_all(b"\n");
                }
            }

            "execute_task" => {
                if let Some(task_id) = request.get("task_id").and_then(Value::as_str) {
                    if let Some(pipeline_config) = request.get("pipeline").cloned() {
                        if let Some(chunk) = request.get("chunk").and_then(Value::as_array) {
                            let chunk_records: ZiRecordBatch = chunk.iter().filter_map(|v| {
                                serde_json::from_value(v.clone()).ok()
                            }).collect();

                            let start_time = Instant::now();
                            let mut result_batch: Vec<crate::record::ZiRecord> = Vec::new();
                            let mut error_msg: Option<String> = None;

                            match ZiPipelineBuilder::new().build_from_config(&pipeline_config.as_array().unwrap_or(&vec![])) {
                                Ok(pipeline) => {
                                    match pipeline.run(chunk_records.clone()) {
                                        Ok(processed) => {
                                            result_batch = processed;
                                        }
                                        Err(e) => {
                                            error_msg = Some(e.to_string());
                                        }
                                    }
                                }
                                Err(e) => {
                                    error_msg = Some(e.to_string());
                                }
                            }

                            let records_processed = if error_msg.is_none() { result_batch.len() } else { 0 };
                            let response = ZiDistributedExecutionResponse {
                                request_id: task_id.to_string(),
                                result: if error_msg.is_none() { Some(result_batch) } else { None },
                                error: error_msg,
                                execution_time_ms: start_time.elapsed().as_millis() as u64,
                                records_processed,
                            };

                            if serde_json::to_writer(&stream, &response).is_ok() {
                                let _ = stream.write_all(b"\n");
                            }
                        }
                    }
                }
            }

            "heartbeat" => {
                if let Some(node_id) = request.get("node_id").and_then(Value::as_str) {
                    let mut workers_guard = workers.lock().unwrap();
                    if let Some(status) = workers_guard.get_mut(node_id) {
                        let mut status_guard = status.lock().unwrap();
                        status_guard.last_heartbeat = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or(Duration::ZERO)
                            .as_secs();
                        status_guard.is_busy = request.get("is_busy").and_then(Value::as_bool).unwrap_or(false);
                        status_guard.records_processed = request.get("records_processed").and_then(Value::as_u64).unwrap_or(0);
                        status_guard.avg_latency_ms = request.get("avg_latency_ms").and_then(Value::as_f64).unwrap_or(0.0);
                        status_guard.memory_usage_mb = request.get("memory_usage_mb").and_then(Value::as_u64).map(|v| v as usize).unwrap_or(0);
                    }
                }

                let response = json!({"status": "ok"});
                let _ = serde_json::to_writer(&stream, &response);
                let _ = stream.write_all(b"\n");
            }

            "get_status" => {
                let workers_guard = workers.lock().unwrap();
                let worker_statuses: Vec<_> = workers_guard.values().map(|s| {
                    let guard = s.lock().unwrap();
                    serde_json::to_value(guard.clone()).unwrap_or(json!({}))
                }).collect();

                let response = json!({
                    "status": "ok",
                    "workers": worker_statuses,
                    "num_workers": workers_guard.len(),
                });

                let _ = serde_json::to_writer(&stream, &response);
                let _ = stream.write_all(b"\n");
            }

            _ => {
                let response = json!({"status": "error", "message": "unknown request type"});
                let _ = serde_json::to_writer(&stream, &response);
                let _ = stream.write_all(b"\n");
            }
        }
    }

    fn handle_worker_connection(mut stream: TcpStream) {
        let mut reader = BufReader::new(&stream);
        let mut line = String::new();

        if reader.read_line(&mut line).is_err() {
            return;
        }

        let request: Value = match serde_json::from_str(&line.trim()) {
            Ok(v) => v,
            Err(_) => return,
        };

        let msg_type = request.get("type").and_then(Value::as_str).unwrap_or("unknown");

        match msg_type {
            "execute" => {
                if let Some(pipeline_config) = request.get("pipeline").cloned() {
                    if let Some(chunk) = request.get("chunk").and_then(Value::as_array) {
                        let chunk_records: ZiRecordBatch = chunk.iter().filter_map(|v| {
                            serde_json::from_value(v.clone()).ok()
                        }).collect();

                        let start_time = Instant::now();
                        let mut result_batch: Vec<crate::record::ZiRecord> = Vec::new();
                        let mut error_msg: Option<String> = None;

                        match ZiPipelineBuilder::new().build_from_config(&pipeline_config.as_array().unwrap_or(&vec![])) {
                            Ok(pipeline) => {
                                match pipeline.run(chunk_records.clone()) {
                                    Ok(processed) => {
                                        result_batch = processed;
                                    }
                                    Err(e) => {
                                        error_msg = Some(e.to_string());
                                    }
                                }
                            }
                            Err(e) => {
                                error_msg = Some(e.to_string());
                            }
                        }

                        let records_processed = if error_msg.is_none() { result_batch.len() } else { 0 };
                        let response = ZiDistributedExecutionResponse {
                            request_id: request.get("request_id").and_then(Value::as_str).unwrap_or("").to_string(),
                            result: if error_msg.is_none() { Some(result_batch) } else { None },
                            error: error_msg,
                            execution_time_ms: start_time.elapsed().as_millis() as u64,
                            records_processed,
                        };

                        if serde_json::to_writer(&stream, &response).is_ok() {
                            let _ = stream.write_all(b"\n");
                        }
                    }
                }
            }

            "shutdown" => {
                let _ = stream.shutdown(Shutdown::Both);
            }

            _ => {
                let response = json!({"status": "error", "message": "unknown request type"});
                let _ = serde_json::to_writer(&stream, &response);
                let _ = stream.write_all(b"\n");
            }
        }
    }

    /// Executes a pipeline in distributed mode across worker nodes.
    ///
    /// Splits the input batch into chunks and distributes them to worker nodes
    /// for parallel execution. Results are aggregated and returned.
    ///
    /// # Arguments
    /// * `pipeline` - The pipeline to execute
    /// * `batch` - Input records to process
    /// * `num_workers` - Number of workers to use (0 = auto)
    ///
    /// # Errors
    /// Returns error if called on a worker node or if workers are unavailable.
    #[allow(non_snake_case)]
    pub fn run_distributed(
        &self,
        pipeline: &ZiPipeline,
        batch: ZiRecordBatch,
        num_workers: usize,
    ) -> Result<ZiRecordBatch> {
        if self.config.role != ZiDistributedNodeRole::Master {
            return Err(ZiError::validation("distributed execution can only be initiated from master node"));
        }

        let workers = self.workers.lock().map_err(|_| {
            ZiError::internal("failed to acquire workers mutex")
        })?;

        if workers.is_empty() {
            drop(workers);
            return pipeline.run(batch);
        }

        let available_workers: Vec<_> = workers.values().filter(|s| {
            let guard = s.lock().unwrap();
            !guard.is_busy
        }).cloned().collect();

        drop(workers);

        let pipeline_config = self.extract_pipeline_config(pipeline)?;
        let chunks = self.split_batch(batch.clone(), num_workers);
        let mut results = Vec::new();

        if available_workers.is_empty() || chunks.len() <= 1 {
            return pipeline.run(batch);
        }

        let mut handles = Vec::new();
        for (chunk_index, chunk) in chunks.into_iter().enumerate() {
            let worker = available_workers[chunk_index % available_workers.len()].clone();
            let pipeline_config = pipeline_config.clone();

            let handle = thread::spawn(move || {
                Self::send_task_to_worker(&worker, &pipeline_config, &chunk, chunk_index)
            });

            handles.push(handle);
        }

        for handle in handles {
            match handle.join() {
                Ok(Ok(result_batch)) => {
                    results.extend(result_batch);
                }
                Ok(Err(e)) => {
                    eprintln!("Worker task failed: {}", e);
                }
                Err(_) => {
                    eprintln!("Worker task panicked");
                }
            }
        }

        Ok(results)
    }

    fn extract_pipeline_config(&self, pipeline: &ZiPipeline) -> Result<Value> {
        let plan = ZiDistributedExecutionPlan::from_pipeline(pipeline);
        let steps: Vec<Value> = plan.nodes.iter().map(|n| {
            json!({
                "operator": n.op_code,
                "config": n.config,
            })
        }).collect();
        Ok(json!({"steps": steps}))
    }

    fn split_batch(&self, batch: ZiRecordBatch, num_chunks: usize) -> Vec<ZiRecordBatch> {
        if num_chunks <= 1 || batch.is_empty() {
            return vec![batch];
        }

        let chunk_size = (batch.len() + num_chunks - 1) / num_chunks;
        let mut chunks = Vec::new();
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

        chunks
    }

    fn send_task_to_worker(
        worker: &Arc<Mutex<ZiWorkerStatus>>,
        pipeline_config: &Value,
        chunk: &ZiRecordBatch,
        chunk_index: usize,
    ) -> Result<ZiRecordBatch> {
        let worker_addr = {
            let guard = worker.lock().unwrap();
            guard.address.clone()
        };

        let stream = TcpStream::connect(&worker_addr).map_err(|e| {
            ZiError::internal(format!("failed to connect to worker: {}", e))
        })?;

        let request = json!({
            "type": "execute",
            "request_id": format!("task_{}", chunk_index),
            "pipeline": pipeline_config,
            "chunk": chunk,
        });

        let mut writer = std::io::BufWriter::new(&stream);
        serde_json::to_writer(&mut writer, &request)?;
        writer.write_all(b"\n")?;
        writer.flush()?;

        let mut reader = BufReader::new(&stream);
        let mut line = String::new();
        reader.read_line(&mut line)?;

        let response: ZiDistributedExecutionResponse = serde_json::from_str(&line.trim())
            .map_err(|e| ZiError::internal(format!("failed to parse response: {}", e)))?;

        if let Some(error) = response.error {
            return Err(ZiError::internal(format!("worker error: {}", error)));
        }

        response.result.ok_or_else(|| ZiError::internal("no result from worker"))
    }

    /// Retrieves the current status of all worker nodes in the cluster.
    ///
    /// Returns a JSON value containing:
    /// - Master node ID
    /// - Number of workers
    /// - Per-worker status (ID, address, busy state, metrics)
    #[allow(non_snake_case)]
    pub fn get_cluster_status(&self) -> Result<Value> {
        let workers = self.workers.lock().map_err(|_| {
            ZiError::internal("failed to acquire workers mutex")
        })?;

        let worker_statuses: Vec<_> = workers.values().map(|s| {
            let guard = s.lock().unwrap();
            json!({
                "node_id": guard.node_id,
                "address": guard.address,
                "is_busy": guard.is_busy,
                "records_processed": guard.records_processed,
                "avg_latency_ms": guard.avg_latency_ms,
                "memory_usage_mb": guard.memory_usage_mb,
            })
        }).collect();

        Ok(json!({
            "master_id": self.config.id,
            "num_workers": workers.len(),
            "workers": worker_statuses,
        }))
    }

    #[allow(non_snake_case)]
    pub fn add_worker(&self, node_config: ZiDistributedNodeConfig) -> Result<()> {
        if node_config.role != ZiDistributedNodeRole::Worker {
            return Err(ZiError::validation("only worker nodes can be added this way"));
        }

        let mut workers = self.workers.lock().map_err(|_| {
            ZiError::internal("failed to acquire workers mutex")
        })?;

        workers.insert(node_config.id.clone(), Arc::new(Mutex::new(ZiWorkerStatus {
            node_id: node_config.id.clone(),
            address: node_config.address.clone(),
            last_heartbeat: 0,
            is_busy: false,
            records_processed: 0,
            avg_latency_ms: 0.0,
            memory_usage_mb: node_config.max_memory_mb.unwrap_or(0),
        })));

        Ok(())
    }
}

pub trait ZiDistributedPipelineExt {
    #[allow(non_snake_case)]
    fn run_distributed(
        &self,
        batch: ZiRecordBatch,
        cluster: &ZiDistributedCluster,
        num_workers: usize,
    ) -> Result<ZiRecordBatch>;
}

impl ZiDistributedPipelineExt for ZiPipeline {
    #[allow(non_snake_case)]
    fn run_distributed(
        &self,
        batch: ZiRecordBatch,
        cluster: &ZiDistributedCluster,
        num_workers: usize,
    ) -> Result<ZiRecordBatch> {
        cluster.run_distributed(self, batch, num_workers)
    }
}

#[allow(non_snake_case)]
pub fn load_pipeline_from_distributed_config(config: &Value) -> Result<ZiPipeline> {
    let builder = ZiPipelineBuilder::new();

    let steps = config.get("pipeline")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("distributed config must contain 'pipeline' array"))?;

    builder.build_from_config(steps)
}

#[allow(non_snake_case)]
pub fn export_pipeline_to_distributed_config(pipeline: &ZiPipeline) -> Result<Value> {
    let plan = ZiDistributedExecutionPlan::from_pipeline(pipeline);
    let pipeline_steps: Vec<Value> = plan.nodes.iter().map(|n| {
        json!({
            "operator": n.op_code,
            "config": n.config,
        })
    }).collect();
    Ok(json!({
        "pipeline": pipeline_steps,
        "version": "1.0",
    }))
}

pub struct ZiDistributedPlanNode {
    pub node_id: String,
    pub op_code: String,
    pub config: Value,
    pub partition_key: Option<String>,
    pub input_nodes: Vec<String>,
}

pub struct ZiDistributedExecutionPlan {
    pub nodes: Vec<ZiDistributedPlanNode>,
    pub edges: Vec<(String, String)>,
}

impl ZiDistributedExecutionPlan {
    #[allow(non_snake_case)]
    pub fn from_pipeline(pipeline: &ZiPipeline) -> Self {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut node_counter = 0;

        Self::extract_nodes(pipeline.root(), &mut nodes, &mut edges, &mut node_counter);

        ZiDistributedExecutionPlan {
            nodes,
            edges,
        }
    }

    fn extract_nodes(
        node: &ZiPipelineNode,
        nodes: &mut Vec<ZiDistributedPlanNode>,
        edges: &mut Vec<(String, String)>,
        counter: &mut u32,
    ) -> String {
        match node {
            ZiPipelineNode::Operator(op) => {
                let node_id = format!("node_{}", *counter);
                *counter += 1;
                nodes.push(ZiDistributedPlanNode {
                    node_id: node_id.clone(),
                    op_code: op.name().to_string(),
                    config: json!({}),
                    partition_key: None,
                    input_nodes: Vec::new(),
                });
                node_id
            }

            ZiPipelineNode::Sequence(nodes_list) => {
                let mut prev_node_id = String::new();
                for n in nodes_list {
                    let curr_node_id = Self::extract_nodes(n, nodes, edges, counter);
                    if !prev_node_id.is_empty() {
                        edges.push((prev_node_id.clone(), curr_node_id.clone()));
                    }
                    prev_node_id = curr_node_id;
                }
                prev_node_id
            }

            ZiPipelineNode::Conditional { predicate, then_branch, else_branch } => {
                // predicate is an operator, not a pipeline node - create a node for it
                let pred_id = format!("node_{}", *counter);
                *counter += 1;
                nodes.push(ZiDistributedPlanNode {
                    node_id: pred_id.clone(),
                    op_code: predicate.name().to_string(),
                    config: json!({"type": "predicate"}),
                    partition_key: None,
                    input_nodes: Vec::new(),
                });

                let then_id = Self::extract_nodes(then_branch, nodes, edges, counter);
                let else_id = Self::extract_nodes(else_branch, nodes, edges, counter);

                edges.push((pred_id.clone(), then_id.clone()));
                edges.push((pred_id.clone(), else_id.clone()));

                format!("merge_{}", *counter)
            }

            ZiPipelineNode::Parallel { branches, num_workers: _ } => {
                let mut branch_outputs = Vec::new();
                for (_i, branch) in branches.iter().enumerate() {
                    let branch_output = Self::extract_nodes(branch, nodes, edges, counter);
                    branch_outputs.push(branch_output);
                }

                for i in 1..branch_outputs.len() {
                    edges.push((branch_outputs[i-1].clone(), branch_outputs[i].clone()));
                }

                branch_outputs.last().cloned().unwrap_or_default()
            }
        }
    }

    #[allow(non_snake_case)]
    pub fn to_json(&self) -> Value {
        json!({
            "nodes": self.nodes.iter().map(|n| json!({
                "id": n.node_id,
                "op": n.op_code,
                "config": n.config,
                "partition_key": n.partition_key,
                "inputs": n.input_nodes,
            })).collect::<Vec<_>>(),
            "edges": self.edges.iter().map(|(src, dst)| json!({
                "from": src,
                "to": dst,
            })).collect::<Vec<_>>(),
        })
    }
}
