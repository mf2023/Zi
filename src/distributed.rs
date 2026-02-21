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
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream, Shutdown};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::errors::{Result, ZiError};
use crate::pipeline::{ZiCPipeline, ZiCPipelineNode, ZiCPipelineBuilder};
use crate::record::ZiCRecordBatch;

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZiCDistributedNodeConfig {
    pub id: String,
    pub address: String,
    pub role: ZiCDistributedNodeRole,
    pub known_nodes: Vec<ZiCDistributedNodeConfig>,
    pub max_memory_mb: Option<usize>,
    pub num_cpus: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ZiCDistributedNodeRole {
    Master,
    Worker,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZiCDistributedExecutionRequest {
    pub request_id: String,
    pub pipeline: Value,
    pub data_chunk: ZiCRecordBatch,
    pub timeout_ms: u64,
    pub priority: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZiCDistributedExecutionResponse {
    pub request_id: String,
    pub result: Option<ZiCRecordBatch>,
    pub error: Option<String>,
    pub execution_time_ms: u64,
    pub records_processed: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZiCDistributedTask {
    pub task_id: String,
    pub chunk_index: usize,
    pub total_chunks: usize,
    pub chunk: ZiCRecordBatch,
    pub pipeline_config: Value,
    pub priority: u8,
    pub created_at: u64,
    pub retry_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZiCWorkerStatus {
    pub node_id: String,
    pub address: String,
    pub last_heartbeat: u64,
    pub is_busy: bool,
    pub records_processed: u64,
    pub avg_latency_ms: f64,
    pub memory_usage_mb: usize,
}

pub struct ZiCDistributedCluster {
    config: ZiCDistributedNodeConfig,
    workers: Arc<Mutex<HashMap<String, Arc<Mutex<ZiCWorkerStatus>>>>>,
    pipelines: Arc<Mutex<HashMap<String, ZiCPipeline>>>,
    tasks: Arc<Mutex<Vec<ZiCDistributedTask>>>,
    results: Arc<Mutex<HashMap<String, ZiCDistributedExecutionResponse>>>,
}

impl ZiCDistributedCluster {
    #[allow(non_snake_case)]
    pub fn ZiFNew(config: ZiCDistributedNodeConfig) -> Self {
        let mut workers = HashMap::new();
        for node in &config.known_nodes {
            if node.role == ZiCDistributedNodeRole::Worker {
                workers.insert(node.id.clone(), Arc::new(Mutex::new(ZiCWorkerStatus {
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

        ZiCDistributedCluster {
            config,
            workers: Arc::new(Mutex::new(workers)),
            pipelines: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(Vec::new())),
            results: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFStart(&mut self) -> Result<()> {
        match self.config.role {
            ZiCDistributedNodeRole::Master => {
                self.start_master()?;
            }
            ZiCDistributedNodeRole::Worker => {
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
            .find(|node| node.role == ZiCDistributedNodeRole::Master)
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
        workers: Arc<Mutex<HashMap<String, Arc<Mutex<ZiCWorkerStatus>>>>>,
        _pipelines: Arc<Mutex<HashMap<String, ZiCPipeline>>>,
        _tasks: Arc<Mutex<Vec<ZiCDistributedTask>>>,
        _results: Arc<Mutex<HashMap<String, ZiCDistributedExecutionResponse>>>,
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
                            workers_guard.insert(node_id.to_string(), Arc::new(Mutex::new(ZiCWorkerStatus {
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
                            let chunk_records: ZiCRecordBatch = chunk.iter().filter_map(|v| {
                                serde_json::from_value(v.clone()).ok()
                            }).collect();

                            let start_time = Instant::now();
                            let mut result_batch: Vec<crate::record::ZiCRecord> = Vec::new();
                            let mut error_msg: Option<String> = None;

                            match ZiCPipelineBuilder::new().build_from_config(&pipeline_config.as_array().unwrap_or(&vec![])) {
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
                            let response = ZiCDistributedExecutionResponse {
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
                        let chunk_records: ZiCRecordBatch = chunk.iter().filter_map(|v| {
                            serde_json::from_value(v.clone()).ok()
                        }).collect();

                        let start_time = Instant::now();
                        let mut result_batch: Vec<crate::record::ZiCRecord> = Vec::new();
                        let mut error_msg: Option<String> = None;

                        match ZiCPipelineBuilder::new().build_from_config(&pipeline_config.as_array().unwrap_or(&vec![])) {
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
                        let response = ZiCDistributedExecutionResponse {
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

    #[allow(non_snake_case)]
    pub fn ZiFRunDistributed(
        &self,
        pipeline: &ZiCPipeline,
        batch: ZiCRecordBatch,
        num_workers: usize,
    ) -> Result<ZiCRecordBatch> {
        if self.config.role != ZiCDistributedNodeRole::Master {
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

    fn extract_pipeline_config(&self, pipeline: &ZiCPipeline) -> Result<Value> {
        let plan = ZiCDistributedExecutionPlan::ZiFFromPipeline(pipeline);
        let steps: Vec<Value> = plan.nodes.iter().map(|n| {
            json!({
                "operator": n.op_code,
                "config": n.config,
            })
        }).collect();
        Ok(json!({"steps": steps}))
    }

    fn split_batch(&self, batch: ZiCRecordBatch, num_chunks: usize) -> Vec<ZiCRecordBatch> {
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
        worker: &Arc<Mutex<ZiCWorkerStatus>>,
        pipeline_config: &Value,
        chunk: &ZiCRecordBatch,
        chunk_index: usize,
    ) -> Result<ZiCRecordBatch> {
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

        let response: ZiCDistributedExecutionResponse = serde_json::from_str(&line.trim())
            .map_err(|e| ZiError::internal(format!("failed to parse response: {}", e)))?;

        if let Some(error) = response.error {
            return Err(ZiError::internal(format!("worker error: {}", error)));
        }

        response.result.ok_or_else(|| ZiError::internal("no result from worker"))
    }

    #[allow(non_snake_case)]
    pub fn ZiFGetClusterStatus(&self) -> Result<Value> {
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
    pub fn ZiFAddWorker(&self, node_config: ZiCDistributedNodeConfig) -> Result<()> {
        if node_config.role != ZiCDistributedNodeRole::Worker {
            return Err(ZiError::validation("only worker nodes can be added this way"));
        }

        let mut workers = self.workers.lock().map_err(|_| {
            ZiError::internal("failed to acquire workers mutex")
        })?;

        workers.insert(node_config.id.clone(), Arc::new(Mutex::new(ZiCWorkerStatus {
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

pub trait ZiCDistributedPipelineExt {
    #[allow(non_snake_case)]
    fn ZiFRunDistributed(
        &self,
        batch: ZiCRecordBatch,
        cluster: &ZiCDistributedCluster,
        num_workers: usize,
    ) -> Result<ZiCRecordBatch>;
}

impl ZiCDistributedPipelineExt for ZiCPipeline {
    #[allow(non_snake_case)]
    fn ZiFRunDistributed(
        &self,
        batch: ZiCRecordBatch,
        cluster: &ZiCDistributedCluster,
        num_workers: usize,
    ) -> Result<ZiCRecordBatch> {
        cluster.ZiFRunDistributed(self, batch, num_workers)
    }
}

#[allow(non_snake_case)]
pub fn ZiFLoadPipelineFromDistributedConfig(config: &Value) -> Result<ZiCPipeline> {
    let builder = ZiCPipelineBuilder::new();

    let steps = config.get("pipeline")
        .and_then(Value::as_array)
        .ok_or_else(|| ZiError::validation("distributed config must contain 'pipeline' array"))?;

    builder.build_from_config(steps)
}

#[allow(non_snake_case)]
pub fn ZiFExportPipelineToDistributedConfig(pipeline: &ZiCPipeline) -> Result<Value> {
    let plan = ZiCDistributedExecutionPlan::ZiFFromPipeline(pipeline);
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

pub struct ZiCDistributedPlanNode {
    pub node_id: String,
    pub op_code: String,
    pub config: Value,
    pub partition_key: Option<String>,
    pub input_nodes: Vec<String>,
}

pub struct ZiCDistributedExecutionPlan {
    pub nodes: Vec<ZiCDistributedPlanNode>,
    pub edges: Vec<(String, String)>,
}

impl ZiCDistributedExecutionPlan {
    #[allow(non_snake_case)]
    pub fn ZiFFromPipeline(pipeline: &ZiCPipeline) -> Self {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut node_counter = 0;

        Self::extract_nodes(pipeline.root(), &mut nodes, &mut edges, &mut node_counter);

        ZiCDistributedExecutionPlan {
            nodes,
            edges,
        }
    }

    fn extract_nodes(
        node: &ZiCPipelineNode,
        nodes: &mut Vec<ZiCDistributedPlanNode>,
        edges: &mut Vec<(String, String)>,
        counter: &mut u32,
    ) -> String {
        match node {
            ZiCPipelineNode::Operator(op) => {
                let node_id = format!("node_{}", *counter);
                *counter += 1;
                nodes.push(ZiCDistributedPlanNode {
                    node_id: node_id.clone(),
                    op_code: op.name().to_string(),
                    config: json!({}),
                    partition_key: None,
                    input_nodes: Vec::new(),
                });
                node_id
            }

            ZiCPipelineNode::Sequence(nodes_list) => {
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

            ZiCPipelineNode::Conditional { predicate, then_branch, else_branch } => {
                // predicate is an operator, not a pipeline node - create a node for it
                let pred_id = format!("node_{}", *counter);
                *counter += 1;
                nodes.push(ZiCDistributedPlanNode {
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

            ZiCPipelineNode::Parallel { branches, num_workers: _ } => {
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
    pub fn ZiFToJson(&self) -> Value {
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
