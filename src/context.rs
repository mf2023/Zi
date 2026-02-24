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

use std::sync::Arc;
use tokio::sync::RwLock;

use dmsc::log::{DMSCLogger, DMSCLogConfig, DMSCLogLevel};
use dmsc::cache::{DMSCCacheModule, DMSCCacheConfig, DMSCCacheBackendType, DMSCCacheManager};
use dmsc::observability::{DMSCMetricsRegistry, DMSCTracer};
use dmsc::fs::DMSCFileSystem;
use dmsc::core::DMSCResult;

pub struct ZiContext {
    logger: DMSCLogger,
    cache: Arc<RwLock<DMSCCacheModule>>,
    metrics: Arc<DMSCMetricsRegistry>,
    tracer: Arc<DMSCTracer>,
    fs: DMSCFileSystem,
}

impl ZiContext {
    #[allow(non_snake_case)]
    pub async fn new() -> DMSCResult<Self> {
        Self::new_with_config(ZiContextConfig::default()).await
    }

    #[allow(non_snake_case)]
    pub async fn new_with_config(config: ZiContextConfig) -> DMSCResult<Self> {
        let fs = DMSCFileSystem::new_auto_root()?;
        
        let log_config = DMSCLogConfig {
            level: config.log_level,
            console_enabled: config.console_enabled,
            file_enabled: config.file_enabled,
            file_name: config.log_file_name.clone(),
            ..Default::default()
        };
        let logger = DMSCLogger::new(&log_config, fs.clone());

        let cache_config = DMSCCacheConfig {
            enabled: config.cache_enabled,
            default_ttl_secs: config.cache_ttl_secs,
            max_memory_mb: config.cache_max_memory_mb as u64,
            backend_type: DMSCCacheBackendType::Memory,
            ..Default::default()
        };
        let cache_module = DMSCCacheModule::new(cache_config).await;
        let cache = Arc::new(RwLock::new(cache_module));

        let metrics = Arc::new(DMSCMetricsRegistry::new());
        let tracer = Arc::new(DMSCTracer::new(1.0));

        Ok(Self {
            logger,
            cache,
            metrics,
            tracer,
            fs,
        })
    }

    #[allow(non_snake_case)]
    pub fn logger(&self) -> &DMSCLogger {
        &self.logger
    }

    #[allow(non_snake_case)]
    pub async fn cache_manager(&self) -> Arc<RwLock<DMSCCacheManager>> {
        self.cache.read().await.cache_manager()
    }

    #[allow(non_snake_case)]
    pub fn metrics(&self) -> Arc<DMSCMetricsRegistry> {
        self.metrics.clone()
    }

    #[allow(non_snake_case)]
    pub fn tracer(&self) -> Arc<DMSCTracer> {
        self.tracer.clone()
    }

    #[allow(non_snake_case)]
    pub fn file_system(&self) -> &DMSCFileSystem {
        &self.fs
    }
}

impl Clone for ZiContext {
    fn clone(&self) -> Self {
        Self {
            logger: self.logger.clone(),
            cache: self.cache.clone(),
            metrics: self.metrics.clone(),
            tracer: self.tracer.clone(),
            fs: self.fs.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ZiContextConfig {
    pub log_level: DMSCLogLevel,
    pub console_enabled: bool,
    pub file_enabled: bool,
    pub log_file_name: String,
    pub cache_enabled: bool,
    pub cache_ttl_secs: u64,
    pub cache_max_memory_mb: usize,
}

impl Default for ZiContextConfig {
    fn default() -> Self {
        Self {
            log_level: DMSCLogLevel::Info,
            console_enabled: true,
            file_enabled: true,
            log_file_name: "zi.log".to_string(),
            cache_enabled: true,
            cache_ttl_secs: 3600,
            cache_max_memory_mb: 512,
        }
    }
}

impl ZiContextConfig {
    #[allow(non_snake_case)]
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(non_snake_case)]
    pub fn log_level(mut self, level: DMSCLogLevel) -> Self {
        self.log_level = level;
        self
    }

    #[allow(non_snake_case)]
    pub fn console_enabled(mut self, enabled: bool) -> Self {
        self.console_enabled = enabled;
        self
    }

    #[allow(non_snake_case)]
    pub fn file_enabled(mut self, enabled: bool) -> Self {
        self.file_enabled = enabled;
        self
    }

    #[allow(non_snake_case)]
    pub fn log_file_name(mut self, name: &str) -> Self {
        self.log_file_name = name.to_string();
        self
    }

    #[allow(non_snake_case)]
    pub fn cache_enabled(mut self, enabled: bool) -> Self {
        self.cache_enabled = enabled;
        self
    }

    #[allow(non_snake_case)]
    pub fn cache_ttl_secs(mut self, secs: u64) -> Self {
        self.cache_ttl_secs = secs;
        self
    }

    #[allow(non_snake_case)]
    pub fn cache_max_memory_mb(mut self, mb: usize) -> Self {
        self.cache_max_memory_mb = mb;
        self
    }
}
