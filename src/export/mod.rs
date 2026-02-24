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

//! # Data Export Module
//!
//! This module provides data export capabilities for the Zi framework,
//! enabling users to write records to various formats and generate dataset manifests.
//!
//! ## Module Components
//!
//! - **Writer** ([writer.rs](writer/index.html)): Stream-based record writing with format support
//! - **Manifest** ([manifest.rs](manifest/index.html)): Dataset metadata and lineage tracking
//!
//! ## Supported Output Formats
//!
//! - **JSONL**: Line-delimited JSON for streaming
//! - **JSON**: Pretty-printed or compact JSON arrays
//! - **CSV**: Comma-separated values with headers
//! - **Parquet**: Columnar format for analytical workloads (with feature flag)
//!
//! ## Usage Patterns
//!
//! ### Writing Records
//!
//! ```rust
//! use zi::export::{ZiStreamWriter, ZiWriterConfig, ZiOutputFormat};
//! use zi::record::ZiRecordBatch;
//!
//! let config = ZiWriterConfig {
//!     format: ZiOutputFormat::Jsonl,
//!     ..Default::default()
//! };
//! let mut writer = ZiStreamWriter::new(config);
//! let stats = writer.write(&batch, &path)?;
//! ```
//!
//! ### Building Manifests
//!
//! ```rust
//! use zi::export::{ZiManifestBuilder, ZiManifestFile};
//!
//! let manifest = ZiManifestBuilder::new()
//!     .with_version("1.0")
//!     .add_file_info("data.jsonl", 1024, "abc123", 100, "jsonl")
//!     .add_source("source.jsonl", "hash123", 50)
//!     .add_transform("filter", serde_json::json!({"field": "value"}))
//!     .build();
//! ```

pub mod writer;
pub mod manifest;

pub use writer::{ZiStreamWriter, ZiWriterConfig, ZiOutputFormat, ZiWriteStats};
pub use manifest::{
    ZiManifest, ZiManifestBuilder, ZiManifestFile, ZiManifestStats,
    ZiLineage, ZiLineageSource, ZiLineageTransform,
    compute_file_hash,
};
