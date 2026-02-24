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

//! # Data Ingestion Module
//!
//! This module provides data ingestion capabilities for the Zi framework,
//! enabling users to read data from various formats and detect file types automatically.
//!
//! ## Module Components
//!
//! - **Format Detection** ([format.rs](format/index.html)): Automatic format and compression detection
//! - **Reader** ([reader.rs](reader/index.html)): Stream-based record reading with progress tracking
//!
//! ## Supported Input Formats
//!
//! - **JSONL**: Line-delimited JSON for streaming
//! - **JSON**: JSON arrays or objects
//! - **CSV**: Comma-separated values with headers
//! - **Parquet**: Apache Parquet columnar format
//!
//! ## Supported Compression
//!
//! - **Gzip**: .gz files
//! - **Zstd**: .zst files
//! - **Bzip2**: .bz2 files
//! - **Xz**: .xz files
//!
//! ## Usage Patterns
//!
//! ### Reading Records
//!
//! ```rust
//! use zi::ingest::{ZiStreamReader, ZiReaderConfig};
//!
//! let reader = ZiStreamReader::new()
//!     .with_config(ZiReaderConfig {
//!         batch_size: 1000,
//!         ..Default::default()
//!     });
//! let batch = reader.read_path(&path)?;
//! ```
//!
//! ### Detecting Format
//!
//! ```rust
//! use zi::ingest::{ZiFormatDetector, ZiDataFormat};
//!
//! let detector = ZiFormatDetector::new();
//! let format = detector.detect_from_path(&path)?;
//! ```

pub mod format;
pub mod reader;

pub use format::{ZiFormatDetector, ZiDataFormat, ZiCompression, ZiFormatInfo};
pub use reader::{ZiStreamReader, ZiReaderConfig, ZiRecordIterator, ProgressCallback, ProgressInfo};
