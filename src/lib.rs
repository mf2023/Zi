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

//! # Zi Core Library
//!
//! This is the main library entry point for the Zi data quality framework.
//! It provides a comprehensive set of APIs for data processing, quality assessment,
//! transformation, and enrichment of multi-modal data (text, images, audio, video).
//!
//! ## Module Overview
//!
//! The library is organized into the following major modules:
//!
//! - **domain**: Core domain encoding types for multi-modal data (text, image, audio, video)
//! - **record**: ZiRecord and related data structures for data representation
//! - **operator**: Core operator traits and execution logic
//! - **operators**: Collection of built-in operators for data processing
//! - **pipeline**: Pipeline building and execution infrastructure
//! - **dag**: Directed Acyclic Graph representation for operator dependencies
//! - **context**: Execution context for managing pipeline state
//! - **ingest**: Data ingestion and format detection
//! - **inspect**: Data profiling, statistics, and diff analysis
//! - **export**: Data export and serialization
//! - **enrich**: Data augmentation and synthesis
//! - **dsl**: Domain-specific language parser and compiler
//! - **orbit**: Plugin system for extensibility
//! - **distributed**: Distributed computing support
//! - **metrics**: Quality metrics and statistics
//!
//! ## Feature Flags
//!
//! - `domain`: Enables multi-modal domain types (text, image, audio, video)
//! - `pyo3`: Enables Python bindings (PyO3 integration)
//! - `distributed`: Enables distributed computing features
//! - `full`: Enables all features
//!
//! ## Quick Start
//!
//! ```rust
//! use zi::{ZiRecord, ZiOperator, ZiPipelineBuilder};
//!
//! // Create records
//! let records = vec![
//!     ZiRecord::new("1", r#"{"text": "hello"}"#, None),
//!     ZiRecord::new("2", r#"{"text": "world"}"#, None),
//! ];
//!
//! // Build a pipeline
//! let pipeline = (ZiPipelineBuilder::new()
//!     .filter("filter.equals", r#"{"path": "text", "value": "hello"}"#)
//!     .build());
//!
//! // Execute
//! let results = pipeline.process(records).unwrap();
//! ```
//!
//! ## Architecture
//!
//! Zi follows a pipeline-based architecture:
//! 1. **Records**: Data is represented as ZiRecord with flexible payload
//! 2. **Operators**: Individual processing steps (filter, transform, validate)
//! 3. **Pipeline**: Composes operators into executable workflows
//! 4. **DAG**: Manages operator dependencies and execution order
//! 5. **Context**: Provides execution state and configuration
//!
//! ## Error Handling
//!
//! All operations return `Result<T, ZiError>` for explicit error handling.
//! Common error types include parsing errors, validation failures, and I/O errors.

#![allow(non_snake_case)]

pub mod errors;
#[cfg(feature = "domain")]
pub mod domain;
pub mod io;
pub mod dag;
pub mod operator;
pub mod operators;
pub mod pipeline;
#[cfg(feature = "pyo3")]
pub mod py;
pub mod record;
pub mod version;
pub mod orbit;
pub mod distributed;
pub mod context;
pub mod metrics;

pub mod ingest;
pub mod inspect;
pub mod export;
pub mod enrich;
pub mod dsl;

pub use context::ZiContext;
pub use metrics::{ZiQualityMetrics, ZiStatisticSummary};

pub use errors::{Result, ZiError};
pub use record::{ZiRecord, ZiMetadata, ZiRecordBatch};
pub use operator::{ZiOperator, execute_operator};
pub use pipeline::{ZiPipeline, ZiPipelineNode, ZiPipelineBuilder, ZiPipelineStageMetrics, ZiCacheConfig, ExecutionMode};
pub use dag::{ZiDAG, ZiGraphNode, ZiNodeId, ZiGraphNodeConfig, ZiCheckpointState, ZiCheckpointStore, ZiSchedulerConfig, ZiScheduler, ZiOperatorFactoryTrait};

pub use ingest::{ZiFormatDetector, ZiDataFormat, ZiStreamReader, ZiReaderConfig, ZiCompression, ZiFormatInfo, ZiRecordIterator, ProgressCallback, ProgressInfo};
pub use inspect::{
    ZiProfileReport, ZiProfiler, ZiFieldProfile, ZiAnomaly, 
    ZiDiffReport, ZiDiffer, ZiDiffChange, ZiChangeType, ZiStatistics, 
    ZiTextStatistics, ZiProfilerConfig, ZiDiffStats, ZiFieldChange, ZiRecordDiff,
};
pub use export::{ZiStreamWriter, ZiWriterConfig, ZiManifest, ZiManifestBuilder, ZiOutputFormat, ZiWriteStats, ZiManifestFile, ZiLineage};
pub use enrich::{
    ZiSynthesizer, ZiSynthesisConfig, ZiSynthesisMode, ZiSynthesisRule,
    ZiAnnotator, ZiAnnotationConfig, ZiAugmenter, ZiAugmentationConfig,
    ZiRuleType, ZiTemplate, ZiTemplateVariable, ZiLLMSynthesisConfig,
};
pub use dsl::{ZiDSLParser, ZiParseResult, ZiDSLNode, ZiDSLProgram, ZiDSLCompiler, ZiCompiledPipeline, ZiDSLParserConfig};

pub use operators::llm::{
    ZiTokenCountConfig, ZiConversationConfig, ZiConversationFormat,
    ZiContextLengthConfig, ZiContextLengthAction, ZiQAExtractConfig, ZiQAPattern,
    ZiInstructionFormatConfig, ZiInstructionFormat,
};
