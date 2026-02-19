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

#![allow(non_snake_case)]

pub mod errors;
#[cfg(feature = "domain")]
pub mod domain;
pub mod io;
pub mod dag;
pub mod operator;
pub mod operators;
pub mod pipeline;
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
pub use metrics::{ZiCQualityMetrics, ZiCStatisticSummary};

pub use ingest::{ZiCFormatDetector, ZiCDataFormat, ZiCStreamReader, ZiCReaderConfig};
pub use inspect::{ZiCProfileReport, ZiCProfiler, ZiCDiffReport, ZiCDiffer, ZiCStatistics};
pub use export::{ZiCStreamWriter, ZiCWriterConfig, ZiCManifest, ZiCManifestBuilder};
pub use enrich::{ZiCSynthesizer, ZiCSynthesisConfig, ZiCAnnotator, ZiCAnnotationConfig, ZiCAugmenter, ZiCAugmentationConfig};
pub use dsl::{ZiCDSLParser, ZiCParseResult, ZiCDSLNode, ZiCDSLProgram, ZiCDSLCompiler, ZiCCompiledPipeline};
