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

//! # Data Enrichment Module
//!
//! This module provides comprehensive data enrichment capabilities for the Zi framework,
//! enabling users to enhance, annotate, augment, and synthesize data records through
//! various transformation techniques.
//!
//! ## Module Components
//!
//! - **Synthesis** ([synthesis.rs](synthesis/index.html)): Generate synthetic data records
//!   using templates, rules, or LLM-based generation
//! - **Annotation** ([annotation.rs](annotation/index.html)): Add labels, scores, and tags
//!   to existing records
//! - **Augmentation** ([augmentation.rs](augmentation/index.html)): Increase dataset diversity
//!   through various augmentation techniques
//!
//! ## Usage Patterns
//!
//! ### Data Synthesis
//!
//! ```rust
//! use zi::enrich::{ZiSynthesizer, ZiSynthesisConfig, ZiSynthesisMode};
//!
//! let config = ZiSynthesisConfig {
//!     mode: ZiSynthesisMode::Template,
//!     template: Some("Hello {{name}}".to_string()),
//!     count: 10,
//!     ..Default::default()
//! };
//! let mut synthesizer = ZiSynthesizer::new(config);
//! let enriched = synthesizer.synthesize(&batch)?;
//! ```
//!
//! ### Data Annotation
//!
//! ```rust
//! use zi::enrich::{ZiAnnotator, ZiAnnotationConfig, ZiAnnotationType};
//!
//! let config = ZiAnnotationConfig {
//!     field: "payload.text".to_string(),
//!     annotation_type: ZiAnnotationType::Score { name: "quality".to_string() },
//! };
//! let annotator = ZiAnnotator::new(config);
//! let annotated = annotator.annotate(&batch)?;
//! ```
//!
//! ### Data Augmentation
//!
//! ```rust
//! use zi::enrich::{ZiAugmenter, ZiAugmentationConfig, ZiAugmentationMethod};
//!
//! let config = ZiAugmentationConfig {
//!     methods: vec![
//!         ZiAugmentationMethod::Duplicate { count: 2 },
//!         ZiAugmentationMethod::Noise { ratio: 0.1 },
//!     ],
//!     preserve_original: true,
//! };
//! let augmenter = ZiAugmenter::new(config);
//! let augmented = augmenter.augment(&batch)?;
//! ```

pub mod synthesis;
pub mod annotation;
pub mod augmentation;

pub use synthesis::{
    ZiSynthesizer, ZiSynthesisConfig, ZiSynthesisMode, ZiSynthesisRule,
    ZiRuleType, ZiTemplate, ZiTemplateVariable, ZiLLMSynthesisConfig,
};
pub use annotation::{ZiAnnotator, ZiAnnotationConfig};
pub use augmentation::{ZiAugmenter, ZiAugmentationConfig};
