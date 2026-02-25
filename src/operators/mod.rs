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

//! # Operators Module
//!
//! This module contains all data processing operators available in Zi.
//! Operators are the fundamental building blocks of Zi pipelines, each implementing
//! specific transformations, filters, or augmentations on data records.
//!
//! ## Operator Categories
//!
//! - **augment**: Data augmentation operators (synonym replacement, noise injection)
//! - **dedup**: Deduplication operators (simhash, minhash, semantic)
//! - **field**: Field manipulation operators (select, rename, drop, copy)
//! - **filter**: Filtering operators (equals, between, regex, etc.)
//! - **lang**: Language detection operators
//! - **limit**: Result limiting operators
//! - **llm**: LLM-related operators (token counting, conversation formatting)
//! - **merge**: Record merging operators (concat, batch, union)
//! - **metadata**: Metadata manipulation operators
//! - **pii**: PII detection and redaction operators
//! - **quality**: Quality assessment operators
//! - **sample**: Sampling operators (random, top, stratified)
//! - **shuffle**: Shuffling operators
//! - **split**: Data splitting operators
//! - **token**: Tokenization operators
//! - **transform**: General transformation operators
//!
//! ## Usage
//!
//! Operators are typically created through factory functions and applied to
//! record batches through the [`ZiOperator`] trait.

pub mod augment;
pub mod dedup;
pub mod field;
pub mod filter;
pub mod lang;
pub mod limit;
pub mod llm;
pub mod merge;
pub mod metadata;
pub mod pii;
pub mod quality;
pub mod sample;
pub mod shuffle;
pub mod split;
pub mod token;
pub mod transform;
