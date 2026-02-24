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

//! # DSL Compiler Module
//!
//! This module provides the compiler for the Zi DSL. It converts the intermediate
//! representation (IR) into executable operator pipelines that can process data.
//!
//! ## Compilation Process
//!
//! ```text
//! ZiDSLProgram (IR)
//!       |
//!       v
//!   ZiDSLCompiler::compile()
//!       |
//!       v
//!  ZiCompiledPipeline
//!       |
//!       v
//!  Pipeline Execution
//! ```
//!
//! ## Supported Operators
//!
//! The compiler supports a wide range of operators organized by category:
//!
//! ### Filter Operators
//! - `filter.equals` - Filter by exact value match
//! - `filter.not_equals` - Filter by value mismatch
//! - `filter.any` - Filter if any condition matches
//! - `filter.between` - Filter by value range
//! - `filter.less_than` - Filter by less than comparison
//! - `filter.greater_than` - Filter by greater than comparison
//! - `filter.is_null` - Filter by null/empty values
//! - `filter.regex` - Filter by regex pattern
//! - `filter.starts_with` - Filter by prefix
//! - `filter.ends_with` - Filter by suffix
//! - `filter.contains` - Filter by substring
//! - `filter.in` - Filter by value in set
//! - `filter.not_in` - Filter by value not in set
//! - `filter.length_range` - Filter by string length
//! - `filter.token_range` - Filter by token count
//!
//! ### Language Operators
//! - `lang.detect` - Detect language
//! - `lang.confidence` - Get language detection confidence
//!
//! ### Quality Operators
//! - `quality.score` - Calculate quality score
//! - `quality.filter` - Filter by quality threshold
//! - `quality.toxicity` - Detect toxicity
//!
//! ### Deduplication Operators
//! - `dedup.simhash` - SimHash based deduplication
//! - `dedup.minhash` - MinHash based deduplication
//! - `dedup.semantic` - Semantic deduplication
//!
//! ### Transform Operators
//! - `transform.normalize` - Normalize text
//!
//! ### Metadata Operators
//! - `metadata.enrich` - Enrich metadata
//! - `metadata.rename` - Rename metadata fields
//! - `metadata.remove` - Remove metadata fields
//! - `metadata.copy` - Copy metadata fields
//! - `metadata.require` - Require metadata fields
//! - `metadata.extract` - Extract metadata
//! - `metadata.keep` - Keep only specified fields
//!
//! ### Limit Operators
//! - `limit` - Limit number of records
//!
//! ### Sample Operators
//! - `sample.random` - Random sampling
//! - `sample.top` - Top-k sampling
//!
//! ### PII Operators
//! - `pii.redact` - Redact PII
//!
//! ### Augmentation Operators
//! - `augment.synonym` - Synonym augmentation
//! - `augment.noise` - Add noise
//!
//! ### LLM Operators
//! - `llm.token_count` - Count tokens
//! - `llm.conversation_format` - Format as conversation
//! - `llm.context_length` - Calculate context length
//! - `llm.qa_extract` - Extract Q&A
//! - `llm.instruction_format` - Format as instruction

use crate::errors::{Result, ZiError};
use crate::dsl::ir::ZiDSLProgram;
use crate::operator::ZiOperator;
use crate::record::ZiRecordBatch;

/// Represents a compiled pipeline ready for execution.
///
/// This struct contains the compiled operators in the order they should be applied
/// to incoming data batches.
///
/// # Usage
///
/// ```rust
/// let compiler = ZiDSLCompiler::new();
/// let pipeline = compiler.compile(&program)?;
/// let result = pipeline.run(batch)?;
/// ```
#[derive(Debug)]
pub struct ZiCompiledPipeline {
    /// Vector of compiled operators in execution order.
    /// Each operator implements ZiOperator trait.
    operators: Vec<Box<dyn ZiOperator + Send + Sync>>,
}

impl ZiCompiledPipeline {
    /// Runs the compiled pipeline on an input batch.
    ///
    /// Processes the input batch through all operators in sequence,
    /// with each operator receiving the output of the previous one.
    ///
    /// # Arguments
    ///
    /// - `batch`: Input ZiRecordBatch to process
    ///
    /// # Returns
    ///
    /// Result containing the processed ZiRecordBatch or an error
    #[allow(non_snake_case)]
    pub fn run(&self, batch: ZiRecordBatch) -> Result<ZiRecordBatch> {
        let mut current = batch;
        for operator in &self.operators {
            current = operator.apply(current)?;
        }
        Ok(current)
    }

    /// Returns the number of operators in the pipeline.
    ///
    /// # Returns
    ///
    /// The count of operators in the compiled pipeline
    #[allow(non_snake_case)]
    pub fn operator_count(&self) -> usize {
        self.operators.len()
    }
}

/// DSL Compiler for converting IR to executable pipelines.
///
/// The compiler resolves operator names to their corresponding factory functions
/// and creates the operator instances with their configurations.
///
/// # Construction
///
/// ```rust
/// use zi::dsl::compiler::ZiDSLCompiler;
///
/// // Create compiler with default settings
/// let compiler = ZiDSLCompiler::new();
///
/// // Create compiler with strict mode
/// let strict_compiler = ZiDSLCompiler::new().strict(true);
/// ```
///
/// # Usage
///
/// ```rust
/// use zi::dsl::{ZiDSLParser, ZiDSLCompiler};
///
/// let parser = ZiDSLParser::new();
/// let result = parser.parse(json_source)?;
///
/// let compiler = ZiDSLCompiler::new();
/// let pipeline = compiler.compile(&result.program)?;
///
/// let processed = pipeline.run(batch)?;
/// ```
#[derive(Debug, Default)]
pub struct ZiDSLCompiler {
    /// If true, unknown operators cause compilation errors.
    /// If false, unknown operators are logged as warnings and skipped.
    strict: bool,
}

impl ZiDSLCompiler {
    /// Creates a new compiler with default settings.
    ///
    /// # Returns
    ///
    /// A new ZiDSLCompiler instance
    #[allow(non_snake_case)]
    pub fn new() -> Self {
        Self { strict: false }
    }

    /// Sets the strict compilation mode.
    ///
    /// In strict mode, unknown operators cause compilation errors.
    /// In non-strict mode, unknown operators are logged as warnings.
    ///
    /// # Arguments
    ///
    /// - `strict`: Whether to use strict mode
    ///
    /// # Returns
    ///
    /// Self for method chaining
    #[allow(non_snake_case)]
    pub fn strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    /// Compiles a DSL program into an executable pipeline.
    ///
    /// This method iterates through all nodes in the program and creates
    /// corresponding operator instances using factory functions.
    ///
    /// # Arguments
    ///
    /// - `program`: Reference to ZiDSLProgram to compile
    ///
    /// # Returns
    ///
    /// Result containing ZiCompiledPipeline or an error
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - An unknown operator is encountered in strict mode
    /// - Operator factory function fails
    #[allow(non_snake_case)]
    pub fn compile(&self, program: &ZiDSLProgram) -> Result<ZiCompiledPipeline> {
        let mut operators = Vec::new();

        for node in &program.nodes {
            let operator = self.compile_node(node)?;
            operators.push(operator);
        }

        Ok(ZiCompiledPipeline { operators })
    }

    /// Compiles a single DSL node into an operator.
    ///
    /// Looks up the operator name in the registry and calls its factory function
    /// with the node's configuration.
    ///
    /// # Arguments
    ///
    /// - `node`: Reference to ZiDSLNode to compile
    ///
    /// # Returns
    ///
    /// Result containing boxed operator or an error
    fn compile_node(&self, node: &crate::dsl::ir::ZiDSLNode) -> Result<Box<dyn ZiOperator + Send + Sync>> {
        let operator_name = node.operator.as_str();
        
        match operator_name {
            // Filter operators
            "filter.equals" => crate::operators::filter::filter_equals_factory(&node.config),
            "filter.not_equals" => crate::operators::filter::filter_not_equals_factory(&node.config),
            "filter.any" => crate::operators::filter::filter_any_factory(&node.config),
            "filter.between" => crate::operators::filter::filter_between_factory(&node.config),
            "filter.less_than" => crate::operators::filter::filter_less_than_factory(&node.config),
            "filter.greater_than" => crate::operators::filter::filter_greater_than_factory(&node.config),
            "filter.is_null" => crate::operators::filter::filter_is_null_factory(&node.config),
            "filter.regex" => crate::operators::filter::filter_regex_factory(&node.config),
            "filter.ends_with" => crate::operators::filter::filter_ends_with_factory(&node.config),
            "filter.starts_with" => crate::operators::filter::filter_starts_with_factory(&node.config),
            "filter.range" => crate::operators::filter::filter_range_factory(&node.config),
            "filter.in" => crate::operators::filter::filter_in_factory(&node.config),
            "filter.not_in" => crate::operators::filter::filter_not_in_factory(&node.config),
            "filter.contains" => crate::operators::filter::filter_contains_factory(&node.config),
            "filter.contains_all" => crate::operators::filter::filter_contains_all_factory(&node.config),
            "filter.contains_any" => crate::operators::filter::filter_contains_any_factory(&node.config),
            "filter.contains_none" => crate::operators::filter::filter_contains_none_factory(&node.config),
            "filter.array_contains" => crate::operators::filter::filter_array_contains_factory(&node.config),
            "filter.exists" => crate::operators::filter::filter_exists_factory(&node.config),
            "filter.not_exists" => crate::operators::filter::filter_not_exists_factory(&node.config),
            "filter.length_range" => crate::operators::filter::filter_length_range_factory(&node.config),
            "filter.token_range" => crate::operators::filter::filter_token_range_factory(&node.config),
            
            // Language operators
            "lang.detect" => crate::operators::lang::lang_detect_factory(&node.config),
            "lang.confidence" => crate::operators::lang::lang_confidence_factory(&node.config),
            
            // Quality operators
            "quality.score" => crate::operators::quality::quality_score_factory(&node.config),
            "quality.filter" => crate::operators::quality::quality_filter_factory(&node.config),
            "quality.toxicity" => crate::operators::quality::toxicity_factory(&node.config),
            
            // Dedup operators
            "dedup.simhash" => crate::operators::dedup::dedup_simhash_factory(&node.config),
            "dedup.minhash" => crate::operators::dedup::dedup_minhash_factory(&node.config),
            "dedup.semantic" => crate::operators::dedup::dedup_semantic_factory(&node.config),
            
            // Transform operators
            "transform.normalize" => crate::operators::transform::transform_normalize_factory(&node.config),
            "transform.map" => crate::operators::transform::transform_map_factory(&node.config),
            "transform.template" => crate::operators::transform::transform_template_factory(&node.config),
            "transform.chain" => crate::operators::transform::transform_chain_factory(&node.config),
            "transform.flat_map" => crate::operators::transform::transform_flat_map_factory(&node.config),
            "transform.coalesce" => crate::operators::transform::transform_coalesce_factory(&node.config),
            "transform.conditional" => crate::operators::transform::transform_conditional_factory(&node.config),
            
            // Field operators
            "field.select" => crate::operators::field::field_select_factory(&node.config),
            "field.rename" => crate::operators::field::field_rename_factory(&node.config),
            "field.drop" => crate::operators::field::field_drop_factory(&node.config),
            "field.copy" => crate::operators::field::field_copy_factory(&node.config),
            "field.move" => crate::operators::field::field_move_factory(&node.config),
            "field.flatten" => crate::operators::field::field_flatten_factory(&node.config),
            "field.default" => crate::operators::field::field_default_factory(&node.config),
            "field.require" => crate::operators::field::field_require_factory(&node.config),
            
            // Shuffle operators
            "shuffle" => crate::operators::shuffle::shuffle_factory(&node.config),
            "shuffle.deterministic" => crate::operators::shuffle::shuffle_deterministic_factory(&node.config),
            "shuffle.block" => crate::operators::shuffle::shuffle_block_factory(&node.config),
            "shuffle.stratified" => crate::operators::shuffle::shuffle_stratified_factory(&node.config),
            "shuffle.window" => crate::operators::shuffle::shuffle_window_factory(&node.config),
            
            // Split operators
            "split.random" => crate::operators::split::split_random_factory(&node.config),
            "split.stratified" => crate::operators::split::split_stratified_factory(&node.config),
            "split.sequential" => crate::operators::split::split_sequential_factory(&node.config),
            "split.kfold" => crate::operators::split::split_k_fold_factory(&node.config),
            "split.chunk" => crate::operators::split::split_chunk_factory(&node.config),
            
            // Merge operators
            "merge.concat" => crate::operators::merge::merge_concat_factory(&node.config),
            "merge.batch" => crate::operators::merge::merge_batch_factory(&node.config),
            "merge.union" => crate::operators::merge::merge_union_factory(&node.config),
            "merge.intersect" => crate::operators::merge::merge_intersect_factory(&node.config),
            "merge.difference" => crate::operators::merge::merge_difference_factory(&node.config),
            "merge.zip" => crate::operators::merge::merge_zip_factory(&node.config),
            
            // Token operators
            "token.count" => crate::operators::token::token_count_factory(&node.config),
            "token.stats" => crate::operators::token::token_stats_factory(&node.config),
            "token.filter" => crate::operators::token::token_filter_factory(&node.config),
            "token.histogram" => crate::operators::token::token_histogram_factory(&node.config),
            
            // Sample operators
            "sample.random" => crate::operators::sample::sample_random_factory(&node.config),
            "sample.top" => crate::operators::sample::sample_top_factory(&node.config),
            "sample.balanced" => crate::operators::sample::sample_balanced_factory(&node.config),
            "sample.by_distribution" => crate::operators::sample::sample_by_distribution_factory(&node.config),
            "sample.by_length" => crate::operators::sample::sample_by_length_factory(&node.config),
            "sample.stratified" => crate::operators::sample::sample_stratified_factory(&node.config),
            
            // Metadata operators
            "metadata.enrich" => crate::operators::metadata::metadata_enrich_factory(&node.config),
            "metadata.rename" => crate::operators::metadata::metadata_rename_factory(&node.config),
            "metadata.remove" => crate::operators::metadata::metadata_remove_factory(&node.config),
            "metadata.copy" => crate::operators::metadata::metadata_copy_factory(&node.config),
            "metadata.require" => crate::operators::metadata::metadata_require_factory(&node.config),
            "metadata.extract" => crate::operators::metadata::metadata_extract_factory(&node.config),
            "metadata.keep" => crate::operators::metadata::metadata_keep_factory(&node.config),
            
            // Limit operator
            "limit" => crate::operators::limit::limit_factory(&node.config),
            
            // PII operators
            "pii.redact" => crate::operators::pii::pii_redact_factory(&node.config),
            
            // Augment operators
            "augment.synonym" => crate::operators::augment::augment_synonym_factory(&node.config),
            "augment.noise" => crate::operators::augment::augment_noise_factory(&node.config),
            
            // LLM operators
            "llm.token_count" => crate::operators::llm::token_count_factory(&node.config),
            "llm.conversation_format" => crate::operators::llm::conversation_format_factory(&node.config),
            "llm.context_length" => crate::operators::llm::context_length_factory(&node.config),
            "llm.qa_extract" => crate::operators::llm::q_a_extract_factory(&node.config),
            "llm.instruction_format" => crate::operators::llm::instruction_format_factory(&node.config),
            
            _ => {
                if self.strict {
                    Err(ZiError::validation(format!("Unknown operator: {}", operator_name)))
                } else {
                    log::warn!("Unknown operator '{}', skipping", operator_name);
                    Err(ZiError::validation(format!("Unknown operator: {}", operator_name)))
                }
            }
        }
    }
}
