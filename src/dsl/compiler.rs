//! Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
//!
//! This file is part of Zi.
//! The Zi project belongs to the Dunimd project team.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//!     http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use crate::errors::{Result, ZiError};
use crate::dsl::ir::ZiCDSLProgram;
use crate::operator::ZiCOperator;
use crate::record::ZiCRecordBatch;

#[derive(Debug)]
pub struct ZiCCompiledPipeline {
    operators: Vec<Box<dyn ZiCOperator + Send + Sync>>,
}

impl ZiCCompiledPipeline {
    #[allow(non_snake_case)]
    pub fn ZiFRun(&self, batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        let mut current = batch;
        for operator in &self.operators {
            current = operator.apply(current)?;
        }
        Ok(current)
    }

    #[allow(non_snake_case)]
    pub fn ZiFOperatorCount(&self) -> usize {
        self.operators.len()
    }
}

#[derive(Debug, Default)]
pub struct ZiCDSLCompiler {
    strict: bool,
}

impl ZiCDSLCompiler {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        Self { strict: false }
    }

    #[allow(non_snake_case)]
    pub fn ZiFStrict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFCompile(&self, program: &ZiCDSLProgram) -> Result<ZiCCompiledPipeline> {
        let mut operators = Vec::new();

        for node in &program.nodes {
            let operator = self.compile_node(node)?;
            operators.push(operator);
        }

        Ok(ZiCCompiledPipeline { operators })
    }

    fn compile_node(&self, node: &crate::dsl::ir::ZiCDSLNode) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
        let operator_name = node.operator.as_str();
        
        match operator_name {
            // Filter operators
            "filter.equals" => crate::operators::filter::ZiFFilterEqualsFactory(&node.config),
            "filter.not_equals" => crate::operators::filter::ZiFFilterNotEqualsFactory(&node.config),
            "filter.any" => crate::operators::filter::ZiFFilterAnyFactory(&node.config),
            "filter.between" => crate::operators::filter::ZiFFilterBetweenFactory(&node.config),
            "filter.less_than" => crate::operators::filter::ZiFFilterLessThanFactory(&node.config),
            "filter.greater_than" => crate::operators::filter::ZiFFilterGreaterThanFactory(&node.config),
            "filter.is_null" => crate::operators::filter::ZiFFilterIsNullFactory(&node.config),
            "filter.regex" => crate::operators::filter::ZiFFilterRegexFactory(&node.config),
            "filter.ends_with" => crate::operators::filter::ZiFFilterEndsWithFactory(&node.config),
            "filter.starts_with" => crate::operators::filter::ZiFFilterStartsWithFactory(&node.config),
            "filter.range" => crate::operators::filter::ZiFFilterRangeFactory(&node.config),
            "filter.in" => crate::operators::filter::ZiFFilterInFactory(&node.config),
            "filter.not_in" => crate::operators::filter::ZiFFilterNotInFactory(&node.config),
            "filter.contains" => crate::operators::filter::ZiFFilterContainsFactory(&node.config),
            "filter.contains_all" => crate::operators::filter::ZiFFilterContainsAllFactory(&node.config),
            "filter.contains_any" => crate::operators::filter::ZiFFilterContainsAnyFactory(&node.config),
            "filter.contains_none" => crate::operators::filter::ZiFFilterContainsNoneFactory(&node.config),
            "filter.array_contains" => crate::operators::filter::ZiFFilterArrayContainsFactory(&node.config),
            "filter.exists" => crate::operators::filter::ZiFFilterExistsFactory(&node.config),
            "filter.not_exists" => crate::operators::filter::ZiFFilterNotExistsFactory(&node.config),
            "filter.length_range" => crate::operators::filter::ZiFFilterLengthRangeFactory(&node.config),
            "filter.token_range" => crate::operators::filter::ZiFFilterTokenRangeFactory(&node.config),
            
            // Language operators
            "lang.detect" => crate::operators::lang::ZiFLangDetectFactory(&node.config),
            "lang.confidence" => crate::operators::lang::ZiFLangConfidenceFactory(&node.config),
            
            // Quality operators
            "quality.score" => crate::operators::quality::ZiFQualityScoreFactory(&node.config),
            "quality.filter" => crate::operators::quality::ZiFQualityFilterFactory(&node.config),
            "quality.toxicity" => crate::operators::quality::ZiFToxicityFactory(&node.config),
            
            // Dedup operators
            "dedup.simhash" => crate::operators::dedup::ZiFDedupSimhashFactory(&node.config),
            "dedup.minhash" => crate::operators::dedup::ZiFDedupMinhashFactory(&node.config),
            "dedup.semantic" => crate::operators::dedup::ZiFDedupSemanticFactory(&node.config),
            
            // Transform operators
            "transform.normalize" => crate::operators::transform::ZiFTransformNormalizeFactory(&node.config),
            
            // Metadata operators
            "metadata.enrich" => crate::operators::metadata::ZiFMetadataEnrichFactory(&node.config),
            "metadata.rename" => crate::operators::metadata::ZiFMetadataRenameFactory(&node.config),
            "metadata.remove" => crate::operators::metadata::ZiFMetadataRemoveFactory(&node.config),
            "metadata.copy" => crate::operators::metadata::ZiFMetadataCopyFactory(&node.config),
            "metadata.require" => crate::operators::metadata::ZiFMetadataRequireFactory(&node.config),
            "metadata.extract" => crate::operators::metadata::ZiFMetadataExtractFactory(&node.config),
            "metadata.keep" => crate::operators::metadata::ZiFMetadataKeepFactory(&node.config),
            
            // Limit operator
            "limit" => crate::operators::limit::ZiFLimitFactory(&node.config),
            
            // Sample operators
            "sample.random" => crate::operators::sample::ZiFSampleRandomFactory(&node.config),
            "sample.top" => crate::operators::sample::ZiFSampleTopFactory(&node.config),
            
            // PII operators
            "pii.redact" => crate::operators::pii::ZiFPiiRedactFactory(&node.config),
            
            // Augment operators
            "augment.synonym" => crate::operators::augment::ZiFAugmentSynonymFactory(&node.config),
            "augment.noise" => crate::operators::augment::ZiFAugmentNoiseFactory(&node.config),
            
            // LLM operators
            "llm.token_count" => crate::operators::llm::ZiFTokenCountFactory(&node.config),
            "llm.conversation_format" => crate::operators::llm::ZiFConversationFormatFactory(&node.config),
            "llm.context_length" => crate::operators::llm::ZiFContextLengthFactory(&node.config),
            "llm.qa_extract" => crate::operators::llm::ZiFQAExtractFactory(&node.config),
            "llm.instruction_format" => crate::operators::llm::ZiFInstructionFormatFactory(&node.config),
            
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
