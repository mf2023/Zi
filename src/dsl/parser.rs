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

use crate::errors::{Result, ZiError};
use crate::dsl::ir::{ZiCDSLNode, ZiCDSLProgram};

#[derive(Clone, Debug)]
pub struct ZiCParseResult {
    pub program: ZiCDSLProgram,
    pub warnings: Vec<String>,
}

#[derive(Debug, Default)]
pub struct ZiCDSLParser {
    strict: bool,
}

impl ZiCDSLParser {
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
    pub fn ZiFParse(&self, source: &str) -> Result<ZiCParseResult> {
        let mut warnings = Vec::new();
        let mut nodes = Vec::new();

        for (line_num, line) in source.lines().enumerate() {
            let trimmed = line.trim();
            
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }

            match self.parse_line(trimmed) {
                Ok(node) => nodes.push(node),
                Err(e) => {
                    if self.strict {
                        return Err(ZiError::validation(format!(
                            "Parse error at line {}: {}",
                            line_num + 1,
                            e
                        )));
                    }
                    warnings.push(format!("Line {}: {}", line_num + 1, e));
                }
            }
        }

        Ok(ZiCParseResult {
            program: ZiCDSLProgram { nodes },
            warnings,
        })
    }

    fn parse_line(&self, line: &str) -> Result<ZiCDSLNode> {
        let parts: Vec<&str> = line.splitn(2, ' ').collect();
        
        if parts.is_empty() {
            return Err(ZiError::validation("Empty line"));
        }

        let operator = parts[0].to_string();
        let config = if parts.len() > 1 {
            serde_json::from_str(parts[1]).unwrap_or_else(|_| {
                serde_json::Value::String(parts[1].to_string())
            })
        } else {
            serde_json::Value::Object(serde_json::Map::new())
        };

        Ok(ZiCDSLNode {
            operator,
            config,
            input: None,
            output: None,
        })
    }
}
