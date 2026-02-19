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

use std::path::Path;

use crate::errors::{Result, ZiError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ZiCDataFormat {
    Jsonl,
    Csv,
    Parquet,
    Json,
    Unknown,
}

#[derive(Clone, Debug, Default)]
pub struct ZiCFormatDetector {
    sample_size: usize,
}

impl ZiCFormatDetector {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        Self { sample_size: 8192 }
    }

    #[allow(non_snake_case)]
    pub fn ZiFWithSampleSize(mut self, size: usize) -> Self {
        self.sample_size = size;
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFDetectFromPath(&self, path: &Path) -> Result<ZiCDataFormat> {
        let extension = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        match extension.as_str() {
            "jsonl" => Ok(ZiCDataFormat::Jsonl),
            "json" => Ok(ZiCDataFormat::Json),
            "csv" => Ok(ZiCDataFormat::Csv),
            "parquet" | "par" => Ok(ZiCDataFormat::Parquet),
            _ => Ok(ZiCDataFormat::Unknown),
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFDetectFromContent(&self, content: &[u8]) -> ZiCDataFormat {
        if content.is_empty() {
            return ZiCDataFormat::Unknown;
        }

        let content_str = String::from_utf8_lossy(content);
        let trimmed = content_str.trim();

        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            if trimmed.contains("\n{") || trimmed.contains("\n\t{") {
                return ZiCDataFormat::Jsonl;
            }
            return ZiCDataFormat::Json;
        }

        if trimmed.contains(',') && trimmed.contains('\n') {
            return ZiCDataFormat::Csv;
        }

        if content.starts_with(b"PAR1") {
            return ZiCDataFormat::Parquet;
        }

        ZiCDataFormat::Unknown
    }
}
