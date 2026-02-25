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

//! # Format Detection Module
//!
//! This module provides automatic format and compression detection for data files.
//! It supports detection through file extension, magic bytes, and content analysis.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::errors::Result;

/// Supported data formats for ingestion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ZiDataFormat {
    /// Line-delimited JSON format.
    Jsonl,
    /// Comma-separated values format.
    Csv,
    /// Apache Parquet columnar format.
    Parquet,
    /// JSON array or object format.
    Json,
    /// Unknown or unsupported format.
    Unknown,
}

/// Supported compression types.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ZiCompression {
    /// No compression.
    None,
    /// Gzip compression.
    Gzip,
    /// Zstandard compression.
    Zstd,
    /// Bzip2 compression.
    Bzip2,
    /// Xz/Lzma compression.
    Xz,
}

impl ZiCompression {
    /// Returns the file extension for the compression type.
    pub fn extension(&self) -> &'static str {
        match self {
            ZiCompression::None => "",
            ZiCompression::Gzip => ".gz",
            ZiCompression::Zstd => ".zst",
            ZiCompression::Bzip2 => ".bz2",
            ZiCompression::Xz => ".xz",
        }
    }

    /// Detects compression type from file extension.
    pub fn from_extension(ext: &str) -> Self {
        match ext.to_lowercase().as_str() {
            "gz" | "gzip" => ZiCompression::Gzip,
            "zst" | "zstd" => ZiCompression::Zstd,
            "bz2" | "bzip2" => ZiCompression::Bzip2,
            "xz" | "lzma" => ZiCompression::Xz,
            _ => ZiCompression::None,
        }
    }
}

/// Combined format and compression information.
#[derive(Clone, Debug)]
pub struct ZiFormatInfo {
    /// Detected data format.
    pub format: ZiDataFormat,
    /// Detected compression type.
    pub compression: ZiCompression,
}

impl Default for ZiFormatInfo {
    fn default() -> Self {
        Self {
            format: ZiDataFormat::Unknown,
            compression: ZiCompression::None,
        }
    }
}

/// Configuration for format detection.
#[derive(Clone, Debug, Default)]
pub struct ZiFormatDetector {
    /// Number of bytes to read for content-based detection.
    sample_size: usize,
}

impl ZiFormatDetector {
    /// Creates a new format detector with default settings.
    #[allow(non_snake_case)]
    pub fn new() -> Self {
        Self { sample_size: 8192 }
    }

    /// Sets the sample size for content-based detection.
    #[allow(non_snake_case)]
    pub fn with_sample_size(mut self, size: usize) -> Self {
        self.sample_size = size;
        self
    }

    /// Detects data format from file path.
    ///
    /// Uses extension-based detection first, then falls back to content analysis.
    #[allow(non_snake_case)]
    pub fn detect_from_path(&self, path: &Path) -> Result<ZiDataFormat> {
        let info = self.detect_info_from_path(path)?;
        Ok(info.format)
    }

    /// Detects both format and compression from file path.
    ///
    /// Uses extension-based detection first, then falls back to content analysis
    /// if format cannot be determined from extension alone.
    #[allow(non_snake_case)]
    pub fn detect_info_from_path(&self, path: &Path) -> Result<ZiFormatInfo> {
        let filename = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_lowercase();

        let mut info = ZiFormatInfo::default();

        let filename_without_compression = self.detect_compression(&filename, &mut info);
        
        let extension = self.extract_base_extension(&filename_without_compression);
        info.format = self.format_from_extension(&extension);

        if info.format == ZiDataFormat::Unknown {
            if let Ok(content) = self.read_sample(path) {
                info.format = self.detect_from_content(&content);
            }
        }

        Ok(info)
    }

    /// Detects data format from file content.
    ///
    /// Analyzes the first few bytes to identify format, including:
    /// - Parquet magic bytes
    /// - JSON structure
    /// - JSONL line structure
    /// - CSV structure
    #[allow(non_snake_case)]
    pub fn detect_from_content(&self, content: &[u8]) -> ZiDataFormat {
        if content.is_empty() {
            return ZiDataFormat::Unknown;
        }

        if self.is_parquet(content) {
            return ZiDataFormat::Parquet;
        }

        let decompressed = self.try_decompress(content);
        let content_str = String::from_utf8_lossy(&decompressed);
        let trimmed = content_str.trim();

        if self.looks_like_jsonl(trimmed) {
            return ZiDataFormat::Jsonl;
        }

        if self.looks_like_json(trimmed) {
            return ZiDataFormat::Json;
        }

        if self.looks_like_csv(trimmed) {
            return ZiDataFormat::Csv;
        }

        ZiDataFormat::Unknown
    }

    /// Detects compression from filename.
    fn detect_compression<'a>(&self, filename: &'a str, info: &mut ZiFormatInfo) -> String {
        let compression_extensions = [".gz", ".zst", ".bz2", ".xz", ".gzip", ".zstd", ".bzip2", ".lzma"];
        
        for ext in compression_extensions {
            if filename.ends_with(ext) {
                info.compression = ZiCompression::from_extension(&ext[1..]);
                return filename[..filename.len() - ext.len()].to_string();
            }
        }
        
        filename.to_string()
    }

    /// Extracts base extension from filename (strips compression extension).
    fn extract_base_extension(&self, filename: &str) -> String {
        let parts: Vec<&str> = filename.rsplitn(2, '.').collect();
        if parts.len() == 2 {
            parts[0].to_string()
        } else {
            String::new()
        }
    }

    /// Maps file extension to data format.
    fn format_from_extension(&self, extension: &str) -> ZiDataFormat {
        match extension.to_lowercase().as_str() {
            "jsonl" | "jl" => ZiDataFormat::Jsonl,
            "json" => ZiDataFormat::Json,
            "csv" | "tsv" => ZiDataFormat::Csv,
            "parquet" | "par" => ZiDataFormat::Parquet,
            _ => ZiDataFormat::Unknown,
        }
    }

    /// Reads a sample of bytes from the file for content analysis.
    fn read_sample(&self, path: &Path) -> Result<Vec<u8>> {
        use std::fs::File;
        use std::io::Read;

        let mut file = File::open(path)?;
        let mut buffer = vec![0u8; self.sample_size];
        let bytes_read = file.read(&mut buffer)?;
        buffer.truncate(bytes_read);
        Ok(buffer)
    }

    /// Checks for Parquet magic bytes at file start.
    fn is_parquet(&self, content: &[u8]) -> bool {
        content.starts_with(b"PAR1")
    }

    /// Attempts to decompress content for format detection.
    fn try_decompress(&self, content: &[u8]) -> Vec<u8> {
        use flate2::read::GzDecoder;
        use std::io::Read;

        if content.len() < 2 {
            return content.to_vec();
        }

        if content.starts_with(&[0x1f, 0x8b]) {
            let mut decoder = GzDecoder::new(content);
            let mut decompressed = Vec::new();
            if decoder.read_to_end(&mut decompressed).is_ok() {
                return decompressed;
            }
        }

        content.to_vec()
    }

    /// Checks if content looks like JSONL (multiple JSON objects on separate lines).
    fn looks_like_jsonl(&self, content: &str) -> bool {
        let lines: Vec<&str> = content.lines().take(10).collect();
        if lines.is_empty() {
            return false;
        }

        let valid_lines = lines
            .iter()
            .filter(|line| !line.trim().is_empty())
            .take(5)
            .count();

        if valid_lines == 0 {
            return false;
        }

        let json_count = lines
            .iter()
            .filter(|line| {
                let trimmed = line.trim();
                (trimmed.starts_with('{') && trimmed.ends_with('}'))
                    || (trimmed.starts_with('[') && trimmed.ends_with(']'))
            })
            .count();

        json_count >= valid_lines / 2
    }

    /// Checks if content looks like JSON (single array or object).
    fn looks_like_json(&self, content: &str) -> bool {
        let trimmed = content.trim();
        (trimmed.starts_with('[') && trimmed.ends_with(']'))
            || (trimmed.starts_with('{') && trimmed.ends_with('}'))
    }

    /// Checks if content looks like CSV.
    fn looks_like_csv(&self, content: &str) -> bool {
        let lines: Vec<&str> = content.lines().take(5).collect();
        if lines.len() < 2 {
            return false;
        }

        for line in &lines {
            let commas = line.matches(',').count();
            if commas >= 2 {
                return true;
            }
        }

        false
    }
}
