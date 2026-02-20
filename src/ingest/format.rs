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

use std::path::Path;

use crate::errors::Result;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ZiCDataFormat {
    Jsonl,
    Csv,
    Parquet,
    Json,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ZiCCompression {
    None,
    Gzip,
    Zstd,
    Bzip2,
    Xz,
}

impl ZiCCompression {
    pub fn extension(&self) -> &'static str {
        match self {
            ZiCCompression::None => "",
            ZiCCompression::Gzip => ".gz",
            ZiCCompression::Zstd => ".zst",
            ZiCCompression::Bzip2 => ".bz2",
            ZiCCompression::Xz => ".xz",
        }
    }

    pub fn from_extension(ext: &str) -> Self {
        match ext.to_lowercase().as_str() {
            "gz" | "gzip" => ZiCCompression::Gzip,
            "zst" | "zstd" => ZiCCompression::Zstd,
            "bz2" | "bzip2" => ZiCCompression::Bzip2,
            "xz" | "lzma" => ZiCCompression::Xz,
            _ => ZiCCompression::None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ZiCFormatInfo {
    pub format: ZiCDataFormat,
    pub compression: ZiCCompression,
}

impl Default for ZiCFormatInfo {
    fn default() -> Self {
        Self {
            format: ZiCDataFormat::Unknown,
            compression: ZiCCompression::None,
        }
    }
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
        let info = self.ZiFDetectInfoFromPath(path)?;
        Ok(info.format)
    }

    #[allow(non_snake_case)]
    pub fn ZiFDetectInfoFromPath(&self, path: &Path) -> Result<ZiCFormatInfo> {
        let filename = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_lowercase();

        let mut info = ZiCFormatInfo::default();

        let filename_without_compression = self.detect_compression(&filename, &mut info);
        
        let extension = self.extract_base_extension(&filename_without_compression);
        info.format = self.format_from_extension(&extension);

        if info.format == ZiCDataFormat::Unknown {
            if let Ok(content) = self.read_sample(path) {
                info.format = self.ZiFDetectFromContent(&content);
            }
        }

        Ok(info)
    }

    #[allow(non_snake_case)]
    pub fn ZiFDetectFromContent(&self, content: &[u8]) -> ZiCDataFormat {
        if content.is_empty() {
            return ZiCDataFormat::Unknown;
        }

        if self.is_parquet(content) {
            return ZiCDataFormat::Parquet;
        }

        let decompressed = self.try_decompress(content);
        let content_str = String::from_utf8_lossy(&decompressed);
        let trimmed = content_str.trim();

        if self.looks_like_jsonl(trimmed) {
            return ZiCDataFormat::Jsonl;
        }

        if self.looks_like_json(trimmed) {
            return ZiCDataFormat::Json;
        }

        if self.looks_like_csv(trimmed) {
            return ZiCDataFormat::Csv;
        }

        ZiCDataFormat::Unknown
    }

    fn detect_compression<'a>(&self, filename: &'a str, info: &mut ZiCFormatInfo) -> String {
        let compression_extensions = [".gz", ".zst", ".bz2", ".xz", ".gzip", ".zstd", ".bzip2", ".lzma"];
        
        for ext in compression_extensions {
            if filename.ends_with(ext) {
                info.compression = ZiCCompression::from_extension(&ext[1..]);
                return filename[..filename.len() - ext.len()].to_string();
            }
        }
        
        filename.to_string()
    }

    fn extract_base_extension(&self, filename: &str) -> String {
        let parts: Vec<&str> = filename.rsplitn(2, '.').collect();
        if parts.len() == 2 {
            parts[0].to_string()
        } else {
            String::new()
        }
    }

    fn format_from_extension(&self, extension: &str) -> ZiCDataFormat {
        match extension.to_lowercase().as_str() {
            "jsonl" | "jl" => ZiCDataFormat::Jsonl,
            "json" => ZiCDataFormat::Json,
            "csv" | "tsv" => ZiCDataFormat::Csv,
            "parquet" | "par" => ZiCDataFormat::Parquet,
            _ => ZiCDataFormat::Unknown,
        }
    }

    fn read_sample(&self, path: &Path) -> Result<Vec<u8>> {
        use std::fs::File;
        use std::io::Read;

        let mut file = File::open(path)?;
        let mut buffer = vec![0u8; self.sample_size];
        let bytes_read = file.read(&mut buffer)?;
        buffer.truncate(bytes_read);
        Ok(buffer)
    }

    fn is_parquet(&self, content: &[u8]) -> bool {
        content.starts_with(b"PAR1")
    }

    fn try_decompress(&self, content: &[u8]) -> Vec<u8> {
        if content.starts_with(&[0x1f, 0x8b]) {
            return self.decompress_gzip(content);
        }
        
        if content.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
            return self.decompress_zstd(content);
        }

        content.to_vec()
    }

    fn decompress_gzip(&self, content: &[u8]) -> Vec<u8> {
        use std::io::Read;
        
        let mut decoder = flate2::read::GzDecoder::new(content);
        
        let mut decompressed = Vec::new();
        if decoder.read_to_end(&mut decompressed).is_ok() {
            decompressed
        } else {
            content.to_vec()
        }
    }

    fn decompress_zstd(&self, content: &[u8]) -> Vec<u8> {
        use std::io::Read;
        
        let mut decoder = match zstd::Decoder::new(content) {
            Ok(d) => d,
            Err(_) => return content.to_vec(),
        };
        
        let mut decompressed = Vec::new();
        if decoder.read_to_end(&mut decompressed).is_ok() {
            decompressed
        } else {
            content.to_vec()
        }
    }

    fn looks_like_jsonl(&self, content: &str) -> bool {
        let lines: Vec<&str> = content.lines().take(10).collect();
        
        if lines.len() < 2 {
            return false;
        }

        let mut json_count = 0;
        for line in lines {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if trimmed.starts_with('{') && trimmed.ends_with('}') {
                if serde_json::from_str::<serde_json::Value>(trimmed).is_ok() {
                    json_count += 1;
                }
            }
        }

        json_count >= 2
    }

    fn looks_like_json(&self, content: &str) -> bool {
        let trimmed = content.trim();
        
        if (trimmed.starts_with('{') && trimmed.ends_with('}')) ||
           (trimmed.starts_with('[') && trimmed.ends_with(']')) {
            return serde_json::from_str::<serde_json::Value>(trimmed).is_ok();
        }
        
        false
    }

    fn looks_like_csv(&self, content: &str) -> bool {
        let lines: Vec<&str> = content.lines().take(10).collect();
        
        if lines.len() < 2 {
            return false;
        }

        let first_line_commas = lines[0].matches(',').count();
        if first_line_commas == 0 {
            return false;
        }

        let mut consistent_lines = 0;
        for line in &lines[1..] {
            if line.matches(',').count() == first_line_commas {
                consistent_lines += 1;
            }
        }

        consistent_lines >= lines.len() / 2
    }
}
