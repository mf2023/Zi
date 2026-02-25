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

//! # Text Domain Module
//!
//! This module provides types for encoding and managing text data within the Zi framework.
//! It defines TextEncoding for text metadata, TextPayload for containing text content,
//! and optional Python bindings for integration with Python applications.
//!
//! ## Text Encoding Parameters
//!
//! The text encoding system captures essential parameters:
//! - **Charset**: Character encoding (UTF-8, ASCII, ISO-8859-1, etc.)
//! - **Language**: Natural language code (e.g., "en", "zh-CN")
//! - **Script**: Writing system (e.g., "Latn", "Hans")
//! - **Line Breaks**: Line ending style (LF, CRLF, CR, Mixed)
//! - **Normalization**: Unicode normalization form (NFC, NFD, NFKC, NFKD)
//!
//! ## Usage Example
//!
//! ```rust
//! use zi::domain::text::{TextEncoding, TextNormalization, TextPayload};
//!
//! let encoding = TextEncoding::new(
//!     "UTF-8",
//!     Some("en"),
//!     Some("Latn"),
//!     LineBreakType::LF,
//!     TextNormalization::NFC,
//! );
//!
//! let payload = TextPayload::new("Hello, World!".to_string())
//!     .with_encoding(encoding);
//! ```

use super::{ZiDomain, ZiSamplePayload};
use serde::{Deserialize, Serialize};

/// Encoding metadata for text data.
///
/// TextEncoding captures all essential parameters needed to properly decode and
/// process text data, including character encoding, language, script, and
/// normalization options.
///
/// # Fields
///
/// - `charset`: Character encoding name (e.g., "UTF-8", "ASCII", "ISO-8859-1")
/// - `language`: Optional IETF language tag (e.g., "en", "zh-CN", "ja")
/// - `script`: Optional ISO 15924 script code (e.g., "Latn", "Hans", "Jpan")
/// - `line_breaks`: Line ending convention used in the text
/// - `normalization`: Unicode normalization form applied to the text
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TextEncoding {
    /// Character encoding name (e.g., "UTF-8", "ASCII", "ISO-8859-1", "GBK")
    /// Determines how bytes are interpreted as characters
    pub charset: String,
    /// Optional IETF language tag (BCP 47) e.g., "en", "zh-CN", "ja", "ar"
    /// Used for language-specific processing and accessibility
    pub language: Option<String>,
    /// Optional ISO 15924 script code e.g., "Latn" (Latin), "Hans" (Simplified Chinese)
    /// Distinguishes between different writing systems within the same language
    pub script: Option<String>,
    /// Line ending convention used in the text file
    /// Affects text processing on different operating systems
    pub line_breaks: LineBreakType,
    /// Unicode normalization form applied to the text
    /// Ensures consistent binary representation of equivalent strings
    pub normalization: TextNormalization,
}

/// Line ending conventions for text files.
///
/// Different operating systems use different characters to mark line endings:
/// - Unix/Linux: LF (Line Feed, \n)
/// - Windows: CRLF (Carriage Return + Line Feed, \r\n)
/// - Old Mac: CR (Carriage Return, \r)
///
/// # Variants
///
/// - `LF`: Unix-style line feed only (\n)
/// - `CRLF`: Windows-style carriage return + line feed (\r\n)
/// - `CR`: Old Mac-style carriage return only (\r)
/// - `Mixed`: Text contains multiple line ending styles
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LineBreakType {
    LF,
    CRLF,
    CR,
    Mixed,
}

/// Unicode text normalization forms.
///
/// Unicode normalization ensures that equivalent strings have a unique binary
/// representation. Different forms have different use cases:
///
/// # Variants
///
/// - `None`: No normalization applied
/// - `NFC`: Canonical Decomposition, then Canonical Composition
///   (recommended for text comparison)
/// - `NFD`: Canonical Decomposition only
/// - `NFKC`: Compatibility Decomposition, then Canonical Composition
///   (more aggressive, converts compatibility characters)
/// - `NFKD`: Compatibility Decomposition only
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TextNormalization {
    None,
    NFC,
    NFD,
    NFKC,
    NFKD,
}

impl TextEncoding {
    /// Creates a new TextEncoding with the specified parameters.
    ///
    /// # Arguments
    ///
    /// - `charset`: Character encoding name (e.g., "UTF-8", "ASCII")
    /// - `language`: Optional language code (e.g., "en", "zh")
    /// - `script`: Optional script code (e.g., "Latn", "Hans")
    /// - `line_breaks`: Line ending style
    /// - `normalization`: Unicode normalization form
    ///
    /// # Returns
    ///
    /// A new TextEncoding instance
    pub fn new(
        charset: impl Into<String>,
        language: Option<impl Into<String>>,
        script: Option<impl Into<String>>,
        line_breaks: LineBreakType,
        normalization: TextNormalization,
    ) -> Self {
        TextEncoding {
            charset: charset.into(),
            language: language.map(Into::into),
            script: script.map(Into::into),
            line_breaks,
            normalization,
        }
    }

    /// Creates a default ASCII text encoding.
    ///
    /// ASCII encoding with no normalization (since ASCII doesn't need it).
    ///
    /// # Returns
    ///
    /// TextEncoding configured for ASCII text
    pub fn default_ascii() -> Self {
        TextEncoding {
            charset: "ASCII".to_string(),
            language: None,
            script: None,
            line_breaks: LineBreakType::LF,
            normalization: TextNormalization::None,
        }
    }

    /// Creates a default UTF-8 text encoding.
    ///
    /// UTF-8 with NFC normalization (recommended default for web and storage).
    ///
    /// # Returns
    ///
    /// TextEncoding configured for UTF-8 text with NFC normalization
    pub fn default_utf8() -> Self {
        TextEncoding {
            charset: "UTF-8".to_string(),
            language: None,
            script: None,
            line_breaks: LineBreakType::LF,
            normalization: TextNormalization::NFC,
        }
    }
}

/// Container for text data and metadata.
///
/// TextPayload bundles the actual text content together with encoding metadata
/// describing how the text should be interpreted.
///
/// # Fields
///
/// - `content`: The actual text content as a String
/// - `encoding`: TextEncoding describing text format and properties
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextPayload {
    pub content: String,
    pub encoding: TextEncoding,
}

impl TextPayload {
    /// Creates a new TextPayload with the specified content.
    ///
    /// Uses default UTF-8 encoding with NFC normalization.
    ///
    /// # Arguments
    ///
    /// - `content`: The text content
    ///
    /// # Returns
    ///
    /// A new TextPayload with default UTF-8 encoding
    pub fn new(content: String) -> Self {
        TextPayload {
            content,
            encoding: TextEncoding::default_utf8(),
        }
    }

    /// Sets a custom encoding for this text payload.
    ///
    /// This method returns a new TextPayload with the specified encoding.
    ///
    /// # Arguments
    ///
    /// - `encoding`: Custom TextEncoding to use
    ///
    /// # Returns
    ///
    /// A new TextPayload with the custom encoding
    pub fn with_encoding(mut self, encoding: TextEncoding) -> Self {
        self.encoding = encoding;
        self
    }

    /// Returns the number of characters in the text.
    ///
    /// This counts Unicode scalar values (not bytes), so for UTF-8 text
    /// containing multi-byte characters, this will differ from byte_count.
    ///
    /// # Returns
    ///
    /// Character count
    pub fn zi_char_count(&self) -> usize {
        self.content.chars().count()
    }

    /// Returns the number of bytes in the text.
    ///
    /// This is the raw byte length, which for UTF-8 may be larger than
    /// the character count due to multi-byte characters.
    ///
    /// # Returns
    ///
    /// Byte count
    pub fn zi_byte_count(&self) -> usize {
        self.content.len()
    }

    /// Returns the number of words in the text.
    ///
    /// Words are defined as sequences of whitespace-separated characters.
    ///
    /// # Returns
    ///
    /// Word count
    pub fn zi_word_count(&self) -> usize {
        self.content.split_whitespace().count()
    }

    /// Returns the number of lines in the text.
    ///
    /// Empty text returns 0. Lines are separated by line break characters.
    ///
    /// # Returns
    ///
    /// Line count
    pub fn zi_line_count(&self) -> usize {
        if self.content.is_empty() {
            return 0;
        }
        self.content.lines().count()
    }

    /// Checks if the text content is empty (whitespace only or zero length).
    ///
    /// Unlike the standard is_empty(), this considers whitespace-only strings
    /// as empty, which is often more useful for text processing.
    ///
    /// # Returns
    ///
    /// true if content is empty or contains only whitespace
    pub fn zi_is_empty(&self) -> bool {
        self.content.trim().is_empty()
    }

    /// Returns the text split into tokens (words).
    ///
    /// Tokens are separated by whitespace. This is a convenience method
    /// for basic tokenization.
    ///
    /// # Returns
    ///
    /// Vector of string slices representing tokens
    pub fn zi_tokens(&self) -> Vec<&str> {
        self.content.split_whitespace().collect()
    }
}

impl ZiSamplePayload for TextPayload {
    /// Returns the domain type as Text with this encoding.
    fn zi_domain(&self) -> ZiDomain {
        ZiDomain::Text(self.encoding.clone())
    }

    /// Returns the size of the text content in bytes.
    fn zi_byte_size(&self) -> usize {
        self.content.len()
    }

    /// Checks if the text content is empty.
    fn zi_is_empty(&self) -> bool {
        self.content.trim().is_empty()
    }
}

/// Python bindings for TextEncoding and TextPayload.
///
/// This module provides Python classes that wrap the Rust TextEncoding and TextPayload
/// types, enabling Python applications to use the Zi domain system directly.
///
/// # Feature Flag
///
/// Requires `pyo3` feature to be enabled in Cargo.toml:
///
/// ```toml
/// [dependencies]
/// zi = { features = ["pyo3"] }
/// ```
///
/// # Python Usage Example
///
/// ```python
/// from zi import TextPayloadPy, TextEncodingPy
///
/// # Create text encoding
/// encoding = TextEncodingPy("UTF-8", "en", "Latn")
///
/// # Create text payload
/// payload = TextPayloadPy.new("Hello, World!")
///
/// # Access properties
/// print(payload.content)
/// print(payload.char_count())
/// print(payload.word_count())
/// ```
#[cfg(feature = "pyo3")]
mod python_bindings {
    use super::*;
    use pyo3::prelude::*;

    /// Python wrapper for TextEncoding.
    ///
    /// This class provides a Python-friendly interface to the Rust TextEncoding type,
    /// with constructor parameters for charset, language, and script.
    #[pyclass]
    #[derive(Debug, Clone)]
    pub struct TextEncodingPy {
        inner: TextEncoding,
    }

    #[pymethods]
    impl TextEncodingPy {
        /// Creates a new TextEncodingPy with the specified parameters.
        ///
        /// # Arguments
        ///
        /// - charset: Character encoding name (e.g., "UTF-8", "ASCII")
        /// - language: Optional language code (e.g., "en", "zh")
        /// - script: Optional script code (e.g., "Latn", "Hans")
        #[new]
        fn new(
            charset: String,
            language: Option<String>,
            script: Option<String>,
        ) -> Self {
            TextEncodingPy {
                inner: TextEncoding::new(
                    charset,
                    language,
                    script,
                    LineBreakType::LF,
                    TextNormalization::NFC,
                ),
            }
        }

        /// Returns the character encoding name.
        #[getter]
        fn charset(&self) -> String {
            self.inner.charset.clone()
        }

        /// Returns the language code if set.
        #[getter]
        fn language(&self) -> Option<String> {
            self.inner.language.clone()
        }
    }

    /// Python wrapper for TextPayload.
    ///
    /// This class provides a Python-friendly interface to the Rust TextPayload type,
    /// with methods for text analysis and manipulation.
    #[pyclass]
    #[derive(Debug, Clone)]
    pub struct TextPayloadPy {
        inner: TextPayload,
    }

    #[pymethods]
    impl TextPayloadPy {
        /// Creates a new TextPayloadPy with the specified content.
        ///
        /// Uses default UTF-8 encoding with NFC normalization.
        ///
        /// # Arguments
        ///
        /// - content: The text content
        #[new]
        fn new(content: String) -> Self {
            TextPayloadPy {
                inner: TextPayload::new(content),
            }
        }

        /// Creates a TextPayloadPy from raw bytes.
        ///
        /// Decodes the byte data using the specified encoding.
        ///
        /// # Arguments
        ///
        /// - data: Raw bytes to decode
        /// - encoding: Character encoding to use (e.g., "utf-8", "gbk")
        ///
        /// # Returns
        ///
        /// A new TextPayloadPy instance
        ///
        /// # Raises
        ///
        /// UnicodeDecodeError if the bytes cannot be decoded
        #[staticmethod]
        fn from_bytes(data: &[u8], encoding: &str) -> PyResult<Self> {
            let content = std::str::from_utf8(data)
                .map_err(|e| pyo3::exceptions::PyUnicodeDecodeError::new_err(e.to_string()))?
                .to_string();

            let mut text_payload = TextPayload::new(content);
            text_payload.encoding.charset = encoding.to_string();

            Ok(TextPayloadPy { inner: text_payload })
        }

        /// Returns the text content.
        #[getter]
        fn content(&self) -> String {
            self.inner.content.clone()
        }

        /// Returns the number of characters in the text.
        #[getter]
        fn char_count(&self) -> usize {
            self.inner.zi_char_count()
        }

        /// Returns the number of bytes in the text.
        #[getter]
        fn byte_count(&self) -> usize {
            self.inner.zi_byte_count()
        }

        /// Returns the number of words in the text.
        #[getter]
        fn word_count(&self) -> usize {
            self.inner.zi_word_count()
        }

        /// Returns the text content as bytes.
        ///
        /// # Returns
        ///
        /// UTF-8 encoded bytes of the content
        fn to_bytes(&self) -> Vec<u8> {
            self.inner.content.as_bytes().to_vec()
        }

        /// Returns the text split into tokens.
        ///
        /// # Returns
        ///
        /// List of tokens (whitespace-separated words)
        fn tokens(&self) -> Vec<String> {
            self.inner.zi_tokens().iter().map(|s| s.to_string()).collect()
        }
    }
}
