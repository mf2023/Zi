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

use super::{ZiCDomain, ZiCSamplePayload};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TextEncoding {
    pub charset: String,
    pub language: Option<String>,
    pub script: Option<String>,
    pub line_breaks: LineBreakType,
    pub normalization: TextNormalization,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LineBreakType {
    LF,
    CRLF,
    CR,
    Mixed,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TextNormalization {
    None,
    NFC,
    NFD,
    NFKC,
    NFKD,
}

impl TextEncoding {
    pub fn ZiFNew(
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

    pub fn default_ascii() -> Self {
        TextEncoding {
            charset: "ASCII".to_string(),
            language: None,
            script: None,
            line_breaks: LineBreakType::LF,
            normalization: TextNormalization::None,
        }
    }

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextPayload {
    pub content: String,
    pub encoding: TextEncoding,
}

impl TextPayload {
    pub fn ZiFNew(content: String) -> Self {
        TextPayload {
            content,
            encoding: TextEncoding::default_utf8(),
        }
    }

    pub fn ZiFWithEncoding(mut self, encoding: TextEncoding) -> Self {
        self.encoding = encoding;
        self
    }

    pub fn zi_char_count(&self) -> usize {
        self.content.chars().count()
    }

    pub fn zi_byte_count(&self) -> usize {
        self.content.len()
    }

    pub fn zi_word_count(&self) -> usize {
        self.content.split_whitespace().count()
    }

    pub fn zi_line_count(&self) -> usize {
        if self.content.is_empty() {
            return 0;
        }
        self.content.lines().count()
    }

    pub fn zi_is_empty(&self) -> bool {
        self.content.trim().is_empty()
    }

    pub fn zi_tokens(&self) -> Vec<&str> {
        self.content.split_whitespace().collect()
    }
}

impl ZiCSamplePayload for TextPayload {
    fn zi_domain(&self) -> ZiCDomain {
        ZiCDomain::Text(self.encoding.clone())
    }

    fn zi_byte_size(&self) -> usize {
        self.content.len()
    }

    fn zi_is_empty(&self) -> bool {
        self.content.trim().is_empty()
    }
}

#[cfg(feature = "pyo3")]
mod python_bindings {
    use super::*;
    use pyo3::prelude::*;

    #[pyclass]
    #[derive(Debug, Clone)]
    pub struct TextEncodingPy {
        inner: TextEncoding,
    }

    #[pymethods]
    impl TextEncodingPy {
        #[new]
        fn new(
            charset: String,
            language: Option<String>,
            script: Option<String>,
        ) -> Self {
            TextEncodingPy {
                inner: TextEncoding::ZiFNew(
                    charset,
                    language,
                    script,
                    LineBreakType::LF,
                    TextNormalization::NFC,
                ),
            }
        }

        #[getter]
        fn charset(&self) -> String {
            self.inner.charset.clone()
        }

        #[getter]
        fn language(&self) -> Option<String> {
            self.inner.language.clone()
        }
    }

    #[pyclass]
    #[derive(Debug, Clone)]
    pub struct TextPayloadPy {
        inner: TextPayload,
    }

    #[pymethods]
    impl TextPayloadPy {
        #[new]
        fn new(content: String) -> Self {
            TextPayloadPy {
                inner: TextPayload::ZiFNew(content),
            }
        }

        #[staticmethod]
        fn from_bytes(data: &[u8], encoding: &str) -> PyResult<Self> {
            let content = std::str::from_utf8(data)
                .map_err(|e| pyo3::exceptions::PyUnicodeDecodeError::new_err(e.to_string()))?
                .to_string();

            let mut text_payload = TextPayload::ZiFNew(content);
            text_payload.encoding.charset = encoding.to_string();

            Ok(TextPayloadPy { inner: text_payload })
        }

        #[getter]
        fn content(&self) -> String {
            self.inner.content.clone()
        }

        #[getter]
        fn char_count(&self) -> usize {
            self.inner.zi_char_count()
        }

        #[getter]
        fn byte_count(&self) -> usize {
            self.inner.zi_byte_count()
        }

        #[getter]
        fn word_count(&self) -> usize {
            self.inner.zi_word_count()
        }

        fn to_bytes(&self) -> Vec<u8> {
            self.inner.content.as_bytes().to_vec()
        }

        fn tokens(&self) -> Vec<String> {
            self.inner.zi_tokens().iter().map(|s| s.to_string()).collect()
        }
    }
}
