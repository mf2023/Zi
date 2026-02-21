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

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::hash::{Hash, Hasher};

pub mod audio;
pub mod image;
pub mod text;
pub mod video;

pub use audio::AudioEncoding;
pub use image::ImageEncoding;
pub use text::TextEncoding;
pub use video::VideoEncoding;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ZiCDomain {
    Text(TextEncoding),
    Image(ImageEncoding),
    Audio(AudioEncoding),
    Video(VideoEncoding),
    MultiModal(MultiModalEncoding),
}

impl ZiCDomain {
    pub fn domain_type(&self) -> &'static str {
        match self {
            ZiCDomain::Text(_) => "text",
            ZiCDomain::Image(_) => "image",
            ZiCDomain::Audio(_) => "audio",
            ZiCDomain::Video(_) => "video",
            ZiCDomain::MultiModal(_) => "multimodal",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MultiModalEncoding {
    pub components: Vec<ZiCDomain>,
    pub alignment: MultiModalAlignment,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MultiModalAlignment {
    None,
    Temporal { start_ms: u64, end_ms: u64 },
    Spatial { source_domain: String },
    Semantic { similarity_score: OrderedF32 },
}

impl MultiModalEncoding {
    pub fn ZiFNew(components: Vec<ZiCDomain>, alignment: MultiModalAlignment) -> Self {
        MultiModalEncoding {
            components,
            alignment,
        }
    }
}

/// Wrapper for f32 to allow Eq and Hash
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct OrderedF32(pub f32);

impl OrderedF32 {
    pub fn new(value: f32) -> Self {
        OrderedF32(value)
    }

    pub fn into_inner(self) -> f32 {
        self.0
    }
}

impl Eq for OrderedF32 {}

impl Hash for OrderedF32 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

impl fmt::Display for OrderedF32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<f32> for OrderedF32 {
    fn from(value: f32) -> Self {
        OrderedF32(value)
    }
}

impl From<OrderedF32> for f32 {
    fn from(value: OrderedF32) -> Self {
        value.0
    }
}

pub trait ZiCSamplePayload: Send + Sync + Clone + fmt::Debug {
    fn zi_domain(&self) -> ZiCDomain;
    fn zi_byte_size(&self) -> usize;
    fn zi_is_empty(&self) -> bool;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ZiCSample<P: ZiCSamplePayload> {
    pub uid: Vec<u8>,
    pub payload: P,
    pub metadata: Value,
    pub timestamp: u64,
    pub version: u32,
}

impl<P: ZiCSamplePayload> ZiCSample<P> {
    #[allow(non_snake_case)]
    pub fn ZiFNew(
        uid: impl Into<Vec<u8>>,
        payload: P,
        metadata: Option<Value>,
    ) -> Self {
        ZiCSample {
            uid: uid.into(),
            payload,
            metadata: metadata.unwrap_or(Value::Null),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            version: 1,
        }
    }

    pub fn zi_with_metadata(mut self, metadata: Value) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn zi_uid_hex(&self) -> String {
        hex::encode(&self.uid)
    }

    pub fn zi_domain(&self) -> ZiCDomain {
        self.payload.zi_domain()
    }
}

pub type ZiCSampleBatch<P> = Vec<ZiCSample<P>>;

pub trait ZiCEncoder<P: ZiCSamplePayload, T> {
    fn zi_encode(&self, sample: &ZiCSample<P>) -> Result<T, ZiCSampleError>;
    fn zi_decode(&self, data: &T) -> Result<ZiCSample<P>, ZiCSampleError>;
}

#[derive(Debug, thiserror::Error)]
pub enum ZiCSampleError {
    #[error("encoding error: {0}")]
    Encoding(String),

    #[error("decoding error: {0}")]
    Decoding(String),

    #[error("invalid data: {0}")]
    InvalidData(String),

    #[error("size exceeded limit: got {0}, limit {1}")]
    SizeExceeded(usize, usize),

    #[error("unsupported format")]
    UnsupportedFormat,

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct ZiCSampleConfig {
    pub max_payload_size: usize,
    pub allow_multimodal: bool,
    pub supported_domains: Vec<&'static str>,
}

impl Default for ZiCSampleConfig {
    fn default() -> Self {
        ZiCSampleConfig {
            max_payload_size: 100 * 1024 * 1024,
            allow_multimodal: true,
            supported_domains: vec!["text", "image", "audio", "video", "multimodal"],
        }
    }
}

impl ZiCSampleConfig {
    pub fn zi_with_max_size(mut self, size: usize) -> Self {
        self.max_payload_size = size;
        self
    }

    pub fn zi_disable_multimodal(mut self) -> Self {
        self.allow_multimodal = false;
        self
    }

    pub fn zi_supports_domain(&self, domain: &str) -> bool {
        self.supported_domains.contains(&domain)
    }
}

pub struct ZiCSampleIterator<P: ZiCSamplePayload> {
    samples: std::vec::IntoIter<ZiCSample<P>>,
}

impl<P: ZiCSamplePayload> Iterator for ZiCSampleIterator<P> {
    type Item = ZiCSample<P>;

    fn next(&mut self) -> Option<Self::Item> {
        self.samples.next()
    }
}

/// Extension trait for ZiCSampleBatch to provide into_iter functionality
pub trait ZiCSampleBatchIter<P: ZiCSamplePayload> {
    fn into_iter_samples(self) -> ZiCSampleIterator<P>;
}

impl<P: ZiCSamplePayload> ZiCSampleBatchIter<P> for ZiCSampleBatch<P> {
    fn into_iter_samples(self) -> ZiCSampleIterator<P> {
        ZiCSampleIterator {
            samples: self.into_iter(),
        }
    }
}

#[cfg(feature = "parquet")]
mod arrow_support {
    use super::*;
    use arrow2::array::StructArray;

    impl<P: ZiCSamplePayload> ZiCSample<P> {
        pub fn zi_to_arrow(&self) -> Result<StructArray, ZiCSampleError> {
            Err(ZiCSampleError::Encoding("Arrow support not implemented".to_string()))
        }

        pub fn zi_from_arrow(_array: &StructArray) -> Result<Self, ZiCSampleError> {
            Err(ZiCSampleError::Decoding("Arrow support not implemented".to_string()))
        }
    }
}
