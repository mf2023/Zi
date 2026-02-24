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

//! # Zi Domain Module
//!
//! This module provides the core domain encoding types and sample management infrastructure
//! for the Zi data processing framework. It defines a unified type system for handling
//! multi-modal data including text, images, audio, and video content.
//!
//! ## Architecture Overview
//!
//! The domain module implements a hierarchical type system:
//! - **ZiDomain**: Enum that categorizes data into distinct modalities (Text, Image, Audio, Video, MultiModal)
//! - **ZiSample<T>**: Generic container for individual data samples with metadata, timestamp, and versioning
//! - **ZiEncoder**: Trait for encoding/decoding samples to/from various formats
//! - **Payload Types**: Domain-specific structs (TextPayload, ImagePayload, AudioPayload, VideoPayload)
//!   that implement the ZiSamplePayload trait
//!
//! ## Design Patterns
//!
//! 1. **Trait-Based Polymorphism**: The ZiSamplePayload trait enables uniform handling of
//!    different payload types while preserving domain-specific information
//! 2. **Builder Pattern**: Payload types use builder methods (with_uri, zi_with_metadata) for
//!    flexible object construction
//! 3. **Zero-Copy Philosophy**: References and clones are used strategically to minimize
//!    unnecessary data copying
//!
//! ## Usage Example
//!
//! ```rust
//! use zi::domain::{TextPayload, ZiSample};
//!
//! let payload = TextPayload::new("Hello, World!".to_string());
//! let sample = ZiSample::new("sample-001", payload, None);
//! assert_eq!(sample.zi_domain_type(), "text");
//! ```
//!
//! ## Feature Flags
//!
//! - `parquet`: Enables Arrow/Parquet serialization support for ZiSample types
//! - `pyo3`: Enables Python bindings for TextEncoding and TextPayload

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

/// Enumeration of supported domain types in the Zi framework.
///
/// This enum serves as a unified type discriminator for multi-modal data samples.
/// Each variant wraps the corresponding domain-specific encoding information,
/// enabling type-safe handling of different media types through a common interface.
///
/// # Variants
///
/// - `Text`: Text content with encoding metadata (charset, language, normalization)
/// - `Image`: Image content with format, dimensions, color space information
/// - `Audio`: Audio content with codec, sample rate, channel configuration
/// - `Video`: Video content combining frame information with audio track metadata
/// - `MultiModal`: Composite content combining multiple domain types with alignment information
///
/// # Usage
///
/// The domain type is primarily used for:
/// - Type identification and dispatch
/// - Validation and routing in processing pipelines
/// - Serialization format selection
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ZiDomain {
    Text(TextEncoding),
    Image(ImageEncoding),
    Audio(AudioEncoding),
    Video(VideoEncoding),
    MultiModal(MultiModalEncoding),
}

impl ZiDomain {
    /// Returns the domain type as a static string slice.
    ///
    /// This method provides a fast, allocation-free way to identify the domain
    /// type of a sample, useful for logging, debugging, and routing decisions.
    ///
    /// # Returns
    ///
    /// - `"text"` for Text domain
    /// - `"image"` for Image domain
    /// - `"audio"` for Audio domain
    /// - `"video"` for Video domain
    /// - `"multimodal"` for MultiModal domain
    pub fn domain_type(&self) -> &'static str {
        match self {
            ZiDomain::Text(_) => "text",
            ZiDomain::Image(_) => "image",
            ZiDomain::Audio(_) => "audio",
            ZiDomain::Video(_) => "video",
            ZiDomain::MultiModal(_) => "multimodal",
        }
    }
}

/// Encoding configuration for multi-modal content.
///
/// MultiModalEncoding represents a composite data structure that combines
/// multiple domain types (e.g., audio with synchronized video, or images
/// with descriptive text). It includes alignment information that describes
/// how the different components relate to each other.
///
/// # Fields
///
/// - `components`: Vector of ZiDomain items representing each modality
/// - `alignment`: Alignment specification describing temporal, spatial, or semantic relationships
///
/// # Alignment Types
///
/// - `None`: No specific alignment between components
/// - `Temporal`: Time-based synchronization (e.g., audio-video sync)
/// - `Spatial`: Position-based relationship (e.g., image regions)
/// - `Semantic`: Content-based similarity scoring
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MultiModalEncoding {
    pub components: Vec<ZiDomain>,
    pub alignment: MultiModalAlignment,
}

/// Specifies the alignment relationship between multi-modal components.
///
/// Alignment defines how different modalities within a MultiModalEncoding
/// relate to each other. This is crucial for maintaining synchronization
/// and understanding the spatial or semantic relationships between data types.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MultiModalAlignment {
    /// No explicit alignment relationship exists between components.
    /// Components are independent and can be processed separately.
    None,
    /// Components are synchronized based on time intervals.
    /// Used for audio-video synchronization, subtitle alignment, etc.
    /// Fields:
    /// - `start_ms`: Start time in milliseconds
    /// - `end_ms`: End time in milliseconds
    Temporal { start_ms: u64, end_ms: u64 },
    /// Components have a spatial relationship.
    /// Used for image-text binding, region-of-interest mapping, etc.
    /// Field: `source_domain`: Identifier for the reference domain
    Spatial { source_domain: String },
    /// Components have semantic similarity.
    /// Used for cross-modal retrieval, content matching, etc.
    /// Field: `similarity_score`: Similarity metric (0.0 to 1.0)
    Semantic { similarity_score: OrderedF32 },
}

impl MultiModalEncoding {
    /// Creates a new MultiModalEncoding with the specified components and alignment.
    ///
    /// # Arguments
    ///
    /// - `components`: Vector of ZiDomain representing each modality component
    /// - `alignment`: MultiModalAlignment describing the relationship between components
    ///
    /// # Returns
    ///
    /// A new MultiModalEncoding instance
    pub fn new(components: Vec<ZiDomain>, alignment: MultiModalAlignment) -> Self {
        MultiModalEncoding {
            components,
            alignment,
        }
    }
}

/// Wrapper type for f32 that implements Eq and Hash traits.
///
/// Floating-point numbers cannot implement Eq and Hash directly due to
/// NaN != NaN and floating-point comparison semantics. This wrapper provides
/// bitwise equality and hashing, treating all bit patterns as distinct values.
///
/// # Use Cases
///
/// - Used in MultiModalAlignment::Semantic for similarity scores
/// - Enables f32 values in hash-based collections (HashMap, HashSet)
/// - Useful for exact bit-pattern comparisons
///
/// # Bit-Level Equality
///
/// Two OrderedF32 values are equal if and only if their underlying bit
/// representations are identical, including all NaN variants.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct OrderedF32(pub f32);

impl OrderedF32 {
    /// Creates a new OrderedF32 from a floating-point value.
    ///
    /// # Arguments
    ///
    /// - `value`: The f32 value to wrap
    ///
    /// # Returns
    ///
    /// A new OrderedF32 containing the value
    pub fn new(value: f32) -> Self {
        OrderedF32(value)
    }

    /// Unwraps the OrderedF32, returning the underlying f32 value.
    ///
    /// # Returns
    ///
    /// The inner f32 value
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

/// Core trait for payload types in the Zi domain system.
///
/// This trait defines the minimum interface that any domain-specific payload
/// must implement to be used within the ZiSample container. It enables generic
/// handling of different media types while preserving domain-specific metadata.
///
/// # Required Implementations
///
/// All implementing types must provide:
/// - `zi_domain()`: Returns the domain classification for this payload
/// - `zi_byte_size()`: Returns the size in bytes of the payload data
/// - `zi_is_empty()`: Indicates whether the payload contains meaningful data
///
/// # Thread Safety
///
/// The trait requires `Send + Sync` bounds to ensure payloads can be safely
/// shared across thread boundaries in concurrent processing pipelines.
pub trait ZiSamplePayload: Send + Sync + Clone + fmt::Debug {
    /// Returns the domain type classification for this payload.
    ///
    /// # Returns
    ///
    /// A ZiDomain enum representing the modality of this payload
    fn zi_domain(&self) -> ZiDomain;

    /// Returns the size of the payload data in bytes.
    ///
    /// This should return the actual size of the raw data, not the size
    /// of any wrapper structures.
    ///
    /// # Returns
    ///
    /// The byte size of the payload data
    fn zi_byte_size(&self) -> usize;

    /// Checks whether the payload contains any meaningful data.
    ///
    /// The definition of "empty" varies by domain:
    /// - Text: content is whitespace-only or zero-length
    /// - Image/Audio/Video: data vector is empty
    ///
    /// # Returns
    ///
    /// `true` if the payload is empty, `false` otherwise
    fn zi_is_empty(&self) -> bool;
}

/// Generic container for a data sample in the Zi framework.
///
/// ZiSample provides a standardized wrapper that encapsulates:
/// - A unique identifier (UID) for tracking and retrieval
/// - The domain-specific payload (text, image, audio, or video)
/// - Optional metadata as key-value pairs
/// - A timestamp for temporal ordering
/// - A version number for schema evolution
///
/// # Type Parameters
///
/// - `P`: The payload type that implements ZiSamplePayload
///
/// # Construction
///
/// Use the `new()` constructor or the builder pattern with `zi_with_metadata()`:
/// ```rust
/// let sample = ZiSample::new("id-001", payload, None)
///     .zi_with_metadata(json!({"source": "file"}));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ZiSample<P: ZiSamplePayload> {
    /// Unique identifier for the sample, stored as raw bytes.
    /// Can be any format (UUID, hash, sequential ID) depending on use case.
    pub uid: Vec<u8>,
    /// The domain-specific payload containing the actual data.
    /// Must implement ZiSamplePayload (TextPayload, ImagePayload, etc.)
    pub payload: P,
    /// Flexible metadata storage using JSON values.
    /// Can contain arbitrary key-value pairs for application-specific data.
    pub metadata: Value,
    /// Unix timestamp in milliseconds when the sample was created.
    /// Used for temporal ordering and caching decisions.
    pub timestamp: u64,
    /// Schema version number for backward compatibility.
    /// Increment when the sample structure changes.
    pub version: u32,
}

impl<P: ZiSamplePayload> ZiSample<P> {
    /// Creates a new ZiSample with the specified parameters.
    ///
    /// The timestamp is automatically set to the current system time,
    /// and the version defaults to 1.
    ///
    /// # Arguments
    ///
    /// - `uid`: Unique identifier (converted to bytes via Into<Vec<u8>>)
    /// - `payload`: The domain-specific payload data
    /// - `metadata`: Optional metadata (None uses JSON Null)
    ///
    /// # Returns
    ///
    /// A new ZiSample instance with auto-generated timestamp and version
    #[allow(non_snake_case)]
    pub fn new(
        uid: impl Into<Vec<u8>>,
        payload: P,
        metadata: Option<Value>,
    ) -> Self {
        ZiSample {
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

    /// Creates a new ZiSample with the provided metadata, replacing any existing metadata.
    ///
    /// This builder method returns a new sample with the updated metadata,
    /// following the immutability pattern.
    ///
    /// # Arguments
    ///
    /// - `metadata`: The new metadata Value to set
    ///
    /// # Returns
    ///
    /// A new ZiSample with the updated metadata
    pub fn zi_with_metadata(mut self, metadata: Value) -> Self {
        self.metadata = metadata;
        self
    }

    /// Returns the UID encoded as a hexadecimal string.
    ///
    /// Useful for logging, debugging, and display purposes where
    /// binary UIDs are not convenient.
    ///
    /// # Returns
    ///
    /// Hex-encoded string representation of the UID
    pub fn zi_uid_hex(&self) -> String {
        hex::encode(&self.uid)
    }

    /// Returns the domain type of this sample.
    ///
    /// Delegates to the payload's zi_domain() method.
    ///
    /// # Returns
    ///
    /// The ZiDomain classification of the payload
    pub fn zi_domain(&self) -> ZiDomain {
        self.payload.zi_domain()
    }
}

/// Type alias for a collection of ZiSample items.
///
/// Represents a batch of samples that can be processed together,
/// useful for bulk operations, streaming, and parallel processing.
pub type ZiSampleBatch<P> = Vec<ZiSample<P>>;

/// Encoder/Decoder trait for ZiSample serialization.
///
/// This trait defines the interface for converting ZiSample instances
/// to and from various serialization formats. Implementations can support
/// formats like JSON, MessagePack, Protocol Buffers, or custom binary formats.
///
/// # Type Parameters
///
/// - `P`: The payload type that implements ZiSamplePayload
/// - `T`: The target serialization format (e.g., Vec<u8>, String, custom struct)
///
/// # Error Handling
///
/// All encoder/decoder implementations should return ZiSampleError on failure,
/// providing meaningful error messages for debugging.
pub trait ZiEncoder<P: ZiSamplePayload, T> {
    /// Encodes a ZiSample into the target format.
    ///
    /// # Arguments
    ///
    /// - `sample`: Reference to the ZiSample to encode
    ///
    /// # Returns
    ///
    /// Result containing the encoded data or a ZiSampleError
    fn zi_encode(&self, sample: &ZiSample<P>) -> Result<T, ZiSampleError>;

    /// Decodes serialized data back into a ZiSample.
    ///
    /// # Arguments
    ///
    /// - `data`: Reference to the serialized data
    ///
    /// # Returns
    ///
    /// Result containing the reconstructed ZiSample or a ZiSampleError
    fn zi_decode(&self, data: &T) -> Result<ZiSample<P>, ZiSampleError>;
}

/// Error types for sample encoding and decoding operations.
///
/// This enum provides comprehensive error handling for all sample-related
/// operations, including serialization failures, validation errors, and I/O issues.
#[derive(Debug, thiserror::Error)]
pub enum ZiSampleError {
    /// Encoding operation failed.
    /// Contains a description of what went wrong during encoding.
    #[error("encoding error: {0}")]
    Encoding(String),

    /// Decoding operation failed.
    /// Contains details about the deserialization failure.
    #[error("decoding error: {0}")]
    Decoding(String),

    /// Data validation failed.
    /// The provided data does not meet expected format or content requirements.
    #[error("invalid data: {0}")]
    InvalidData(String),

    /// Payload size exceeds configured limits.
    /// First value is the actual size, second is the allowed limit.
    #[error("size exceeded limit: got {0}, limit {1}")]
    SizeExceeded(usize, usize),

    /// The requested format is not supported by this encoder/decoder.
    #[error("unsupported format")]
    UnsupportedFormat,

    /// I/O operation failed.
    /// Wraps standard Rust I/O errors.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Configuration for ZiSample validation and processing.
///
/// This struct provides runtime configuration for sample handling,
/// including size limits, domain restrictions, and multimodal support.
pub struct ZiSampleConfig {
    /// Maximum allowed payload size in bytes.
    /// Samples exceeding this size will be rejected during validation.
    pub max_payload_size: usize,
    /// Whether to allow multi-modal samples (combining multiple domains).
    /// When false, MultiModalEncoding variants will be rejected.
    pub allow_multimodal: bool,
    /// List of supported domain types as string identifiers.
    pub supported_domains: Vec<&'static str>,
}

impl Default for ZiSampleConfig {
    /// Returns the default configuration with sensible limits.
    ///
    /// Default values:
    /// - max_payload_size: 100 MB
    /// - allow_multimodal: true
    /// - supported_domains: all standard domains
    fn default() -> Self {
        ZiSampleConfig {
            max_payload_size: 100 * 1024 * 1024,
            allow_multimodal: true,
            supported_domains: vec!["text", "image", "audio", "video", "multimodal"],
        }
    }
}

impl ZiSampleConfig {
    /// Creates a new configuration with the specified maximum payload size.
    ///
    /// # Arguments
    ///
    /// - `size`: Maximum payload size in bytes
    ///
    /// # Returns
    ///
    /// A new ZiSampleConfig with the specified size limit
    pub fn zi_with_max_size(mut self, size: usize) -> Self {
        self.max_payload_size = size;
        self
    }

    /// Disables multi-modal sample support.
    ///
    /// When called, samples containing MultiModalEncoding will be rejected.
    ///
    /// # Returns
    ///
    /// A new ZiSampleConfig with multimodal disabled
    pub fn zi_disable_multimodal(mut self) -> Self {
        self.allow_multimodal = false;
        self
    }

    /// Checks if a domain is supported by this configuration.
    ///
    /// # Arguments
    ///
    /// - `domain`: The domain string to check (e.g., "text", "image")
    ///
    /// # Returns
    ///
    /// true if the domain is in the supported_domains list
    pub fn zi_supports_domain(&self, domain: &str) -> bool {
        self.supported_domains.contains(&domain)
    }
}

/// Iterator over a collection of ZiSample items.
///
/// Provides standard Rust Iterator semantics for sample batches,
/// enabling use in for loops, map operations, and other iteration patterns.
pub struct ZiSampleIterator<P: ZiSamplePayload> {
    samples: std::vec::IntoIter<ZiSample<P>>,
}

impl<P: ZiSamplePayload> Iterator for ZiSampleIterator<P> {
    type Item = ZiSample<P>;

    fn next(&mut self) -> Option<Self::Item> {
        self.samples.next()
    }
}

/// Extension trait providing iterator conversion for ZiSampleBatch.
///
/// This trait adds the `into_iter_samples()` method to ZiSampleBatch,
/// enabling ergonomic iteration over sample collections.
pub trait ZiSampleBatchIter<P: ZiSamplePayload> {
    /// Converts the batch into an iterator over individual samples.
    ///
    /// # Returns
    ///
    /// A ZiSampleIterator over the samples
    fn into_iter_samples(self) -> ZiSampleIterator<P>;
}

impl<P: ZiSamplePayload> ZiSampleBatchIter<P> for ZiSampleBatch<P> {
    fn into_iter_samples(self) -> ZiSampleIterator<P> {
        ZiSampleIterator {
            samples: self.into_iter(),
        }
    }
}

/// Arrow/Parquet serialization support for ZiSample.
///
/// This module is conditionally compiled when the "parquet" feature is enabled.
/// It provides conversion methods between ZiSample and Arrow/Parquet formats,
/// enabling integration with big data processing pipelines and columnar storage.
///
/// # Feature Flag
///
/// Requires `parquet` feature to be enabled in Cargo.toml:
///
/// ```toml
/// [dependencies]
/// zi = { features = ["parquet"] }
/// ```
#[cfg(feature = "parquet")]
mod arrow_support {
    use super::*;
    use arrow2::array::StructArray;

    impl<P: ZiSamplePayload> ZiSample<P> {
        /// Converts a ZiSample to an Arrow StructArray for columnar storage.
        ///
        /// This method enables efficient storage and querying of samples
        /// in Apache Arrow/Parquet format, suitable for big data analytics.
        ///
        /// # Returns
        ///
        /// Result containing the Arrow StructArray or an encoding error
        pub fn zi_to_arrow(&self) -> Result<StructArray, ZiSampleError> {
            Err(ZiSampleError::Encoding("Arrow support not implemented".to_string()))
        }

        /// Reconstructs a ZiSample from an Arrow StructArray.
        ///
        /// This method deserializes a sample from its Arrow columnar format,
        /// enabling integration with data loading from Parquet files.
        ///
        /// # Arguments
        ///
        /// - `array`: Reference to the Arrow StructArray containing sample data
        ///
        /// # Returns
        ///
        /// Result containing the reconstructed ZiSample or a decoding error
        pub fn zi_from_arrow(_array: &StructArray) -> Result<Self, ZiSampleError> {
            Err(ZiSampleError::Decoding("Arrow support not implemented".to_string()))
        }
    }
}
