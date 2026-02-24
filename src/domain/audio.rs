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

//! # Audio Domain Module
//!
//! This module provides types for encoding and managing audio data within the Zi framework.
//! It defines the AudioEncoding struct for describing audio metadata and AudioPayload for
//! containing actual audio data along with Voice Activity Detection (VAD) regions.
//!
//! ## Audio Encoding Parameters
//!
//! The audio encoding system captures essential parameters for audio processing:
//! - **Codec**: Compression format (PCM, MP3, AAC, Opus, FLAC, etc.)
//! - **Sample Rate**: Samples per second (Hz), typically 8000, 16000, 44100, or 48000
//! - **Channels**: Number of audio channels (1=mono, 2=stereo)
//! - **Bit Depth**: Bits per sample (8, 16, 24, 32)
//! - **Duration**: Length of audio in milliseconds
//!
//! ## Usage Example
//!
//! ```rust
//! use zi::domain::audio::{AudioCodec, AudioEncoding, AudioPayload};
//!
//! let encoding = AudioEncoding::new(
//!     AudioCodec::PCM,
//!     44100,
//!     2,
//!     16,
//!     None,
//!     5000, // 5 seconds
//! );
//!
//! let payload = AudioPayload::new(vec![0u8; 44100 * 2 * 2], encoding);
//! ```

use super::{ZiDomain, ZiSamplePayload};
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

/// Encoding metadata for audio data.
///
/// AudioEncoding captures all essential parameters needed to decode and process
/// audio data, including compression format, sample rate, channel configuration,
/// bit depth, and duration. This information is critical for proper audio
/// encoding/decoding and for calculating storage requirements.
///
/// # Fields
///
/// - `codec`: The audio compression format
/// - `sample_rate`: Number of samples per second in Hz
/// - `channels`: Number of audio channels (1=mono, 2=stereo, 6=surround, etc.)
/// - `bit_depth`: Bits per sample (8, 16, 24, 32)
/// - `bit_rate`: Optional bit rate in bits per second (for compressed formats)
/// - `duration_ms`: Audio duration in milliseconds
/// - `has_vad`: Whether Voice Activity Detection has been performed
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioEncoding {
    pub codec: AudioCodec,
    pub sample_rate: u32,
    pub channels: u16,
    pub bit_depth: u16,
    pub bit_rate: Option<u32>,
    pub duration_ms: u64,
    pub has_vad: bool,
}

/// Supported audio codec formats.
///
/// This enum defines the available audio compression codecs, ranging from
/// uncompressed PCM to various lossy and lossless compressed formats.
///
/// # Variants
///
/// - `PCM`: Pulse Code Modulation, uncompressed raw audio
/// - `MP3`: MPEG-1 Audio Layer III, lossy compression
/// - `AAC`: Advanced Audio Coding, lossy compression
/// - `Opus`: Interactive audio codec, lossy compression
/// - `FLAC`: Free Lossless Audio Codec, lossless compression
/// - `Vorbis`: Ogg Vorbis, lossy compression
/// - `AMR`: Adaptive Multi-Rate, optimized for speech
/// - `Unknown(String)`: Vendor-specific or unrecognized format
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum AudioCodec {
    PCM,
    MP3,
    AAC,
    Opus,
    FLAC,
    Vorbis,
    AMR,
    Unknown(String),
}

impl AudioEncoding {
    /// Creates a new AudioEncoding with the specified parameters.
    ///
    /// # Arguments
    ///
    /// - `codec`: The audio compression format
    /// - `sample_rate`: Samples per second (Hz), common values: 8000, 16000, 44100, 48000
    /// - `channels`: Number of audio channels (1=mono, 2=stereo)
    /// - `bit_depth`: Bits per sample (8, 16, 24, 32)
    /// - `bit_rate`: Optional bit rate in bps for compressed formats
    /// - `duration_ms`: Duration of the audio in milliseconds
    ///
    /// # Returns
    ///
    /// A new AudioEncoding instance with has_vad set to false
    pub fn new(
        codec: AudioCodec,
        sample_rate: u32,
        channels: u16,
        bit_depth: u16,
        bit_rate: Option<u32>,
        duration_ms: u64,
    ) -> Self {
        AudioEncoding {
            codec,
            sample_rate,
            channels,
            bit_depth,
            bit_rate,
            duration_ms,
            has_vad: false,
        }
    }

    /// Calculates the number of bytes per sample.
    ///
    /// This is calculated as bit_depth divided by 8.
    /// For example, 16-bit audio yields 2 bytes per sample.
    ///
    /// # Returns
    ///
    /// The number of bytes per sample
    pub fn bytes_per_sample(&self) -> usize {
        (self.bit_depth / 8) as usize
    }

    /// Calculates the data rate in bytes per second.
    ///
    /// This is the product of sample_rate, channels, and bytes_per_sample.
    /// For uncompressed audio, this represents the actual data rate.
    ///
    /// # Returns
    ///
    /// Estimated bytes per second for this audio configuration
    pub fn bytes_per_second(&self) -> u64 {
        self.sample_rate as u64 * self.channels as u64 * self.bytes_per_sample() as u64
    }

    /// Estimates the uncompressed size of the audio data in bytes.
    ///
    /// This calculation uses bytes_per_second multiplied by duration.
    /// Note that for compressed formats (MP3, AAC, etc.), the actual size
    /// will typically be much smaller than this estimate.
    ///
    /// # Returns
    ///
    /// Estimated size in bytes
    pub fn estimated_size(&self) -> u64 {
        self.bytes_per_second() * self.duration_ms / 1000
    }

    /// Checks if the audio is mono (single channel).
    ///
    /// # Returns
    ///
    /// true if channels equals 1
    pub fn is_mono(&self) -> bool {
        self.channels == 1
    }

    /// Checks if the audio is stereo (two channels).
    ///
    /// # Returns
    ///
    /// true if channels equals 2
    pub fn is_stereo(&self) -> bool {
        self.channels == 2
    }

    /// Calculates the Nyquist frequency for this audio.
    ///
    /// The Nyquist frequency is half the sample rate and represents
    /// the maximum frequency that can be accurately represented.
    /// Audio frequencies above this will alias.
    ///
    /// # Returns
    ///
    /// The Nyquist frequency in Hz
    pub fn nyquist_frequency(&self) -> f32 {
        self.sample_rate as f32 / 2.0
    }
}

impl Hash for AudioEncoding {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.codec.hash(state);
        self.sample_rate.hash(state);
        self.channels.hash(state);
        self.bit_depth.hash(state);
        self.duration_ms.hash(state);
    }
}

/// Container for audio data and metadata.
///
/// AudioPayload bundles the raw audio data bytes together with encoding
/// information and optional URI for external resources. It also supports
/// Voice Activity Detection (VAD) regions for speech detection.
///
/// # Fields
///
/// - `data`: Raw audio bytes (encoded according to encoding specification)
/// - `encoding`: AudioEncoding describing the audio format
/// - `uri`: Optional URI pointing to external audio resource
/// - `vad_regions`: Voice Activity Detection regions if VAD has been performed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioPayload {
    pub data: Vec<u8>,
    pub encoding: AudioEncoding,
    pub uri: Option<String>,
    pub vad_regions: Vec<VADRegion>,
}

/// Voice Activity Detection region.
///
/// VADRegion represents a time range where voice activity was detected
/// or not detected. This is useful for speech processing applications,
/// speaker diarization, and audio segmentation.
///
/// # Fields
///
/// - `start_ms`: Start timestamp in milliseconds
/// - `end_ms`: End timestamp in milliseconds
/// - `confidence`: Detection confidence score (0.0 to 1.0)
/// - `speech`: true if speech was detected, false for silence/noise
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VADRegion {
    pub start_ms: u64,
    pub end_ms: u64,
    pub confidence: f32,
    pub speech: bool,
}

impl AudioPayload {
    /// Creates a new AudioPayload with the specified data and encoding.
    ///
    /// The URI and VAD regions are initialized as empty/None.
    ///
    /// # Arguments
    ///
    /// - `data`: Raw audio bytes
    /// - `encoding`: AudioEncoding describing the audio format
    ///
    /// # Returns
    ///
    /// A new AudioPayload instance
    pub fn new(data: Vec<u8>, encoding: AudioEncoding) -> Self {
        AudioPayload {
            data,
            encoding,
            uri: None,
            vad_regions: Vec::new(),
        }
    }

    /// Sets the URI for external audio resource.
    ///
    /// This method returns a new AudioPayload with the specified URI,
    /// following the builder pattern for immutability.
    ///
    /// # Arguments
    ///
    /// - `uri`: URI string pointing to external audio resource
    ///
    /// # Returns
    ///
    /// A new AudioPayload with the URI set
    pub fn with_uri(mut self, uri: impl Into<String>) -> Self {
        self.uri = Some(uri.into());
        self
    }

    /// Returns the duration of the audio in milliseconds.
    ///
    /// This is a convenience method that delegates to the encoding's duration.
    ///
    /// # Returns
    ///
    /// Duration in milliseconds
    pub fn zi_duration_ms(&self) -> u64 {
        self.encoding.duration_ms
    }

    /// Sets the VAD regions for this audio payload.
    ///
    /// This method returns a new AudioPayload with the specified VAD regions,
    /// marking the encoding as having VAD performed.
    ///
    /// # Arguments
    ///
    /// - `regions`: Vector of VADRegion representing voice activity segments
    ///
    /// # Returns
    ///
    /// A new AudioPayload with VAD regions set
    pub fn zi_with_vad_regions(mut self, regions: Vec<VADRegion>) -> Self {
        self.vad_regions = regions;
        self
    }
}

impl ZiSamplePayload for AudioPayload {
    /// Returns the domain type as Audio with this encoding.
    fn zi_domain(&self) -> ZiDomain {
        ZiDomain::Audio(self.encoding.clone())
    }

    /// Returns the size of the audio data in bytes.
    fn zi_byte_size(&self) -> usize {
        self.data.len()
    }

    /// Checks if the audio data is empty.
    fn zi_is_empty(&self) -> bool {
        self.data.is_empty()
    }
}
