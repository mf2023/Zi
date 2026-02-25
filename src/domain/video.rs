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

//! # Video Domain Module
//!
//! This module provides types for encoding and managing video data within the Zi framework.
//! It defines VideoEncoding for video metadata, VideoPayload for containing video data,
//! and includes video codec and pixel format enums.
//!
//! ## Video Encoding Parameters
//!
//! The video encoding system captures essential parameters:
//! - **Format**: Container format (MP4, MKV, AVI, WebM, etc.)
//! - **Dimensions**: Frame width and height in pixels
//! - **Frame Rate**: Frames per second
//! - **Duration**: Video length in milliseconds
//! - **Codec**: Video compression codec (H264, H265, VP9, AV1, etc.)
//! - **Bit Rate**: Optional bit rate for compressed video
//! - **Audio**: Optional audio codec information
//! - **Pixel Format**: Color sampling format (YUV420P, RGB24, etc.)
//!
//! ## Usage Example
//!
//! ```rust
//! use zi::domain::video::{VideoFormat, VideoCodec, VideoEncoding, VideoPayload};
//!
//! let encoding = VideoEncoding::new(
//!     VideoFormat::MP4,
//!     1920,
//!     1080,
//!     30.0,
//!     60000, // 1 minute
//!     VideoCodec::H264,
//!     Some(5_000_000), // 5 Mbps
//!     true, // has audio
//! );
//!
//! let payload = VideoPayload::new(vec![0u8; 1000000], encoding);
//! ```

use super::{ZiDomain, ZiSamplePayload};
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

/// Encoding metadata for video data.
///
/// VideoEncoding captures all essential parameters needed to decode and process
/// video data, including container format, dimensions, frame rate, codec, and
/// audio track information.
///
/// # Fields
///
/// - `format`: Container format (MP4, MKV, WebM, etc.)
/// - `width`: Frame width in pixels
/// - `height`: Frame height in pixels
/// - `frame_rate`: Frames per second
/// - `duration_ms`: Video duration in milliseconds
/// - `codec`: Video compression codec
/// - `bit_rate`: Optional bit rate in bits per second
/// - `has_audio`: Whether the video contains an audio track
/// - `audio_codec`: Optional audio codec if has_audio is true
/// - `pixel_format`: Pixel color sampling format
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VideoEncoding {
    pub format: VideoFormat,
    pub width: u32,
    pub height: u32,
    pub frame_rate: f32,
    pub duration_ms: u64,
    pub codec: VideoCodec,
    pub bit_rate: Option<u32>,
    pub has_audio: bool,
    pub audio_codec: Option<super::audio::AudioCodec>,
    pub pixel_format: PixelFormat,
}

/// Video container formats.
///
/// This enum defines common video container (wrapper) formats that hold
/// encoded video and audio streams together.
///
/// # Variants
///
/// - `MP4`: MPEG-4 Part 14, widely supported
/// - `MKV`: Matroska, flexible open-source container
/// - `AVI`: Audio Video Interleave, legacy Microsoft format
/// - `MOV`: QuickTime format, common on macOS
/// - `WebM`: WebM, designed for web (VP8/VP9/AV1)
/// - `FLV`: Flash Video, legacy web format
/// - `Unknown(String)`: Vendor-specific or unrecognized format
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum VideoFormat {
    MP4,
    MKV,
    AVI,
    MOV,
    WebM,
    FLV,
    Unknown(String),
}

/// Video compression codecs.
///
/// This enum defines common video encoding formats (codecs). Each codec
/// has different compression characteristics and support.
///
/// # Variants
///
/// - `H264`: AVC (Advanced Video Coding), most widely supported
/// - `H265`: HEVC (High Efficiency Video Coding), H.264 successor
/// - `VP8`: On2 VP8, open-source codec
/// - `VP9`: Google VP9, VP8 successor
/// - `AV1`: AOMedia Video 1, newest open-source codec
/// - `MPEG4`: MPEG-4 Part 2, older format
/// - `FFV1`: FFV1, lossless codec for archival
/// - `Unknown(String)`: Vendor-specific or unrecognized codec
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum VideoCodec {
    H264,
    H265,
    VP8,
    VP9,
    AV1,
    MPEG4,
    FFV1,
    Unknown(String),
}

/// Pixel color sampling formats.
///
/// This enum defines how color information is sampled and stored in video frames.
/// Different formats trade off between color accuracy and bandwidth/storage.
///
/// # Variants
///
/// - `YUV420P`: 4:2:0 planar, most common (1 Y per pixel, 1 U/V per 4 pixels)
/// - `YUV422P`: 4:2:2 planar, higher color resolution
/// - `YUV444P`: 4:4:4 planar, full color resolution (no chroma subsampling)
/// - `RGB24`: Packed RGB, 3 bytes per pixel
/// - `RGBA`: RGB with alpha channel, 4 bytes per pixel
/// - `GRAY8`: Grayscale, 1 byte per pixel
/// - `Unknown(String)`: Non-standard format
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum PixelFormat {
    YUV420P,
    YUV422P,
    YUV444P,
    RGB24,
    RGBA,
    GRAY8,
    Unknown(String),
}

impl VideoEncoding {
    /// Creates a new VideoEncoding with the specified parameters.
    ///
    /// # Arguments
    ///
    /// - `format`: Container format
    /// - `width`: Frame width in pixels
    /// - `height`: Frame height in pixels
    /// - `frame_rate`: Frames per second (e.g., 24.0, 30.0, 60.0)
    /// - `duration_ms`: Duration in milliseconds
    /// - `codec`: Video compression codec
    /// - `bit_rate`: Optional bit rate in bits per second
    /// - `has_audio`: Whether the video has an audio track
    ///
    /// # Returns
    ///
    /// A new VideoEncoding with default pixel format (YUV420P)
    pub fn new(
        format: VideoFormat,
        width: u32,
        height: u32,
        frame_rate: f32,
        duration_ms: u64,
        codec: VideoCodec,
        bit_rate: Option<u32>,
        has_audio: bool,
    ) -> Self {
        VideoEncoding {
            format,
            width,
            height,
            frame_rate,
            duration_ms,
            codec,
            bit_rate,
            has_audio,
            audio_codec: None,
            pixel_format: PixelFormat::YUV420P,
        }
    }

    /// Calculates the aspect ratio of video frames.
    ///
    /// This is the ratio of width to height. Common values include:
    /// - 1.77 (16:9) for widescreen video
    /// - 1.33 (4:3) for standard video
    /// - 1.0 for square video
    ///
    /// # Returns
    ///
    /// The aspect ratio as f32 (width/height)
    pub fn aspect_ratio(&self) -> f32 {
        self.width as f32 / self.height as f32
    }

    /// Calculates the total number of frames in the video.
    ///
    /// This is calculated as frame_rate multiplied by duration in seconds.
    ///
    /// # Returns
    ///
    /// Total frame count
    pub fn total_frames(&self) -> u64 {
        (self.frame_rate as u64 * self.duration_ms / 1000) as u64
    }

    /// Calculates the frame resolution in megapixels.
    ///
    /// Megapixels are calculated as (width * height) / 1,000,000.
    ///
    /// # Returns
    ///
    /// Resolution in megapixels
    pub fn megapixels(&self) -> f32 {
        (self.width as f32 * self.height as f32) / 1_000_000.0
    }

    /// Estimates the video file size in bytes.
    ///
    /// If bit_rate is provided, uses bit_rate * duration to calculate.
    /// Otherwise, estimates using uncompressed YUV420P size approximation.
    ///
    /// Note: For compressed video, actual file size will typically be
    /// much smaller than this estimate.
    ///
    /// # Returns
    ///
    /// Estimated file size in bytes
    pub fn estimated_video_size(&self) -> u64 {
        if let Some(bit_rate) = self.bit_rate {
            bit_rate as u64 * self.duration_ms / 8000
        } else {
            let bytes_per_frame = (self.width as u64 * self.height as u64 * 3) / 2;
            bytes_per_frame * self.total_frames() / 1000
        }
    }

    /// Checks if the video is in landscape orientation.
    ///
    /// Landscape means width >= height.
    ///
    /// # Returns
    ///
    /// true if width >= height
    pub fn is_landscape(&self) -> bool {
        self.width >= self.height
    }

    /// Checks if the video is in portrait orientation.
    ///
    /// Portrait means height > width.
    ///
    /// # Returns
    ///
    /// true if height > width
    pub fn is_portrait(&self) -> bool {
        self.height > self.width
    }
}

impl Hash for VideoEncoding {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.format.hash(state);
        self.width.hash(state);
        self.height.hash(state);
        self.frame_rate.to_bits().hash(state);
        self.duration_ms.hash(state);
        self.codec.hash(state);
        self.has_audio.hash(state);
    }
}

/// Container for video data and metadata.
///
/// VideoPayload bundles the raw video data bytes together with encoding
/// information, optional URI for external resources, and keyframe timestamps.
///
/// # Fields
///
/// - `data`: Raw video bytes (encoded according to format/codec)
/// - `encoding`: VideoEncoding describing the video format
/// - `uri`: Optional URI pointing to external video resource
/// - `keyframes`: List of keyframe timestamps in milliseconds
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoPayload {
    pub data: Vec<u8>,
    pub encoding: VideoEncoding,
    pub uri: Option<String>,
    pub keyframes: Vec<u64>,
}

impl VideoPayload {
    /// Creates a new VideoPayload with the specified data and encoding.
    ///
    /// The URI and keyframes are initialized as empty/None.
    ///
    /// # Arguments
    ///
    /// - `data`: Raw video bytes
    /// - `encoding`: VideoEncoding describing the video format
    ///
    /// # Returns
    ///
    /// A new VideoPayload instance
    pub fn new(data: Vec<u8>, encoding: VideoEncoding) -> Self {
        VideoPayload {
            data,
            encoding,
            uri: None,
            keyframes: Vec::new(),
        }
    }

    /// Sets the URI for external video resource.
    ///
    /// This method returns a new VideoPayload with the specified URI,
    /// following the builder pattern for immutability.
    ///
    /// # Arguments
    ///
    /// - `uri`: URI string pointing to external video resource
    ///
    /// # Returns
    ///
    /// A new VideoPayload with the URI set
    pub fn with_uri(mut self, uri: impl Into<String>) -> Self {
        self.uri = Some(uri.into());
        self
    }

    /// Returns the duration of the video in milliseconds.
    ///
    /// This is a convenience method that delegates to the encoding's duration.
    ///
    /// # Returns
    ///
    /// Duration in milliseconds
    pub fn zi_duration_ms(&self) -> u64 {
        self.encoding.duration_ms
    }

    /// Sets the keyframe timestamps for this video payload.
    ///
    /// Keyframes are frames that can be decoded independently without
    /// reference to previous frames. They are essential for random access
    /// and efficient seeking in video playback.
    ///
    /// # Arguments
    ///
    /// - `keyframes`: Vector of keyframe timestamps in milliseconds
    ///
    /// # Returns
    ///
    /// A new VideoPayload with keyframes set
    pub fn zi_with_keyframes(mut self, keyframes: Vec<u64>) -> Self {
        self.keyframes = keyframes;
        self
    }
}

impl ZiSamplePayload for VideoPayload {
    /// Returns the domain type as Video with this encoding.
    fn zi_domain(&self) -> ZiDomain {
        ZiDomain::Video(self.encoding.clone())
    }

    /// Returns the size of the video data in bytes.
    fn zi_byte_size(&self) -> usize {
        self.data.len()
    }

    /// Checks if the video data is empty.
    fn zi_is_empty(&self) -> bool {
        self.data.is_empty()
    }
}
