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
use std::hash::{Hash, Hasher};

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
    pub fn ZiFNew(
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

    pub fn aspect_ratio(&self) -> f32 {
        self.width as f32 / self.height as f32
    }

    pub fn total_frames(&self) -> u64 {
        (self.frame_rate as u64 * self.duration_ms / 1000) as u64
    }

    pub fn megapixels(&self) -> f32 {
        (self.width as f32 * self.height as f32) / 1_000_000.0
    }

    pub fn estimated_video_size(&self) -> u64 {
        if let Some(bit_rate) = self.bit_rate {
            bit_rate as u64 * self.duration_ms / 8000
        } else {
            let bytes_per_frame = (self.width as u64 * self.height as u64 * 3) / 2;
            bytes_per_frame * self.total_frames() / 1000
        }
    }

    pub fn is_landscape(&self) -> bool {
        self.width >= self.height
    }

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoPayload {
    pub data: Vec<u8>,
    pub encoding: VideoEncoding,
    pub uri: Option<String>,
    pub keyframes: Vec<u64>,
}

impl VideoPayload {
    pub fn ZiFNew(data: Vec<u8>, encoding: VideoEncoding) -> Self {
        VideoPayload {
            data,
            encoding,
            uri: None,
            keyframes: Vec::new(),
        }
    }

    pub fn ZiFWithUri(mut self, uri: impl Into<String>) -> Self {
        self.uri = Some(uri.into());
        self
    }

    pub fn zi_duration_ms(&self) -> u64 {
        self.encoding.duration_ms
    }

    pub fn zi_with_keyframes(mut self, keyframes: Vec<u64>) -> Self {
        self.keyframes = keyframes;
        self
    }
}

impl ZiCSamplePayload for VideoPayload {
    fn zi_domain(&self) -> ZiCDomain {
        ZiCDomain::Video(self.encoding.clone())
    }

    fn zi_byte_size(&self) -> usize {
        self.data.len()
    }

    fn zi_is_empty(&self) -> bool {
        self.data.is_empty()
    }
}
