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
    pub fn ZiFNew(
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

    pub fn bytes_per_sample(&self) -> usize {
        (self.bit_depth / 8) as usize
    }

    pub fn bytes_per_second(&self) -> u64 {
        self.sample_rate as u64 * self.channels as u64 * self.bytes_per_sample() as u64
    }

    pub fn estimated_size(&self) -> u64 {
        self.bytes_per_second() * self.duration_ms / 1000
    }

    pub fn is_mono(&self) -> bool {
        self.channels == 1
    }

    pub fn is_stereo(&self) -> bool {
        self.channels == 2
    }

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioPayload {
    pub data: Vec<u8>,
    pub encoding: AudioEncoding,
    pub uri: Option<String>,
    pub vad_regions: Vec<VADRegion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VADRegion {
    pub start_ms: u64,
    pub end_ms: u64,
    pub confidence: f32,
    pub speech: bool,
}

impl AudioPayload {
    pub fn ZiFNew(data: Vec<u8>, encoding: AudioEncoding) -> Self {
        AudioPayload {
            data,
            encoding,
            uri: None,
            vad_regions: Vec::new(),
        }
    }

    pub fn ZiFWithUri(mut self, uri: impl Into<String>) -> Self {
        self.uri = Some(uri.into());
        self
    }

    pub fn zi_duration_ms(&self) -> u64 {
        self.encoding.duration_ms
    }

    pub fn zi_with_vad_regions(mut self, regions: Vec<VADRegion>) -> Self {
        self.vad_regions = regions;
        self
    }
}

impl ZiCSamplePayload for AudioPayload {
    fn zi_domain(&self) -> ZiCDomain {
        ZiCDomain::Audio(self.encoding.clone())
    }

    fn zi_byte_size(&self) -> usize {
        self.data.len()
    }

    fn zi_is_empty(&self) -> bool {
        self.data.is_empty()
    }
}
