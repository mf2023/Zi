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

use super::{ZiCDomain, ZiCSamplePayload, ZiCSampleError};
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImageEncoding {
    pub format: ImageFormat,
    pub width: u32,
    pub height: u32,
    pub color_space: ColorSpace,
    pub bit_depth: u8,
    pub has_alpha: bool,
    pub exif: Option<ImageExif>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum ImageFormat {
    JPEG,
    PNG,
    GIF,
    BMP,
    WebP,
    TIFF,
    AVIF,
    Unknown(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum ColorSpace {
    RGB,
    RGBA,
    Grayscale,
    CMYK,
    YCbCr,
    Lab,
    Unknown(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ImageExif {
    pub camera_make: Option<String>,
    pub camera_model: Option<String>,
    pub focal_length: Option<f32>,
    pub aperture: Option<f32>,
    pub iso: Option<u32>,
    pub shutter_speed: Option<String>,
    pub datetime: Option<String>,
    pub gps: Option<(f64, f64)>,
}

impl ImageEncoding {
    pub fn ZiFNew(
        format: ImageFormat,
        width: u32,
        height: u32,
        color_space: ColorSpace,
        bit_depth: u8,
        has_alpha: bool,
    ) -> Self {
        ImageEncoding {
            format,
            width,
            height,
            color_space,
            bit_depth,
            has_alpha,
            exif: None,
        }
    }

    pub fn aspect_ratio(&self) -> f32 {
        self.width as f32 / self.height as f32
    }

    pub fn megapixels(&self) -> f32 {
        (self.width as f32 * self.height as f32) / 1_000_000.0
    }

    pub fn pixel_count(&self) -> u64 {
        self.width as u64 * self.height as u64
    }

    pub fn bytes_per_pixel(&self) -> usize {
        let channels = match &self.color_space {
            ColorSpace::RGB | ColorSpace::YCbCr | ColorSpace::Lab => 3,
            ColorSpace::RGBA | ColorSpace::CMYK => 4,
            ColorSpace::Grayscale => 1,
            ColorSpace::Unknown(_) => 3,
        };
        (self.bit_depth as usize * channels) / 8
    }

    pub fn estimated_uncompressed_size(&self) -> u64 {
        self.pixel_count() * self.bytes_per_pixel() as u64
    }
}

impl Hash for ImageEncoding {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.format.hash(state);
        self.width.hash(state);
        self.height.hash(state);
        self.color_space.hash(state);
        self.bit_depth.hash(state);
        self.has_alpha.hash(state);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImagePayload {
    pub data: Vec<u8>,
    pub encoding: ImageEncoding,
    pub uri: Option<String>,
}

impl ImagePayload {
    pub fn ZiFNew(data: Vec<u8>, encoding: ImageEncoding) -> Self {
        ImagePayload {
            data,
            encoding,
            uri: None,
        }
    }

    pub fn ZiFWithUri(mut self, uri: impl Into<String>) -> Self {
        self.uri = Some(uri.into());
        self
    }

    pub fn zi_with_exif(mut self, exif: ImageExif) -> Self {
        self.encoding.exif = Some(exif);
        self
    }
}

impl ZiCSamplePayload for ImagePayload {
    fn zi_domain(&self) -> ZiCDomain {
        ZiCDomain::Image(self.encoding.clone())
    }

    fn zi_byte_size(&self) -> usize {
        self.data.len()
    }

    fn zi_is_empty(&self) -> bool {
        self.data.is_empty()
    }
}
