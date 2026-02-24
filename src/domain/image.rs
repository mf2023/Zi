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

//! # Image Domain Module
//!
//! This module provides types for encoding and managing image data within the Zi framework.
//! It defines ImageEncoding for image metadata, ImagePayload for containing image data,
//! and ImageExif for optional EXIF metadata from digital cameras.
//!
//! ## Image Encoding Parameters
//!
//! The image encoding system captures essential parameters:
//! - **Format**: Image file format (JPEG, PNG, GIF, WebP, etc.)
//! - **Dimensions**: Width and height in pixels
//! - **Color Space**: Color representation (RGB, RGBA, Grayscale, CMYK, YCbCr, Lab)
//! - **Bit Depth**: Bits per pixel component
//! - **Alpha Channel**: Whether transparency is supported
//! - **EXIF**: Optional camera metadata (make, model, GPS, etc.)
//!
//! ## Usage Example
//!
//! ```rust
//! use zi::domain::image::{ImageFormat, ColorSpace, ImageEncoding, ImagePayload};
//!
//! let encoding = ImageEncoding::new(
//!     ImageFormat::JPEG,
//!     1920,
//!     1080,
//!     ColorSpace::RGB,
//!     8,
//!     false,
//! );
//!
//! let payload = ImagePayload::new(vec![0u8; 1920*1080*3], encoding);
//! ```

use super::{ZiDomain, ZiSamplePayload};
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

/// Encoding metadata for image data.
///
/// ImageEncoding captures all essential parameters needed to decode and process
/// image data, including file format, dimensions, color space, and bit depth.
/// This information is critical for proper image decoding and for calculating
/// memory/storage requirements.
///
/// # Fields
///
/// - `format`: Image file format (JPEG, PNG, WebP, etc.)
/// - `width`: Image width in pixels
/// - `height`: Image height in pixels
/// - `color_space`: Color representation model
/// - `bit_depth`: Bits per pixel component
/// - `has_alpha`: Whether alpha channel/transparency is present
/// - `exif`: Optional EXIF metadata from digital cameras
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

/// Supported image file formats.
///
/// This enum defines common image formats used in digital media and computer vision.
///
/// # Variants
///
/// - `JPEG`: Joint Photographic Experts Group, lossy compression
/// - `PNG`: Portable Network Graphics, lossless compression with transparency
/// - `GIF`: Graphics Interchange Format, animated images
/// - `BMP`: Bitmap, uncompressed image format
/// - `WebP`: Web Picture format, lossy/lossless compression
/// - `TIFF`: Tagged Image File Format, high-quality archival
/// - `AVIF`: AV1 Image format, modern high-efficiency format
/// - `Unknown(String)`: Vendor-specific or unrecognized format
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

/// Color space models for image representation.
///
/// This enum defines how color information is encoded in pixel data.
/// Different color spaces have different numbers of channels and
/// are suitable for different use cases.
///
/// # Variants
///
/// - `RGB`: Red-Green-Blue additive color model (3 channels)
/// - `RGBA`: RGB with Alpha/transparency channel (4 channels)
/// - `Grayscale`: Single channel for black and white (1 channel)
/// - `CMYK`: Cyan-Magenta-Yellow-Key(Black) subtractive model (4 channels)
/// - `YCbCr`: Luminance and chrominance separation (3 channels)
/// - `Lab`: CIE LAB perceptually uniform color space (3 channels)
/// - `Unknown(String)`: Non-standard or proprietary color space
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

/// EXIF metadata extracted from digital camera images.
///
/// This struct contains technical information about how the image was captured,
/// including camera settings and optionally geographic location data.
///
/// # Fields
///
/// - `camera_make`: Manufacturer of the camera (e.g., "Canon", "Nikon")
/// - `camera_model`: Model of the camera (e.g., "EOS R5")
/// - `focal_length`: Lens focal length in millimeters
/// - `aperture`: F-number (e.g., 2.8 for f/2.8)
/// - `iso`: ISO sensitivity setting
/// - `shutter_speed`: Exposure time as string (e.g., "1/250")
/// - `datetime`: Date/time when photo was taken
/// - `gps`: Geographic coordinates (latitude, longitude)
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
    /// Creates a new ImageEncoding with the specified parameters.
    ///
    /// # Arguments
    ///
    /// - `format`: Image file format
    /// - `width`: Image width in pixels
    /// - `height`: Image height in pixels
    /// - `color_space`: Color space model
    /// - `bit_depth`: Bits per pixel component (typically 8)
    /// - `has_alpha`: Whether alpha channel is present
    ///
    /// # Returns
    ///
    /// A new ImageEncoding instance with exif set to None
    pub fn new(
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

    /// Calculates the aspect ratio of the image.
    ///
    /// This is the ratio of width to height. Common values include:
    /// - 1.77 (16:9) for widescreen
    /// - 1.33 (4:3) for standard
    /// - 1.0 for square images
    ///
    /// # Returns
    ///
    /// The aspect ratio as f32 (width/height)
    pub fn aspect_ratio(&self) -> f32 {
        self.width as f32 / self.height as f32
    }

    /// Calculates the image resolution in megapixels.
    ///
    /// Megapixels are calculated as (width * height) / 1,000,000.
    ///
    /// # Returns
    ///
    /// Resolution in megapixels
    pub fn megapixels(&self) -> f32 {
        (self.width as f32 * self.height as f32) / 1_000_000.0
    }

    /// Calculates the total number of pixels in the image.
    ///
    /// # Returns
    ///
    /// Total pixel count
    pub fn pixel_count(&self) -> u64 {
        self.width as u64 * self.height as u64
    }

    /// Calculates the number of bytes per pixel.
    ///
    /// This is calculated based on the color space and bit depth.
    /// For example, 8-bit RGB has 3 bytes (24 bits) per pixel.
    ///
    /// # Returns
    ///
    /// Bytes per pixel
    pub fn bytes_per_pixel(&self) -> usize {
        let channels = match &self.color_space {
            ColorSpace::RGB | ColorSpace::YCbCr | ColorSpace::Lab => 3,
            ColorSpace::RGBA | ColorSpace::CMYK => 4,
            ColorSpace::Grayscale => 1,
            ColorSpace::Unknown(_) => 3,
        };
        (self.bit_depth as usize * channels) / 8
    }

    /// Estimates the uncompressed memory size of the image.
    ///
    /// This calculation uses pixel_count multiplied by bytes_per_pixel.
    /// Note that for compressed formats (JPEG, WebP, etc.), the actual file
    /// size will typically be much smaller than this estimate.
    ///
    /// # Returns
    ///
    /// Estimated size in bytes when uncompressed
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

/// Container for image data and metadata.
///
/// ImagePayload bundles the raw image data bytes together with encoding
/// information and optional URI for external resources.
///
/// # Fields
///
/// - `data`: Raw image bytes (encoded according to format specification)
/// - `encoding`: ImageEncoding describing the image format
/// - `uri`: Optional URI pointing to external image resource
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImagePayload {
    pub data: Vec<u8>,
    pub encoding: ImageEncoding,
    pub uri: Option<String>,
}

impl ImagePayload {
    /// Creates a new ImagePayload with the specified data and encoding.
    ///
    /// The URI is initialized as None.
    ///
    /// # Arguments
    ///
    /// - `data`: Raw image bytes
    /// - `encoding`: ImageEncoding describing the image format
    ///
    /// # Returns
    ///
    /// A new ImagePayload instance
    pub fn new(data: Vec<u8>, encoding: ImageEncoding) -> Self {
        ImagePayload {
            data,
            encoding,
            uri: None,
        }
    }

    /// Sets the URI for external image resource.
    ///
    /// This method returns a new ImagePayload with the specified URI,
    /// following the builder pattern for immutability.
    ///
    /// # Arguments
    ///
    /// - `uri`: URI string pointing to external image resource
    ///
    /// # Returns
    ///
    /// A new ImagePayload with the URI set
    pub fn with_uri(mut self, uri: impl Into<String>) -> Self {
        self.uri = Some(uri.into());
        self
    }

    /// Sets the EXIF metadata for this image payload.
    ///
    /// This method returns a new ImagePayload with the specified EXIF data.
    ///
    /// # Arguments
    ///
    /// - `exif`: ImageExif containing camera and capture metadata
    ///
    /// # Returns
    ///
    /// A new ImagePayload with EXIF set
    pub fn zi_with_exif(mut self, exif: ImageExif) -> Self {
        self.encoding.exif = Some(exif);
        self
    }
}

impl ZiSamplePayload for ImagePayload {
    /// Returns the domain type as Image with this encoding.
    fn zi_domain(&self) -> ZiDomain {
        ZiDomain::Image(self.encoding.clone())
    }

    /// Returns the size of the image data in bytes.
    fn zi_byte_size(&self) -> usize {
        self.data.len()
    }

    /// Checks if the image data is empty.
    fn zi_is_empty(&self) -> bool {
        self.data.is_empty()
    }
}
