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

pub mod profile;
pub mod diff;
pub mod statistics;
pub mod distribution;

pub use profile::{
    ZiProfileReport, ZiProfiler, ZiFieldProfile, ZiAnomaly, ZiAnomalySeverity,
    ZiTextStatistics, ZiProfilerConfig,
};
pub use diff::{
    ZiDiffReport, ZiDiffer, ZiDiffChange, ZiChangeType, ZiDiffStats,
    ZiFieldChange, ZiRecordDiff, ZiDifferConfig,
};
pub use statistics::ZiStatistics;
pub use distribution::{
    ZiDistributionAnalyzer, ZiDistributionReport, ZiCorrelation,
    ZiHistogram, ZiHistogramBin, ZiPercentiles, ZiValueDistribution,
};
