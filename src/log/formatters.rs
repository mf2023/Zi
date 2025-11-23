//! Copyright © 2025 Wenze Wei. All Rights Reserved.
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

use serde_json::Value;

use crate::log::core::ZiCLogRecord;

pub struct ZiCJsonFormatter;

impl ZiCJsonFormatter {
    #[allow(non_snake_case)]
    pub fn ZiFFormat(record: &ZiCLogRecord) -> String {
        record.ZiFToJson().to_string()
    }
}

pub struct ZiCTextFormatter;

impl ZiCTextFormatter {
    #[allow(non_snake_case)]
    pub fn ZiFFormat(record: &ZiCLogRecord) -> String {
        let v: Value = record.ZiFToJson();
        // Simple human-readable formatting; for now reuse the JSON but could
        // be made more compact.
        v.to_string()
    }
}
