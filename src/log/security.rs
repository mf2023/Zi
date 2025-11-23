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

use std::collections::VecDeque;
use std::time::SystemTime;

use regex::Regex;

use crate::log::core::ZiCLogRecord;

#[allow(dead_code)]
pub struct ZiCSecurityAuditor {
    patterns: Vec<Regex>,
    recent: VecDeque<(SystemTime, String)>,
    max_recent: usize,
}

impl ZiCSecurityAuditor {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        let patterns = vec![
            Regex::new(r#"password\s*[:=]\s*['"][^'"]+['"]"#).unwrap(),
            Regex::new(r#"secret\s*[:=]\s*['"][^'"]+['"]"#).unwrap(),
            Regex::new(r#"key\s*[:=]\s*['"][^'"]+['"]"#).unwrap(),
            Regex::new(r#"token\s*[:=]\s*['"][^'"]+['"]"#).unwrap(),
        ];
        ZiCSecurityAuditor {
            patterns,
            recent: VecDeque::with_capacity(1000),
            max_recent: 1000,
        }
    }

    #[allow(non_snake_case)]
    pub fn ZiFAudit(&mut self, record: &ZiCLogRecord) -> bool {
        let msg = &record.message;
        for re in &self.patterns {
            if re.is_match(msg) {
                let ts = SystemTime::now();
                if self.recent.len() == self.max_recent {
                    self.recent.pop_front();
                }
                self.recent.push_back((ts, msg.clone()));
                return true;
            }
        }
        false
    }

    #[allow(non_snake_case)]
    pub fn ZiFRecentFindings(&self) -> &VecDeque<(SystemTime, String)> {
        &self.recent
    }
}
