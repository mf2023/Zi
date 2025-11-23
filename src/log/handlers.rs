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

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

use crate::log::core::ZiCLogRecord;
use crate::log::formatters::{ZiCJsonFormatter, ZiCTextFormatter};

pub trait ZiCLogHandler {
    fn handle(&self, record: &ZiCLogRecord);
}

pub struct ZiCStdoutHandler {
    json: bool,
}

impl ZiCStdoutHandler {
    #[allow(non_snake_case)]
    pub fn ZiFNew(json: bool) -> Self {
        ZiCStdoutHandler { json }
    }
}

impl ZiCLogHandler for ZiCStdoutHandler {
    fn handle(&self, record: &ZiCLogRecord) {
        if self.json {
            let line = ZiCJsonFormatter::ZiFFormat(record);
            println!("{}", line);
        } else {
            let line = ZiCTextFormatter::ZiFFormat(record);
            println!("{}", line);
        }
    }
}

pub struct ZiCFileHandler {
    path: String,
    json: bool,
    rotate_when: Option<String>,
    max_bytes: Option<u64>,
    backup_count: Option<u32>,
    file: Mutex<()>,
}

impl ZiCFileHandler {
    #[allow(non_snake_case)]
    pub fn ZiFNew(
        path: String,
        json: bool,
        rotate_when: Option<String>,
        max_bytes: Option<u64>,
        backup_count: Option<u32>,
    ) -> Self {
        ZiCFileHandler {
            path,
            json,
            rotate_when,
            max_bytes,
            backup_count,
            file: Mutex::new(()),
        }
    }

    fn rotate_if_needed(&self) {
        if self.rotate_when.as_deref() != Some("size") {
            return;
        }
        let max_bytes = match self.max_bytes {
            Some(v) => v,
            None => return,
        };
        let backup_count = self.backup_count.unwrap_or(7);
        let path = Path::new(&self.path);
        if let Ok(meta) = fs::metadata(path) {
            if meta.len() <= max_bytes {
                return;
            }
        } else {
            return;
        }

        // Simple size-based rotation: path.N -> path.(N+1), path -> path.1
        for idx in (1..=backup_count).rev() {
            let from = if idx == 1 {
                path.to_path_buf()
            } else {
                path.with_extension(format!("log.{}", idx - 1))
            };
            if from.exists() {
                let to = path.with_extension(format!("log.{}", idx));
                let _ = fs::rename(&from, &to);
            }
        }
    }
}

impl ZiCLogHandler for ZiCFileHandler {
    fn handle(&self, record: &ZiCLogRecord) {
        let _guard = self.file.lock().unwrap();
        self.rotate_if_needed();

        let line = if self.json {
            ZiCJsonFormatter::ZiFFormat(record)
        } else {
            ZiCTextFormatter::ZiFFormat(record)
        };
        if let Ok(mut f) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        {
            let _ = writeln!(f, "{}", line);
        }
    }
}
