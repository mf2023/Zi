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

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::record::{ZiCRecord, ZiCRecordBatch};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZiCDiffChange {
    pub path: String,
    pub old_value: Option<Value>,
    pub new_value: Option<Value>,
    pub change_type: ZiCChangeType,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZiCChangeType {
    Added,
    Removed,
    Modified,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZiCDiffReport {
    pub added_count: usize,
    pub removed_count: usize,
    pub modified_count: usize,
    pub changes: Vec<ZiCDiffChange>,
}

#[derive(Clone, Debug, Default)]
pub struct ZiCDiffer {
    max_changes: usize,
}

impl ZiCDiffer {
    #[allow(non_snake_case)]
    pub fn ZiFNew() -> Self {
        Self { max_changes: 1000 }
    }

    #[allow(non_snake_case)]
    pub fn ZiFWithMaxChanges(mut self, max: usize) -> Self {
        self.max_changes = max;
        self
    }

    #[allow(non_snake_case)]
    pub fn ZiFDiff(&self, old: &ZiCRecordBatch, new: &ZiCRecordBatch) -> ZiCDiffReport {
        let old_map: std::collections::HashMap<String, &ZiCRecord> = old
            .iter()
            .filter_map(|r| r.id.as_ref().map(|id| (id.clone(), r)))
            .collect();

        let new_map: std::collections::HashMap<String, &ZiCRecord> = new
            .iter()
            .filter_map(|r| r.id.as_ref().map(|id| (id.clone(), r)))
            .collect();

        let mut report = ZiCDiffReport::default();

        for (id, new_record) in &new_map {
            if let Some(old_record) = old_map.get(id) {
                let changes = self.diff_records(old_record, new_record);
                for change in changes {
                    if report.changes.len() >= self.max_changes {
                        break;
                    }
                    match change.change_type {
                        ZiCChangeType::Added => report.added_count += 1,
                        ZiCChangeType::Removed => report.removed_count += 1,
                        ZiCChangeType::Modified => report.modified_count += 1,
                    }
                    report.changes.push(change);
                }
            } else {
                if report.changes.len() >= self.max_changes {
                    break;
                }
                report.added_count += 1;
                report.changes.push(ZiCDiffChange {
                    path: id.clone(),
                    old_value: None,
                    new_value: Some(new_record.payload.clone()),
                    change_type: ZiCChangeType::Added,
                });
            }
        }

        for (id, _old_record) in &old_map {
            if !new_map.contains_key(id) {
                if report.changes.len() >= self.max_changes {
                    break;
                }
                report.removed_count += 1;
                report.changes.push(ZiCDiffChange {
                    path: id.clone(),
                    old_value: None,
                    new_value: None,
                    change_type: ZiCChangeType::Removed,
                });
            }
        }

        report
    }

    fn diff_records(&self, old: &ZiCRecord, new: &ZiCRecord) -> Vec<ZiCDiffChange> {
        let mut changes = Vec::new();
        self.diff_values("payload", &old.payload, &new.payload, &mut changes);
        changes
    }

    fn diff_values(&self, path: &str, old: &Value, new: &Value, changes: &mut Vec<ZiCDiffChange>) {
        if old == new {
            return;
        }

        match (old, new) {
            (Value::Object(old_map), Value::Object(new_map)) => {
                for (key, old_val) in old_map {
                    let new_path = format!("{}.{}", path, key);
                    if let Some(new_val) = new_map.get(key) {
                        self.diff_values(&new_path, old_val, new_val, changes);
                    } else {
                        changes.push(ZiCDiffChange {
                            path: new_path,
                            old_value: Some(old_val.clone()),
                            new_value: None,
                            change_type: ZiCChangeType::Removed,
                        });
                    }
                }
                for (key, new_val) in new_map {
                    if !old_map.contains_key(key) {
                        let new_path = format!("{}.{}", path, key);
                        changes.push(ZiCDiffChange {
                            path: new_path,
                            old_value: None,
                            new_value: Some(new_val.clone()),
                            change_type: ZiCChangeType::Added,
                        });
                    }
                }
            }
            _ => {
                changes.push(ZiCDiffChange {
                    path: path.to_string(),
                    old_value: Some(old.clone()),
                    new_value: Some(new.clone()),
                    change_type: ZiCChangeType::Modified,
                });
            }
        }
    }
}
