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

use serde_json::Value;

use crate::errors::{Result, ZiError};
use crate::operator::ZiCOperator;
use crate::operators::filter::ZiCFieldPath;
use crate::record::ZiCRecordBatch;

#[derive(Debug)]
struct _TransformNormalize {
    path: ZiCFieldPath,
    lowercase: bool,
}

impl _TransformNormalize {
    #[allow(non_snake_case)]
    fn ZiFNew(path: ZiCFieldPath, lowercase: bool) -> Self {
        Self { path, lowercase }
    }

    #[allow(non_snake_case)]
    fn ZiFNorm(&self, s: &str) -> String {
        let mut t = s.trim().split_whitespace().collect::<Vec<_>>().join(" ");
        if self.lowercase {
            t = t.to_lowercase();
        }
        t
    }
}

impl ZiCOperator for _TransformNormalize {
    fn name(&self) -> &'static str {
        "transform.normalize"
    }
    fn apply(&self, mut batch: ZiCRecordBatch) -> Result<ZiCRecordBatch> {
        for record in &mut batch {
            if let Some(Value::String(text)) = self.path.ZiFResolve(record) {
                let nt = self.ZiFNorm(text);
                let _ = self.path.ZiFSetValue(record, Value::String(nt));
            }
        }
        Ok(batch)
    }
}

#[allow(non_snake_case)]
pub fn ZiFTransformNormalizeFactory(config: &Value) -> Result<Box<dyn ZiCOperator + Send + Sync>> {
    let obj = config
        .as_object()
        .ok_or_else(|| ZiError::validation("transform.normalize config must be object"))?;
    let path = obj
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ZiError::validation("transform.normalize requires string 'path'"))?;
    let lowercase = obj
        .get("lowercase")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let field_path = ZiCFieldPath::ZiFParse(path)?;
    Ok(Box::new(_TransformNormalize::ZiFNew(field_path, lowercase)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::ZiCRecord;
    use serde_json::json;
    #[test]
    fn normalize_basic() {
        let op = _TransformNormalize::ZiFNew(ZiCFieldPath::ZiFParse("payload.text").unwrap(), true);
        let rec = ZiCRecord::ZiFNew(None, json!({"text": "  Hello   WORLD "}));
        let out = op.apply(vec![rec]).unwrap();
        assert_eq!(out[0].payload["text"], json!("hello world"));
    }
}
