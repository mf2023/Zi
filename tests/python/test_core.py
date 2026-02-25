#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright © 2025-2026 Wenze Wei. All Rights Reserved.
#
# This file is part of Zi.
# The Zi project belongs to the Dunimd Team.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Copyright © 2025-2026 Wenze Wei. All Rights Reserved.

This file is part of Zi.
The Zi project belongs to the Dunimd project team.

Core functionality tests for Zi framework.
"""

import sys
import json
sys.path.insert(0, 'python')

from zix import ZiRecord, ZiOperator, ZiMetrics, ZiTextProcessor, ZiVersionInfo

def test_record_creation():
    """Test ZiRecord creation"""
    record = ZiRecord(id="1", payload='{"text": "hello"}')
    assert record.id == "1"
    assert json.loads(record.payload)["text"] == "hello"
    print("✓ test_record_creation")

def test_record_with_metadata():
    """Test ZiRecord with metadata"""
    record = ZiRecord(id="1", payload='{"text": "hello"}')
    record.metadata = {"score": 0.9}
    assert record.metadata["score"] == 0.9
    print("✓ test_record_with_metadata")

def test_record_batch():
    """Test multiple records"""
    records = [
        ZiRecord(id="1", payload='{"text": "hello"}'),
        ZiRecord(id="2", payload='{"text": "world"}'),
    ]
    assert len(records) == 2
    print("✓ test_record_batch")

def test_record_id_options():
    """Test record ID options"""
    record_with_id = ZiRecord(id="test_id", payload='{"data": 1}')
    record_without_id = ZiRecord(id=None, payload='{"data": 2}')
    
    assert record_with_id.id is not None
    assert record_without_id.id is None
    print("✓ test_record_id_options")

def test_record_payload_access():
    """Test payload access"""
    record = ZiRecord(id="1", payload='{"name": "test", "value": 42, "nested": {"a": 1}}')
    payload = json.loads(record.payload)
    
    assert payload["name"] == "test"
    assert payload["value"] == 42
    assert payload["nested"]["a"] == 1
    print("✓ test_record_payload_access")

def test_metrics():
    """Test ZiMetrics"""
    metrics = ZiMetrics()
    assert metrics is not None
    print("✓ test_metrics")

def test_version_info():
    """Test ZiVersionInfo"""
    version = ZiVersionInfo()
    assert version.version is not None
    print(f"✓ test_version_info (version: {version.version})")

def test_text_processor():
    """Test ZiTextProcessor"""
    processor = ZiTextProcessor()
    lang, confidence = processor.detect_language("Hello world")
    assert lang is not None
    print(f"✓ test_text_processor (detected: {lang})")

def test_operator_creation():
    """Test ZiOperator creation"""
    op = ZiOperator("filter.equals", '{"path": "payload.text", "equals": "hello"}')
    assert op.name == "filter.equals"
    print("✓ test_operator_creation")

def test_operator_apply():
    """Test ZiOperator apply"""
    records = [
        ZiRecord(id="1", payload='{"text": "hello"}'),
        ZiRecord(id="2", payload='{"text": "world"}'),
    ]
    
    op = ZiOperator("filter.equals", '{"path": "payload.text", "equals": "hello"}')
    result = op.apply(records)
    
    assert len(result) == 1
    assert result[0].id == "1"
    print("✓ test_operator_apply")

def test_operator_repr():
    """Test ZiOperator string representation"""
    op = ZiOperator("filter.equals", "{}")
    repr_str = str(op)
    assert "filter.equals" in repr_str
    print("✓ test_operator_repr")

def run_core_tests():
    """Run all core tests"""
    print("\n" + "="*50)
    print("Running Zi Core Tests")
    print("="*50 + "\n")
    
    test_record_creation()
    test_record_with_metadata()
    test_record_batch()
    test_record_id_options()
    test_record_payload_access()
    test_metrics()
    test_version_info()
    test_text_processor()
    test_operator_creation()
    test_operator_apply()
    test_operator_repr()
    
    print("\n" + "="*50)
    print("All core tests passed!")
    print("="*50)


if __name__ == "__main__":
    run_core_tests()
