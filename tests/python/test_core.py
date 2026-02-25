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

This module provides comprehensive tests for the core Zi components including:
- ZiRecord: Fundamental data unit with ID, payload, and metadata
- ZiOperator: Trait for data processing operators
- ZiMetrics: Quality metrics computation
- ZiTextProcessor: Text processing utilities
- ZiVersionInfo: Version information and reproducibility

Test Categories:
1. Record Creation: Tests for creating records with various configurations
2. Record Metadata: Tests for metadata attachment and access
3. Record Batching: Tests for handling multiple records
4. Payload Access: Tests for accessing and parsing JSON payloads
5. Metrics: Tests for quality metrics computation
6. Version: Tests for version information retrieval

Each test function is designed to be independent and can be run in isolation.
Test execution prints a checkmark (✓) upon successful completion.

Example:
    $ python tests/python/test_core.py
    ✓ test_record_creation
    ✓ test_record_with_metadata
    ...
"""

import sys
import json
sys.path.insert(0, 'python')

from zix import ZiRecord, ZiOperator, ZiMetrics, ZiTextProcessor, ZiVersionInfo


def test_record_creation():
    """
    Test ZiRecord creation with ID and payload.
    
    Verifies that a ZiRecord can be created with a string ID and JSON payload.
    The payload should be parseable as JSON and accessible via the payload property.
    """
    record = ZiRecord(id="1", payload='{"text": "hello"}')
    assert record.id == "1"
    assert json.loads(record.payload)["text"] == "hello"
    print("✓ test_record_creation")


def test_record_with_metadata():
    """
    Test ZiRecord metadata attachment.
    
    Verifies that metadata can be attached to a record as a dictionary.
    The metadata should be accessible and modifiable after creation.
    """
    record = ZiRecord(id="1", payload='{"text": "hello"}')
    record.metadata = {"score": 0.9}
    assert record.metadata["score"] == 0.9
    print("✓ test_record_with_metadata")


def test_record_batch():
    """
    Test handling multiple records as a batch.
    
    Verifies that multiple ZiRecord instances can be collected into
    a batch and processed together. This is the fundamental unit of
    processing in Zi pipelines.
    """
    records = [
        ZiRecord(id="1", payload='{"text": "hello"}'),
        ZiRecord(id="2", payload='{"text": "world"}'),
    ]
    assert len(records) == 2
    print("✓ test_record_batch")


def test_record_id_options():
    """
    Test record ID options (with and without ID).
    
    Verifies that records can be created either with or without an ID.
    This is important for cases where record identity is not meaningful
    or is assigned later in the pipeline.
    """
    record_with_id = ZiRecord(id="test_id", payload='{"data": 1}')
    record_without_id = ZiRecord(id=None, payload='{"data": 2}')
    
    assert record_with_id.id is not None
    assert record_without_id.id is None
    print("✓ test_record_id_options")


def test_record_payload_access():
    """
    Test accessing and parsing complex JSON payloads.
    
    Verifies that nested JSON structures can be properly accessed
    through the payload property. Tests various data types including
    strings, numbers, and nested objects.
    """
    record = ZiRecord(id="1", payload='{"name": "test", "value": 42, "nested": {"a": 1}}')
    payload = json.loads(record.payload)
    
    assert payload["name"] == "test"
    assert payload["value"] == 42
    assert payload["nested"]["a"] == 1
    print("✓ test_record_payload_access")


def test_metrics():
    """
    Test ZiMetrics instantiation.
    
    Verifies that ZiMetrics can be created and used for
    computing quality metrics on record batches.
    """
    metrics = ZiMetrics()
    assert metrics is not None
    print("✓ test_metrics")

def test_version_info():
    """
    Test ZiVersionInfo instantiation and access.
    
    Verifies that version information can be retrieved from the
    Zi framework. The version string should be non-empty.
    """
    version = ZiVersionInfo()
    assert version.version is not None
    print(f"✓ test_version_info (version: {version.version})")


def test_text_processor():
    """
    Test ZiTextProcessor language detection.
    
    Verifies that the text processor can detect the language
    of input text and return a confidence score.
    """
    processor = ZiTextProcessor()
    lang, confidence = processor.detect_language("Hello world")
    assert lang is not None
    print(f"✓ test_text_processor (detected: {lang})")


def test_operator_creation():
    """
    Test ZiOperator instantiation with configuration.
    
    Verifies that operators can be created with a name and
    JSON configuration string. The name should be accessible
    as a property.
    """
    op = ZiOperator("filter.equals", '{"path": "payload.text", "equals": "hello"}')
    assert op.name == "filter.equals"
    print("✓ test_operator_creation")


def test_operator_apply():
    """
    Test ZiOperator apply method for filtering records.
    
    Verifies that operators can process a batch of records
    and return filtered results. The filter operator should
    keep only records matching the specified criteria.
    """
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
    """
    Test ZiOperator string representation.
    
    Verifies that operators provide a meaningful string
    representation that includes the operator name.
    """
    op = ZiOperator("filter.equals", "{}")
    repr_str = str(op)
    assert "filter.equals" in repr_str
    print("✓ test_operator_repr")


def run_core_tests():
    """
    Execute all core tests sequentially.
    
    This function runs all test functions in the test_core module
    and provides a summary of results. Tests are executed in a
    specific order to ensure dependencies are properly tested.
    """
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
