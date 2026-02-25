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
Zi (Zi Data Quality Framework) - Python bindings for Zi Core.

This Python package provides high-performance bindings to the Zi Rust library,
enabling Python applications to leverage comprehensive data quality assessment, cleaning,
transformation, sampling, and augmentation capabilities.

Example Usage:
    from zix import ZiRecord, ZiTextProcessor, ZiOperator
    
    # Create records
    records = [ZiRecord(id="1", payload='{"text": "hello"}')]
    
    # Use text processor
    processor = ZiTextProcessor()
    lang, confidence = processor.detect_language("Hello, world!")
    
    # Use operators directly
    op = ZiOperator("filter.equals", '{"path": "payload.text", "value": "hello"}')
    filtered = op.apply(records)
"""

__version__ = "0.1.0"
__author__ = "Dunimd Team"
__license__ = "Apache-2.0"

from zix import (
    ZiRecord,
    ZiMetrics,
    ZiTextProcessor,
    ZiVersionInfo,
    ZiOperator,
    ZiPipelineBuilder,
    ZiPipeline,
)

__all__ = [
    'ZiRecord',
    'ZiMetrics',
    'ZiTextProcessor',
    'ZiVersionInfo',
    'ZiOperator',
    'ZiPipelineBuilder',
    'ZiPipeline',
]
