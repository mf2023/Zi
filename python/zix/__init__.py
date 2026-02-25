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

################################################################################
# Zi Python Package Initialization Module
################################################################################
#
# This module serves as the main entry point for the Zi Python bindings package.
# It provides Python users with access to the Zi Rust library's functionality
# through PyO3-generated Python extensions.
#
# Package Overview:
# Zi is a comprehensive data quality framework that provides:
# - Data quality assessment and metrics calculation
# - Data cleaning and validation
# - Data transformation and augmentation
# - Text processing capabilities (language detection, etc.)
# - Pipeline-based data processing workflows
#
# Architecture:
# This package acts as a Python wrapper around the native Rust implementation.
# All core functionality is implemented in Rust for performance, while this
# module provides Pythonic interfaces and automatic exports.
#
# Key Components:
# - ZiRecord: Core data structure for representing individual records
# - ZiMetrics: Data quality metrics calculation and storage
# - ZiTextProcessor: Natural language processing utilities
# - ZiVersionInfo: Version information access
# - ZiOperator: Individual data processing operators
# - ZiPipelineBuilder: Fluent API for building processing pipelines
# - ZiPipeline: Executable data processing pipelines
#
# Dependencies:
# - PyO3: Rust-Python binding framework
# - Python 3.8+ (supports older Python versions for broad compatibility)
#
# Build System:
# This package is built using maturin, which handles Rust compilation and
# Python wheel generation. The underlying Rust library must be compiled
# with the 'pyo3' feature enabled.
################################################################################

"""
Zi (Zi Data Quality Framework) - Python bindings for Zi Core.

This Python package provides high-performance bindings to the Zi Rust library,
enabling Python applications to leverage comprehensive data quality assessment, cleaning,
transformation, sampling, and augmentation capabilities.

The package is built on PyO3, which generates native Python extensions from Rust code,
providing near-native performance for all data processing operations.

Main Features:
    1. ZiRecord: Immutable record representation with support for various data types
    2. ZiMetrics: Comprehensive data quality metrics including completeness, validity,
       consistency, and uniqueness scores
    3. ZiTextProcessor: Text analysis including language detection with confidence scores
    4. ZiOperator: Individual data processing operators that can be composed
    5. ZiPipelineBuilder: Fluent builder API for constructing complex processing pipelines
    6. ZiPipeline: Executable pipeline that processes records through multiple stages

Data Flow:
    Records -> ZiOperator(s) -> ZiPipeline -> Processed Records
              or
    Records -> ZiOperator.apply() -> Filtered/Transformed Records

Performance Characteristics:
    - Near-native Rust performance for all operations
    - Zero-copy record access where possible
    - Parallel processing support for large datasets
    - Memory-efficient streaming for large-scale processing

Example Usage:
    from zix import ZiRecord, ZiTextProcessor, ZiOperator
    
    # Create records with different payload types
    records = [
        ZiRecord(id="1", payload='{"text": "hello", "value": 42}'),
        ZiRecord(id="2", payload='{"text": "world", "value": 100}'),
    ]
    
    # Use text processor for language detection
    processor = ZiTextProcessor()
    lang, confidence = processor.detect_language("Hello, world!")
    # Returns: ('en', 0.95) for English with 95% confidence
    
    # Use operators for data filtering
    op = ZiOperator("filter.equals", '{"path": "payload.text", "value": "hello"}')
    filtered = op.apply(records)
    # Returns only records where payload.text equals "hello"
    
    # Use pipeline for complex workflows
    from zix import ZiPipelineBuilder
    pipeline = (ZiPipelineBuilder()
        .filter("filter.equals", '{"path": "payload.value", "value": 42}')
        .transform("transform.uppercase", '{"paths": ["payload.text"]}')
        .build())
    result = pipeline.process(records)

Supported Platforms:
    - Linux x86_64 and ARM64
    - Windows x86_64
    - macOS x86_64 and ARM64 (Apple Silicon)

Installation:
    pip install zix

Or install from source:
    pip install maturin
    maturin build --release --features pyo3
    pip install target/dist/*.whl
"""

# ================================================================================
# Package Version Information
# ================================================================================
#
# Version follows Semantic Versioning (https://semver.org/)
# Major version: Incompatible API changes
# Minor version: New backward-compatible functionality
# Patch version: Backward-compatible bug fixes
#
# Note: This version tracks the Python binding version. The underlying
# Rust library may have its own version that could differ.
__version__ = "0.1.0"

# Package author information
# Reflects the Dunimd Team as the original project maintainers
__author__ = "Dunimd Team"

# License identifier
# Apache License 2.0 is a permissive open-source license that allows
# commercial use, modification, distribution, and private use
__license__ = "Apache-2.0"

# ================================================================================
# Public API Exports
# ================================================================================
#
# This section imports all public classes and functions from the underlying
# Rust-generated extension module. These are re-exported here to provide
# a clean, stable public API.
#
# Import Strategy:
# The 'from _zix import ...' syntax imports directly from the compiled
# extension module (_zix.*.so or _zix.*.pyd), which contains the PyO3 bindings.
#
# All imported symbols are re-exported via __all__ to provide a stable API
# surface. Users should import these symbols directly from the zix package
# rather than from the extension module directly.

from _zix import (
    # ZiRecord: Core data structure representing a single data record
    # Used as the fundamental unit of data throughout the framework
    # Supports various payload types (JSON, string, binary)
    ZiRecord,
    
    # ZiMetrics: Container for data quality metrics
    # Calculates and stores quality scores including completeness,
    # validity, consistency, and uniqueness metrics
    ZiMetrics,
    
    # ZiTextProcessor: Natural language processing utilities
    # Provides language detection with confidence scores and other
    # text analysis capabilities
    ZiTextProcessor,
    
    # ZiVersionInfo: Version information accessor
    # Provides access to both the Python binding version and underlying
    # Rust library version for debugging and compatibility checking
    ZiVersionInfo,
    
    # ZiOperator: Individual data processing operator
    # Represents a single operation (filter, transform, validate, etc.)
    # that can be applied to records
    ZiOperator,
    
    # ZiPipelineBuilder: Fluent builder for constructing pipelines
    # Provides a chainable API for building complex multi-stage
    # processing pipelines with type-safe operator composition
    ZiPipelineBuilder,
    
    # ZiPipeline: Executable processing pipeline
    # Represents a complete, executable data processing workflow
    # that processes records through multiple operator stages
    ZiPipeline,
)

# ================================================================================
# Public API Definition
# ================================================================================
#
# __all__ defines the public API surface for this package.
# Any symbol not listed here is considered private and may change without notice.
#
# This list should be updated whenever new public classes are added.
# The order reflects logical grouping and common usage patterns.
__all__ = [
    # Core data structures
    'ZiRecord',
    'ZiMetrics',
    
    # Text processing
    'ZiTextProcessor',
    
    # Version information
    'ZiVersionInfo',
    
    # Operators and pipelines
    'ZiOperator',
    'ZiPipelineBuilder',
    'ZiPipeline',
]
