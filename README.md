<div align="center">

# Zi

English | [简体中文](README.zh.md)

<a href="https://space.bilibili.com/3493284091529457" target="_blank">
    <img alt="BiliBili" src="https://img.shields.io/badge/BiliBili-Dunimd-00A1D6?style=flat-square&logo=bilibili"/>
</a>
<a href="https://gitee.com/dunimd" target="_blank">
    <img alt="Gitee" src="https://img.shields.io/badge/Gitee-Dunimd-C71D23?style=flat-square&logo=gitee"/>
</a>
<a href="https://github.com/mf2023/Zi" target="_blank">
    <img alt="GitHub" src="https://img.shields.io/badge/GitHub-Zi-181717?style=flat-square&logo=github"/>
</a>
<a href="https://huggingface.co/dunimd" target="_blank">
    <img alt="Hugging Face" src="https://img.shields.io/badge/Hugging%20Face-Dunimd-FFD21E?style=flat-square&logo=huggingface"/>
</a>
<a href="https://modelscope.cn/organization/dunimd" target="_blank">
    <img alt="ModelScope" src="https://img.shields.io/badge/ModelScope-Dunimd-1E6CFF?style=flat-square&logo=data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTQiIGhlaWdodD0iMTQiIHZpZXdCb3g9IjAgMCAxNCAxNCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHBhdGggZD0iTTcuMDA2IDBDMy4xNDIgMCAwIDMuMTQyIDAgNy4wMDZTMy4xNDIgMTQuMDEyIDcuMDA2IDE0LjAxMkMxMC44NyAxNC4wMTIgMTQuMDEyIDEwLjg3IDE0LjAxMiA3LjAwNkMxNC4wMTIgMy4xNDIgMTAuODcgMCA3LjAwNiAwWiIgZmlsbD0iIzFFNkNGRiIvPgo8L3N2Zz4K"/>
</a>

A high-performance data processing engine built with Rust, designed for modern machine learning workflows. Zi provides a unified framework for data quality assessment, cleaning, transformation, sampling, and augmentation with exceptional speed and reliability.

</div>

## 🎯 Project Overview

Zi is a Rust-based data processing library that implements a pipeline architecture for data transformation and quality assessment. The project focuses on providing a type-safe, efficient, and extensible framework for data processing operations.

## 🏗️ Architecture

### Core Components

- **Pipeline Engine**: Sequential processing of data through configurable operators
- **Operator Framework**: Type-safe trait-based operator system
- **Record Processing**: JSON-based data records with metadata support
- **Plugin System**: Dynamic loading of custom operators via shared libraries (optional)

### Naming Convention

To keep the public API consistent, Zi follows a strict naming rule:

- Public structs/enums/traits use the `ZiC*` prefix (e.g. `ZiCRecord`, `ZiCPipeline`).
- Public functions and associated constructors use the `ZiF*` prefix (e.g. `ZiFNew`, `ZiFLoadJsonl`).
- Internal helpers are prefixed with `_`.

Every example in this README adopts the same convention so that the snippets match the actual codebase.

### Operator Categories

Based on the actual codebase, Zi supports the following operator categories:

#### 1. Filter Operators (`filter.*`)
- `filter.equals` - Field equality filtering
- `filter.not_equals` - Field inequality filtering  
- `filter.any` - Any field matching value
- `filter.in` - Value inclusion filtering
- `filter.not_in` - Value exclusion filtering
- `filter.exists` - Field existence checking
- `filter.not_exists` - Field non-existence checking
- `filter.contains` - String containment filtering
- `filter.contains_all` - Multiple string containment
- `filter.contains_any` - Any string containment
- `filter.contains_none` - String exclusion filtering
- `filter.length_range` - Text length filtering
- `filter.token_range` - Token count filtering
- `filter.array_contains` - Array element filtering
- `filter.starts_with` - String prefix filtering
- `filter.ends_with` - String suffix filtering
- `filter.regex` - Regular expression filtering
- `filter.is_null` - Null value filtering
- `filter.greater_than` - Numeric greater than filtering
- `filter.less_than` - Numeric less than filtering
- `filter.between` - Numeric range filtering
- `filter.range` - Numeric range filtering (alternative)

#### 2. Quality Operators (`quality.*`)
- `quality.score` - Text quality scoring based on ASCII ratio, non-printable characters, and repetition
- `quality.filter` - Quality threshold filtering
- `quality.toxicity` - Toxicity detection using built-in lexicon

#### 3. Language Operators (`lang.*`)
- `lang.detect` - Language detection (en, zh, ar, ru) based on script analysis
- `lang.confidence` - Language confidence scoring

#### 4. Metadata Operators (`metadata.*`)
- `metadata.enrich` - Add metadata fields
- `metadata.rename` - Rename metadata fields
- `metadata.remove` - Remove metadata fields
- `metadata.copy` - Copy metadata fields
- `metadata.require` - Require metadata fields
- `metadata.extract` - Extract values to metadata
- `metadata.keep` - Keep only specified metadata fields

#### 5. Limit Operators (`limit`)
- `limit` - Record count limiting

#### 6. Dedup Operators (`dedup.*`)
- `dedup.simhash` - SimHash-based deduplication
- `dedup.minhash` - MinHash-based deduplication
- `dedup.semantic` - Semantic deduplication

#### 7. PII Operators (`pii.*`)
- `pii.redact` - PII redaction with custom patterns

#### 8. Transform Operators (`transform.*`)
- `transform.normalize` - Text normalization

#### 9. Sample Operators (`sample.*`)
- `sample.random` - Random sampling
- `sample.top` - Top-k sampling

#### 10. Augment Operators (`augment.*`)
- `augment.synonym` - Synonym-based text augmentation
- `augment.noise` - Noise injection augmentation

## 🚀 Quick Start

### Rust Usage

```rust
use serde_json::json;
use Zi::pipeline::ZiCPipelineBuilder;
use Zi::record::ZiCRecord;

// Create sample data
let records = vec![
    ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "Hello world"})),
    ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "你好世界"})),
];

// Define pipeline steps (slice of serde_json::Value)
let steps = [
    json!({
        "operator": "lang.detect",
        "config": {"path": "payload.text", "key": "language"}
    }),
    json!({
        "operator": "quality.score",
        "config": {"path": "payload.text", "key": "quality_score"}
    }),
    json!({
        "operator": "quality.filter",
        "config": {"key": "quality_score", "min": 0.5}
    }),
];

// Build processing pipeline
let pipeline = ZiCPipelineBuilder::with_defaults()
    .build_from_config(&steps)
    .expect("valid pipeline definition");

// Process data
let processed = pipeline
    .run(records)
    .expect("pipeline execution succeeds");
```

### Configuration Format

Operators are configured using JSON with the following structure:

```json
[
  {
    "operator": "operator.name",
    "config": {
      // Operator-specific configuration
    }
  }
]
```

### Field Path Syntax

Field paths use dot notation to navigate JSON structures:
- `payload.text` - Access text field in payload
- `metadata.field` - Access field in metadata
- `payload.nested.field` - Access nested fields

## 📊 Performance

Zi is built with Rust for maximum performance:

- **Zero-copy operations** where possible
- **Memory-safe processing** with Rust's ownership system
- **Efficient JSON processing** with serde_json
- **Streaming support** for large datasets

## 🏗️ Building from Source

### Prerequisites
- Rust 1.70+
- Cargo

### Build Commands
```bash
# Clone the repository
git clone https://github.com/mf2023/Zi.git
cd Zi

# Build in release mode
cargo build --release

# Run tests
cargo test

# Run benchmarks
cargo bench
```

## 📁 Project Structure

```
src/
├── lib.rs              # Library entry point
├── errors.rs           # Error handling types
├── io.rs               # I/O utilities
├── metrics.rs          # Quality metrics
├── operator.rs         # Core operator traits
├── pipeline.rs         # Pipeline engine
├── record.rs           # Data record types
└── operators/          # Operator implementations
    ├── augment.rs      # Data augmentation operators
    ├── dedup.rs        # Deduplication operators
    ├── filter.rs       # Filtering operators
    ├── lang.rs         # Language processing operators
    ├── limit.rs        # Limiting operators
    ├── metadata.rs     # Metadata operators
    ├── mod.rs          # Operators module
    ├── pii.rs          # PII processing operators
    ├── quality.rs      # Quality assessment operators
    ├── sample.rs       # Sampling operators
    └── transform.rs    # Text transformation operators
```

## 🔧 Plugin System

Zi supports dynamic loading of custom operators through shared libraries:

```rust
let mut builder = ZiCPipelineBuilder::with_defaults();
builder.load_plugin("path/to/plugin.so")?;
```

Plugins must implement the `zi_register_operators` function and register their operators with the builder.

## 🎯 Use Cases

### Data Quality Assessment
- Text quality scoring based on multiple metrics
- Language detection and confidence scoring
- Toxicity detection for content moderation

### Data Filtering
- Complex filtering based on field values
- Regular expression matching
- Range-based numeric filtering

### Data Transformation
- Metadata enrichment and manipulation
- Text normalization
- PII redaction

### Data Deduplication
- SimHash-based near-duplicate detection
- MinHash-based similarity detection
- Semantic deduplication

## 🔮 Future Development

### Planned Features
- Additional language support beyond basic script detection
- Advanced quality metrics
- Machine learning-based operators
- Distributed processing support
- Web UI for pipeline configuration

## 📄 License

This project is licensed under the Apache License 2.0 — see [LICENSE](LICENSE).

---

## 🌏 Community & Citation
- Issues and pull requests are welcome!
- GitHub: https://github.com/mf2023/Zi.git
- Gitee: https://gitee.com/dunimd/zi.git

## 🙏 Acknowledgments

Built with excellent Rust ecosystem tools:
- [Serde](https://serde.rs/) for JSON processing
- [Regex](https://docs.rs/regex/) for pattern matching
- [Arrow2](https://github.com/jorgecarleitao/arrow2) for columnar data processing
- [Libloading](https://docs.rs/libloading/) for plugin support

<h3 align="center">Where intuition navigates the depths of data · And empathy gives form to intelligence</h3>