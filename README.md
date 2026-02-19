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

**Unified Data Quality Assessment, Cleaning, Transformation, Sampling, and Augmentation Framework.**

</div>

<h2 align="center">🏗️ Core Architecture</h2>

### 📐 Modular Design

Zi adopts a modular architecture optimized for data processing workflows:

<div align="center">

| Module | Description |
|:--------|:-------------|
| **pipeline** | Sequential processing through configurable operators |
| **dag** | DAG-based execution with topological sorting for parallel optimization |
| **operator** | Type-safe trait-based operator system |
| **operators** | Operator implementations (filter, quality, lang, etc.) |
| **cache** | Content-addressable cache with triple hashing (data/code/environment) |
| **monitor** | Runtime metrics collection and configurable quality thresholds |
| **py** | PyO3-based Python bindings for Python ecosystems |
| **io** | I/O support (JSONL, CSV, Parquet, Arrow) |
| **record** | Data record types and management |
| **orbit** | Plugin system for dynamic operator loading |
| **distributed** | Distributed processing support |
| **metrics** | Quality metrics computation |
| **log** | Structured logging subsystem |
| **errors** | Error types and handling |

</div>

### 🚀 Key Features

#### 🔍 Pipeline Processing
- Sequential processing through configurable operators
- DAG-based execution with topological sorting
- Content-addressable caching with triple hashing
- Incremental processing support

#### 📊 Quality Assessment
- Multi-metric text quality scoring (ASCII ratio, non-printable chars, repetition)
- Toxicity detection using built-in lexicon
- Language detection (en, zh, ar, ru) based on script analysis
- Configurable quality thresholds and filtering

#### 🔧 Data Transformation
- Rich filtering operators (equals, contains, regex, range, etc.)
- Metadata enrichment and manipulation
- PII redaction with custom patterns
- Text normalization and standardization

#### 📝 Deduplication
- SimHash-based near-duplicate detection
- MinHash-based similarity estimation
- Semantic deduplication support

#### 🎲 Sampling & Augmentation
- Random sampling for dataset reduction
- Top-k sampling for quality selection
- Synonym-based text augmentation
- Noise injection for data diversity

<h2 align="center">⚡ Quick Start</h2>

### Rust

```rust
use serde_json::json;
use Zi::pipeline::ZiCPipelineBuilder;
use Zi::record::ZiCRecord;

let records = vec![
    ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "Hello world"})),
    ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "你好世界"})),
];

let steps = [
    json!({"operator": "lang.detect", "config": {"path": "payload.text"}}),
    json!({"operator": "quality.score", "config": {"path": "payload.text"}}),
    json!({"operator": "quality.filter", "config": {"min": 0.5}}),
];

let pipeline = ZiCPipelineBuilder::with_defaults()
    .build_from_config(&steps)
    .expect("valid pipeline");

pipeline.run(records).expect("execution succeeds");
```

### Python

```python
import zi_core

# Utility functions
zi_core.compute_simhash("hello world")
zi_core.detect_language("hola")        # returns (lang, confidence)
zi_core.redact_pii("email: test@example.com")
zi_core.normalize_text("  Hello   WORLD  ")
zi_core.quality_score("quality text")
zi_core.toxicity_score("bad content")
zi_core.generate_prometheus_metrics()  # returns Prometheus format string
zi_core.version_info()                 # returns dict with version info
```

<h2 align="center">🔧 Configuration</h2>

### Configuration Format

```json
[
  {
    "operator": "operator.name",
    "config": { "path": "payload.text", "key": "field_name" }
  }
]
```

### Field Path Syntax

- `payload.text` — Access payload field
- `metadata.field` — Access metadata field
- `payload.nested.field` — Access nested field

### Feature Flags

```toml
[features]
default = ["full"]
full = ["parquet", "csv", "parallel"]
parquet = ["arrow2/io_parquet"]
csv = ["arrow2/io_csv", "dep:csv"]
parallel = ["rayon"]
pyo3 = ["pyo3/extension-module"]
```

<h2 align="center">🧪 Installation & Environment</h2>

### Prerequisites

- **Rust**: 1.70+
- **Cargo**: 1.70+
- **Platforms**: Linux, macOS, Windows

### Quick Setup

Add Zi to your project's `Cargo.toml`:

```toml
[dependencies]
zi = { git = "https://github.com/mf2023/Zi" }
```

Or use cargo add:

```bash
cargo add zi --git https://github.com/mf2023/Zi
```

### Build

```bash
# Default (full features)
cargo build --release

# Explicit full features
cargo build --release --features full

# With Python bindings
cargo build --release --features pyo3

cargo test
cargo bench
```

<h2 align="center">🛠️ Plugin System</h2>

### Plugin Usage

Dynamic operator loading via shared libraries:

```rust
let mut builder = ZiCPipelineBuilder::with_defaults();
builder.load_plugin("path/to/plugin.so")?;
```

Plugins must implement `zi_register_operators`.

<h2 align="center">🔒 Version Management</h2>

### Triple-Hash Versioning

Zi uses triple-hash versioning for reproducible processing:

- **Data Hash** — Input data hash
- **Code Hash** — Operator code hash
- **Environment Hash** — Execution environment hash

This enables precise data lineage tracking and exact result reproduction.

<h2 align="center">❓ Frequently Asked Questions</h2>

**Q: How to add a new operator?**
A: Implement the `ZiCOperator` trait and register it via the operator registry.

**Q: How to enable parallel execution?**
A: Enable the `parallel` feature flag and configure DAG scheduler for parallel execution.

**Q: How to configure quality gates?**
A: Set quality thresholds in the pipeline configuration under `monitor` section.

**Q: How to use content-addressable caching?**
A: Enable cache in pipeline configuration, Zi automatically handles caching based on triple hashing.

**Q: How to extend with Python operators?**
A: Use PyO3 bindings to create custom operators that integrate with the pipeline.

<h2 align="center">🌏 Community</h2>

- GitHub: https://github.com/mf2023/Zi
- Gitee: https://gitee.com/dunimd/zi

<div align="center">

## 📄 License & Open Source Agreements

### 🏛️ Project License

<p align="center">
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="Apache License 2.0">
  </a>
</p>

This project uses **Apache License 2.0** open source agreement, see [LICENSE](LICENSE) file.

### 📋 Dependency Package Open Source Agreements

<div align="center">

| 📦 Package | 📜 License |
|:-----------|:-----------|
| serde | Apache 2.0 / MIT |
| serde_json | MIT |
| regex | MIT |
| rayon | Apache 2.0 / MIT |
| pyo3 | Apache 2.0 / MIT |
| arrow2 | Apache 2.0 / MIT |
| csv | MIT |
| simhash | MIT |
| once_cell | MIT / Apache 2.0 |
| tempfile | MIT / Apache 2.0 |
| dashmap | MIT |
| tracing | MIT |
| thiserror | MIT |
| hex | MIT / Apache 2.0 |
| base64 | MIT |

</div>

</div>
