<div align="center">

# Zi

English | [简体中文](README.zh.md)

[Help Documentation](https://mf2023.github.io/zi/zix/) | [Changelog](CHANGELOG.md) | [Security](SECURITY.md) | [Contributing](CONTRIBUTING.md) | [Code of Conduct](CODE_OF_CONDUCT.md)

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

**Unified LLM Dataset Processing Engine — Data Quality Assessment, Cleaning, Transformation, Sampling, and Augmentation Framework.**

</div>

<h2 align="center">🏗️ Core Architecture</h2>

### 📐 Modular Design

Zi adopts a modular architecture optimized for LLM data processing workflows:

<div align="center">

| Module | Description |
|:--------|:-------------|
| **pipeline** | Sequential/parallel/conditional processing through configurable operators |
| **dag** | DAG-based execution with topological sorting for parallel optimization |
| **operator** | Type-safe trait-based operator system |
| **operators** | Operator implementations (filter, quality, lang, LLM, etc.) |
| **ingest** | Data ingestion (JSONL/JSON/CSV/Parquet streaming read) |
| **export** | Data export (compression, sharding, Manifest) |
| **inspect** | Data inspection (Profile, Diff, Statistics) |
| **enrich** | Data enrichment (synthesis, annotation, augmentation) |
| **dsl** | DSL parser (YAML/JSON configuration) |
| **version** | Triple-hash versioning (data/code/environment) |
| **orbit** | Plugin system for dynamic operator loading |
| **distributed** | Distributed processing support |
| **context** | DMSC integration (log/cache/metrics/trace) |

</div>

### 🚀 Key Features

#### 🔍 Pipeline Processing
- Sequential/parallel/conditional processing through configurable operators
- DAG-based execution with topological sorting
- Content-addressable caching with triple hashing
- Incremental processing support

#### 📊 Quality Assessment
- Multi-metric text quality scoring (ASCII ratio, entropy, readability)
- Toxicity detection using built-in lexicon
- Language detection based on script analysis
- Configurable quality thresholds and filtering

#### 🔧 Data Transformation
- Rich filtering operators (equals, contains, regex, range, etc.)
- Metadata enrichment and manipulation
- PII redaction with custom patterns
- Text normalization and standardization
- Field operations (select, rename, drop, copy, move, flatten)
- Template-based value rendering

#### 📝 Deduplication
- SimHash-based near-duplicate detection
- MinHash-based similarity estimation
- Semantic deduplication support

#### 🤖 LLM-Specific Operators
- Token counting (Chinese/English mixed estimation)
- Conversation format conversion (ChatML, ShareGPT, Alpaca, OpenAI)
- Context length filtering/truncation/splitting
- QA pair extraction (Markdown, numbered, auto-detection)
- Instruction tuning data formatting (Alpaca, Vicuna, Llama2, ChatML)

#### 📥 Data Ingestion/Export
- Streaming read (large file support)
- Auto format detection (JSONL/JSON/CSV/Parquet)
- Compression support (Gzip, Zstd)
- Sharded write, atomic write
- Manifest with lineage tracking

#### 🔬 Data Inspection
- Data Profile (field statistics, frequency distribution, anomaly detection)
- Dataset Diff (record-level, field-level comparison)
- Text statistics (word frequency, N-gram)
- Distribution analysis (histogram, percentiles, correlation)

#### ✨ Data Augmentation
- Template-based data synthesis
- Rule-driven data generation (random, UUID, Faker)
- LLM-assisted synthesis interface

#### 📦 Dataset Operations
- Dataset merging (concat, union, intersect, difference, zip)
- Dataset splitting (random, stratified, sequential, k-fold, chunk)
- Balanced sampling (undersample, oversample, hybrid)
- Data shuffling (Fisher-Yates, block, stratified, window)

<h2 align="center">⚡ Quick Start</h2>

### Rust

```rust
use serde_json::json;
use zix::{ZiPipelineBuilder, ZiRecord};

let records = vec![
    ZiRecord::new(Some("1".into()), json!({"text": "Hello world"})),
    ZiRecord::new(Some("2".into()), json!({"text": "你好世界"})),
];

let steps = [
    json!({"operator": "lang.detect", "config": {"path": "payload.text"}}),
    json!({"operator": "quality.score", "config": {"path": "payload.text"}}),
    json!({"operator": "llm.token_count", "config": {"text_field": "payload.text"}}),
    json!({"operator": "quality.filter", "config": {"min": 0.5}}),
];

let pipeline = ZiPipelineBuilder::with_defaults()
    .build_from_config(&steps)
    .expect("valid pipeline");

let result = pipeline.run(records).expect("execution succeeds");
```

### Data Ingestion & Export

```rust
use zix::ingest::{ZiStreamReader, ZiReaderConfig};
use zix::export::{ZiStreamWriter, ZiWriterConfig, ZiOutputFormat};
use std::path::Path;

// Read data
let config = ZiReaderConfig {
    path: "data.jsonl".to_string(),
    batch_size: 10000,
    ..Default::default()
};
let reader = ZiStreamReader::new(config)?;
let batch = reader.read_all()?;

// Export data
let config = ZiWriterConfig {
    path: "output.jsonl".to_string(),
    format: ZiOutputFormat::Jsonl,
    batch_size: 1000,
    ..Default::default()
};
let writer = ZiStreamWriter::new(config);
let stats = writer.write(&batch)?;
```

### DSL Configuration

```yaml
# pipeline.yaml
steps:
  - operator: lang.detect
    config:
      path: payload.text
      
  - operator: quality.score
    config:
      path: payload.text
      
  - operator: llm.token_count
    config:
      text_field: payload.text
      output_field: metadata.token_count
      
  - operator: llm.context_length
    config:
      text_field: payload.text
      max_tokens: 8192
      action: Filter
      
  - operator: quality.filter
    config:
      min: 0.5
```

```rust
use zix::dsl::{ZiDSLParser, ZiDSLCompiler};

let parser = ZiDSLParser::new();
let result = parser.parse_file(Path::new("pipeline.yaml"))?;

let compiler = ZiDSLCompiler::new();
let pipeline = compiler.compile(&result.program)?;

let output = pipeline.run(batch)?;
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
full = ["parquet", "csv", "parallel", "domain", "distributed", "plugin", "compression"]
parquet = ["dep:parquet", "dep:arrow"]
csv = ["dep:csv"]
parallel = ["rayon"]
domain = []
distributed = []
plugin = ["wasmtime"]
compression = ["dep:flate2", "dep:zstd"]
pyo3 = ["dep:pyo3", "pyo3/extension-module"]
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
let mut builder = ZiPipelineBuilder::with_defaults();
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

<h2 align="center">📋 Operator List</h2>

### Filter Operators (filter.*)
| Operator | Description |
|:---------|:------------|
| `filter.equals` | Field equality filter |
| `filter.not_equals` | Field inequality filter |
| `filter.in` / `filter.not_in` | Inclusion/exclusion filter |
| `filter.contains` | String contains filter |
| `filter.regex` | Regular expression filter |
| `filter.range` | Numeric range filter |
| `filter.exists` / `filter.not_exists` | Field existence check |

### Quality Operators (quality.*)
| Operator | Description |
|:---------|:------------|
| `quality.score` | Text quality scoring |
| `quality.filter` | Quality threshold filter |
| `quality.toxicity` | Toxicity detection |

### Dedup Operators (dedup.*)
| Operator | Description |
|:---------|:------------|
| `dedup.simhash` | SimHash deduplication |
| `dedup.minhash` | MinHash deduplication |
| `dedup.semantic` | Semantic deduplication |

### LLM Operators (llm.*)
| Operator | Description |
|:---------|:------------|
| `llm.token_count` | Token counting |
| `llm.conversation_format` | Conversation format conversion |
| `llm.context_length` | Context length filtering |
| `llm.qa_extract` | QA pair extraction |
| `llm.instruction_format` | Instruction formatting |

### Merge Operators (merge.*)
| Operator | Description |
|:---------|:------------|
| `merge.concat` | Concatenate datasets |
| `merge.batch` | Batch merge records |
| `merge.union` | Union with deduplication |
| `merge.intersect` | Intersection of datasets |
| `merge.difference` | Difference of datasets |
| `merge.zip` | Zip merge fields |

### Split Operators (split.*)
| Operator | Description |
|:---------|:------------|
| `split.random` | Random split (train/valid/test) |
| `split.stratified` | Stratified split |
| `split.sequential` | Sequential split |
| `split.kfold` | K-fold split |
| `split.chunk` | Chunk split |

### Token Operators (token.*)
| Operator | Description |
|:---------|:------------|
| `token.count` | Token count per record |
| `token.stats` | Token statistics |
| `token.filter` | Filter by token count |
| `token.histogram` | Token distribution histogram |

### Field Operators (field.*)
| Operator | Description |
|:---------|:------------|
| `field.select` | Select fields |
| `field.rename` | Rename fields |
| `field.drop` | Drop fields |
| `field.copy` | Copy field |
| `field.move` | Move field |
| `field.flatten` | Flatten nested fields |
| `field.default` | Set default value |
| `field.require` | Require fields |

### Transform Operators (transform.*)
| Operator | Description |
|:---------|:------------|
| `transform.normalize` | Text normalization |
| `transform.map` | Field value mapping |
| `transform.template` | Template rendering |
| `transform.chain` | Chain transforms |
| `transform.flat_map` | Flatten and map |
| `transform.coalesce` | Coalesce values |
| `transform.conditional` | Conditional transform |

### Sample Operators (sample.*)
| Operator | Description |
|:---------|:------------|
| `sample.random` | Random sampling |
| `sample.top` | Top-K sampling |
| `sample.balanced` | Balanced sampling |
| `sample.by_distribution` | Distribution-based sampling |
| `sample.by_length` | Length-based sampling |
| `sample.stratified` | Stratified sampling |

### Shuffle Operators (shuffle.*)
| Operator | Description |
|:---------|:------------|
| `shuffle` | Random shuffle |
| `shuffle.deterministic` | Deterministic shuffle |
| `shuffle.block` | Block shuffle |
| `shuffle.stratified` | Stratified shuffle |
| `shuffle.window` | Window shuffle |

### Distribution Operators (distribution.*)
| Operator | Description |
|:---------|:------------|
| `distribution.analyze` | Field distribution analysis |
| `distribution.report` | Distribution report |
| `distribution.correlation` | Correlation analysis |

### Other Operators
| Operator | Description |
|:---------|:------------|
| `lang.detect` | Language detection |
| `metadata.enrich` | Metadata enrichment |
| `limit` | Record count limit |
| `pii.redact` | PII redaction |

<h2 align="center">❓ Frequently Asked Questions</h2>

**Q: How to add a new operator?**
A: Implement the `ZiOperator` trait and register it via the operator registry.

**Q: How to enable parallel execution?**
A: Enable the `parallel` feature flag and configure DAG scheduler for parallel execution.

**Q: How to handle large files?**
A: Use `ZiRecordIterator` for streaming batch processing.

**Q: How to use DSL configuration?**
A: Use `ZiDSLParser` to parse YAML/JSON configuration files.

**Q: How to track data lineage?**
A: Use `ZiManifest` and `ZiLineage` to record processing history.

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
| dmsc | Apache 2.0 |
| serde | Apache 2.0 / MIT |
| serde_json | MIT |
| serde_yaml | MIT / Apache 2.0 |
| regex | MIT |
| rayon | Apache 2.0 / MIT |
| pyo3 | Apache 2.0 / MIT |
| arrow | Apache 2.0 |
| parquet | Apache 2.0 |
| csv | MIT |
| blake3 | Apache 2.0 / MIT |
| chrono | MIT / Apache 2.0 |
| tokio | MIT |
| rand | MIT / Apache 2.0 |
| flate2 | MIT |
| zstd | MIT |
| thiserror | MIT |
| anyhow | MIT |

</div>

</div>
