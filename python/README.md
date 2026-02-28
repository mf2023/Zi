<div align="center">

<h1 style="display: flex; flex-direction: column; align-items: center; gap: 12px; margin-bottom: 8px;">
  <span style="display: flex; align-items: center; gap: 12px;">Zi</span>
  <span style="font-size: 0.6em; color: #666; font-weight: normal;">Zi Library for Python</span>
</h1>

English | [简体中文](README.zh.md)

[Help Documentation](https://mf2023.github.io/zi/zix/) | [Changelog](../CHANGELOG.md) | [Security](../SECURITY.md) | [Contributing](../CONTRIBUTING.md) | [Code of Conduct](../CODE_OF_CONDUCT.md)

<a href="https://space.bilibili.com/3493284091529457" target="_blank">
    <img alt="BiliBili" src="https://img.shields.io/badge/BiliBili-Dunimd-00A1D6?style=flat-square&logo=bilibili"/>
</a>
<a href="https://x.com/Dunimd2025" target="_blank">
    <img alt="X" src="https://img.shields.io/badge/X-Dunimd-000000?style=flat-square&logo=x"/>
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

<a href="https://pypi.org/project/zix/" target="_blank">
    <img alt="PyPI" src="https://img.shields.io/badge/PyPI-ZiX-3775A9?style=flat-square&logo=pypi"/>
</a>

**Zi** — A unified LLM dataset processing engine with Python bindings. Built for high-performance data quality assessment, cleaning, transformation, sampling, and augmentation.

</div>

<h2 align="center">🏗️ Core Architecture</h2>

### 📐 Modular Design

Zi adopts a highly modular architecture with 12 core modules, enabling on-demand composition and seamless extension:

<div align="center">

| Module | Description | Python Support |
|:--------|:------------|:---------------|
| **record** | Record management and batch processing | ✅ Full |
| **pipeline** | Sequential/parallel/conditional processing | ✅ Full |
| **dag** | DAG-based execution with topological sorting | ✅ Full |
| **operator** | Type-safe trait-based operator system | ✅ Full |
| **operators** | 90+ operator implementations | ✅ Full |
| **ingest** | Data ingestion (JSONL/JSON/CSV/Parquet) | ✅ Full |
| **export** | Data export (compression, sharding) | ✅ Full |
| **inspect** | Data profiling, statistics, diff analysis | ✅ Full |
| **enrich** | Data synthesis, augmentation, annotation | ✅ Full |
| **dsl** | DSL parser (YAML/JSON configuration) | ✅ Full |
| **orbit** | Plugin system for dynamic operators | ✅ Full |
| **distributed** | Distributed processing support | ✅ Full |

</div>

### 🚀 Key Features

#### 📊 Comprehensive Data Processing
- 90+ built-in operators for filtering, transformation, and enrichment
- Support for multiple data formats (JSON, JSONL, CSV, Parquet)
- Streaming read/write for large files
- Compression support (Gzip, Zstd)

#### 🔍 Quality Assessment
- Multi-metric text quality scoring (ASCII ratio, entropy, readability)
- Toxicity detection using built-in lexicon
- Language detection based on script analysis
- Configurable quality thresholds and filtering

#### 🤖 LLM-Specific Operators
- Token counting (Chinese/English mixed estimation)
- Conversation format conversion (ChatML, ShareGPT, Alpaca, OpenAI)
- Context length filtering/truncation/splitting
- QA pair extraction (Markdown, numbered, auto-detection)
- Instruction tuning data formatting

#### 📝 Deduplication
- SimHash-based near-duplicate detection
- MinHash-based similarity estimation
- Semantic deduplication support

#### 🔬 Data Inspection
- Statistical analysis (numeric, string, text statistics)
- Data profiling with anomaly detection
- Diff analysis for data comparison
- Distribution analysis with histograms and percentiles

<h2 align="center">🛠️ Installation & Environment</h2>

### Prerequisites

- **Python**: 3.8+ (Windows ARM64 requires 3.11+)
- **pip**: Latest version
- **Platforms**: Linux, macOS, Windows

### Quick Setup

Install Zi Python package:

```bash
pip install zix
```

Or add to your `requirements.txt`:

```
zix==0.1.0
```

### Build from Source

```bash
# Clone the repository
git clone https://github.com/mf2023/Zi.git
cd Zi

# Install maturin
pip install maturin

# Build and install
maturin develop --features pyo3
```

<h2 align="center">⚡ Quick Start</h2>

### Basic Usage

```python
from zix import ZiRecordPy, ZiOperatorPy, ZiPipelineBuilderPy

# Create records
records = [
    ZiRecordPy(id="1", payload='{"text": "Hello world"}'),
    ZiRecordPy(id="2", payload='{"text": "你好世界"}'),
]

# Use single operator
filter_op = ZiOperatorPy("filter.contains", '{"path": "payload.text", "value": "Hello"}')
filtered = filter_op.apply(records)
print(f"Filtered: {len(filtered)} records")
```

### Pipeline Processing

```python
from zix import ZiRecordPy, ZiPipelineBuilderPy

# Create records
records = [
    ZiRecordPy(id="1", payload='{"text": "Hello world"}'),
    ZiRecordPy(id="2", payload='{"text": "你好世界"}'),
]

# Build pipeline with multiple operators
pipeline = (ZiPipelineBuilderPy()
    .add_operator("lang.detect", '{"path": "payload.text"}')
    .add_operator("quality.score", '{"path": "payload.text"}')
    .add_operator("llm.token_count", '{"text_field": "payload.text"}')
    .add_operator("quality.filter", '{"min": 0.5}')
    .build())

# Execute pipeline
result = pipeline.run(records)
print(f"Processed: {len(result)} records")
```

### Text Processing

```python
from zix import ZiTextProcessor

processor = ZiTextProcessor()

# Language detection
lang, conf = processor.detect_language("Hello, world!")
print(f"Language: {lang}, Confidence: {conf}")

# Quality scoring
quality = processor.quality_score("This is a well-written text.")
print(f"Quality score: {quality}")

# Token counting
tokens = processor.count_tokens("Hello world", "cl100k_base")
print(f"Token count: {tokens}")

# PII redaction
redacted = processor.redact_pii("My email is test@example.com")
print(f"Redacted: {redacted}")
```

### Data Ingestion & Export

```python
from zix import ZiStreamReader, ZiStreamWriter

# Read data from file
reader = ZiStreamReader("data.jsonl", batch_size=10000)
batch = reader.read_all()

# Process data...

# Export data
writer = ZiStreamWriter("output.jsonl")
stats = writer.write(batch)
print(f"Written: {stats.records_written} records")
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
      
  - operator: quality.filter
    config:
      min: 0.5
```

```python
from zix import ZiDSLParser, ZiDSLCompiler

# Parse DSL configuration
parser = ZiDSLParser()
program = parser.parse_file("pipeline.yaml")

# Compile to pipeline
compiler = ZiDSLCompiler()
pipeline = compiler.compile(program)

# Execute
result = pipeline.run(records)
```

<h2 align="center">📋 Available Operators</h2>

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

### Field Operators (field.*)

| Operator | Description |
|:---------|:------------|
| `field.select` | Select fields |
| `field.rename` | Rename fields |
| `field.drop` | Drop fields |
| `field.copy` | Copy field |
| `field.move` | Move field |
| `field.flatten` | Flatten nested fields |

### Transform Operators (transform.*)

| Operator | Description |
|:---------|:------------|
| `transform.normalize` | Text normalization |
| `transform.map` | Field value mapping |
| `transform.template` | Template rendering |
| `transform.chain` | Chain transforms |

### Sample Operators (sample.*)

| Operator | Description |
|:---------|:------------|
| `sample.random` | Random sampling |
| `sample.top` | Top-K sampling |
| `sample.balanced` | Balanced sampling |
| `sample.stratified` | Stratified sampling |

<h2 align="center">🔧 Configuration</h2>

### Environment Variables

| Variable | Description | Default |
|:---------|:------------|:--------|
| `ZI_LOG_LEVEL` | Logging level | INFO |
| `ZI_BATCH_SIZE` | Default batch size | 10000 |

### Feature Flags

When building from source, you can enable specific features:

```bash
# Build with all features
maturin develop --features pyo3

# Build with specific features
maturin develop --features "pyo3,parquet,csv"
```

<h2 align="center">🧪 Development & Testing</h2>

### Running Tests

```bash
# Install development dependencies
pip install -e .

# Run Python tests
python -m pytest tests/python/

# Run specific test module
python -m pytest tests/python/test_core.py
```

<h2 align="center">❓ Frequently Asked Questions</h2>

**Q: How to use Zi in my project?**
A: Install via `pip install zix` and import the classes you need.

**Q: What operators are available?**
A: 90+ operators including filter, transform, quality, token, dedup, and more.

**Q: Can I use Zi with pandas?**
A: Yes! Convert pandas DataFrames to `ZiRecordPy` objects, then process with operators.

**Q: How do I handle large files?**
A: Use `ZiStreamReader` for streaming batch processing.

**Q: How to add custom operators?**
A: Implement the `ZiOperator` trait in Rust and register via the operator registry.

**Q: What Python versions are supported?**
A: Python 3.8 and above are supported. Note: Windows ARM64 requires Python 3.11+.

<h2 align="center">🌏 Community & Citation</h2>

- Welcome to submit Issues and PRs!
- Gitee: https://gitee.com/dunimd/zi.git
- GitHub: https://github.com/mf2023/Zi.git

<div align="center">

## 📄 License & Open Source Agreements

### 🏛️ Project License

<p align="center">
  <a href="../LICENSE">
    <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="Apache License 2.0">
  </a>
</p>

This project uses **Apache License 2.0** open source agreement, see [LICENSE](../LICENSE) file.

### 📋 Dependencies License

| 📦 Package | 📜 License |
|:-----------|:-----------|
| setuptools | MIT |
| maturin | Apache 2.0 |
| pyo3 | Apache 2.0 |
| pytest | MIT |
| pytest-asyncio | Apache 2.0 |
| black | MIT |
| isort | MIT |
| mypy | MIT |

</div>
