<div align="center">
  <h1 style="display: flex; flex-direction: column; align-items: center; gap: 12px; margin-bottom: 8px;">
    <span style="display: flex; align-items: center; gap: 12px;">
      <img src="../assets/svg/zi.svg" width="48" height="48" alt="Zi">
      <span style="font-size: 0.6em; color: #666; font-weight: normal;">Zi Data Quality Framework</span>
    </span>
  </h1>

  English | [简体中文](README.zh.md)
</div>

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

## 🏗️ Core Architecture

### 📐 Modular Design
Zi adopts a highly modular architecture with 8 core modules, enabling on-demand composition and seamless extension:

| Module | Description | Python Support |
|:--------|:------------|:---------------|
| **record** | Record management and batch processing | ✅ Full |
| **operators** | Data processing operators (90 operators) | ✅ Full |
| **pipeline** | Data pipeline construction and execution | ✅ Full |
| **ingest** | Data format detection and streaming | ✅ Full |
| **export** | Data export and streaming | ✅ Full |
| **inspect** | Data profiling, statistics, and diff analysis | ✅ Full |
| **enrich** | Data synthesis, augmentation, and annotation | ✅ Full |

### 🚀 Key Features

#### 📊 Comprehensive Data Processing
- 90 built-in operators for filtering, transformation, and enrichment
- Support for multiple data formats (JSON, JSONL, CSV, Parquet, Avro)
- Advanced data quality assessment and profiling
- Data synthesis and augmentation capabilities

#### 🔍 Data Inspection
- Statistical analysis (numeric, string, text statistics)
- Data profiling with anomaly detection
- Diff analysis for data comparison
- Distribution analysis with histograms and percentiles

#### 📝 Data Pipeline
- Builder pattern for pipeline construction
- Execution with metrics tracking
- Support for complex pipeline topologies

### 🛠️ Installation

#### Prerequisites
- **Python**: 3.8+ (Windows ARM64 requires 3.11+)
- **pip**: Latest version
- **Platforms**: Linux, macOS, Windows

#### Install from PyPI

```bash
pip install zix
```

Or install from source:

```bash
# Install maturin
pip install maturin

# Build and install
maturin develop
```

### 🚀 Quick Start

#### Basic Usage

```python
from zix import ZiRecordPy, ZiTextProcessor, ZiOperatorPy

# Create a record
record = ZiRecordPy(id="1", payload='{"text": "hello"}')

# Use text processor
processor = ZiTextProcessor()
lang, confidence = processor.detect_language("Hello, world!")

# Use operators directly
op = ZiOperatorPy("filter.equals", '{"path": "payload.text", "value": "hello"}')
filtered = op.apply([record])
```

#### Text Processing

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

#### Operator Usage

```python
from zix import ZiRecordPy, ZiOperatorPy

# Create records
records = [
    ZiRecordPy(id="1", payload='{"text": "hello world"}'),
    ZiRecordPy(id="2", payload='{"text": "goodbye world"}'),
]

# Use filter operator
filter_op = ZiOperatorPy("filter.contains", '{"path": "payload.text", "value": "hello"}')
filtered = filter_op.apply(records)
print(f"Filtered: {len(filtered)} records")

# Use quality operator
quality_op = ZiOperatorPy("quality.score", '{"path": "payload.text"}')
scored = quality_op.apply(records)

# Use language detection operator
lang_op = ZiOperatorPy("lang.detect", '{"path": "payload.text"}')
detected = lang_op.apply(records)
```

### 📋 Available Operators

| Category | Operators |
|:---------|:----------|
| **Filter** | `filter.equals`, `filter.contains`, `filter.regex`, `filter.range`, `filter.in`, `filter.exists` |
| **Quality** | `quality.score`, `quality.filter`, `quality.toxicity` |
| **Dedup** | `dedup.simhash`, `dedup.minhash`, `dedup.semantic` |
| **LLM** | `llm.token_count`, `llm.conversation_format`, `llm.context_length`, `llm.qa_extract` |
| **Transform** | `transform.normalize`, `transform.map`, `transform.template`, `transform.chain` |
| **Field** | `field.select`, `field.rename`, `field.drop`, `field.copy`, `field.move`, `field.flatten` |
| **Merge** | `merge.concat`, `merge.union`, `merge.intersect`, `merge.difference`, `merge.zip` |
| **Split** | `split.random`, `split.stratified`, `split.sequential`, `split.k_fold`, `split.chunk` |
| **Sample** | `sample.random`, `sample.top`, `sample.balanced`, `sample.stratified` |
| **Shuffle** | `shuffle`, `shuffle.deterministic`, `shuffle.block`, `shuffle.stratified` |
| **Token** | `token.count`, `token.stats`, `token.filter`, `token.histogram` |
| **Lang** | `lang.detect`, `lang.confidence` |
| **Metadata** | `metadata.enrich`, `metadata.remove`, `metadata.keep`, `metadata.rename` |
| **PII** | `pii.redact` |
| **Augment** | `augment.synonym`, `augment.noise` |
| **Limit** | `limit` |

### 🔧 Configuration

#### Python Package Configuration

Zi uses `maturin` for building and packaging. The package configuration is defined in `pyproject.toml`:

- **Package Name**: `zix`
- **Module Name**: `zix`
- **Build System**: `maturin`
- **Python Version**: 3.8+

#### Environment Variables

- `ZI_LOG_LEVEL`: Set logging level (DEBUG, INFO, WARN, ERROR)
- `ZI_BATCH_SIZE`: Default batch size for operations

### 📁 Documentation

For detailed API documentation, see:
- [API Reference](../doc/en/04-api-reference/)
- [Usage Examples](../doc/en/05-usage-examples/)

### 🧪 Development & Testing

#### Running Tests

```bash
# Install development dependencies
pip install -e .

# Run Python tests
python -m pytest tests/
```

### ❓ Frequently Asked Questions

**Q: How to use Zi in my project?**
A: Install via `pip install zix` and import the classes you need.

**Q: What operators are available?**
A: 90 operators including filter, transform, quality, token, dedup, and more.

**Q: Can I use Zi with pandas?**
A: Yes! Convert pandas DataFrames to `ZiRecordPy` objects, then process with `ZiOperatorPy`.

**Q: How do I get started?**
A: See the Quick Start section above for basic usage examples.

### 🌏 Community & Citation

- Welcome to submit Issues and PRs!
- Gitee: https://gitee.com/dunimd/zi.git
- GitHub: https://github.com/mf2023/Zi.git

### 📄 License & Open Source Agreements

### 🏛️ Project License

<p align="center">
  <a href="../LICENSE">
    <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="Apache License 2.0">
  </a>
</p>

This project uses **Apache License 2.0** open source agreement, see [LICENSE](../LICENSE) file.

### 📋 Dependencies License

<p align="center">
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
</p>
