<div align="center">

# Zi

[English](README.md) | 简体中文

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

**统一的大模型数据集处理引擎 — 数据质量评估、清洗、转换、采样与增强框架。**

</div>

<h2 align="center">🏗️ 核心架构</h2>

### 📐 模块化设计

Zi 采用针对 LLM 数据处理工作流优化的模块化架构：

<div align="center">

| 模块 | 描述 |
|:--------|:-------------|
| **pipeline** | 通过可配置算子进行顺序/并行/条件处理 |
| **dag** | 基于 DAG 的执行，支持拓扑排序实现并行优化 |
| **operator** | 基于 trait 的类型安全算子系统 |
| **operators** | 算子实现（过滤、质量、语言、LLM 等） |
| **ingest** | 数据摄入（JSONL/JSON/CSV/Parquet 流式读取） |
| **export** | 数据导出（压缩、分片、Manifest 清单） |
| **inspect** | 数据检查（Profile、Diff、Statistics） |
| **enrich** | 数据增强（合成、标注、增强） |
| **dsl** | DSL 解析器（YAML/JSON 配置） |
| **version** | 三哈希版本控制（数据/代码/环境） |
| **orbit** | 用于动态加载算子的插件系统 |
| **distributed** | 分布式处理支持 |
| **context** | DMSC 集成（日志/缓存/指标/追踪） |

</div>

### 🚀 核心特性

#### 🔍 管道处理
- 通过可配置算子进行顺序/并行/条件处理
- 基于 DAG 的执行，支持拓扑排序
- 使用三哈希的内容寻址缓存
- 支持增量处理

#### 📊 质量评估
- 多指标文本质量评分（ASCII 比例、熵、可读性）
- 使用内置词典的毒性检测
- 基于脚本分析的语言检测
- 可配置的质量阈值和过滤

#### 🔧 数据转换
- 丰富的过滤算子（等于、包含、正则、范围等）
- 元数据丰富和操作
- 支持自定义模式的 PII 编辑
- 文本规范化和标准化

#### 📝 去重
- 基于 SimHash 的近重复检测
- 基于 MinHash 的相似度估计
- 支持语义去重

#### 🤖 LLM 专用算子
- Token 统计（支持中英文混合估算）
- 对话格式转换（ChatML、ShareGPT、Alpaca、OpenAI）
- 上下文长度过滤/截断/分割
- QA 对提取（Markdown、编号、自动检测）
- 指令微调数据格式化（Alpaca、Vicuna、Llama2、ChatML）

#### 📥 数据摄入/导出
- 流式读取（支持大文件）
- 格式自动检测（JSONL/JSON/CSV/Parquet）
- 压缩文件支持（Gzip、Zstd）
- 分片写入、原子写入
- Manifest 清单与血缘追踪

#### 🔬 数据检查
- 数据 Profile（字段统计、频率分布、异常检测）
- 数据集 Diff（记录级、字段级对比）
- 文本统计（词频、N-gram）

#### ✨ 数据增强
- 模板化数据合成
- 规则驱动数据生成（随机数、UUID、Faker）
- LLM 辅助合成接口

<h2 align="center">⚡ 快速开始</h2>

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
    json!({"operator": "llm.token_count", "config": {"text_field": "payload.text"}}),
    json!({"operator": "quality.filter", "config": {"min": 0.5}}),
];

let pipeline = ZiCPipelineBuilder::with_defaults()
    .build_from_config(&steps)
    .expect("合法的管道配置");

let result = pipeline.run(records).expect("管道执行成功");
```

### 数据摄入与导出

```rust
use Zi::ingest::{ZiCStreamReader, ZiCReaderConfig};
use Zi::export::{ZiCStreamWriter, ZiCWriterConfig, ZiCOutputFormat};
use std::path::Path;

// 读取数据
let reader = ZiCStreamReader::ZiFNew()
    .ZiFWithConfig(ZiCReaderConfig {
        batch_size: 10000,
        skip_errors: true,
        ..Default::default()
    });

let batch = reader.ZiFReadPath(Path::new("data.jsonl"))?;

// 导出数据
let mut writer = ZiCStreamWriter::ZiFNew();
let config = ZiCWriterConfig {
    format: ZiCOutputFormat::Jsonl,
    compression: ZiCCompression::Gzip,
    split_by_count: Some(100000),
    ..Default::default()
};

let stats = writer.ZiFWrite(&batch, Path::new("output.jsonl.gz"))?;
```

### DSL 配置

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
use Zi::dsl::{ZiCDSLParser, ZiCDSLCompiler};

let parser = ZiCDSLParser::ZiFNew();
let result = parser.ZiFParseFile(Path::new("pipeline.yaml"))?;

let compiler = ZiCDSLCompiler::ZiFNew();
let pipeline = compiler.ZiFCompile(&result.program)?;

let output = pipeline.ZiFRun(batch)?;
```

<h2 align="center">🔧 配置</h2>

### 配置格式

```json
[
  {
    "operator": "operator.name",
    "config": { "path": "payload.text", "key": "field_name" }
  }
]
```

### 字段路径语法

- `payload.text` — 访问 payload 字段
- `metadata.field` — 访问元数据字段
- `payload.nested.field` — 访问嵌套字段

### 特性标志

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

<h2 align="center">🧪 安装与环境</h2>

### 前置要求

- **Rust**: 1.70+
- **Cargo**: 1.70+
- **平台**: Linux、macOS、Windows

### 快速安装

在项目的 `Cargo.toml` 中添加 Zi：

```toml
[dependencies]
zi = { git = "https://github.com/mf2023/Zi" }
```

或使用 cargo add：

```bash
cargo add zi --git https://github.com/mf2023/Zi
```

### 构建

```bash
# 默认（完整功能）
cargo build --release

# 显式完整功能
cargo build --release --features full

# 包含 Python 绑定
cargo build --release --features pyo3

cargo test
cargo bench
```

<h2 align="center">🛠️ 插件系统</h2>

### 插件使用

通过共享库动态加载算子：

```rust
let mut builder = ZiCPipelineBuilder::with_defaults();
builder.load_plugin("path/to/plugin.so")?;
```

插件必须实现 `zi_register_operators` 函数。

<h2 align="center">🔒 版本管理</h2>

### 三哈希版本控制

Zi 使用三哈希版本控制实现可重复处理：

- **数据哈希** — 输入数据哈希
- **代码哈希** — 算子代码哈希
- **环境哈希** — 执行环境哈希

这实现了精确的数据血缘追踪和结果精确重现。

<h2 align="center">📋 算子列表</h2>

### 过滤算子 (filter.*)
| 算子 | 描述 |
|:-----|:-----|
| `filter.equals` | 字段相等过滤 |
| `filter.not_equals` | 字段不等过滤 |
| `filter.in` / `filter.not_in` | 包含/排除过滤 |
| `filter.contains` | 字符串包含过滤 |
| `filter.regex` | 正则表达式过滤 |
| `filter.range` | 数值范围过滤 |
| `filter.exists` / `filter.not_exists` | 字段存在检查 |

### 质量算子 (quality.*)
| 算子 | 描述 |
|:-----|:-----|
| `quality.score` | 文本质量评分 |
| `quality.filter` | 质量阈值过滤 |
| `quality.toxicity` | 毒性检测 |

### 去重算子 (dedup.*)
| 算子 | 描述 |
|:-----|:-----|
| `dedup.simhash` | SimHash 去重 |
| `dedup.minhash` | MinHash 去重 |
| `dedup.semantic` | 语义去重 |

### LLM 算子 (llm.*)
| 算子 | 描述 |
|:-----|:-----|
| `llm.token_count` | Token 统计 |
| `llm.conversation_format` | 对话格式转换 |
| `llm.context_length` | 上下文长度过滤 |
| `llm.qa_extract` | QA 对提取 |
| `llm.instruction_format` | 指令格式化 |

### 其他算子
| 算子 | 描述 |
|:-----|:-----|
| `lang.detect` | 语言检测 |
| `metadata.enrich` | 元数据丰富 |
| `limit` | 记录数量限制 |
| `sample.random` | 随机采样 |
| `pii.redact` | PII 脱敏 |

<h2 align="center">❓ 常见问题</h2>

**Q: 如何添加新算子？**
A: 实现 `ZiCOperator` trait 并通过算子注册表注册。

**Q: 如何启用并行执行？**
A: 启用 `parallel` 特性标志并配置 DAG 调度器进行并行执行。

**Q: 如何处理大文件？**
A: 使用 `ZiCRecordIterator` 进行流式批处理。

**Q: 如何使用 DSL 配置？**
A: 使用 `ZiCDSLParser` 解析 YAML/JSON 配置文件。

**Q: 如何追踪数据血缘？**
A: 使用 `ZiCManifest` 和 `ZiCLineage` 记录处理过程。

<h2 align="center">🌏 社区</h2>

- GitHub: https://github.com/mf2023/Zi
- Gitee: https://gitee.com/dunimd/zi

<div align="center">

## 📄 许可证与开源协议

### 🏛️ 项目许可证

<p align="center">
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="Apache License 2.0">
  </a>
</p>

本项目使用 **Apache License 2.0** 开源协议，详见 [LICENSE](LICENSE) 文件。

### 📋 依赖包开源协议

<div align="center">

| 📦 包 | 📜 许可证 |
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
