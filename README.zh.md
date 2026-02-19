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

**统一的数据质量评估、清洗、转换、采样与增强框架。**

</div>

<h2 align="center">🏗️ 核心架构</h2>

### 📐 模块化设计

Zi 采用针对数据处理工作流优化的模块化架构：

<div align="center">

| 模块 | 描述 |
|:--------|:-------------|
| **pipeline** | 通过可配置算子进行顺序处理 |
| **dag** | 基于 DAG 的执行，支持拓扑排序实现并行优化 |
| **operator** | 基于 trait 的类型安全算子系统 |
| **operators** | 算子实现（过滤、质量、语言等） |
| **cache** | 内容寻址缓存，支持三哈希（数据/代码/环境） |
| **monitor** | 运行时指标收集和可配置的质量阈值 |
| **py** | 基于 PyO3 的 Python 绑定 |
| **io** | I/O 支持（JSONL、CSV、Parquet、Arrow） |
| **record** | 数据记录类型和管理 |
| **orbit** | 用于动态加载算子的插件系统 |
| **distributed** | 分布式处理支持 |
| **metrics** | 质量指标计算 |
| **log** | 结构化日志子系统 |
| **errors** | 错误类型和处理 |

</div>

### 🚀 核心特性

#### 🔍 管道处理
- 通过可配置算子进行顺序处理
- 基于 DAG 的执行，支持拓扑排序
- 使用三哈希的内容寻址缓存
- 支持增量处理

#### 📊 质量评估
- 多指标文本质量评分（ASCII 比例、非打印字符、重复度）
- 使用内置词典的毒性检测
- 基于脚本分析的语言检测（en、zh、ar、ru）
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

#### 🎲 采样与增强
- 随机采样用于数据集缩减
- Top-k 采样用于质量选择
- 基于同义词的文本增强
- 噪声注入用于数据多样性

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
    json!({"operator": "quality.filter", "config": {"min": 0.5}}),
];

let pipeline = ZiCPipelineBuilder::with_defaults()
    .build_from_config(&steps)
    .expect("合法的管道配置");

pipeline.run(records).expect("管道执行成功");
```

### Python

```python
import zi_core

# 工具函数
zi_core.compute_simhash("hello world")
zi_core.detect_language("hola")        # 返回 (语言, 置信度)
zi_core.redact_pii("email: test@example.com")
zi_core.normalize_text("  Hello   WORLD  ")
zi_core.quality_score("高质量文本")
zi_core.toxicity_score("不良内容")
zi_core.generate_prometheus_metrics()  # 返回 Prometheus 格式字符串
zi_core.version_info()                 # 返回包含版本信息的字典
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
full = ["parquet", "csv", "parallel"]
parquet = ["arrow2/io_parquet"]
csv = ["arrow2/io_csv", "dep:csv"]
parallel = ["rayon"]
pyo3 = ["pyo3/extension-module"]
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

<h2 align="center">❓ 常见问题</h2>

**Q: 如何添加新算子？**
A: 实现 `ZiCOperator` trait 并通过算子注册表注册。

**Q: 如何启用并行执行？**
A: 启用 `parallel` 特性标志并配置 DAG 调度器进行并行执行。

**Q: 如何配置质量门控？**
A: 在管道配置的 `monitor` 部分设置质量阈值。

**Q: 如何使用内容寻址缓存？**
A: 在管道配置中启用缓存，Zi 基于三哈希自动处理缓存。

**Q: 如何使用 Python 扩展算子？**
A: 使用 PyO3 绑定创建与管道集成的自定义算子。

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
