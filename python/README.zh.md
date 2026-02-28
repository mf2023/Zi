<div align="center">

<h1 style="display: flex; flex-direction: column; align-items: center; gap: 12px; margin-bottom: 8px;">
  <span style="display: flex; align-items: center; gap: 12px;">Zi</span>
  <span style="font-size: 0.6em; color: #666; font-weight: normal;">Zi Python 库</span>
</h1>

[English](README.md) | 简体中文

[帮助文档](https://mf2023.github.io/zi/zix/) | [更新日志](../CHANGELOG.md) | [安全策略](../SECURITY.md) | [贡献指南](../CONTRIBUTING.md) | [行为准则](../CODE_OF_CONDUCT.md)

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

**Zi** — 统一的 LLM 数据集处理引擎，提供 Python 绑定。专为高性能数据质量评估、清洗、转换、采样和增强而构建。

</div>

<h2 align="center">🏗️ 核心架构</h2>

### 📐 模块化设计

Zi 采用高度模块化的架构，拥有 12 个核心模块，支持按需组合和无缝扩展：

<div align="center">

| 模块 | 描述 | Python 支持 |
|:--------|:------------|:---------------|
| **record** | 记录管理和批处理 | ✅ 完整 |
| **pipeline** | 顺序/并行/条件处理 | ✅ 完整 |
| **dag** | 基于 DAG 的拓扑排序执行 | ✅ 完整 |
| **operator** | 类型安全的 trait 算子系统 | ✅ 完整 |
| **operators** | 90+ 算子实现 | ✅ 完整 |
| **ingest** | 数据摄取（JSONL/JSON/CSV/Parquet） | ✅ 完整 |
| **export** | 数据导出（压缩、分片） | ✅ 完整 |
| **inspect** | 数据分析、统计、差异对比 | ✅ 完整 |
| **enrich** | 数据合成、增强、标注 | ✅ 完整 |
| **dsl** | DSL 解析器（YAML/JSON 配置） | ✅ 完整 |
| **orbit** | 动态算子插件系统 | ✅ 完整 |
| **distributed** | 分布式处理支持 | ✅ 完整 |

</div>

### 🚀 核心特性

#### 📊 全面的数据处理
- 90+ 内置算子，支持过滤、转换和增强
- 支持多种数据格式（JSON、JSONL、CSV、Parquet）
- 大文件流式读写
- 压缩支持（Gzip、Zstd）

#### 🔍 质量评估
- 多指标文本质量评分（ASCII 比例、熵、可读性）
- 基于内置词典的毒性检测
- 基于脚本分析的语言检测
- 可配置的质量阈值和过滤

#### 🤖 LLM 专用算子
- Token 计数（中英文混合估算）
- 对话格式转换（ChatML、ShareGPT、Alpaca、OpenAI）
- 上下文长度过滤/截断/分割
- QA 对提取（Markdown、编号、自动检测）
- 指令微调数据格式化

#### 📝 去重
- 基于 SimHash 的近似重复检测
- 基于 MinHash 的相似度估计
- 语义去重支持

#### 🔬 数据检查
- 统计分析（数值、字符串、文本统计）
- 异常检测数据画像
- 数据差异对比分析
- 直方图和百分位数分布分析

<h2 align="center">🛠️ 安装与环境</h2>

### 环境要求

- **Python**: 3.8+（Windows ARM64 需要 3.11+）
- **pip**: 最新版本
- **平台**: Linux、macOS、Windows

### 快速安装

安装 Zi Python 包：

```bash
pip install zix
```

或添加到 `requirements.txt`：

```
zix==0.1.0
```

### 从源码构建

```bash
# 克隆仓库
git clone https://github.com/mf2023/Zi.git
cd Zi

# 安装 maturin
pip install maturin

# 构建并安装
maturin develop --features pyo3
```

<h2 align="center">⚡ 快速开始</h2>

### 基本使用

```python
from zix import ZiRecordPy, ZiOperatorPy, ZiPipelineBuilderPy

# 创建记录
records = [
    ZiRecordPy(id="1", payload='{"text": "Hello world"}'),
    ZiRecordPy(id="2", payload='{"text": "你好世界"}'),
]

# 使用单个算子
filter_op = ZiOperatorPy("filter.contains", '{"path": "payload.text", "value": "Hello"}')
filtered = filter_op.apply(records)
print(f"过滤后: {len(filtered)} 条记录")
```

### 管道处理

```python
from zix import ZiRecordPy, ZiPipelineBuilderPy

# 创建记录
records = [
    ZiRecordPy(id="1", payload='{"text": "Hello world"}'),
    ZiRecordPy(id="2", payload='{"text": "你好世界"}'),
]

# 构建包含多个算子的管道
pipeline = (ZiPipelineBuilderPy()
    .add_operator("lang.detect", '{"path": "payload.text"}')
    .add_operator("quality.score", '{"path": "payload.text"}')
    .add_operator("llm.token_count", '{"text_field": "payload.text"}')
    .add_operator("quality.filter", '{"min": 0.5}')
    .build())

# 执行管道
result = pipeline.run(records)
print(f"处理后: {len(result)} 条记录")
```

### 文本处理

```python
from zix import ZiTextProcessor

processor = ZiTextProcessor()

# 语言检测
lang, conf = processor.detect_language("Hello, world!")
print(f"语言: {lang}, 置信度: {conf}")

# 质量评分
quality = processor.quality_score("This is a well-written text.")
print(f"质量分数: {quality}")

# Token 计数
tokens = processor.count_tokens("Hello world", "cl100k_base")
print(f"Token 数量: {tokens}")

# PII 脱敏
redacted = processor.redact_pii("My email is test@example.com")
print(f"脱敏后: {redacted}")
```

### 数据摄取与导出

```python
from zix import ZiStreamReader, ZiStreamWriter

# 从文件读取数据
reader = ZiStreamReader("data.jsonl", batch_size=10000)
batch = reader.read_all()

# 处理数据...

# 导出数据
writer = ZiStreamWriter("output.jsonl")
stats = writer.write(batch)
print(f"已写入: {stats.records_written} 条记录")
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
      
  - operator: quality.filter
    config:
      min: 0.5
```

```python
from zix import ZiDSLParser, ZiDSLCompiler

# 解析 DSL 配置
parser = ZiDSLParser()
program = parser.parse_file("pipeline.yaml")

# 编译为管道
compiler = ZiDSLCompiler()
pipeline = compiler.compile(program)

# 执行
result = pipeline.run(records)
```

<h2 align="center">📋 可用算子</h2>

### 过滤算子 (filter.*)

| 算子 | 描述 |
|:---------|:------------|
| `filter.equals` | 字段相等过滤 |
| `filter.not_equals` | 字段不等过滤 |
| `filter.in` / `filter.not_in` | 包含/排除过滤 |
| `filter.contains` | 字符串包含过滤 |
| `filter.regex` | 正则表达式过滤 |
| `filter.range` | 数值范围过滤 |
| `filter.exists` / `filter.not_exists` | 字段存在检查 |

### 质量算子 (quality.*)

| 算子 | 描述 |
|:---------|:------------|
| `quality.score` | 文本质量评分 |
| `quality.filter` | 质量阈值过滤 |
| `quality.toxicity` | 毒性检测 |

### 去重算子 (dedup.*)

| 算子 | 描述 |
|:---------|:------------|
| `dedup.simhash` | SimHash 去重 |
| `dedup.minhash` | MinHash 去重 |
| `dedup.semantic` | 语义去重 |

### LLM 算子 (llm.*)

| 算子 | 描述 |
|:---------|:------------|
| `llm.token_count` | Token 计数 |
| `llm.conversation_format` | 对话格式转换 |
| `llm.context_length` | 上下文长度过滤 |
| `llm.qa_extract` | QA 对提取 |
| `llm.instruction_format` | 指令格式化 |

### 合并算子 (merge.*)

| 算子 | 描述 |
|:---------|:------------|
| `merge.concat` | 数据集拼接 |
| `merge.batch` | 批量合并记录 |
| `merge.union` | 去重合并 |
| `merge.intersect` | 数据集交集 |
| `merge.difference` | 数据集差集 |
| `merge.zip` | 字段合并 |

### 分割算子 (split.*)

| 算子 | 描述 |
|:---------|:------------|
| `split.random` | 随机分割（训练/验证/测试） |
| `split.stratified` | 分层分割 |
| `split.sequential` | 顺序分割 |
| `split.kfold` | K 折分割 |
| `split.chunk` | 分块分割 |

### 字段算子 (field.*)

| 算子 | 描述 |
|:---------|:------------|
| `field.select` | 选择字段 |
| `field.rename` | 重命名字段 |
| `field.drop` | 删除字段 |
| `field.copy` | 复制字段 |
| `field.move` | 移动字段 |
| `field.flatten` | 展平嵌套字段 |

### 转换算子 (transform.*)

| 算子 | 描述 |
|:---------|:------------|
| `transform.normalize` | 文本标准化 |
| `transform.map` | 字段值映射 |
| `transform.template` | 模板渲染 |
| `transform.chain` | 链式转换 |

### 采样算子 (sample.*)

| 算子 | 描述 |
|:---------|:------------|
| `sample.random` | 随机采样 |
| `sample.top` | Top-K 采样 |
| `sample.balanced` | 平衡采样 |
| `sample.stratified` | 分层采样 |

<h2 align="center">🔧 配置</h2>

### 环境变量

| 变量 | 描述 | 默认值 |
|:---------|:------------|:--------|
| `ZI_LOG_LEVEL` | 日志级别 | INFO |
| `ZI_BATCH_SIZE` | 默认批次大小 | 10000 |

### 特性标志

从源码构建时，可以启用特定特性：

```bash
# 构建所有特性
maturin develop --features pyo3

# 构建特定特性
maturin develop --features "pyo3,parquet,csv"
```

<h2 align="center">🧪 开发与测试</h2>

### 运行测试

```bash
# 安装开发依赖
pip install -e .

# 运行 Python 测试
python -m pytest tests/python/

# 运行特定测试模块
python -m pytest tests/python/test_core.py
```

<h2 align="center">❓ 常见问题</h2>

**Q: 如何在项目中使用 Zi？**
A: 通过 `pip install zix` 安装，然后导入所需的类。

**Q: 有哪些算子可用？**
A: 90+ 算子，包括过滤、转换、质量、token、去重等。

**Q: 可以与 pandas 配合使用吗？**
A: 可以！将 pandas DataFrame 转换为 `ZiRecordPy` 对象，然后使用算子处理。

**Q: 如何处理大文件？**
A: 使用 `ZiStreamReader` 进行流式批处理。

**Q: 如何添加自定义算子？**
A: 在 Rust 中实现 `ZiOperator` trait 并通过算子注册表注册。

**Q: 支持哪些 Python 版本？**
A: 支持 Python 3.8 及以上版本。注意：Windows ARM64 需要 Python 3.11+。

<h2 align="center">🌏 社区与引用</h2>

- 欢迎提交 Issue 和 PR！
- Gitee: https://gitee.com/dunimd/zi.git
- GitHub: https://github.com/mf2023/Zi.git

<div align="center">

## 📄 许可证与开源协议

### 🏛️ 项目许可证

<p align="center">
  <a href="../LICENSE">
    <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="Apache License 2.0">
  </a>
</p>

本项目使用 **Apache License 2.0** 开源协议，详见 [LICENSE](../LICENSE) 文件。

### 📋 依赖包开源协议

| 📦 包 | 📜 许可证 |
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
