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

基于 Rust 构建的高性能数据处理引擎，专为现代机器学习工作流设计。Zi 提供统一的数据质量评估、清洗、转换、采样与增强框架，具有卓越的速度和可靠性。

</div>

## 🎯 项目概述

Zi 是一个基于 Rust 的数据处理库，实现了数据转换和质量评估的管道架构。该项目专注于提供类型安全、高效且可扩展的数据处理操作框架。

## 🏗️ 架构设计

### 核心组件

- **管道引擎**：通过可配置算子进行数据的顺序处理
- **算子框架**：基于 trait 的类型安全算子系统
- **记录处理**：支持元数据的 JSON 数据记录
- **插件系统**：可选地通过共享库动态加载自定义算子

### 命名约定

为保持公共 API 一致性，Zi 遵循严格的命名规则：

- 公共结构体 / 枚举 / Trait 统一使用 `ZiC*` 前缀（如 `ZiCRecord`、`ZiCPipeline`）。
- 公共函数及构造器统一使用 `ZiF*` 前缀（如 `ZiFNew`、`ZiFLoadJsonl`）。
- 内部帮助函数统一使用 `_` 前缀。

本文档中的示例均已按照实际代码的命名约定更新，确保开箱即用。

### 算子类别

基于实际代码库，Zi 支持以下算子类别：

#### 1. 过滤算子 (`filter.*`)
- `filter.equals` - 字段相等过滤
- `filter.not_equals` - 字段不等过滤
- `filter.any` - 任意字段匹配值
- `filter.in` - 值包含过滤
- `filter.not_in` - 值排除过滤
- `filter.exists` - 字段存在检查
- `filter.not_exists` - 字段不存在检查
- `filter.contains` - 字符串包含过滤
- `filter.contains_all` - 多字符串包含
- `filter.contains_any` - 任意字符串包含
- `filter.contains_none` - 字符串排除过滤
- `filter.length_range` - 文本长度过滤
- `filter.token_range` - 词元数量过滤
- `filter.array_contains` - 数组元素过滤
- `filter.starts_with` - 字符串前缀过滤
- `filter.ends_with` - 字符串后缀过滤
- `filter.regex` - 正则表达式过滤
- `filter.is_null` - 空值过滤
- `filter.greater_than` - 数值大于过滤
- `filter.less_than` - 数值小于过滤
- `filter.between` - 数值范围过滤
- `filter.range` - 数值范围过滤（替代）

#### 2. 质量算子 (`quality.*`)
- `quality.score` - 基于 ASCII 比例、非打印字符和重复的文本质量评分
- `quality.filter` - 质量阈值过滤
- `quality.toxicity` - 使用内置词典的毒性检测

#### 3. 语言算子 (`lang.*`)
- `lang.detect` - 基于脚本分析的语言检测（en、zh、ar、ru）
- `lang.confidence` - 语言置信度评分

#### 4. 元数据算子 (`metadata.*`)
- `metadata.enrich` - 添加元数据字段
- `metadata.rename` - 重命名元数据字段
- `metadata.remove` - 移除元数据字段
- `metadata.copy` - 复制元数据字段
- `metadata.require` - 要求元数据字段
- `metadata.extract` - 提取值到元数据
- `metadata.keep` - 仅保留指定元数据字段

#### 5. 限制算子 (`limit`)
- `limit` - 记录数量限制

#### 6. 去重算子 (`dedup.*`)
- `dedup.simhash` - 基于 SimHash 的去重
- `dedup.minhash` - 基于 MinHash 的去重
- `dedup.semantic` - 语义去重

#### 7. PII 算子 (`pii.*`)
- `pii.redact` - 支持自定义模式的 PII 编辑

#### 8. 转换算子 (`transform.*`)
- `transform.normalize` - 文本规范化

#### 9. 采样算子 (`sample.*`)
- `sample.random` - 随机采样
- `sample.top` - Top-k 采样

#### 10. 增强算子 (`augment.*`)
- `augment.synonym` - 基于同义词的文本增强
- `augment.noise` - 噪声注入增强

## 🚀 快速开始

### Rust 使用

```rust
use serde_json::json;
use Zi::pipeline::ZiCPipelineBuilder;
use Zi::record::ZiCRecord;

// 创建示例数据
let records = vec![
    ZiCRecord::ZiFNew(Some("1".into()), json!({"text": "Hello world"})),
    ZiCRecord::ZiFNew(Some("2".into()), json!({"text": "你好世界"})),
];

// 定义算子步骤（serde_json::Value 切片）
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

// 构建处理管道
let pipeline = ZiCPipelineBuilder::with_defaults()
    .build_from_config(&steps)
    .expect("合法的管道配置");

// 处理数据
let processed = pipeline
    .run(records)
    .expect("管道执行成功");
```

### 配置格式

算子使用 JSON 配置，结构如下：

```json
[
  {
    "operator": "operator.name",
    "config": {
      // 算子特定配置
    }
  }
]
```

### 字段路径语法

字段路径使用点表示法导航 JSON 结构：
- `payload.text` - 访问有效负载中的文本字段
- `metadata.field` - 访问元数据中的字段
- `payload.nested.field` - 访问嵌套字段

## 📊 性能特性

Zi 使用 Rust 构建以获得最佳性能：

- **零拷贝操作**（可能情况下）
- **内存安全处理** 使用 Rust 所有权系统
- **高效 JSON 处理** 使用 serde_json
- **流式支持** 用于大数据集
- **流式处理** 支持大规模数据集

## 🏗️ 从源码构建

### 前置要求
- Rust 1.70+
- Cargo

### 构建命令
```bash
# 克隆仓库
git clone https://github.com/mf2023/Zi.git
cd Zi

# 发布模式构建
cargo build --release

# 运行测试
cargo test

# 运行基准测试
cargo bench
```

## 📁 项目结构

```
src/
├── lib.rs              # 库入口点
├── errors.rs           # 错误处理类型
├── io.rs               # I/O 工具
├── metrics.rs          # 质量指标
├── operator.rs         # 核心算子 trait
├── pipeline.rs         # 管道引擎
├── record.rs           # 数据记录类型
└── operators/          # 算子实现
    ├── augment.rs      # 数据增强算子
    ├── dedup.rs        # 去重算子
    ├── filter.rs       # 过滤算子
    ├── lang.rs         # 语言处理算子
    ├── limit.rs        # 限制算子
    ├── metadata.rs     # 元数据算子
    ├── mod.rs          # 算子模块
    ├── pii.rs          # PII 处理算子
    ├── quality.rs      # 质量评估算子
    ├── sample.rs       # 采样算子
    └── transform.rs    # 文本转换算子
```

## 🔧 插件系统

Zi 支持通过共享库动态加载自定义算子：

```rust
let mut builder = ZiCPipelineBuilder::with_defaults();
builder.load_plugin("path/to/plugin.so")?;
```

插件必须实现 `zi_register_operators` 函数并向构建器注册其算子。

## 🎯 使用场景

### 数据质量评估
- 基于多指标的文本质量评分
- 语言检测和置信度评分
- 内容审核的毒性检测

### 数据过滤
- 基于字段值的复杂过滤
- 正则表达式匹配
- 基于范围的数值过滤

### 数据转换
- 元数据丰富和操作
- 文本规范化
- PII 编辑

### 数据去重
- 基于 SimHash 的近重复检测
- 基于 MinHash 的相似性检测
- 语义去重

## 🔮 未来开发

### 计划功能
- 超越基本脚本检测的额外语言支持
- 高级质量指标
- 基于机器学习的算子
- 分布式处理支持
- 管道配置的 Web UI

## 📄 许可证

本项目采用 Apache License 2.0 许可 — 详见 [LICENSE](LICENSE)。

---

## 🌏 社区与引用
- 欢迎提交问题与拉取请求！
- GitHub: https://github.com/mf2023/Zi.git
- Gitee: https://gitee.com/dunimd/zi.git

## 🙏 致谢

使用优秀的 Rust 生态系统工具构建：
- [Serde](https://serde.rs/) 用于 JSON 处理
- [Regex](https://docs.rs/regex/) 用于模式匹配
- [Arrow2](https://github.com/jorgecarleitao/arrow2) 用于列式数据处理
- [Libloading](https://docs.rs/libloading/) 用于插件支持

<h3 align="center">直觉导航数据深处 · 共情赋予智能形态</h3>