<div align="center">

<h1 style="display: flex; flex-direction: column; align-items: center; gap: 12px; margin-bottom: 8px;">
  <span style="display: flex; align-items: center; gap: 12px;">Zi</span>
  <span style="font-size: 0.6em; color: #666; font-weight: normal;">Contributing Guide</span>
</h1>

</div>

First off, thank you for considering contributing to Zi! It's people like you that make Zi such a great tool.

This document provides guidelines and instructions for contributing to the Zi project. By participating, you are expected to uphold this code and help us maintain a welcoming and productive community.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [How Can I Contribute?](#how-can-i-contribute)
  - [Reporting Bugs](#reporting-bugs)
  - [Suggesting Enhancements](#suggesting-enhancements)
  - [Pull Requests](#pull-requests)
- [Development Guidelines](#development-guidelines)
  - [Setting Up Development Environment](#setting-up-development-environment)
  - [Building the Project](#building-the-project)
  - [Running Tests](#running-tests)
  - [Code Style](#code-style)
  - [Commit Messages](#commit-messages)
- [Project Structure](#project-structure)
- [Community](#community)
- [License](#license)

## Code of Conduct

This project and everyone participating in it is governed by our [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to the project maintainers.

## Getting Started

- Make sure you have a [GitHub account](https://github.com/signup/free)
- Fork the repository on GitHub
- Set up your development environment (see [Development Guidelines](#development-guidelines))
- Familiarize yourself with the [project structure](#project-structure)

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check the [existing issues](https://github.com/mf2023/Zi/issues) to see if the problem has already been reported. When you are creating a bug report, please include as many details as possible:

#### Before Submitting a Bug Report

- **Check the [documentation](https://mf2023.github.io/zi/zix/)** for information that might help
- **Check if the bug has already been reported** by searching on GitHub under [Issues](https://github.com/mf2023/Zi/issues)
- **Determine which repository the problem should be reported in**

#### How to Submit a Good Bug Report

Bugs are tracked as [GitHub issues](https://github.com/mf2023/Zi/issues). Create an issue and provide the following information:

- **Use a clear and descriptive title** for the issue to identify the problem
- **Describe the exact steps to reproduce the problem** in as many details as possible
- **Provide specific examples to demonstrate the steps**. Include links to files or GitHub projects, or copy/pasteable snippets
- **Describe the behavior you observed** and why it's a problem
- **Explain which behavior you expected to see instead and why**
- **Include code samples and screenshots** which show you demonstrating the problem

**Example:**

```markdown
**Description:**
Pipeline fails when processing large JSONL files with nested fields

**Steps to Reproduce:**
1. Create a ZiPipeline with field.flatten operator
2. Process a JSONL file with deeply nested objects
3. Observe the error

**Expected Behavior:**
Nested fields should be flattened correctly

**Actual Behavior:**
Error: "maximum recursion depth exceeded"

**Environment:**
- OS: Ubuntu 22.04
- Zi Version: 0.1.0
- Rust Version: 1.75.0
```

### Suggesting Enhancements

Enhancement suggestions are tracked as [GitHub issues](https://github.com/mf2023/Zi/issues). When creating an enhancement suggestion, please include:

- **Use a clear and descriptive title** for the issue to identify the suggestion
- **Provide a step-by-step description of the suggested enhancement** in as many details as possible
- **Provide specific examples to demonstrate the enhancement**
- **Explain why this enhancement would be useful** to most Zi users
- **List some other data processing frameworks where this enhancement exists**

### Pull Requests

1. Fork the repo and create your branch from `master`
2. If you've added code that should be tested, add tests
3. If you've changed APIs, update the documentation
4. Ensure the test suite passes
5. Make sure your code follows the style guidelines
6. Issue that pull request!

#### Pull Request Process

1. Update the [CHANGELOG.md](CHANGELOG.md) with details of changes if applicable
2. Update the [README.md](README.md) with details of changes to the interface if applicable
3. The PR will be merged once you have the sign-off of at least one maintainer

## Development Guidelines

### Setting Up Development Environment

#### Prerequisites

- **Rust** (1.70+): [Install Rust](https://www.rust-lang.org/tools/install)
- **Python** (3.8+ for Python bindings development): [Install Python](https://www.python.org/downloads/)
- **Cargo**: Comes with Rust installation

#### Clone the Repository

```bash
git clone https://github.com/mf2023/Zi.git
cd Zi
```

#### Install Python Dependencies (for Python bindings)

```bash
pip install maturin
```

### Building the Project

#### Build Rust Library

```bash
# Build with all features
cargo build --release

# Build with specific features
cargo build --release --features "parquet,csv,parallel"

# Build with Python bindings
cargo build --release --features pyo3
```

#### Build Python Wheels

```bash
maturin build --release --features pyo3
```

### Running Tests

#### Rust Tests

```bash
# Run all tests
cargo test

# Run tests with all features
cargo test --all-features

# Run tests for a specific module
cargo test operator::
```

#### Python Tests

```bash
pip install -e .
python -m pytest tests/python/
```

### Code Style

#### Rust Code Style

We follow the official [Rust Style Guide](https://doc.rust-lang.org/style-guide/) and use `rustfmt` for formatting:

```bash
# Format code
cargo fmt

# Check formatting without making changes
cargo fmt -- --check

# Run clippy for linting
cargo clippy --all-features
```

#### Python Code Style

For Python bindings, we follow [PEP 8](https://www.python.org/dev/peps/pep-0008/):

```bash
# Format with black
black python/

# Lint with flake8
flake8 python/
```

#### Documentation

- All public APIs must have documentation comments
- Use `cargo doc` to generate documentation
- Documentation should include examples where appropriate

### Commit Messages

This project uses **date-based commit messages** in the format `YYYY.MM.DD`:

```
2026.02.28
```

#### Format

- Use the **current date** in `YYYY.MM.DD` format
- No additional description
- No body or footer

#### Examples

```bash
# Good
git commit -m "2026.02.28"

# Bad - don't use conventional commits or descriptions
git commit -m "feat(operator): add new filter operator"
git commit -m "fix bug in pipeline"
```

#### Why Date-Based?

- **Simple**: No need to think about commit message format
- **Clear timeline**: Easy to see when changes were made
- **Consistent**: All commits follow the same pattern
- **Changelog**: Detailed changes are tracked in [CHANGELOG.md](CHANGELOG.md)

#### Tracking Changes

Since commit messages are minimal, detailed change information is maintained in:

- **[CHANGELOG.md](CHANGELOG.md)**: Version history and release notes
- **GitHub Issues/PRs**: Detailed discussion and context
- **Code comments**: Inline documentation for complex changes

## Project Structure

```
Zi/
├── src/                    # Rust source code
│   ├── operators/         # Operator implementations (filter, quality, lang, etc.)
│   ├── pipeline.rs        # Pipeline execution engine
│   ├── dag.rs             # DAG-based execution
│   ├── operator.rs        # Operator trait definition
│   ├── record.rs          # ZiRecord data structure
│   ├── ingest/            # Data ingestion (JSONL, JSON, CSV, Parquet)
│   ├── export/            # Data export with compression and sharding
│   ├── domain/            # Domain types (text, image, audio, video)
│   ├── dsl/               # DSL parser (YAML/JSON config)
│   ├── orbit/             # Plugin system
│   ├── distributed.rs     # Distributed processing
│   └── context.rs         # DMSC integration
├── python/                # Python bindings
│   └── README.md         # Python package documentation
├── tests/                 # Test files
│   ├── rust/             # Rust tests
│   └── python/           # Python tests
├── .github/               # GitHub configuration
│   └── workflows/        # CI/CD workflows
├── Cargo.toml            # Rust package configuration
├── README.md             # Project readme
├── CHANGELOG.md          # Version changelog
└── LICENSE               # Apache 2.0 License
```

## Operator Contribution Guidelines

### Adding a New Operator

1. Create a new file under `src/operators/` or add to an existing category
2. Implement the `ZiOperator` trait
3. Register the operator in the operator registry
4. Add Python bindings if applicable
5. Add documentation and examples
6. Add unit tests

### Operator Requirements

Each operator should:
- Have a clear purpose and scope
- Follow the existing error handling patterns
- Include comprehensive documentation
- Provide Python bindings (if applicable)
- Include unit tests

### Example Operator

```rust
use crate::{ZiOperator, ZiRecord, ZiError, ZiResult};

/// A simple filter operator that checks field equality
pub struct ZiFilterEquals {
    field: String,
    value: serde_json::Value,
}

impl ZiFilterEquals {
    pub fn new(field: String, value: serde_json::Value) -> Self {
        Self { field, value }
    }
}

impl ZiOperator for ZiFilterEquals {
    fn run(&self, batch: Vec<ZiRecord>) -> ZiResult<Vec<ZiRecord>> {
        Ok(batch
            .into_iter()
            .filter(|record| {
                record.get(&self.field)
                    .map(|v| v == &self.value)
                    .unwrap_or(false)
            })
            .collect())
    }
}
```

## Community

### Communication Channels

- **Gitee Issues** (Primary): Bug reports, feature requests, and general discussion - https://gitee.com/dunimd/zi/issues
- **GitHub Issues** (Mirror): Alternative access - https://github.com/mf2023/Zi/issues
- **GitHub Discussions**: For questions and community interaction

### Repositories

- **Gitee** (Primary): https://gitee.com/dunimd/zi.git
- **GitHub** (Mirror): https://github.com/mf2023/Zi.git

### Recognition

Contributors will be recognized in our [CHANGELOG.md](CHANGELOG.md) and release notes.

## License

By contributing to Zi, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).

---

Thank you for contributing to Zi! 🎉
