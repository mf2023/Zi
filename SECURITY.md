<div align="center">

<h1 style="display: flex; flex-direction: column; align-items: center; gap: 12px; margin-bottom: 8px;">
  <span style="display: flex; align-items: center; gap: 12px;">Zi</span>
  <span style="font-size: 0.6em; color: #666; font-weight: normal;">Security Policy</span>
</h1>

</div>

The Zi project takes security seriously. This document outlines our security policy, including supported versions, how to report vulnerabilities, and our disclosure process.

## Supported Versions

The following versions of Zi are currently supported with security updates:

| Version | Supported          | Status                |
| ------- | ------------------ | --------------------- |
| 0.1.x   | :white_check_mark: | Current stable series |
| < 0.1.0 | :x:                | No longer supported   |

We provide security updates for the latest minor version in each major version series. Users are encouraged to upgrade to the latest version to receive security patches.

## Reporting a Vulnerability

If you discover a security vulnerability in Zi, please report it to us as soon as possible. We appreciate your efforts to responsibly disclose your findings.

### How to Report

**Please do not report security vulnerabilities through public GitHub issues or Gitee issues.**

Instead, please report security vulnerabilities via:

📧 **Email**: dunimd@outlook.com

For general questions and non-security issues, please use:
- **Gitee Issues** (Primary): https://gitee.com/dunimd/zi/issues
- **GitHub Issues** (Mirror): https://github.com/mf2023/Zi/issues

Please include the following information in your report:

- **Description**: A clear and concise description of the vulnerability
- **Impact**: What kind of vulnerability is it and what impact could it have
- **Affected Versions**: Which versions of Zi are affected
- **Steps to Reproduce**: Detailed steps to reproduce the vulnerability
- **Proof of Concept**: If possible, include a proof-of-concept or exploit code
- **Suggested Fix**: If you have suggestions for how to fix the vulnerability
- **Your Contact Information**: How we can reach you for clarifications (optional)

### What to Expect

When you submit a security report, you can expect the following:

1. **Acknowledgment**: We will acknowledge receipt of your report within 48 hours
2. **Initial Assessment**: We will provide an initial assessment within 5 business days
3. **Investigation**: We will investigate the vulnerability and determine its impact
4. **Fix Development**: If confirmed, we will work on a fix and may reach out for additional information
5. **Disclosure**: We will coordinate with you on the disclosure timeline

### Response Time

Our target response times are:

| Severity | Initial Response | Fix Timeline |
|----------|-----------------|--------------|
| Critical | 24 hours | 7 days |
| High | 48 hours | 14 days |
| Medium | 5 business days | 30 days |
| Low | 10 business days | 60 days |

## Security Considerations

### Data Processing Security

Zi processes large datasets for LLM training pipelines. Consider the following security aspects:

#### Input Validation
- Always validate input data sources before processing
- Be cautious when processing untrusted data files (JSONL, JSON, CSV, Parquet)
- Use appropriate file size limits to prevent DoS attacks

#### PII Handling
- Zi provides PII redaction operators (`pii.redact`)
- Configure appropriate PII patterns for your use case
- Review redacted output before using in production

#### File System Operations
- Zi performs file I/O operations for data ingestion and export
- Ensure proper file permissions on input/output directories
- Use atomic write operations for critical data exports

### Plugin System Security

Zi supports dynamic operator loading via shared libraries:

#### Plugin Loading Risks
- Only load plugins from trusted sources
- Verify plugin integrity before loading
- Plugins run with the same privileges as the host application

#### Plugin Best Practices
- Use code signing for plugin verification
- Implement plugin sandboxing when possible
- Audit plugin code before deployment

### DSL Configuration Security

Zi supports YAML/JSON-based pipeline configuration:

#### Configuration Risks
- Validate configuration files from untrusted sources
- Be cautious with template rendering in configurations
- Avoid executing arbitrary code through configuration

#### Safe Configuration Practices
- Use allowlists for permitted operators
- Validate all configuration parameters
- Implement configuration schema validation

### Network Security

When using distributed processing features:

#### Distributed Processing
- Use TLS for inter-node communication
- Implement proper authentication between nodes
- Validate all incoming data from remote sources

### Memory Safety

Zi is written in Rust, which provides memory safety guarantees:

- No buffer overflows by design
- No use-after-free vulnerabilities
- No null pointer dereferences

However, unsafe code blocks and external dependencies should be audited regularly.

## Security Best Practices

When using Zi in your applications:

### 1. Keep Dependencies Updated

Regularly update Zi and its dependencies to receive security patches:

```bash
cargo update
cargo audit  # Use cargo-audit to check for known vulnerabilities
```

### 2. Use Latest Stable Version

Always use the latest stable version of Zi to ensure you have the latest security fixes.

### 3. Enable Security Features

Build Zi with appropriate features:

```bash
cargo build --release --features full
```

### 4. Validate Input Data

Always validate input data before processing:

```rust
use zix::{ZiRecord, ZiPipelineBuilder};

// Add validation operators at the start of your pipeline
let steps = vec![
    json!({"operator": "field.require", "config": {"fields": ["text"]}}),
    json!({"operator": "limit", "config": {"max_records": 1000000}}),
];
```

### 5. Secure File Handling

Follow secure file handling practices:

- Use absolute paths for file operations
- Implement file size limits
- Use atomic writes for critical data

### 6. Monitor Processing

Enable logging and monitoring:

- Log processing statistics
- Monitor for unusual patterns
- Set up alerts for processing failures

### 7. Secure Deployment

Follow secure deployment practices:

- Use container security best practices
- Implement proper access controls
- Regular security audits

## Known Security Limitations

### Current Limitations

1. **Plugin System**: Dynamic library loading introduces potential security risks. Only load trusted plugins.

2. **Large File Processing**: Processing very large files may consume significant memory. Implement appropriate resource limits.

3. **Distributed Mode**: Network communication in distributed mode should be secured with TLS.

### Security Considerations for Production

- Implement proper input validation
- Use resource limits to prevent DoS
- Enable audit logging
- Regular security audits of custom operators

## Security Updates

Security updates will be announced through:

- GitHub Security Advisories
- GitHub Releases (with security fix notes)
- CHANGELOG.md (with security-related changes marked)

## Vulnerability Disclosure Policy

### Our Commitment

- We will acknowledge receipt of vulnerability reports within 48 hours
- We will provide regular updates on our progress
- We will credit researchers who responsibly disclose vulnerabilities (unless they prefer to remain anonymous)
- We will not take legal action against researchers who follow this policy

### Disclosure Timeline

1. **Day 0**: Vulnerability reported
2. **Day 1-2**: Acknowledgment and initial assessment
3. **Day 3-14**: Investigation and fix development
4. **Day 15-30**: Testing and validation
5. **Day 30+**: Coordinated disclosure

We aim to disclose vulnerabilities within 90 days of the initial report, or sooner if a fix is available.

### Public Disclosure

We will publicly disclose vulnerabilities after:

- A fix has been developed and tested
- Affected users have had reasonable time to update
- The vulnerability has been assigned a CVE identifier (if applicable)

## Security-Related Configuration

### Environment Variables

The following environment variables affect security:

| Variable | Description | Security Impact |
|----------|-------------|-----------------|
| `RUST_LOG` | Logging level | May expose sensitive data if set to `trace` |
| `ZI_ENV` | Environment (dev/staging/prod) | Affects security defaults |

### Configuration Options

Review security-related configuration options:

- Plugin loading paths
- File I/O directories
- Resource limits
- Operator allowlists

## Third-Party Security Audits

We welcome third-party security audits. If you are conducting a security audit of Zi:

1. Please follow responsible disclosure practices
2. Contact us in advance if you plan to publish findings
3. We appreciate receiving a copy of the audit report

## Security Resources

- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [Rust Security Guidelines](https://doc.rust-lang.org/nomicon/)
- [Cargo Audit](https://github.com/RustSec/cargo-audit)
- [RustSec Advisory Database](https://rustsec.org/)

## Contact

For security-related inquiries:

- **Email**: dunimd@outlook.com
- **GPG Key**: [Available upon request]

For general questions and non-security issues, please use:

- **Gitee Issues** (Primary): https://gitee.com/dunimd/zi/issues
- **GitHub Issues** (Mirror): https://github.com/mf2023/Zi/issues
- **GitHub Discussions**: https://github.com/mf2023/Zi/discussions

## Acknowledgments

We thank the following security researchers who have responsibly disclosed vulnerabilities:

*This list will be updated as vulnerabilities are reported and fixed.*

---

**Last Updated**: 2026-02-28

**Version**: 1.0
