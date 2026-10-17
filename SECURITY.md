# Security Policy

## Supported Versions

Only the latest minor release branch is actively supported with security updates.

| Version | Supported |
| :--- | :--- |
| `1.2.x` | Yes |
| `< 1.2.0` | No |

---

## Reporting a Vulnerability

If you discover a security vulnerability in `@mayurbhusare/resilient-queue`, please disclose it responsibly. Do **not** open a public issue on GitHub.

### How to Report

1. **GitHub Security Advisory**: Submit a private vulnerability report via GitHub at [https://github.com/resilient-queue/resilient-queue/security/advisories/new](https://github.com/resilient-queue/resilient-queue/security/advisories/new).
2. **Direct Contact**: If GitHub Security Advisories are unavailable, contact the maintainer directly via GitHub or email at `bhusaremayur@gmail.com`.

### Information to Include

Please provide:
- A clear description of the vulnerability.
- Steps to reproduce the issue, including minimal reproduction code or proof of concept.
- Affected versions of `@mayurbhusare/resilient-queue`, Node.js, and Redis.
- Assessment of potential impact and exploitability.

### Response Timeline

- **Initial Response**: We aim to acknowledge receipt within 48 to 72 hours.
- **Assessment & Triage**: We will confirm the vulnerability and provide an estimated timeline for remediation.
- **Fix & Disclosure**: A patched release will be published to npm and GitHub. Public disclosure will be coordinated following release.

---

## Security Best Practices

When operating `ResilientQueue` in production:

1. **Redis Authentication & Encryption**: Always configure Redis authentication (`password` or ACL `username`/`password`) and enable TLS encryption (`tls: {}`) when transmitting data across untrusted networks or cloud VPCs.
2. **Sensitive Data in Payloads**: Job data serialized to Redis is stored in plaintext JSON. Do not pass unencrypted secrets, authentication tokens, or sensitive personal data (PII) in job payloads. Encrypt sensitive fields prior to calling `queue.enqueue()`.
3. **Cluster Key Isolation**: When using Redis Cluster, enforce single-slot hash tags (e.g. `keyPrefix: "{my-queue}"`) to prevent cross-slot execution failures.
4. **Visibility Lease Fencing**: Handlers performing irreversible external operations should inspect `job.signal` and `job.isLeaseLost` to prevent duplicate execution when network delays cause lease expiration.
