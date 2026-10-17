# Contributing to Resilient Queue

Thank you for your interest in contributing to `@mayurbhusare/resilient-queue`. We welcome bug fixes, documentation improvements, test coverage enhancements, and feature proposals.

---

## Active Development & Contributions

This project is actively maintained. We welcome pull requests and issues that address bugs, reliability enhancements, performance optimizations, feature additions, and documentation improvements. For substantial architectural changes or new features, opening an issue first is recommended to discuss the design.

---

## Development Prerequisites

- **Node.js**: Node.js 18 or later.
- **Redis Server**: A running Redis instance at `127.0.0.1:6379` (Redis 6.2+ recommended; Redis 5.0+ supported).
- **Git**: Git 2.20 or later.

---

## Getting Started

### 1. Fork and Clone

Fork the repository on GitHub, then clone your local fork:

```bash
git clone https://github.com/bhusareMayur/resilient-queue.git
cd resilient-queue
```

### 2. Install Dependencies

Install development dependencies:

```bash
npm install
```

### 3. Create a Feature Branch

Create a branch with a descriptive name:

```bash
# For a bug fix
git checkout -b fix/heartbeat-lock-renewal

# For documentation
git checkout -b docs/clarify-cluster-hash-tags

# For a new feature
git checkout -b feat/custom-serializer
```

---

## Running Tests

All unit and integration tests are written in Jest and run in-band to prevent concurrent Redis state conflicts across suites.

Ensure your local Redis server is running:

```bash
# Verify Redis is responsive
redis-cli ping
# Output: PONG
```

Run the test suite:

```bash
npm test
```

> [!NOTE]
> The test suite flushes Redis during execution (`redis.flushall()`). Do not point `npm test` against a production or shared Redis database.

All tests must pass before submitting a pull request.

---

## Code Guidelines

1. **Lightweight & Dependency-Minimal**: The library relies exclusively on `ioredis` and `uuid`. Avoid introducing additional third-party dependencies unless strictly necessary.
2. **CommonJS**: The package uses CommonJS (`require` and `module.exports`) to maintain broad Node.js ecosystem compatibility.
3. **TypeScript Definitions**: If you modify public API signatures, options, or data structures, update `index.d.ts` accordingly.
4. **Documentation**: Update relevant guides in `docs/` and `README.md` for any user-facing API or configuration changes.
5. **No Breaking Changes**: Maintain backward compatibility whenever feasible. If a breaking change is unavoidable, clearly highlight it in the PR description.

---

## Commit Message Guidelines

We follow Conventional Commits formatting:

```text
<type>(<scope>): <short description>
```

Common types:
- `feat`: A new feature or capability.
- `fix`: A bug fix.
- `docs`: Documentation updates.
- `refactor`: Code reorganization without functional changes.
- `test`: Adding or updating test cases.
- `chore`: Build tools, package metadata, or dependency updates.

Example:
```bash
git commit -m "fix(stalled): fence lock renewal when lease is lost"
```

---

## Pull Request Process

1. Push your branch to your fork on GitHub:
   ```bash
   git push origin feat/your-feature-name
   ```
2. Open a Pull Request against the `main` branch.
3. Fill out the PR description with:
   - What was changed and why.
   - Any edge cases considered.
   - Confirmation that `npm test` passed locally.
4. Maintainers will review your submission and provide feedback.

---

## Code of Conduct

All contributors and maintainers are expected to abide by our [Code of Conduct](./CODE_OF_CONDUCT.md).

---

## Security

To report security vulnerabilities, please refer to our [Security Policy](./SECURITY.md).