# Contributing to yggdrasil

Thank you for your interest in contributing! This document explains how to contribute
and the legal requirements for doing so.

## Quick Start

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Sign your commits: `git commit -s -m "Description"`
5. Open a pull request
6. Sign the CLA (first-time contributors only)

## Developer Certificate of Origin (DCO)

All commits must include a `Signed-off-by` line certifying you have the right to submit the code:

```bash
git commit -s -m "Add feature X"
```

This adds:
```
Signed-off-by: Your Name <your.email@example.com>
```

To configure git for signing:
```bash
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
```

The DCO is a lightweight way to certify that you wrote or have the right to submit the code.
See [developercertificate.org](https://developercertificate.org) for the full text.

## Contributor License Agreement (CLA)

First-time contributors will be asked to sign our CLA via the CLA Assistant bot.
This is a one-time requirement.

**Why a CLA?**

The CLA grants replikativ the right to:
- Distribute your contributions under Apache-2.0 (the project's open source license)
- Offer your contributions under commercial license terms (for customers who prefer them)

**Our commitment to you:**
- The project will **always** remain available under an OSI-approved open source license
- Commercial licensing is **in addition to**, not instead of, the open source license
- Both versions receive the same contributions

## Code Guidelines

### Clojure Style

- Follow existing code conventions in the project
- Use `clj-kondo` for linting
- Format with `cljfmt` if available
- Write cross-platform code (`.cljc`) where possible

### Commit Messages

- Use present tense ("Add feature" not "Added feature")
- Keep first line under 72 characters
- Reference issues when applicable: "Fix #123: Description"

### Testing

Run tests before submitting:
```bash
clj -M:test
```

Add tests for new functionality when possible.

## Pull Request Process

1. **Create focused PRs** - One feature/fix per PR
2. **Write clear descriptions** - Explain what and why
3. **Respond to feedback** - We may request changes
4. **Be patient** - Reviews may take a few days

## Getting Help

- Open an issue for bugs or feature requests
- Discussions for questions and ideas
- Tag maintainers if urgent

## License

By contributing, you agree that your contributions will be licensed under Apache-2.0
and may be included in commercially licensed distributions per the CLA.
