# Contributing to EtlOrchestrator

Thank you for considering contributing to EtlOrchestrator! This document outlines the process and guidelines for contributing to this project.

## Code of Conduct

By participating in this project, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md). Please read it before contributing.

## How Can I Contribute?

### Reporting Bugs

This section guides you through submitting a bug report for EtlOrchestrator. Following these guidelines helps maintainers understand your report, reproduce the issue, and find related reports.

* Use a clear and descriptive title for the issue
* Describe the exact steps to reproduce the problem
* Describe the behavior you observed and what you expected to see
* Include screenshots and animated GIFs if possible
* Include details about your environment (OS, .NET version, etc.)

### Suggesting Enhancements

This section guides you through submitting an enhancement suggestion for EtlOrchestrator, including completely new features and minor improvements to existing functionality.

* Use a clear and descriptive title for the issue
* Provide a step-by-step description of the suggested enhancement
* Describe the current behavior and explain the behavior you expected to see
* Explain why this enhancement would be useful to most users

### Pull Requests

* Fill in the required template
* Do not include issue numbers in the PR title
* Include screenshots and animated GIFs if possible
* Follow the C# coding style
* Include unit tests for new features
* Document new code based on the project's documentation style
* End all files with a newline

## Development Process

### Setting Up Your Development Environment

1. Fork and clone the repository
2. Install dependencies:
   ```
   dotnet restore
   ```
3. Set up the database:
   ```
   dotnet ef database update
   ```
4. Run the tests:
   ```
   dotnet test
   ```

### Branch Naming Convention

* Use `feature/` prefix for new features
* Use `bugfix/` prefix for bug fixes
* Use `docs/` prefix for documentation changes
* Use `test/` prefix for test-related changes

### Commit Message Guidelines

* Use the present tense ("Add feature" not "Added feature")
* Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
* Limit the first line to 72 characters or less
* Reference issues and pull requests liberally after the first line

## Styleguides

### C# Styleguide

* Follow the [Microsoft C# Coding Conventions](https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/inside-a-program/coding-conventions)
* Use camelCase for private fields and use readonly where possible
* Use PascalCase for property names
* Use four spaces for indentation
* Use Allman style braces
* Include XML documentation for public APIs

### Testing Styleguide

* Test all public methods
* Write clear test names that explain what the test is verifying
* Follow the Arrange-Act-Assert pattern
* Keep tests independent from each other

## Additional Notes

### Issue and Pull Request Labels

This section lists the labels we use to help us track and manage issues and pull requests.

* `bug` - Issues that are bugs
* `enhancement` - Issues that are feature requests
* `documentation` - Issues or PRs related to documentation
* `good first issue` - Good for newcomers
* `help wanted` - Extra attention is needed
* `wontfix` - We don't plan to fix these issues

### Project Structure

```
EtlOrchestrator/
├── src/
│   ├── Core/                   # Contains core interfaces and models
│   │   └── EtlOrchestrator.Core/
│   ├── Infrastructure/         # Implementation of interfaces
│   │   └── EtlOrchestrator.Infrastructure/
│   └── API/                    # API endpoints (if applicable)
│       └── EtlOrchestrator.Api/
├── tests/                      # Test projects
│   ├── EtlOrchestrator.Core.Tests/
│   ├── EtlOrchestrator.Infrastructure.Tests/
│   └── EtlOrchestrator.Api.Tests/
├── docs/                       # Documentation
├── samples/                    # Sample projects
└── tools/                      # Tools and scripts
```

## Thank You!

Your contributions to open source, large or small, make projects like this possible. Thank you for taking the time to contribute. 