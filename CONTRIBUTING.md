# Contributing to RollingRaft

Thank you for your interest in contributing to RollingRaft! This document provides guidelines and information for contributors.

## Table of Contents

* [Code of Conduct](#code-of-conduct)
* [Getting Started](#getting-started)
* [Development Workflow](#development-workflow)
* [Code Style](#code-style)
* [Commit Message Guidelines](#commit-message-guidelines)
* [Testing](#testing)
* [Pull Request Process](#pull-request-process)
* [Release Process](#release-process)

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/version/2/0/code_of_conduct/).
By participating, you are expected to uphold this code.

## Getting Started

### Prerequisites

* C++20 compatible compiler (GCC 10+, Clang 12+, MSVC 2019+)
* CMake 3.11+
* Git 2.20+

### Build the Project

```bash
# Clone with submodules
git clone --recursive https://github.com/Aqiu16717/rollingraft.git
cd rollingraft

# Create build directory
mkdir build && cd build

# Configure and build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### Run Tests

```bash
# Run unit tests
./build/tests/unit_tests

# Run integration tests
./build/tests/integration_tests
```

## Development Workflow

### 1. Fork and Branch

```bash
# Fork the repository on GitHub, then clone your fork
git clone https://github.com/YOUR_USERNAME/rollingraft.git

# Create a feature branch
git checkout -b feature/your-feature-name

# Or for bug fixes
git checkout -b fix/bug-description
```

### 2. Make Changes

* Follow the [Code Style](#code-style) guidelines
* Write tests for new functionality
* Update documentation if needed
* Keep commits atomic and focused

### 3. Test Your Changes

```bash
# Build with tests
cmake .. -DBUILD_TESTING=ON
make -j$(nproc)

# Run all tests
ctest --output-on-failure

# Run specific test
./tests/unit_tests --gtest_filter="*YourTest*"
```

### 4. Commit

Follow the [Commit Message Guidelines](#commit-message-guidelines).

```bash
git add .
git commit -m "feat: add your feature description"
```

### 5. Push and Create PR

```bash
git push origin feature/your-feature-name
```

Then create a Pull Request on GitHub.

## Code Style

We follow the [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html) with some modifications.

### Formatting

* Use `.clang-format` in the project root
* Run `clang-format` before committing:

```bash
# Format specific file
clang-format -i src/your_file.cpp

# Format all modified files
git diff --name-only | grep -E '\.(cpp|h)$' | xargs clang-format -i
```

### Naming Conventions

| Type | Style | Example |
|------|-------|---------|
| Classes/Structs | PascalCase | `RaftNode`, `LogEntry` |
| Functions | PascalCase for public | `Start()`, `Propose()` |
| Variables | snake_case | `current_term`, `is_leader` |
| Member variables | trailing underscore | `term_`, `state_` |
| Constants | k_ prefix | `k_default_port` |
| Enums | PascalCase type, UPPER_SNAKE_CASE values | `FOLLOWER`, `LEADER` |
| Template parameters | PascalCase | `typename T`, `typename Alloc` |

### Code Organization

```cpp
// Good: Clear structure
class Example {
 public:  // Public interface first
  Example();
  ~Example();
  
  void PublicMethod();
  
 private:  // Private implementation
  void PrivateMethod();
  
  int member_variable_;
};
```

### Comments

* All comments in **English**
* Use `/** */` Doxygen style for public APIs
* Use `//` for inline comments
* Explain "why", not "what"

See [doc/comment_style_guide.md](doc/comment_style_guide.md) for details.

## Commit Message Guidelines

We follow [Conventional Commits](https://www.conventionalcommits.org/) specification.

### Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types

| Type | Description |
|------|-------------|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation changes |
| `style` | Code style changes (formatting, no logic change) |
| `refactor` | Code refactoring |
| `perf` | Performance improvements |
| `test` | Adding or updating tests |
| `chore` | Build, dependencies, tooling |

### Examples

```
feat: add ReadIndex for linearizable reads

* Leader sends heartbeats to confirm authority
* Callback triggered when read is safe to execute
* Add unit tests for read index flow

fix: handle race condition in log replication

Prevent concurrent access to pending_reads_ queue
when commit_index advances during snapshot install.

docs: update README with membership change API

* Add AddNode/RemoveNode to API reference
* Add linearizable reads usage example
* Fix filename typo in project structure
test: add snapshot transfer unit tests

* StateMachine snapshot creation and restore
* Persister snapshot save/load operations
* Waiter triggering for read index
```

### Rules

* Use **imperative mood**: "add" not "added" or "adds"
* Don't capitalize first letter
* No period at the end of subject line
* Subject line max 50 characters
* Wrap body at 72 characters
* Use `*` for bullet points in body

## Testing

### Writing Tests

* All new features must have unit tests
* Integration tests for complex scenarios
* Place tests in appropriate directory:
  * `tests/unit/` - Unit tests
  * `tests/integration/` - Integration tests

### Test Naming

```cpp
// Format: <Class>Test.<Scenario>_<ExpectedBehavior>
TEST_F(RaftNodeTest, Propose_AsFollower_ReturnsNotLeader) {
    // ...
}

TEST_F(LogReplicationTest, Follower_RejectPropose) {
    // ...
}
```

### Mock Usage

Use provided mocks for unit testing:

```cpp
#include "mock/mock_network.h"
#include "mock/mock_state_machine.h"
#include "mock/mock_persister.h"
#include "mock/mock_timer.h"

// Example: Test with mock network
MockNetworkTransport network;
network.SetAutoResponse("{\"success\": true}", true);
```

## Pull Request Process

### Before Submitting

* [ ] Code builds without warnings (`-Wall -Wextra`)
* [ ] All tests pass
* [ ] New tests added for new functionality
* [ ] Code formatted with `clang-format`
* [ ] Documentation updated (if needed)
* [ ] Commit messages follow guidelines

### PR Description Template

```markdown
## Description
Brief description of changes

## Type of Change
* [ ] Bug fix
* [ ] New feature
* [ ] Breaking change
* [ ] Documentation update

## Testing
* [ ] Unit tests added/updated
* [ ] Integration tests pass
* [ ] Manual testing performed

## Checklist
* [ ] Code follows style guidelines
* [ ] Self-review completed
* [ ] Comments added for complex logic
```

### Review Process

1. Maintainers will review PR within 3-5 days
2. Address review comments with additional commits
3. Squash commits if requested
4. PR will be merged once approved and CI passes

## Release Process

Releases are managed by maintainers:

1. Version bump in CMakeLists.txt
2. Update CHANGELOG.md
3. Create git tag: `git tag -a v1.0.0 -m "Release version 1.0.0"`
4. Push tag: `git push origin v1.0.0`
5. Create GitHub release with notes

## Getting Help

* Open an [Issue](https://github.com/Aqiu16717/rollingraft/issues) for bugs or feature requests
* Start a [Discussion](https://github.com/Aqiu16717/rollingraft/discussions) for questions
* Email: aq1u@outlook.com

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
