# Contributing to RocksQueue

We welcome contributions to RocksQueue! This document provides guidelines for contributing to the project.

## Getting Started

### Prerequisites

- **JDK 17+** (OpenJDK or Oracle JDK)
- **Git** for version control
- **IDE** (IntelliJ IDEA, Eclipse, or VS Code recommended)

### Development Setup

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/your-username/rocksqueue.git
   cd rocksqueue
   ```
3. **Build the project**:
   ```bash
   ./gradlew clean build
   ```
4. **Run tests** to ensure everything works:
   ```bash
   ./gradlew test
   ```

## Development Workflow

### Creating a Feature Branch

1. Create a new branch from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
2. Make your changes
3. Commit with descriptive messages
4. Push to your fork
5. Create a Pull Request

### Branch Naming Convention

- `feature/feature-name` - New features
- `bugfix/issue-description` - Bug fixes
- `docs/documentation-update` - Documentation improvements
- `refactor/component-name` - Code refactoring
- `test/test-improvement` - Test additions or improvements

## Code Standards

### Java Code Style

- **Follow standard Java conventions**
- **Use meaningful variable and method names**
- **Keep methods focused and concise**
- **Add JavaDoc for all public APIs**
- **Use proper exception handling**

### Example Code Style

```java
/**
 * Enqueues an item for execution at the specified timestamp.
 * 
 * @param item the item to enqueue (must not be null)
 * @param executeAtMillis when the item should become available
 * @throws IllegalArgumentException if item is null
 * @throws IllegalStateException if queue is closed
 */
public void enqueue(T item, long executeAtMillis) {
    Objects.requireNonNull(item, "item cannot be null");
    // Implementation...
}
```

### Formatting

- **Indentation**: 4 spaces (no tabs)
- **Line length**: 120 characters maximum
- **Braces**: Opening brace on same line
- **Imports**: Group and organize imports properly

## Testing Guidelines

### Test Requirements

- **Unit tests** for all new functionality
- **Integration tests** for complex features
- **Performance tests** for throughput-critical changes
- **All tests must pass** before submitting PR

### Test Structure

```java
@Test
void should_enqueue_and_dequeue_items_in_fifo_order() {
    // Given
    TimeQueue<String> queue = createTestQueue();
    
    // When
    queue.enqueue("first", 1000);
    queue.enqueue("second", 1000);
    
    // Then
    assertEquals("first", queue.dequeue());
    assertEquals("second", queue.dequeue());
}
```

### Running Tests

```bash
# Run all tests
./gradlew test

# Run specific test class
./gradlew test --tests "RocksTimeQueueTest"

# Run tests with specific pattern
./gradlew test --tests "*Integration*"

# Run performance tests with profiling
./gradlew throughputJfr
```

## Documentation

### JavaDoc Requirements

- **All public classes and methods** must have JavaDoc
- **Include parameter descriptions** with `@param`
- **Document return values** with `@return`
- **Document exceptions** with `@throws`
- **Provide usage examples** for complex APIs

### README Updates

- Update README.md if adding new features
- Include code examples for new functionality
- Update configuration documentation as needed

## Performance Considerations

### Guidelines

- **Avoid unnecessary object allocation** in hot paths
- **Use appropriate data structures** for the use case
- **Consider memory usage** for large-scale operations
- **Profile performance-critical changes**

### Benchmarking

- Run existing benchmarks before and after changes
- Add new benchmarks for performance-critical features
- Document performance characteristics in PR description

## Pull Request Process

### Before Submitting

1. **Ensure all tests pass**: `./gradlew test`
2. **Run static analysis**: Check for warnings
3. **Update documentation** as needed
4. **Add/update tests** for your changes
5. **Rebase on latest main** if needed

### PR Description Template

```markdown
## Description
Brief description of the changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Manual testing performed

## Performance Impact
- [ ] No performance impact
- [ ] Performance improvement
- [ ] Potential performance regression (explain)

## Checklist
- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] Tests added and passing
```

### Review Process

1. **Automated checks** must pass (CI/CD)
2. **Code review** by maintainers
3. **Address feedback** promptly
4. **Squash commits** if requested
5. **Merge** after approval

## Issue Reporting

### Bug Reports

Include the following information:

- **RocksQueue version**
- **JDK version and vendor**
- **Operating system**
- **Minimal reproduction case**
- **Expected vs actual behavior**
- **Stack trace** (if applicable)

### Feature Requests

- **Clear description** of the proposed feature
- **Use case** and motivation
- **Proposed API** (if applicable)
- **Implementation considerations**

## Community Guidelines

### Code of Conduct

- **Be respectful** and inclusive
- **Provide constructive feedback**
- **Help others learn** and grow
- **Focus on the code**, not the person

### Communication

- **Use GitHub Issues** for bug reports and feature requests
- **Use GitHub Discussions** for questions and general discussion
- **Be patient** with response times
- **Search existing issues** before creating new ones

## Getting Help

### Resources

- **Documentation**: Check the README and wiki
- **Issues**: Search existing issues for similar problems
- **Discussions**: Ask questions in GitHub Discussions
- **Code**: Review existing code for patterns and examples

### Maintainer Contact

For urgent issues or security concerns, contact the maintainers directly through GitHub.

---

Thank you for contributing to RocksQueue! Your contributions help make this project better for everyone.
