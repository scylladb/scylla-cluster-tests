# Groovy Pipeline Unit Tests

This directory contains Groovy unit tests for Jenkins pipelines using [JenkinsPipelineUnit](https://github.com/jenkinsci/JenkinsPipelineUnit).

## Prerequisites

- Java 11 or higher
- Maven 3.6 or higher
- Or use Docker (recommended)

## Running Tests

### Option 1: Using Docker (Recommended)

Run tests in a Maven Docker container:

```bash
# From the repository root
docker run --rm -v "$(pwd):/workspace" -w /workspace maven:3.8-openjdk-11 mvn clean test
```

### Option 2: Using Local Maven

If you have Maven installed locally:

```bash
# From the repository root
mvn clean test
```

### Option 3: Run specific test

```bash
docker run --rm -v "$(pwd):/workspace" -w /workspace maven:3.8-openjdk-11 mvn test -Dtest=Tier1ParallelPipelineTest
```

## Test Structure

- `test/groovy/Tier1ParallelPipelineTest.groovy` - Unit tests for tier1ParallelPipeline
- `pom.xml` - Maven configuration with JenkinsPipelineUnit dependencies
- `vars/tier1ParallelPipeline.groovy` - The pipeline being tested

## What's Being Tested

The tests verify:
- Pipeline loads correctly
- Job folder auto-detection for master (`scylla-master`)
- Job folder auto-detection for releases (`branch-2025.4`)
- Explicit job_folder parameter handling
- Skip jobs parameter parsing
- ARM64 architecture support

## Adding New Tests

To add new tests, create methods in `Tier1ParallelPipelineTest.groovy` with the `@Test` annotation:

```groovy
@Test
void testNewFeature() {
    def script = loadScript('vars/tier1ParallelPipeline.groovy')
    // Your test logic here
    assert true
}
```

## CI Integration

These tests can be integrated into CI pipelines:

```yaml
# Example for GitHub Actions
- name: Run Groovy Tests
  run: docker run --rm -v "$PWD:/workspace" -w /workspace maven:3.8-openjdk-11 mvn clean test
```

## References

- [JenkinsPipelineUnit Documentation](https://github.com/jenkinsci/JenkinsPipelineUnit)
- [Groovy Testing Guide](https://groovy-lang.org/testing.html)
