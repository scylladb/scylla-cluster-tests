# find-ami-equivalent Command

## Overview

The `find-ami-equivalent` command is a new hydra CLI tool that helps you find equivalent AWS AMIs (Amazon Machine Images) in different regions or architectures based on image tags.

## Use Cases

1. **Cross-Region Deployment**: Convert an AMI ID from one AWS region to its equivalent in another region
2. **Architecture Migration**: Find ARM64 equivalents of x86_64 AMIs (or vice versa)
3. **Multi-Region Validation**: Verify AMI availability across multiple AWS regions
4. **Pipeline Automation**: Integrate with CI/CD pipelines for automated multi-region deployments

## How It Works

The command:
1. Retrieves the source AMI and extracts its tags (Name, scylla_version, build-id, etc.)
2. Searches for AMIs in target regions/architectures that have matching tags
3. Returns results sorted by creation date (newest first)

The matching is based on ScyllaDB's AMI tagging conventions, particularly:
- `Name` tag (most reliable identifier)
- `scylla_version` tag
- `build-id` or `build_id` tag

## Usage

### Basic Syntax

```bash
./sct.py find-ami-equivalent \
    --ami-id <source-ami-id> \
    --source-region <source-region> \
    [--target-region <region>]... \
    [--target-arch <architecture>] \
    [--output-format <format>]
```

### Parameters

- `--ami-id` (required): Source AMI ID to find equivalents for
- `--source-region` (required): AWS region where the source AMI is located
- `--target-region` (optional, multiple): Target region(s) to search in. Can be specified multiple times. If not specified, searches in source region only.
- `--target-arch` (optional): Target architecture (`x86_64` or `arm64`). If not specified, uses same architecture as source AMI.
- `--output-format` (optional): Output format - `table` (default) or `json`

### Examples

#### 1. Find equivalent in the same region (validation)
```bash
./sct.py find-ami-equivalent \
    --ami-id ami-0d9726c9053daff76 \
    --source-region us-east-1
```

#### 2. Find equivalent in different regions
```bash
./sct.py find-ami-equivalent \
    --ami-id ami-0d9726c9053daff76 \
    --source-region us-east-1 \
    --target-region us-west-2 \
    --target-region eu-west-1
```

#### 3. Find ARM64 equivalent of an x86_64 AMI
```bash
./sct.py find-ami-equivalent \
    --ami-id ami-0d9726c9053daff76 \
    --source-region us-east-1 \
    --target-arch arm64
```

#### 4. Get JSON output for pipeline use
```bash
./sct.py find-ami-equivalent \
    --ami-id ami-0d9726c9053daff76 \
    --source-region us-east-1 \
    --target-region us-west-2 \
    --output-format json
```

#### 5. Comprehensive multi-region search
```bash
./sct.py find-ami-equivalent \
    --ami-id ami-0d9726c9053daff76 \
    --source-region us-east-1 \
    --target-region us-east-1 \
    --target-region us-west-2 \
    --target-region eu-west-1 \
    --target-region eu-central-1 \
    --target-arch arm64 \
    --output-format table
```

## Output Formats

### Table Format (Default)

Human-readable table showing:
- Region
- AMI ID
- Name
- Architecture
- Creation Date
- Name Tag
- Scylla Version
- Build ID (truncated to 6 chars)
- Owner ID

Example:
```
+------------+-----------------------+---------------------------+---------------+-------------------------+
|   Region   |        AMI ID         |           Name            | Architecture  |     Creation Date       |
+------------+-----------------------+---------------------------+---------------+-------------------------+
| us-west-2  | ami-0abc123def456     | scylla-5.2.0-x86_64-...   | x86_64        | 2024-01-15T10:00:00.000Z|
| eu-west-1  | ami-0def456ghi789     | scylla-5.2.0-x86_64-...   | x86_64        | 2024-01-15T10:00:00.000Z|
+------------+-----------------------+---------------------------+---------------+-------------------------+
```

### JSON Format

Machine-readable JSON for pipeline integration:
```json
{
  "source_ami_id": "ami-0d9726c9053daff76",
  "source_region": "us-east-1",
  "target_arch": null,
  "results": [
    {
      "region": "us-west-2",
      "ami_id": "ami-0abc123def456",
      "name": "scylla-5.2.0-x86_64-2024-01-15",
      "architecture": "x86_64",
      "creation_date": "2024-01-15T10:00:00.000Z",
      "name_tag": "scylla-5.2.0-x86_64-2024-01-15",
      "scylla_version": "5.2.0",
      "build_id": "abc123def456",
      "owner_id": "797456418907"
    }
  ]
}
```

## Requirements

- AWS credentials configured (via environment variables, AWS CLI, or IAM role)
- Appropriate IAM permissions to describe EC2 images in the target regions

## Error Handling

The command gracefully handles:
- Source AMI not found
- Source AMI has no tags
- No equivalent AMIs found in target regions
- AWS API errors (with appropriate error messages)

## Implementation Details

### Core Function

Location: `sdcm/utils/common.py:find_equivalent_ami()`

The function:
1. Loads the source AMI and extracts its tags
2. Determines search parameters (architecture, regions)
3. Searches both standard EC2 resources and ScyllaDB-specific image resources
4. Matches AMIs based on Name, scylla_version, and build-id tags
5. Returns sorted results (newest first)

### CLI Command

Location: `sct.py:find-ami-equivalent`

Built using Click framework, following the same patterns as other SCT commands like `list-images`.

## Testing

Unit tests are located in `unit_tests/test_find_ami_equivalent.py` and cover:
- Basic functionality with mocked AWS API calls
- Cross-architecture searches
- Multi-region searches
- Error cases (missing AMI, no tags, etc.)
- Result sorting

Integration tests with actual AWS API calls are marked with `@pytest.mark.integration` and require AWS credentials to run.

## See Also

- `./sct.py list-images` - List available machine images
- `examples_find_ami_equivalent.sh` - Usage examples script
