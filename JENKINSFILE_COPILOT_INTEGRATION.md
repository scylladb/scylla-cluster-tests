# GitHub Copilot Integration for Jenkinsfile Descriptions

## Overview

The `add_jenkinsfile_descriptions.py` script has been extended to use GitHub Copilot CLI (npm version) to automatically generate and improve job descriptions for `.jenkinsfile` files.

## What's New

### Features

1. **GitHub Copilot CLI Integration**: Uses the npm-based `@githubnext/github-copilot-cli` package
2. **Smart Description Generation**: Leverages Copilot's AI to understand test intent and generate better descriptions
3. **Instruction-Based**: Follows guidelines from `.github/instructions/jobs.instructions.md`
4. **Fallback Support**: Falls back to basic generation if Copilot is unavailable or fails
5. **Batch Processing**: Can process multiple files with optional rate limiting

### New Command-Line Option

```bash
--use-copilot    Use GitHub Copilot CLI to improve descriptions
```

## Installation

### 1. Install GitHub Copilot CLI

```bash
npm install -g @githubnext/github-copilot-cli
```

### 2. Authenticate

```bash
github-copilot-cli auth
```

Follow the prompts to authenticate with your GitHub account that has Copilot access.

### 3. Verify Installation

```bash
github-copilot-cli --version
```

## Usage Examples

### Generate Description for Single File

```bash
# Without Copilot (basic generation)
python add_jenkinsfile_descriptions.py --file path/to/test.jenkinsfile

# With Copilot (AI-powered generation)
python add_jenkinsfile_descriptions.py --use-copilot --file path/to/test.jenkinsfile
```

### Improve Existing Description

```bash
# Copilot will review and improve the existing description
python add_jenkinsfile_descriptions.py --use-copilot --file path/to/test.jenkinsfile
```

### Batch Process All Missing Descriptions

```bash
# Process all files without descriptions (with Copilot)
python add_jenkinsfile_descriptions.py --use-copilot --add-all

# Process first 10 files only
python add_jenkinsfile_descriptions.py --use-copilot --add-all --limit 10
```

### Dry Run (Preview Changes)

```bash
# See what would be generated without making changes
python add_jenkinsfile_descriptions.py --use-copilot --dry-run --file path/to/test.jenkinsfile
```

### Review Status

```bash
# Check which files are missing descriptions
python add_jenkinsfile_descriptions.py --review
```

## How It Works

### Without Copilot (Basic Mode)

1. Parses jenkinsfile to extract test configurations
2. Analyzes YAML config files referenced in the jenkinsfile
3. Identifies stress tools, nemesis, cluster topology, etc.
4. Generates a structured description based on patterns

### With Copilot (AI Mode)

1. Reads instructions from `.github/instructions/jobs.instructions.md`
2. Extracts jenkinsfile content (first 200 lines)
3. Sends prompt to GitHub Copilot CLI with:
   - Formatting instructions
   - Jenkinsfile content
   - Existing description (if any)
4. Copilot analyzes and generates/improves the description
5. Script extracts and applies the description

### Fallback Behavior

If Copilot fails (timeout, error, or can't extract description), the script automatically falls back to basic generation mode.

## Description Format

All descriptions follow this format (from `.github/instructions/jobs.instructions.md`):

```groovy
/** jobDescription
    4-hour longevity test on a 6-node cluster with ~100GB dataset using mixed read/write workload.
    Runs cassandra-stress via cql-stress wrapper with SizeTieredCompactionStrategy.
    Tests SisyphusMonkey nemesis with encryption enabled (server+client) and parallel node operations.
    This test serves as part of the sanity suite for longevity tests.
    Main load stress tool: cql-stress-cassandra-stress
    Labels: longevity, sanity, cql-stress, mixed-workload, encryption, size-tiered-compaction, sisyphus, nemesis, 100gb
*/
```

## Benefits of Using Copilot

1. **Better Understanding**: Copilot understands test intent beyond pattern matching
2. **Natural Language**: Generates more readable, natural descriptions
3. **Context Aware**: Considers the full jenkinsfile context
4. **Consistent**: Follows instructions consistently
5. **Improvement**: Can review and improve existing descriptions

## Troubleshooting

### Copilot CLI Not Found

```
❌ GitHub Copilot CLI is not available.
   Please install it with: npm install -g @githubnext/github-copilot-cli
```

**Solution**: Install the CLI using npm as shown above.

### Authentication Issues

```bash
# Re-authenticate
github-copilot-cli auth
```

### Timeout Issues

If Copilot times out (90 seconds), the script falls back to basic generation. You can:
- Process files one at a time
- Use `--limit` for smaller batches
- Check network connectivity

### Rate Limiting

Processing many files quickly might hit rate limits. Use `--limit` to process in batches:

```bash
# Process 5 files at a time
python add_jenkinsfile_descriptions.py --use-copilot --add-all --limit 5
```

## Best Practices

1. **Preview First**: Always use `--dry-run` to preview generated descriptions
2. **Start Small**: Test with a single file before batch processing
3. **Review Output**: Always review generated descriptions before committing
4. **Batch Processing**: Use `--limit` for large batch operations
5. **Update Instructions**: Keep `.github/instructions/jobs.instructions.md` up to date

## Technical Details

### Commands Used

The script tries these commands in order:
1. `github-copilot-cli query` (npm package)
2. `copilot query` (fallback alias)

### Prompt Structure

```
Generate a job description comment for a Jenkinsfile based on these instructions:

[Instructions from .github/instructions/jobs.instructions.md]

Here is the jenkinsfile content:

```groovy
[First 200 lines of jenkinsfile]
```

[Current description if exists]

Return ONLY the jobDescription comment block in Groovy format, nothing else.
```

### Extraction Logic

The script extracts the description from Copilot's response using multiple strategies:
1. Direct regex match for `/** jobDescription ... */`
2. Extraction from code blocks (```groovy ... ```)
3. Line-by-line extraction if comment marker is found

## Related Documentation

- [Full Usage Guide](docs/jenkinsfile_descriptions_copilot.md)
- [Jenkinsfile Instructions](.github/instructions/jobs.instructions.md)
- [GitHub Copilot CLI Docs](https://docs.github.com/en/copilot/how-tos/set-up/install-copilot-cli)

## Workflow Integration

### Pre-commit Hook (Optional)

You can add a check to ensure all jenkinsfiles have descriptions:

```yaml
# .pre-commit-config.yaml
- repo: local
  hooks:
    - id: check-jenkinsfile-descriptions
      name: Check jenkinsfile descriptions
      entry: python add_jenkinsfile_descriptions.py --review
      language: system
      pass_filenames: false
```

### CI/CD Integration

```bash
# In your CI pipeline
python add_jenkinsfile_descriptions.py --review
if [ $? -ne 0 ]; then
  echo "Some jenkinsfiles are missing descriptions"
  exit 1
fi
```

## Examples

### Example 1: Single File with Copilot

```bash
$ python add_jenkinsfile_descriptions.py --use-copilot --file jenkins-pipelines/longevity-100gb-4h.jenkinsfile
✅ GitHub Copilot CLI is available

Processing 1 .jenkinsfile files

  🤖 Asking GitHub Copilot for description...
  ✅ Generated jenkins-pipelines/longevity-100gb-4h.jenkinsfile

======================================================================
Summary:
  Added: 1
  Skipped (already has description): 0
  Errors: 0
======================================================================
```

### Example 2: Batch Processing with Limit

```bash
$ python add_jenkinsfile_descriptions.py --use-copilot --add-all --limit 5
✅ GitHub Copilot CLI is available

Processing 5 .jenkinsfile files

  🤖 Asking GitHub Copilot for description...
  ✅ Generated jenkins-pipelines/test1.jenkinsfile
  🤖 Asking GitHub Copilot for description...
  ✅ Generated jenkins-pipelines/test2.jenkinsfile
...
======================================================================
Summary:
  Added: 5
  Skipped (already has description): 0
  Errors: 0
======================================================================
```

### Example 3: Improve Existing Description

```bash
$ python add_jenkinsfile_descriptions.py --use-copilot --file jenkins-pipelines/existing.jenkinsfile
✅ GitHub Copilot CLI is available

Processing 1 .jenkinsfile files

  🤖 Asking GitHub Copilot for description...
  ✨ Improved jenkins-pipelines/existing.jenkinsfile

======================================================================
Summary:
  Improved: 1
  Added: 0
  Skipped (already has description): 0
  Errors: 0
======================================================================
```

## Future Enhancements

Potential improvements for future versions:
- Interactive mode to review each description before applying
- Parallel processing for faster batch operations
- Support for custom instruction files per directory
- Integration with PR review automation
- Metrics on description quality improvement

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review the [full documentation](docs/jenkinsfile_descriptions_copilot.md)
3. Open an issue in the repository

## License

This feature is part of the scylla-cluster-tests project and follows the same license.
