# Jenkinsfile Descriptions with GitHub Copilot

This document explains how to use GitHub Copilot CLI to automatically generate and improve job descriptions for `.jenkinsfile` files.

## Prerequisites

1. **Node.js and npm installed**: Make sure you have Node.js and npm installed
   ```bash
   # Check if npm is installed
   npm --version
   ```

2. **GitHub Copilot CLI**: Install the Copilot CLI via npm
   ```bash
   npm install -g @githubnext/github-copilot-cli
   ```

   After installation, authenticate with GitHub:
   ```bash
   github-copilot-cli auth
   ```

3. **GitHub Copilot subscription**: You need an active GitHub Copilot subscription

For more details, see: https://docs.github.com/en/copilot/how-tos/set-up/install-copilot-cli

## Usage

The `add_jenkinsfile_descriptions.py` script now supports GitHub Copilot CLI integration with the `--use-copilot` flag.

### Review Missing Descriptions

First, review which files are missing descriptions:

```bash
python add_jenkinsfile_descriptions.py --review
```

### Generate Description for a Single File (Basic)

Use the basic generation without Copilot:

```bash
python add_jenkinsfile_descriptions.py --file path/to/test.jenkinsfile
```

### Generate Description with Copilot (Single File)

Use Copilot to generate a high-quality description:

```bash
python add_jenkinsfile_descriptions.py --use-copilot --file path/to/test.jenkinsfile
```

### Improve Existing Description with Copilot

If a file already has a description, use `--use-copilot` to improve it:

```bash
python add_jenkinsfile_descriptions.py --use-copilot --file path/to/test.jenkinsfile
```

Copilot will:
- Review the existing description
- Check it against the instructions in `.github/instructions/jobs.instructions.md`
- Suggest improvements or corrections

### Batch Process All Files (Basic)

Add descriptions to all files missing them (without Copilot):

```bash
python add_jenkinsfile_descriptions.py --add-all
```

### Batch Process with Copilot

Generate descriptions for all missing files using Copilot:

```bash
python add_jenkinsfile_descriptions.py --use-copilot --add-all
```

**Warning**: This can take a long time as it calls Copilot for each file. Consider using `--limit` to process in batches:

```bash
# Process first 10 files
python add_jenkinsfile_descriptions.py --use-copilot --add-all --limit 10
```

### Dry Run

Preview what would be added without making changes:

```bash
python add_jenkinsfile_descriptions.py --use-copilot --file path/to/test.jenkinsfile --dry-run
```

## How It Works

### Basic Generation (without Copilot)

The script analyzes:
1. Test configuration files referenced in the jenkinsfile
2. Test name and sub-tests
3. Stress tools (cassandra-stress, scylla-bench, ycsb, gemini)
4. Nemesis configurations
5. Dataset size
6. Cluster topology
7. Special features (LWT, CDC, Alternator, encryption, tablets)

It then generates a structured description with:
- Main summary line
- Details about stress tools, nemesis, duration
- Special configurations
- Test suite membership
- Labels/tags

### Copilot Generation (with --use-copilot)

When using Copilot:
1. The script reads `.github/instructions/jobs.instructions.md` for formatting guidelines
2. It sends the jenkinsfile content and instructions to GitHub Copilot CLI
3. Copilot analyzes the file and generates a description following the instructions
4. The script extracts and applies the description to the file

**Advantages of Copilot:**
- Better understanding of test intent
- More natural language descriptions
- Better label/tag selection
- Consistent formatting per instructions
- Can improve existing descriptions

## Description Format

All descriptions follow this format (defined in `.github/instructions/jobs.instructions.md`):

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

Key elements:
- **First line**: Concise summary (test type, cluster size, dataset, workload)
- **Stress tool**: Which tool is used for load generation
- **Nemesis**: If any disruptive operations are tested
- **Test suite**: Whether it's part of sanity, tier1, upgrades, etc.
- **Labels**: Comma-separated tags for categorization

## Troubleshooting

### Copilot CLI not available

```
❌ GitHub Copilot CLI is not available.
   Please install it with: npm install -g @githubnext/github-copilot-cli
```

**Solution**: Install the GitHub Copilot CLI via npm:
```bash
npm install -g @githubnext/github-copilot-cli
```

Then authenticate:
```bash
github-copilot-cli auth
```

### Copilot timeout

If Copilot times out (after 90 seconds), the script will fall back to basic generation.

### Authentication issues

Make sure you're authenticated with GitHub Copilot CLI:
```bash
github-copilot-cli auth
```

Follow the prompts to authenticate with your GitHub account.

### Rate limiting

If processing many files with Copilot, you might hit rate limits. Use `--limit` to process in smaller batches:

```bash
# Process 5 files at a time
python add_jenkinsfile_descriptions.py --use-copilot --add-all --limit 5
```

## Best Practices

1. **Review before commit**: Always review generated descriptions before committing
2. **Use dry-run first**: Test with `--dry-run` to see what would be generated
3. **Process in batches**: For large updates, use `--limit` to avoid timeouts
4. **Improve incrementally**: Use Copilot to improve existing descriptions over time
5. **Keep instructions updated**: Update `.github/instructions/jobs.instructions.md` as standards evolve

## Examples

### Example 1: Basic Usage

```bash
# Review all files
python add_jenkinsfile_descriptions.py --review

# Generate for a specific file
python add_jenkinsfile_descriptions.py --file jenkins-pipelines/longevity-100gb-4h.jenkinsfile
```

### Example 2: Using Copilot

```bash
# Generate with Copilot for better quality
python add_jenkinsfile_descriptions.py --use-copilot --file jenkins-pipelines/longevity-100gb-4h.jenkinsfile

# Preview without making changes
python add_jenkinsfile_descriptions.py --use-copilot --dry-run --file jenkins-pipelines/longevity-100gb-4h.jenkinsfile
```

### Example 3: Batch Processing

```bash
# Process first 10 files without descriptions
python add_jenkinsfile_descriptions.py --use-copilot --add-all --limit 10

# After reviewing, process next batch
python add_jenkinsfile_descriptions.py --use-copilot --add-all --limit 10
```

### Example 4: Improving Existing Descriptions

```bash
# Improve description for files that already have them
python add_jenkinsfile_descriptions.py --use-copilot --file jenkins-pipelines/existing-test.jenkinsfile
```

## Integration with Pre-commit

While this script can be run manually, consider adding a pre-commit check that verifies all `.jenkinsfile` files have descriptions:

```bash
# Check for missing descriptions in pre-commit
python add_jenkinsfile_descriptions.py --review | grep "Missing descriptions: [^0]" && exit 1 || exit 0
```

## See Also

- [GitHub Copilot Instructions](.github/copilot-instructions.md)
- [Jenkinsfile Instructions](.github/instructions/jobs.instructions.md)
- [Contributing Guide](contrib.md)
