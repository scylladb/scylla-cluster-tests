#!/usr/bin/env python3
"""
Script to add/review job descriptions for .jenkinsfile files based on their configuration.

Usage:
    # Review all files and report missing/wrong descriptions
    ./utils/add_jenkinsfile_descriptions.py --review

    # Add descriptions to all files missing them
    ./utils/add_jenkinsfile_descriptions.py --add-all

    # Add description to specific file
    ./utils/add_jenkinsfile_descriptions.py --file path/to/file.jenkinsfile

    # Process all files in a directory
    ./utils/add_jenkinsfile_descriptions.py --file jenkins-pipelines/oss/

    # Process files matching a glob pattern
    ./utils/add_jenkinsfile_descriptions.py --file "jenkins-pipelines/**/*longevity*.jenkinsfile"

    # Dry run (show what would be added without making changes)
    ./utils/add_jenkinsfile_descriptions.py --dry-run

    # Use GitHub Copilot CLI to improve descriptions
    ./utils/add_jenkinsfile_descriptions.py --use-copilot --file path/to/file.jenkinsfile

    # Batch improve all files in directory with Copilot
    ./utils/add_jenkinsfile_descriptions.py --use-copilot --file jenkins-pipelines/oss/nemesis/

    # Batch improve all missing descriptions with Copilot
    ./utils/add_jenkinsfile_descriptions.py --use-copilot --add-all
"""

import os
import re
import yaml
import argparse
import subprocess
import sys
import glob as glob_module
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def parse_yaml_config(yaml_path: str) -> Dict:
    """Parse a YAML configuration file."""
    try:
        with open(yaml_path, 'r') as f:
            return yaml.safe_load(f) or {}
    except (OSError, yaml.YAMLError) as e:
        print(f"Warning: Could not parse {yaml_path}: {e}")
        return {}


def extract_test_config_files(jenkinsfile_content: str) -> List[str]:
    """Extract test configuration file paths from jenkinsfile."""
    configs = []
    # Look for test_config parameter
    config_match = re.search(r'test_config:\s*["\']?\[?([^]"\'\n]+)', jenkinsfile_content)
    if config_match:
        config_str = config_match.group(1)
        # Extract individual YAML files
        yaml_files = re.findall(r'([^\s",\[\]]+\.yaml)', config_str)
        configs.extend(yaml_files)
    return configs


def extract_stress_tool(config: Dict) -> Optional[str]:
    """Extract the main stress tool from configuration."""
    # Check various stress command keys
    stress_keys = [
        'stress_cmd', 'stress_cmd_w', 'stress_cmd_r', 'stress_cmd_m',
        'prepare_write_cmd', 'stress_cmd_read', 'stress_cmd_write',
        'stress_cmd_mixed'
    ]

    for key in stress_keys:
        if key in config:
            cmd = config[key]
            if isinstance(cmd, list):
                cmd = ' '.join(str(c) for c in cmd)
            cmd_str = str(cmd).lower()

            # Identify stress tools
            if 'cassandra-stress' in cmd_str:
                return 'cassandra-stress'
            elif 'scylla-bench' in cmd_str:
                return 'scylla-bench'
            elif 'ycsb' in cmd_str:
                return 'ycsb'
            elif 'gemini' in cmd_str:
                return 'gemini'
            elif 'cql-stress' in cmd_str or 'cassandra-harry' in cmd_str:
                return 'cql-stress'

    return None


def extract_nemesis(config: Dict) -> Optional[str]:
    """Extract nemesis class from configuration."""
    nemesis_keys = ['nemesis_class_name', 'nemesis', 'nemesis_during_prepare']

    for key in nemesis_keys:
        if key in config:
            nemesis = config[key]
            if isinstance(nemesis, str) and nemesis != 'NoOpMonkey' and nemesis:
                return nemesis

    return None


def extract_dataset_size(config: Dict, filename: str) -> Optional[str]:
    """Extract dataset size from config or filename."""
    # Try from filename first
    size_match = re.search(r'(\d+)(gb|GB|tb|TB|mb|MB)', filename.lower())
    if size_match:
        return size_match.group(1) + size_match.group(2).upper()

    # Try from config
    if 'n_db_nodes' in config and 'prepare_write_cmd' in config:
        # Rough estimation logic could go here
        pass

    return None


def extract_test_suite(filename: str, test_name: str) -> Optional[str]:
    """Determine if test is part of a suite."""
    filename_lower = filename.lower()
    test_name_lower = test_name.lower()

    if 'sanity' in filename_lower or 'sanity' in test_name_lower:
        return 'sanity'
    elif 'upgrade' in filename_lower or 'upgrade' in test_name_lower:
        return 'upgrades'
    elif 'artifact' in filename_lower or 'artifact' in test_name_lower:
        return 'artifacts'
    elif 'longevity' in filename_lower or 'longevity' in test_name_lower:
        return 'longevity'
    elif 'performance' in filename_lower or 'perf' in filename_lower:
        return 'performance'

    return None


def generate_labels(config: Dict, filename: str, test_name: str) -> List[str]:
    """Generate labels/tags for the job."""
    labels = []

    filename_lower = filename.lower()
    test_name_lower = test_name.lower()

    # Test type labels
    if 'longevity' in filename_lower or 'longevity' in test_name_lower:
        labels.append('longevity')
    if 'performance' in filename_lower or 'perf' in filename_lower:
        labels.append('performance')
    if 'latency' in filename_lower:
        labels.append('latency')
    if 'throughput' in filename_lower:
        labels.append('throughput')
    if 'upgrade' in filename_lower:
        labels.append('upgrade')
    if 'sanity' in filename_lower:
        labels.append('sanity')

    # Technology labels
    if 'lwt' in filename_lower:
        labels.append('lwt')
    if 'cdc' in filename_lower:
        labels.append('cdc')
    if 'alternator' in filename_lower:
        labels.append('alternator')
    if 'ycsb' in filename_lower:
        labels.append('ycsb')
    if 'gemini' in filename_lower:
        labels.append('gemini')
    if 'tablets' in filename_lower:
        labels.append('tablets')
    if 'vnodes' in filename_lower:
        labels.append('vnodes')

    # Workload labels
    if 'mixed' in filename_lower:
        labels.append('mixed-workload')
    if 'read' in filename_lower:
        labels.append('read-workload')
    if 'write' in filename_lower:
        labels.append('write-workload')

    # Configuration labels
    if 'encryption' in str(config).lower():
        labels.append('encryption')
    if 'auth' in str(config).lower():
        labels.append('authentication')

    # Dataset size
    dataset_size = extract_dataset_size(config, filename)
    if dataset_size:
        labels.append(dataset_size.lower())

    # Nemesis
    nemesis = extract_nemesis(config)
    if nemesis and nemesis != 'NoOpMonkey':
        labels.append('nemesis')

    # Remove duplicates and sort
    return sorted(list(set(labels)))


def generate_description(jenkinsfile_path: str, jenkinsfile_content: str) -> str:  # noqa: PLR0912, PLR0914, PLR0915
    """Generate job description for a jenkinsfile."""

    # Extract test name
    test_name_match = re.search(r'test_name:\s*["\']([^"\']+)["\']', jenkinsfile_content)
    test_name = test_name_match.group(1) if test_name_match else "Unknown Test"

    # Extract sub tests
    sub_tests_match = re.search(r'sub_tests:\s*\[([^]]+)\]', jenkinsfile_content)
    sub_tests = []
    if sub_tests_match:
        sub_tests_str = sub_tests_match.group(1)
        sub_tests = [s.strip(' "\'') for s in sub_tests_str.split(',')]

    # Extract config files
    config_files = extract_test_config_files(jenkinsfile_content)

    # Parse config files
    merged_config = {}
    sct_root = Path(__file__).parent.resolve()
    for config_file in config_files:
        config_path = sct_root / config_file
        if config_path.exists():
            config_data = parse_yaml_config(str(config_path))
            merged_config.update(config_data)

    filename = os.path.basename(jenkinsfile_path)

    # Extract key information
    stress_tool = extract_stress_tool(merged_config)
    nemesis = extract_nemesis(merged_config)
    test_suite = extract_test_suite(filename, test_name)
    labels = generate_labels(merged_config, filename, test_name)

    # Build description
    lines = []

    # Line 1: Main summary
    summary_parts = []

    # Test type
    if 'latency' in filename.lower():
        summary_parts.append("Latency performance regression test")
    elif 'throughput' in filename.lower():
        summary_parts.append("Throughput performance regression test")
    elif 'longevity' in filename.lower():
        summary_parts.append("Longevity test")
    elif 'upgrade' in filename.lower():
        summary_parts.append("Upgrade test")
    elif 'grow' in filename.lower() or 'shrink' in filename.lower():
        summary_parts.append("Cluster elasticity test")
    elif 'manager' in filename.lower():
        summary_parts.append("Scylla Manager test")
    else:
        summary_parts.append("Test")

    # Cluster details
    if merged_config.get('n_db_nodes'):
        n_nodes = merged_config['n_db_nodes']
        summary_parts.append(f"on {n_nodes}-node cluster")

    # Dataset size
    dataset_size = extract_dataset_size(merged_config, filename)
    if dataset_size:
        summary_parts.append(f"with ~{dataset_size} dataset")

    # Main workload type from sub_tests or filename
    workload_types = []
    for st in sub_tests:
        if 'write' in st.lower():
            workload_types.append('write')
        if 'read' in st.lower():
            workload_types.append('read')
        if 'mixed' in st.lower():
            workload_types.append('mixed')

    if workload_types:
        workload_str = '/'.join(workload_types)
        summary_parts.append(f"using {workload_str} workload")
    elif 'mixed' in filename.lower():
        summary_parts.append("using mixed workload")

    lines.append(' '.join(summary_parts) + '.')

    # Additional details
    details = []

    # Stress tool
    if stress_tool:
        if 'cassandra-stress' in stress_tool:
            # Check for cql-stress wrapper
            if 'cql-stress' in str(merged_config).lower():
                details.append("Runs cassandra-stress via cql-stress wrapper")
            else:
                details.append("Stress tool: cassandra-stress")
        else:
            details.append(f"Stress tool: {stress_tool}")

    # Nemesis
    if nemesis and nemesis != 'NoOpMonkey':
        details.append(f"Tests {nemesis} nemesis")

    # Duration
    if merged_config.get('test_duration'):
        duration_min = int(merged_config['test_duration']) // 60
        if duration_min >= 60:
            duration_hours = duration_min / 60
            details.append(f"Runs for {duration_hours:.1f} hours")
        else:
            details.append(f"Runs for {duration_min} minutes")

    # Special configurations
    if 'encryption' in str(merged_config).lower():
        details.append("Encryption enabled")

    if 'tablets' in filename.lower():
        if 'disabled' in str(config_files).lower():
            details.append("Tablets disabled")
        else:
            details.append("Tablets enabled")

    if 'vnodes' in filename.lower():
        details.append("Virtual nodes (vnodes) enabled")

    # Compaction strategy
    compaction_strategy = merged_config.get('compaction_strategy', '')
    if isinstance(compaction_strategy, str):
        if 'size' in compaction_strategy.lower():
            details.append("Using SizeTieredCompactionStrategy")
        elif 'time' in compaction_strategy.lower() or 'twcs' in filename.lower():
            details.append("Using TimeWindowCompactionStrategy")
        elif 'leveled' in compaction_strategy.lower() or 'lcs' in filename.lower():
            details.append("Using LeveledCompactionStrategy")

    # LWT
    if 'lwt' in filename.lower():
        details.append("Tests lightweight transactions (LWT)")

    # CDC
    if 'cdc' in filename.lower():
        details.append("Tests Change Data Capture (CDC)")

    # Alternator
    if 'alternator' in filename.lower():
        details.append("Tests DynamoDB-compatible Alternator API")

    # Test suite
    if test_suite:
        details.append(f"Part of the {test_suite} suite")

    # Add details to description
    if details:
        for detail in details:
            lines.append(detail + '.')

    # Labels
    if labels:
        lines.append(f"Labels: {', '.join(labels)}")

    return '\n    '.join(lines)


def review_jenkinsfile(jenkinsfile_path: str) -> Tuple[bool, str]:
    """Review a jenkinsfile and return (has_description, proposed_description)."""

    with open(jenkinsfile_path, 'r') as f:
        content = f.read()

    has_description = bool(re.search(r'/\*\*\s*jobDescription', content))

    try:
        proposed = generate_description(jenkinsfile_path, content)
        # Format as comment
        comment_lines = ["/** jobDescription"]
        for line in proposed.split('\n'):
            comment_lines.append(f"    {line}")
        comment_lines.append("*/")
        proposed_formatted = '\n'.join(comment_lines)
    except (OSError, yaml.YAMLError, ValueError, KeyError) as e:
        proposed_formatted = f"Error generating description: {e}"

    return has_description, proposed_formatted


def review_all_jenkinsfiles(sct_root: Path):
    """Review all jenkinsfiles and report status."""
    jenkinsfiles = sorted(sct_root.glob('**/*.jenkinsfile'))

    missing = []
    existing = []

    for jf in jenkinsfiles:
        rel_path = jf.relative_to(sct_root)
        has_desc, _ = review_jenkinsfile(str(jf))

        if has_desc:
            existing.append(rel_path)
        else:
            missing.append(rel_path)

    print(f"\n{'='*70}")
    print("Jenkinsfile Description Review")
    print(f"{'='*70}")
    print(f"\nTotal files: {len(jenkinsfiles)}")
    print(f"✅ With descriptions: {len(existing)}")
    print(f"❌ Missing descriptions: {len(missing)}")

    if missing:
        print(f"\n{'='*70}")
        print("Files missing descriptions:")
        print(f"{'='*70}")
        for path in missing[:20]:  # Show first 20
            print(f"  • {path}")
        if len(missing) > 20:
            print(f"  ... and {len(missing) - 20} more")

    print(f"\n{'='*70}")
    return missing


def check_copilot_cli_available() -> bool:
    """Check if GitHub Copilot CLI is available."""
    # Try the npm-based CLI first (github-copilot-cli)
    try:
        result = subprocess.run(
            ['github-copilot-cli', '--version'],
            capture_output=True,
            text=True,
            timeout=5, check=False
        )
        if result.returncode == 0:
            return True
    except (subprocess.SubprocessError, FileNotFoundError):
        pass

    # Fallback to checking for 'copilot' command
    try:
        result = subprocess.run(
            ['copilot', '--version'],
            capture_output=True,
            text=True,
            timeout=5, check=False
        )
        return result.returncode == 0
    except (subprocess.SubprocessError, FileNotFoundError):
        return False


def expand_file_patterns(pattern: str, sct_root: Path) -> List[Path]:
    """Expand file pattern to list of jenkinsfiles.

    Supports:
    - Specific files: path/to/file.jenkinsfile
    - Glob patterns: path/**/*.jenkinsfile
    - Directories: path/to/directory/ (finds all .jenkinsfile files in directory)

    Args:
        pattern: File pattern, glob, or directory path
        sct_root: Root directory of the project

    Returns:
        List of Path objects for matching jenkinsfiles
    """
    pattern_path = Path(pattern)

    # If it's an absolute path, use it as is
    if pattern_path.is_absolute():
        base_path = pattern_path
    else:
        # Otherwise, resolve relative to sct_root
        base_path = sct_root / pattern

    # If it's a directory, find all .jenkinsfile files in it
    if base_path.is_dir():
        jenkinsfiles = sorted(base_path.glob('**/*.jenkinsfile'))
        return jenkinsfiles

    # If it's a specific file that exists, return it
    if base_path.is_file():
        return [base_path]

    # Try glob expansion
    # First try relative to sct_root
    matches = list(sct_root.glob(pattern))

    # If no matches, try absolute glob
    if not matches:
        matches = [Path(p) for p in glob_module.glob(pattern, recursive=True)]

    # Filter to only .jenkinsfile files
    jenkinsfiles = [p for p in matches if p.is_file() and p.suffix == '.jenkinsfile']

    return sorted(jenkinsfiles)


def load_copilot_instructions() -> str:
    """Load the GitHub Copilot instructions for jenkinsfile descriptions."""
    sct_root = Path(__file__).parent.parent.resolve()
    instructions_path = sct_root / '.github' / 'instructions' / 'jobs.instructions.md'

    if not instructions_path.exists():
        print(f"Warning: Instructions file not found at {instructions_path}")
        return ""

    with open(instructions_path, 'r') as f:
        return f.read()


def improve_description_with_copilot(jenkinsfile_path: str, current_description: str = "") -> Optional[str]:  # noqa: PLR0911
    """Use GitHub Copilot CLI to improve or generate a description for a jenkinsfile.

    Args:
        jenkinsfile_path: Path to the jenkinsfile
        current_description: Existing description if any

    Returns:
        Improved description or None if copilot fails
    """
    instructions = load_copilot_instructions()

    if not instructions:
        print("  ⚠️  No Copilot instructions found, falling back to basic generation")
        return None

    # Read jenkinsfile content (limit to first 200 lines to avoid token limits)
    with open(jenkinsfile_path, 'r') as f:
        lines = f.readlines()
        jenkinsfile_content = ''.join(lines[:200])

    # Build the prompt for Copilot
    prompt_parts = [
        "Generate a job description comment for a Jenkinsfile based on these instructions:\n\n",
        instructions,
        "\n\nHere is the jenkinsfile content:\n\n```groovy\n",
        jenkinsfile_content,
        "\n```\n",
    ]

    if current_description:
        prompt_parts.append(f"\nCurrent description:\n{current_description}\n")
        prompt_parts.append("\nImprove or correct the above description according to the instructions.")
    else:
        prompt_parts.append("\nGenerate a job description according to the instructions.")

    prompt_parts.append("\n\nReturn ONLY the jobDescription comment block in Groovy format, nothing else.")

    prompt = ''.join(prompt_parts)

    try:
        print(f"  🤖 Asking GitHub Copilot for description for {jenkinsfile_path}...")

        # Try npm-based CLI first (github-copilot-cli)
        cmd = None
        try:
            # Check if github-copilot-cli is available
            subprocess.run(['github-copilot-cli', '--version'],
                           capture_output=True, timeout=2, check=True)
            cmd = ['github-copilot-cli', 'query']
        except (subprocess.SubprocessError, FileNotFoundError):
            # Fallback to 'copilot' command
            try:
                subprocess.run(['copilot', '--version'],
                               capture_output=True, timeout=2, check=True)
                cmd = ['copilot', '-p']
            except (subprocess.SubprocessError, FileNotFoundError):
                print("  ⚠️  No Copilot CLI command found")
                return None

        if not cmd:
            return None

        # Run copilot with the prompt via stdin
        result = subprocess.run(
            cmd + ["\n".join(prompt.splitlines())],
            capture_output=True,
            text=True,
            timeout=90, check=False
        )

        if result.returncode != 0:
            print(f"  ⚠️  Copilot CLI returned error: {result.stderr}")
            return None

        output = result.stdout

        # Extract the jobDescription comment from the output
        # Copilot might wrap it in various ways, so we need to be flexible
        match = re.search(r'/\*\*\s*jobDescription.*?\*/', output, re.DOTALL)
        if match:
            return match.group(0)

        # Try to find it in code blocks
        code_blocks = re.findall(r'```(?:groovy)?\n(.*?)\n```', output, re.DOTALL)
        for block in code_blocks:
            match = re.search(r'/\*\*\s*jobDescription.*?\*/', block, re.DOTALL)
            if match:
                return match.group(0)

        # If no match, check if the entire output looks like a comment
        if '/** jobDescription' in output:
            lines = output.split('\n')
            comment_lines = []
            in_comment = False
            for line in lines:
                if '/** jobDescription' in line:
                    in_comment = True
                if in_comment:
                    comment_lines.append(line)
                if in_comment and '*/' in line:
                    break
            if comment_lines:
                return '\n'.join(comment_lines)

        print("  ⚠️  Could not extract jobDescription from Copilot output")
        print(f"  Output preview: {output[:200]}...")
        return None

    except subprocess.TimeoutExpired:
        print("  ⚠️  Copilot CLI timed out")
        return None
    except (subprocess.SubprocessError, OSError) as e:
        print(f"  ⚠️  Error calling Copilot CLI: {e}")
        return None


def main():  # noqa: PLR0912, PLR0914, PLR0915
    """Main function to process jenkinsfiles based on command-line arguments."""
    parser = argparse.ArgumentParser(description='Manage job descriptions in .jenkinsfile files')
    parser.add_argument('--review', action='store_true', help='Review all files and report status')
    parser.add_argument('--add-all', action='store_true', help='Add descriptions to all files missing them')
    parser.add_argument('--file', type=str,
                        help='Process file(s) matching pattern (supports globs like "jenkins-pipelines/**/*.jenkinsfile" or directories)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be added without making changes')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of files to process')
    parser.add_argument('--use-copilot', action='store_true', help='Use GitHub Copilot CLI to improve descriptions')

    args = parser.parse_args()

    # Check if Copilot CLI is available when requested
    if args.use_copilot:
        if not check_copilot_cli_available():
            print("❌ GitHub Copilot CLI is not available.")
            print("   Please install it with: npm install -g @githubnext/github-copilot-cli")
            print("   See: https://docs.github.com/en/copilot/how-tos/set-up/install-copilot-cli")
            sys.exit(1)
        print("✅ GitHub Copilot CLI is available\n")

    sct_root = Path(__file__).parent.parent.resolve()

    if args.review:
        review_all_jenkinsfiles(sct_root)
        return

    if args.file:
        jenkinsfiles = expand_file_patterns(args.file, sct_root)
        if not jenkinsfiles:
            print(f"❌ No jenkinsfiles found matching pattern: {args.file}")
            sys.exit(1)
    else:
        jenkinsfiles = sorted(sct_root.glob('**/*.jenkinsfile'))

    if args.limit:
        jenkinsfiles = jenkinsfiles[:args.limit]

    print(f"Processing {len(jenkinsfiles)} .jenkinsfile files")
    if args.dry_run:
        print("(DRY RUN - no files will be modified)\n")
    else:
        print()

    added_count = 0
    skipped_count = 0
    error_count = 0
    improved_count = 0

    for jenkinsfile in jenkinsfiles:
        rel_path = jenkinsfile.relative_to(sct_root) if jenkinsfile.is_relative_to(sct_root) else jenkinsfile

        try:
            with open(jenkinsfile, 'r') as f:
                content = f.read()

            has_description = bool(re.search(r'/\*\*\s*jobDescription', content))

            if has_description and not args.use_copilot:
                print(f"  ⏭️  {rel_path}")
                skipped_count += 1
                continue

            # Generate or improve description
            if args.use_copilot:
                # Extract existing description if present
                existing_desc = ""
                match = re.search(r'/\*\*\s*jobDescription.*?\*/', content, re.DOTALL)
                if match:
                    existing_desc = match.group(0)

                # Use Copilot to generate/improve
                job_description = improve_description_with_copilot(str(jenkinsfile), existing_desc)

                if not job_description:
                    # Fallback to basic generation
                    print(f"  ⚠️  Falling back to basic generation for {rel_path}")
                    description = generate_description(str(jenkinsfile), content)
                    comment_lines = ["/** jobDescription"]
                    for line in description.split('\n'):
                        comment_lines.append(f"    {line}")
                    comment_lines.append("*/")
                    job_description = '\n'.join(comment_lines)
                elif has_description:
                    improved_count += 1
                    print(f"  ✨ Improved {rel_path}")
                else:
                    added_count += 1
                    print(f"  ✅ Generated {rel_path}")
            else:
                # Basic generation (original logic)
                description = generate_description(str(jenkinsfile), content)

                # Format as Groovy multi-line comment
                comment_lines = ["/** jobDescription"]
                for line in description.split('\n'):
                    comment_lines.append(f"    {line}")
                comment_lines.append("*/")
                job_description = '\n'.join(comment_lines)

            if args.dry_run:
                print(f"\n  📄 {rel_path}")
                print("     Would add:\n")
                for line in job_description.split('\n'):
                    print(f"     {line}")
                print()
                if not has_description:
                    added_count += 1
                continue

            # Insert or replace description
            if has_description and args.use_copilot:
                # Replace existing description
                new_content = re.sub(
                    r'/\*\*\s*jobDescription.*?\*/',
                    job_description,
                    content,
                    flags=re.DOTALL
                )
            else:
                # Insert after shebang and before first actual code
                lines = content.split('\n')
                insert_index = 0

                # Skip shebang if present
                if lines and lines[0].startswith('#!'):
                    insert_index = 1

                # Skip empty lines
                while insert_index < len(lines) and not lines[insert_index].strip():
                    insert_index += 1

                # Insert description
                lines.insert(insert_index, job_description)
                lines.insert(insert_index + 1, '')

                new_content = '\n'.join(lines)

            # Write back
            with open(jenkinsfile, 'w') as f:
                f.write(new_content)

            if not (args.use_copilot and has_description):
                print(f"  ✅ {rel_path}")
                added_count += 1

        except (OSError, yaml.YAMLError, ValueError, KeyError, subprocess.SubprocessError) as e:
            print(f"  ❌ {rel_path}: {e}")
            error_count += 1

    print(f"\n{'='*70}")
    print("Summary:")
    if args.use_copilot:
        print(f"  {'Would improve' if args.dry_run else 'Improved'}: {improved_count}")
    print(f"  {'Would add' if args.dry_run else 'Added'}: {added_count}")
    print(f"  Skipped (already has description): {skipped_count}")
    print(f"  Errors: {error_count}")
    print(f"{'='*70}")


if __name__ == '__main__':
    main()
