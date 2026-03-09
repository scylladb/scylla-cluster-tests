"""Parser for Jenkins pipeline (.jenkinsfile) files.

Extracts pipeline function names and their named parameters from Jenkinsfile
files used in the SCT CI system. Uses regex-based parsing targeting the
consistent pattern used across all ~997 pipeline files.
"""

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

# Known pipeline functions that contain test_config and should be linted
LINTABLE_PIPELINE_FUNCTIONS = frozenset(
    {
        "longevityPipeline",
        "managerPipeline",
        "rollingUpgradePipeline",
        "perfRegressionParallelPipeline",
        "perfSearchBestConfigParallelPipeline",
        "artifactsPipeline",
        "jepsenPipeline",
        "rollingOperatorUpgradePipeline",
    }
)

# Pipeline functions that have no test_config and should be skipped
SKIPPED_PIPELINE_FUNCTIONS = frozenset(
    {
        "byoLongevityPipeline",
        "createTestJobPipeline",
    }
)

# Regex to find the pipeline function call: functionName( ... )
# Captures: function_name and the arguments block between parens
_PIPELINE_CALL_RE = re.compile(
    r"^(\w+Pipeline)\s*\(",
    re.MULTILINE,
)

# Regex to extract named parameters from the argument block.
# Handles: key: 'value', key: "value", key: '''value''', key: """value""",
#           key: true/false, key: [list], key: number
_NAMED_PARAM_RE = re.compile(
    r"""
    (\w+)                       # parameter name
    \s*:\s*                     # colon separator
    (                           # value group
        '{3}(.*?)'{3}           # triple-single-quoted string
      | "{3}(.*?)"{3}           # triple-double-quoted string
      | '([^']*)'              # single-quoted string
      | "([^"]*)"              # double-quoted string
      | (\[.*?\])               # list/map literal (non-greedy)
      | (true|false)            # boolean
      | (\d+(?:\.\d+)?)        # number
    )
    """,
    re.VERBOSE | re.DOTALL,
)


@dataclass
class PipelineConfig:
    """Structured representation of a parsed Jenkins pipeline file.

    Attributes:
        file_path: Path to the .jenkinsfile.
        pipeline_function: Name of the pipeline function (e.g. "longevityPipeline").
        test_config: List of YAML config file paths extracted from test_config param.
        params: All named parameters from the pipeline function call.
    """

    file_path: str
    pipeline_function: str
    test_config: list[str]
    params: dict[str, str] = field(default_factory=dict)

    @property
    def backend(self) -> str | None:
        return self.params.get("backend")

    @property
    def region(self) -> str | None:
        return self.params.get("region")

    @property
    def gce_datacenter(self) -> str | None:
        return self.params.get("gce_datacenter")


def _extract_string_value(match: re.Match) -> str:
    """Extract the actual string value from a regex match of a parameter value.

    The regex has multiple capture groups for different quoting styles.
    Returns the content of whichever group matched.
    """
    # Groups: 3=triple-single, 4=triple-double, 5=single, 6=double,
    #         7=list/map, 8=boolean, 9=number
    for group_idx in (3, 4, 5, 6, 7, 8, 9):
        val = match.group(group_idx)
        if val is not None:
            return val
    # Fallback: return the full value group (group 2) stripped of quotes
    return match.group(2).strip("'\"")


def _parse_test_config(raw_value: str) -> list[str]:
    """Parse test_config value into a list of config file paths.

    Handles three formats:
    1. Single file string: 'test-cases/foo.yaml'
    2. Python/JSON list string: '["test-cases/a.yaml", "configs/b.yaml"]'
    3. Already a list literal: ["test-cases/a.yaml", "configs/b.yaml"]
    """
    value = raw_value.strip()
    if not value:
        return []

    # Try JSON array parse first (handles both ["a","b"] and ['a','b'] after normalization)
    if value.startswith("["):
        try:
            # Replace single quotes with double quotes for JSON compatibility
            json_str = value.replace("'", '"')
            parsed = json.loads(json_str)
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        except json.JSONDecodeError, ValueError:
            pass

    # Single file string
    return [value]


def _extract_params_block(content: str, call_start: int) -> str | None:
    """Extract the parenthesized argument block from a pipeline function call.

    Handles nested parentheses (e.g. in timeout: [time: 530, unit: 'MINUTES']).
    Returns the content between the outermost parentheses, or None if unbalanced.
    """
    # Find the opening paren
    paren_pos = content.index("(", call_start)
    depth = 0
    start = paren_pos + 1
    for i in range(paren_pos, len(content)):
        char = content[i]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return content[start:i]
    return None


def parse_jenkinsfile(path: Path) -> PipelineConfig | None:
    """Parse a .jenkinsfile and extract pipeline parameters.

    Args:
        path: Path to the .jenkinsfile to parse.

    Returns:
        PipelineConfig with extracted parameters, or None if the file
        doesn't contain a lintable pipeline function call (e.g. raw
        pipeline {} blocks, byoLongevityPipeline, createTestJobPipeline).
    """
    content = path.read_text(encoding="utf-8")

    # Find pipeline function call
    match = _PIPELINE_CALL_RE.search(content)
    if not match:
        logger.debug("No pipeline function call found in %s", path)
        return None

    func_name = match.group(1)

    # Skip pipeline functions without test_config
    if func_name in SKIPPED_PIPELINE_FUNCTIONS:
        logger.debug("Skipping %s (function %s has no test_config)", path, func_name)
        return None

    if func_name not in LINTABLE_PIPELINE_FUNCTIONS:
        logger.warning("Unknown pipeline function '%s' in %s", func_name, path)
        return None

    # Extract the argument block
    params_block = _extract_params_block(content, match.start())
    if params_block is None:
        logger.warning("Unbalanced parentheses in %s", path)
        return None

    # Extract named parameters
    params = {}
    for param_match in _NAMED_PARAM_RE.finditer(params_block):
        param_name = param_match.group(1)
        param_value = _extract_string_value(param_match)
        params[param_name] = param_value

    # Extract test_config
    raw_test_config = params.get("test_config", "")
    test_config = _parse_test_config(raw_test_config)

    if not test_config:
        logger.debug("No test_config found in %s (function %s)", path, func_name)
        return None

    return PipelineConfig(
        file_path=str(path),
        pipeline_function=func_name,
        test_config=test_config,
        params=params,
    )


def discover_pipeline_files(root: Path) -> list[Path]:
    """Recursively find all .jenkinsfile files under the given root directory.

    Args:
        root: Root directory to search (typically jenkins-pipelines/).

    Returns:
        Sorted list of Path objects for all discovered .jenkinsfile files.
    """
    if not root.is_dir():
        raise FileNotFoundError(f"Pipeline directory not found: {root}")
    files = sorted(root.rglob("*.jenkinsfile"))
    logger.debug("Discovered %d .jenkinsfile files under %s", len(files), root)
    return files
