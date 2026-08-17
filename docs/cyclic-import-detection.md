# Cyclic Import Detection

SCT uses a pre-commit hook to detect cyclic imports in `sdcm/`. This prevents new circular dependencies from being introduced while allowing existing ones to be fixed incrementally.

## Quick Reference

```bash
# Check for new cyclic imports (what pre-commit runs):
uv run python scripts/detect_cyclic_imports.py

# Show all existing cycles:
uv run python scripts/detect_cyclic_imports.py --verbose

# Regenerate allowlist after fixing a cycle:
uv run python scripts/detect_cyclic_imports.py --update-allowlist
```

## How It Works

1. The script parses every `.py` file under `sdcm/` using Python's `ast` module
2. Builds a directed graph of internal (`sdcm.*`) import edges
3. Finds strongly connected components (cycles) via Tarjan's algorithm
4. Compares found cyclic edges against the allowlist (`data/known_cyclic_imports.yml`)
5. If any NEW edge participates in a cycle that isn't allowlisted — **exit 1** (blocks commit)

Key behaviors:
- **TYPE_CHECKING-guarded imports are excluded** — these don't create runtime cycles
- **Function-level (lazy) imports are excluded** — imports inside `def`/`async def` bodies are deferred to call time and don't cause import-time cycles
- **Only `sdcm.*` internal imports are tracked** — stdlib and third-party are ignored
- **Relative imports are resolved** — `from .base import X` is correctly mapped to its full module path

## Fixing a Cyclic Import

### Workflow

```bash
# 1. See what cycles exist:
uv run python scripts/detect_cyclic_imports.py --verbose | grep "sdcm.X"

# 2. Investigate a specific cycle:
python3 -c "
import ast
from pathlib import Path
tree = ast.parse(Path('sdcm/module_a.py').read_text())
for node in ast.walk(tree):
    if isinstance(node, ast.ImportFrom) and node.module and 'module_b' in node.module:
        names = [a.name for a in node.names]
        print(f'line {node.lineno}: from {node.module} import {\", \".join(names)}')
"

# 3. Apply one of the fix strategies below

# 4. Regenerate the allowlist (edge count should decrease):
uv run python scripts/detect_cyclic_imports.py --update-allowlist

# 5. Verify the fix:
uv run python scripts/detect_cyclic_imports.py
# Exit 0 = good

# 6. Commit both the code fix AND the updated allowlist:
git add sdcm/... data/known_cyclic_imports.yml
git commit -m "fix(imports): break cyclic import between module_a and module_b"
```

### Strategy 1: Move to `TYPE_CHECKING` guard (most common)

**When**: The import is only needed for type annotations, not called at runtime.

```python
# BEFORE — creates cycle:
from sdcm.cluster import BaseCluster

def restart_node(cluster: BaseCluster) -> None:
    cluster.restart()

# AFTER — breaks cycle:
from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sdcm.cluster import BaseCluster

def restart_node(cluster: BaseCluster) -> None:
    cluster.restart()  # still works — object is passed in at runtime
```

**Why it works**: `from __future__ import annotations` makes all type annotations lazy strings. The actual class is never imported at module load time — it's only used by type checkers (mypy, pyright) and IDE tooling.

**Requirements**:
- Add `from __future__ import annotations` at the top of the file (after the license header)
- The imported symbol must NOT be used at runtime (no `isinstance()`, no calling constructors, no accessing class attributes directly)

### Strategy 2: Lazy import inside a function

**When**: You need the symbol at runtime, but only in one specific function.

```python
# BEFORE — creates cycle at module load:
from sdcm.cluster import TestConfig

def get_skip_stages():
    return TestConfig().tester_obj().skip_test_stages

# AFTER — defers import to call time:
def get_skip_stages():
    from sdcm.cluster import TestConfig  # noqa: PLC0415
    return TestConfig().tester_obj().skip_test_stages
```

**Why it works**: The import only executes when the function is called, by which time both modules are fully loaded.

**Note**: The detection script correctly recognizes function-level imports as NOT creating import-time cycles — they won't appear in the dependency graph or trigger the pre-commit hook. This is the most common pattern used in SCT to break cycles.

### Strategy 3: Extract shared code to a third module

**When**: Two modules legitimately need each other's functionality at runtime.

```
BEFORE (cycle):
  sdcm/utils/aws_region.py  →  imports tags_as_ec2_tags from aws_utils
  sdcm/utils/aws_utils.py   →  imports AwsRegion from aws_region

AFTER (no cycle):
  sdcm/utils/aws_tags.py    ←  new file, contains tags_as_ec2_tags
  sdcm/utils/aws_region.py  →  imports from aws_tags (no cycle)
  sdcm/utils/aws_utils.py   →  imports from aws_tags + aws_region (one-way)
```

**Why it works**: The shared dependency is moved to a leaf module that doesn't import back.

**When to use this**:
- Both imports are used at runtime (can't use TYPE_CHECKING)
- The imported function/class logically belongs in a shared utility anyway
- Multiple modules need the same symbol

### Strategy 4: Pass dependencies as parameters (dependency injection)

**When**: A utility function imports a heavy module just to access one thing.

```python
# BEFORE — decorators.py imports cluster.py for check_cluster_layout:
from sdcm.cluster import BaseCluster

def validate(cluster: BaseCluster):
    if not cluster.is_ready():
        raise RuntimeError("Cluster not ready")

# AFTER — accept the check as a callable parameter:
from typing import Protocol

class ClusterLike(Protocol):
    def is_ready(self) -> bool: ...

def validate(cluster: ClusterLike):
    if not cluster.is_ready():
        raise RuntimeError("Cluster not ready")
```

**Why it works**: The function depends on an interface (Protocol), not a concrete class. No import needed.

## Understanding the Allowlist

The file `data/known_cyclic_imports.yml` contains directed edges:

```yaml
edges:
  - source: sdcm.cluster
    target: sdcm.utils.decorators
  - source: sdcm.utils.decorators
    target: sdcm.cluster
```

Each entry means "module `source` imports module `target` AND this edge is part of a cycle." A mutual cycle (A→B and B→A) has TWO entries.

**Rules**:
- Never edit the allowlist manually — always use `--update-allowlist`
- When you fix a cycle, the edge count should **decrease** after regeneration
- If you accidentally introduce a new cycle, the pre-commit hook will block your commit

## Common Scenarios

### "I'm adding a new import and pre-commit fails"

```
ERROR: New cyclic import edge(s) detected in sdcm/:
  sdcm.foo -> sdcm.bar (NEW — not in allowlist)
```

Options:
1. **Fix it**: Use `TYPE_CHECKING` or lazy import (preferred)
2. **Allowlist it**: `uv run python scripts/detect_cyclic_imports.py --update-allowlist` — but this should be a last resort

### "I fixed a cycle but the edge count didn't change"

The cycle may be part of a larger strongly connected component. Example: if A→B→C→A exists, fixing A→B might still leave B→C→A→...→B as a cycle through other paths. Use `--verbose` to trace the full cycle.

### "I want to see which cycles involve my module"

```bash
uv run python scripts/detect_cyclic_imports.py --verbose | grep "sdcm.my_module"
```

### "I moved code and the allowlist needs updating"

```bash
uv run python scripts/detect_cyclic_imports.py --update-allowlist
git add data/known_cyclic_imports.yml
# Include in the same commit as the code move
```

## Picking Low-Hanging Fruit

The simplest cycles to fix are 2-node mutual cycles where one direction is typing-only:

```bash
# Find simple A <-> B mutual cycles:
uv run python scripts/detect_cyclic_imports.py --verbose | sort | \
  python3 -c "
import sys
from collections import defaultdict
edges = set()
for line in sys.stdin:
    if ' -> ' in line.strip():
        s, t = line.strip().split(' -> ')
        edges.add((s, t))
mutual = [(a,b) for a,b in edges if (b,a) in edges and a < b]
print(f'Found {len(mutual)} mutual (2-node) cycles:')
for a, b in sorted(mutual):
    print(f'  {a} <-> {b}')
"
```

For each mutual pair, check if one direction is only used for type annotations — that's a quick `TYPE_CHECKING` fix.
