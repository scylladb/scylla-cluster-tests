#!/bin/bash
# SessionEnd hook: back up full session transcript and extract plans separately.
# Plans go to .claude/plan-backups/, full sessions go to .claude/session-backups/.
INPUT=$(cat)

PROJECT_DIR="${CLAUDE_PROJECT_DIR:-.}"
TRANSCRIPT=$(echo "$INPUT" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d.get('transcript_path', ''))
")

if [ -z "$TRANSCRIPT" ] || [ ! -f "$TRANSCRIPT" ]; then
    exit 0
fi

TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# ── 1. Save full session transcript ──────────────────────────────────
SESSION_DIR="$PROJECT_DIR/.claude/session-backups"
mkdir -p "$SESSION_DIR"
cp "$TRANSCRIPT" "$SESSION_DIR/session-${TIMESTAMP}.jsonl"

# ── 2. Extract plans into separate files ─────────────────────────────
# Plans are assistant messages containing markdown with "## " headers that
# look like implementation plans. Extract them so they're easy to find.
python3 - "$TRANSCRIPT" "$PROJECT_DIR/.claude/plan-backups" "$TIMESTAMP" <<'PYEOF'
import json
import os
import sys

transcript_path = sys.argv[1]
plan_dir = sys.argv[2]
timestamp = sys.argv[3]

# Heuristics: a plan message contains multiple "## " headers and keywords
PLAN_KEYWORDS = {"implementation", "phase", "goals", "current state", "problem statement",
                 "testing requirements", "risk mitigation", "success criteria"}

plans_found = 0
with open(transcript_path) as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue

        if msg.get("role") != "assistant":
            continue

        # Extract text content from the message
        content = msg.get("content", "")
        if isinstance(content, list):
            text_parts = [p.get("text", "") for p in content if isinstance(p, dict) and p.get("type") == "text"]
            content = "\n".join(text_parts)

        if not isinstance(content, str):
            continue

        content_lower = content.lower()
        h2_count = content_lower.count("\n## ")
        keyword_hits = sum(1 for kw in PLAN_KEYWORDS if kw in content_lower)

        # A plan typically has 3+ sections and 2+ keyword hits
        if h2_count >= 3 and keyword_hits >= 2:
            os.makedirs(plan_dir, exist_ok=True)
            plans_found += 1
            # Try to extract a title from the first "# " line
            title = "untitled"
            for cline in content.split("\n"):
                if cline.startswith("# ") and not cline.startswith("## "):
                    title = cline[2:].strip().lower()
                    title = "".join(c if c.isalnum() or c in "-_ " else "" for c in title)
                    title = title.replace(" ", "-")[:60]
                    break

            plan_file = os.path.join(plan_dir, f"plan-{timestamp}-{plans_found}-{title}.md")
            with open(plan_file, "w") as pf:
                pf.write(content)

if plans_found > 0:
    print(f"Saved {plans_found} plan(s) to {plan_dir}", file=sys.stderr)
PYEOF
