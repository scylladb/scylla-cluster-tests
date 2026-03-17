#!/bin/bash
# SessionEnd hook: back up the full session transcript for plan recovery and debugging.
INPUT=$(cat)

TRANSCRIPT=$(echo "$INPUT" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d.get('transcript_path', ''))
")

if [ -n "$TRANSCRIPT" ] && [ -f "$TRANSCRIPT" ]; then
    BACKUP_DIR="${CLAUDE_PROJECT_DIR:-.}/.claude/session-backups"
    mkdir -p "$BACKUP_DIR"
    cp "$TRANSCRIPT" "$BACKUP_DIR/session-$(date +%Y%m%d_%H%M%S).jsonl"
fi
