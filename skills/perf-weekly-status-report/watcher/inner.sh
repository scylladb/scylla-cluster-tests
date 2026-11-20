#!/bin/bash
# Runs inside the perf-watcher tmux session: fills prompt.md from the config and starts Claude.
# An interactive session never exits by itself, so `timeout` closes it:
# 4 hours for WEDNESDAY (three reports), 90 minutes for the other modes.
set -u
MODE="$1"
CONFIG="$2"
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=/dev/null
. "$CONFIG"
LIMIT=90m
[ "$MODE" = "WEDNESDAY" ] && LIMIT=4h

ALLOWED="Bash,Read,Write,Edit,Glob,Grep,Skill,ToolSearch,Artifact,ArtifactData"
ALLOWED+=",mcp__claude_ai_Slack__slack_send_message"
ALLOWED+=",mcp__claude_ai_Gmail__create_draft,mcp__claude_ai_Gmail__list_drafts,mcp__claude_ai_Gmail__get_draft"
ALLOWED+=",mcp__claude_ai_Atlassian_MCP__getJiraIssue,mcp__claude_ai_Atlassian_MCP__searchJiraIssuesUsingJql"

PROMPT="$(
    OWNER_NAME="$OWNER_NAME" SLACK_USER_ID="$SLACK_USER_ID" REPORTS_DIR="$REPORTS_DIR" \
    WATCHER_HOME="$WATCHER_HOME" SCT_PYTHON="$SCT_PYTHON" DASHBOARD_URL="${DASHBOARD_URL:-}" \
    DASHBOARD_SHARE_URL="${DASHBOARD_SHARE_URL:-}" EMAIL_TO="$EMAIL_TO" SIGNATURE="$SIGNATURE" \
    python3 -c '
import os, re, sys
text = open(sys.argv[1], encoding="utf-8").read()
print(re.sub(r"\{\{(\w+)\}\}", lambda m: os.environ.get(m.group(1)) or "(not set)", text))
' "$HERE/prompt.md"
)

MODE: $MODE
NOW: $(date -Is)
When everything for this mode is done, write the summary file and stop; a timer closes the session."

cd "$SCT_DIR" || { echo "$(date -Is) SCT_DIR missing: $SCT_DIR" >>"$WATCHER_HOME/logs/cron.log"; exit 1; }
# The prompt must come before --allowedTools, or it is read as one more tool rule.
# --foreground keeps Claude in the terminal's foreground; without it the terminal stops the
# process (state T) the moment it draws its screen, and the run hangs doing nothing.
timeout --foreground "$LIMIT" claude "$PROMPT" --allowedTools "$ALLOWED"
echo "$(date -Is) $MODE session ended (exit $?)" >>"$WATCHER_HOME/logs/cron.log"
