#!/bin/bash
# Perf weekly watcher entry point, called from crontab:
#   run.sh SUNDAY|MONITOR|WEDNESDAY [config.env]
# Starts an interactive Claude Code session in a detached tmux session, because `claude -p`
# started from cron loads no claude.ai connectors (Slack, Gmail) and no Artifact tool.
# Watch a run live: tmux -S /tmp/tmux-$(id -u)/default attach -t perf-watcher
set -u
MODE="${1:?usage: run.sh SUNDAY|MONITOR|WEDNESDAY [config.env]}"
CONFIG="${2:-$HOME/perf-watcher/config.env}"
HERE="$(cd "$(dirname "$0")" && pwd)"

case "$MODE" in SUNDAY|MONITOR|WEDNESDAY) ;; *) echo "unknown mode: $MODE" >&2; exit 2 ;; esac
[ -r "$CONFIG" ] || { echo "config not found: $CONFIG (copy config.example.env)" >&2; exit 2; }
# shellcheck source=/dev/null
. "$CONFIG"
mkdir -p "$WATCHER_HOME/logs" "$WATCHER_HOME/work"

TMUX_CMD="tmux -S /tmp/tmux-$(id -u)/default"
if $TMUX_CMD has-session -t perf-watcher 2>/dev/null; then
    # The previous run is still inside its time limit; never run two at once.
    echo "$(date -Is) previous run still active, skipping $MODE" >>"$WATCHER_HOME/logs/cron.log"
    exit 0
fi

$TMUX_CMD new-session -d -s perf-watcher -x 220 -y 60 "$HERE/inner.sh $MODE $CONFIG"
echo "$(date -Is) started $MODE" >>"$WATCHER_HOME/logs/cron.log"
