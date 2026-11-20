# Install the Perf Weekly Watcher

The watcher runs this skill unattended from cron: it checks the week's master and release runs,
messages you on Slack about CapacityReservationError failures, and on Wednesday builds the
reports and creates draft emails. Files are in [../watcher/](../watcher/).

| Mode | When | What it does |
|---|---|---|
| SUNDAY | Sunday 09:07 | Inventory of runs since the previous Wednesday 08:00 (the last report); Slack DM if any failed on capacity |
| MONITOR | Sunday 17:07, Monday and Tuesday 09:07 and 17:07 | New triggers and status changes; Slack DM only for new capacity failures |
| WEDNESDAY | Wednesday 08:07 | Master, release and microbenchmark reports, Perf Weekly Trends dashboard refresh; drafts only for reports whose runs are all complete; Slack summary |

It never sends email, never re-triggers jobs and never deletes anything.

## Phase 1: Check Access

**Entry:** a Linux machine that stays on and logged in during the week (cron plus your desktop
session; the watcher uses your logins).

1. `argus run list --test-id a0063c73-efcf-4878-988d-72af779dc59d --limit 1 --url https://argus.scylladb.com`
   returns JSON without opening a browser.
2. The SCT KeyStore works from your checkout: `.venv/bin/python -c "from sdcm.keystore import KeyStore; KeyStore().get_json('jenkins.json')"`.
3. Claude Code is logged in with your claude.ai account (not an API key), and Slack and Gmail are
   connected under claude.ai connectors (`claude mcp list` shows them Connected).
4. For the microbenchmark report: you can edit the dashboard artifact. Its owner shares it with
   edit access, or you create your own with `perf-microbenchmark-trends` and use its URL.
5. For the trends dashboard: you can edit the artifact named in
   [update-trends-dashboard.md](update-trends-dashboard.md). Without edit access the watcher skips
   it and says so in the Wednesday Slack summary.

**Exit:** all checks pass (4 and 5 are optional; the matching dashboard is skipped without them).

## Phase 2: Configure

1. `mkdir -p ~/perf-watcher && cp watcher/config.example.env ~/perf-watcher/config.env`
2. Fill in `OWNER_NAME`, `SLACK_USER_ID`, `SCT_DIR`, `SCT_PYTHON`, `SIGNATURE`, and the dashboard
   URLs (empty `DASHBOARD_URL` skips the microbenchmark report). Keep the file outside the repo:
   a share link opens the dashboard for anyone who has it.

**Exit:** `~/perf-watcher/config.env` filled in.

## Phase 3: Test One Run

1. Run `watcher/run.sh MONITOR` by hand from a normal terminal. It starts a tmux session named
   `perf-watcher` on the default tmux server. Do not start it from inside a Claude Code session:
   the inherited `CLAUDE_CODE_*` variables make the new session hang without doing anything.
2. Watch it: `tmux -S /tmp/tmux-$(id -u)/default attach -t perf-watcher` (detach with Ctrl-b d).
   Answer nothing: a run that stops at a permission prompt shows a tool missing from `ALLOWED` in
   `inner.sh`.
3. Check `~/perf-watcher/state.json` and `~/perf-watcher/logs/summary-*-MONITOR.md`.

**Exit:** the summary file exists and lists the week's runs.

## Phase 4: Schedule

1. Copy [crontab.example](../watcher/crontab.example), replace `SCT_DIR`, `USER` and the uid, and
   install it with `crontab -e`.
2. `crontab -l` shows the four entries.

**Exit:** the schedule is installed. `~/perf-watcher/logs/cron.log` records every start and end.

## Why Interactive Claude in tmux, Not `claude -p`

A `claude -p` started from cron, with no parent Claude session, connects no claude.ai connectors
(no Slack, no Gmail) and has no Artifact tool, so it could neither alert you nor update the
dashboard. An interactive session started in a detached tmux session loads them. Because an
interactive session does not exit when it finishes, `inner.sh` closes it with `timeout`
(90 minutes, 4 hours on Wednesday), and `run.sh` skips a start while the previous session is alive.
