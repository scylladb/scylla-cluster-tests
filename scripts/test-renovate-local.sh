#!/usr/bin/env bash
# Usage:
#   ./scripts/test-renovate-local.sh --jsonata-only   (no PAT, tests JSONata against live S3)
#   ./scripts/test-renovate-local.sh                  (full Renovate with LOCAL config, needs PAT + Docker)
set -euo pipefail

REPO="scylladb/scylla-cluster-tests"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PAT_FILE="$HOME/.config/renovate/github-pat"
LOG_FILE="/tmp/renovate-local-run.log"

if [[ "${1:-}" == "--jsonata-only" ]]; then
    echo "=== Testing JSONata transform against live S3 data ==="

    if ! command -v node &>/dev/null; then
        echo "ERROR: node is required" >&2
        exit 1
    fi

    TMPDIR="${TMPDIR:-/tmp}/renovate-test"
    if [[ ! -d "$TMPDIR/node_modules/jsonata" ]]; then
        echo "Installing jsonata npm package..."
        npm install --prefix "$TMPDIR" jsonata >/dev/null 2>&1
    fi

    TRANSFORM=$(python3 -c "
import json, sys
with open('$REPO_ROOT/renovate.json') as f:
    cfg = json.load(f)
ds = cfg.get('customDatasources', {}).get('scylla-doctor', {})
templates = ds.get('transformTemplates', [])
if templates:
    print(templates[0])
else:
    print('ERROR: no transformTemplates found', file=sys.stderr)
    sys.exit(1)
")

    echo "Transform expression:"
    echo "  $TRANSFORM"
    echo

    NODE_PATH="$TMPDIR/node_modules" node -e "
const jsonata = require('jsonata');
const https = require('https');

const url = 'https://s3.amazonaws.com/downloads.scylladb.com?prefix=downloads/scylla-doctor/tar/&delimiter=/';

https.get(url, (res) => {
    let data = '';
    res.on('data', chunk => data += chunk);
    res.on('end', async () => {
        const lines = data.split('\\n').filter(l => l.trim());
        const input = { releases: lines.map(l => ({ version: l })) };

        console.log('S3 response: ' + lines.length + ' line(s), ' + data.length + ' bytes');
        console.log();

        try {
            const transform = process.argv[1];
            const expr = await jsonata(transform);
            const result = await expr.evaluate(input);

            console.log('=== Transform result ===');
            console.log(JSON.stringify(result, null, 2));
            console.log();

            const versions = (result.releases || []).map(r => r.version);
            if (versions.length === 0) {
                console.log('❌ NO VERSIONS EXTRACTED — transform is broken');
                process.exit(1);
            } else {
                console.log('✅ Extracted ' + versions.length + ' versions: ' + versions.join(', '));

                const current = '1.10';
                const newer = versions.filter(v => {
                    const [ma, mi, pa] = v.split('.').map(Number);
                    const [ca, ci, cp] = current.split('.').map(Number);
                    return ma > ca || (ma === ca && (mi || 0) > (ci || 0)) || (ma === ca && (mi || 0) === (ci || 0) && (pa || 0) > (cp || 0));
                });
                if (newer.length > 0) {
                    console.log('✅ Updates available (current=' + current + '): ' + newer.join(', '));
                } else {
                    console.log('⚠️  No updates newer than ' + current);
                }
            }
        } catch (e) {
            console.error('❌ JSONata error:', e.message);
            process.exit(1);
        }
    });
}).on('error', (e) => {
    console.error('❌ HTTP error:', e.message);
    process.exit(1);
});
" "$TRANSFORM"
    exit $?
fi

echo "=== Renovate local dry-run (using LOCAL renovate.json) ==="

if [[ -n "${RENOVATE_TOKEN:-}" ]]; then
    TOKEN="$RENOVATE_TOKEN"
elif [[ -f "$PAT_FILE" ]]; then
    TOKEN="$(cat "$PAT_FILE")"
else
    echo "ERROR: No GitHub PAT found." >&2
    echo "" >&2
    echo "Either:" >&2
    echo "  export RENOVATE_TOKEN=ghp_..." >&2
    echo "  or save PAT to $PAT_FILE" >&2
    echo "" >&2
    echo "Create a fine-grained PAT at https://github.com/settings/personal-access-tokens/new" >&2
    echo "  Scope: $REPO | Permissions: Contents=Read, Pull requests=Read" >&2
    echo "" >&2
    echo "Or run: $0 --jsonata-only  (no PAT needed)" >&2
    exit 1
fi

if command -v docker &>/dev/null; then
    RUNTIME=docker
elif command -v podman &>/dev/null; then
    RUNTIME=podman
else
    echo "ERROR: docker or podman required" >&2
    exit 1
fi

echo "Using: $RUNTIME"
echo "Testing LOCAL renovate.json from: $REPO_ROOT/renovate.json"
echo

$RUNTIME run --rm \
    -v "$REPO_ROOT:/usr/src/app:ro" \
    -e RENOVATE_TOKEN="$TOKEN" \
    -e RENOVATE_PLATFORM=local \
    -e RENOVATE_DRY_RUN=full \
    -e LOG_LEVEL=debug \
    -e LOG_FORMAT=json \
    -w /usr/src/app \
    renovate/renovate:latest 2>&1 | tee "$LOG_FILE" | \
    python3 -c "
import sys, json

LEVELS = {10:'TRACE',20:'DEBUG',30:'INFO ',40:'WARN ',50:'ERROR'}
found_scylla_doctor = False
result_status = None

for line in sys.stdin:
    line = line.strip()
    if not line.startswith('{'):
        continue
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        continue

    level_num = obj.get('level', 0)
    level = LEVELS.get(level_num, '?????')
    msg = obj.get('msg', '')
    full = json.dumps(obj)

    if level_num >= 40:
        ts = obj.get('time','')[:19].replace('T',' ')
        print(f'{ts}  {level}  {msg}')

    if 'scylla-doctor' in full or 'scylla_doctor' in full or 'custom.' in msg.lower():
        found_scylla_doctor = True
        ts = obj.get('time','')[:19].replace('T',' ')
        print(f'{ts}  {level}  {msg}')

        for k, v in obj.items():
            if k in ('name','hostname','pid','level','logContext','v','msg','time','repository'):
                continue
            vs = json.dumps(v) if isinstance(v, (dict, list)) else str(v)
            if 'scylla' in vs.lower() or 'doctor' in vs.lower() or 'custom' in vs.lower():
                if len(vs) > 300:
                    vs = vs[:300] + '...'
                print(f'    {k}: {vs}')

    if 'no-result' in full.lower() or 'no result' in full.lower():
        result_status = 'FAIL'
    elif 'Lookup statistics' in msg:
        stats = {k: v for k, v in obj.items() if 'custom' in k.lower() or 'scylla' in k.lower()}
        if stats:
            for k, v in stats.items():
                print(f'    {k}: {json.dumps(v)}')

print()
if result_status == 'FAIL':
    print('❌ scylla-doctor lookup FAILED (no-result)')
elif found_scylla_doctor:
    print('✅ scylla-doctor lookup was processed (check above for details)')
else:
    print('⚠️  No scylla-doctor references found in log')
"

echo
echo "Full JSON log: $LOG_FILE"
echo "Filter scylla-doctor: python3 -c \"import json,sys;[print(json.dumps(json.loads(l),indent=2)) for l in open('$LOG_FILE') if l.strip().startswith('{') and 'scylla-doctor' in l]\""
