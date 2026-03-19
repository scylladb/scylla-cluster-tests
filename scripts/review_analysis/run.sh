#!/usr/bin/env bash
# PR Review Taxonomy Analysis Pipeline
# Runs all 4 phases: collect → classify → analyze → report
set -euo pipefail

# Script directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Defaults
CACHE_DIR="${SCRIPT_DIR}/cache"
DELAY="2.0"
SINCE="2019-01-01"
OUTPUT="${SCRIPT_DIR}/reports/pr-review-taxonomy-report.md"
NO_COLLECT=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --cache-dir)
            CACHE_DIR="$2"
            shift 2
            ;;
        --delay)
            DELAY="$2"
            shift 2
            ;;
        --since)
            SINCE="$2"
            shift 2
            ;;
        --output)
            OUTPUT="$2"
            shift 2
            ;;
        --no-collect)
            NO_COLLECT=true
            shift
            ;;
        --help)
            cat <<EOF
Usage: ./run.sh [OPTIONS]

PR Review Taxonomy Analysis Pipeline
Runs all 4 phases: collect → classify → analyze → report

Options:
  --cache-dir DIR       Cache directory (default: ./cache)
  --delay SECONDS       API request delay in seconds (default: 2.0)
  --since DATE          Fetch PRs since this date (default: 2019-01-01)
  --output FILE         Output report path (default: ./reports/pr-review-taxonomy-report.md)
  --no-collect          Skip phase 1 (collect) if cache already exists
  --help                Show this help message

Examples:
  # First run (takes ~7 hours)
  ./run.sh --since 2019-01-01 --delay 2.0

  # Quarterly update (skip collection, ~30 minutes)
  ./run.sh --no-collect

  # Fetch only new PRs since a date
  ./run.sh --since 2026-01-01

EOF
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            echo "Use --help for usage information" >&2
            exit 1
            ;;
    esac
done

# Derived paths
CACHE_RAW="${CACHE_DIR}/raw"
CACHE_CLASSIFIED="${CACHE_DIR}/classified"
REPORT_DATA="${CACHE_DIR}/report_data.json"
REPORTS_DIR="$(dirname "$OUTPUT")"
TAXONOMY="${SCRIPT_DIR}/taxonomy.yaml"

# Create necessary directories
mkdir -p "$CACHE_RAW" "$CACHE_CLASSIFIED" "$REPORTS_DIR"

# Print header
echo "================================================================================"
echo "PR Review Taxonomy Analysis Pipeline"
echo "================================================================================"
echo ""
echo "Configuration:"
echo "  Cache directory:  $CACHE_DIR"
echo "  API delay:        ${DELAY}s"
echo "  Since date:       $SINCE"
echo "  Output report:    $OUTPUT"
echo "  Taxonomy file:    $TAXONOMY"
echo ""

# Determine which phases to run
PHASES=()
[[ "$NO_COLLECT" == false ]] && PHASES+=("collect") || echo "  (Skipping Phase 1: collect)"
PHASES+=("classify" "analyze" "report")

echo "Phases to run: ${PHASES[*]}"
echo ""
echo "================================================================================"
echo ""

# Phase 1: Collect (unless --no-collect)
if [[ "$NO_COLLECT" == false ]]; then
    echo "[Phase 1/4] Collecting PR data from GitHub..."
    echo "  Command: python collect.py --cache-dir $CACHE_RAW --delay $DELAY --since $SINCE --resume"
    echo ""
    python "$SCRIPT_DIR/collect.py" \
        --cache-dir "$CACHE_RAW" \
        --delay "$DELAY" \
        --since "$SINCE" \
        --resume
    echo ""
    echo "✓ Phase 1 complete"
    echo ""
fi

# Phase 2: Classify
echo "[Phase 2/4] Classifying comments using taxonomy..."
echo "  Command: python classify.py --input $CACHE_RAW --output $CACHE_CLASSIFIED --taxonomy $TAXONOMY"
echo ""
python "$SCRIPT_DIR/classify.py" \
    --input "$CACHE_RAW" \
    --output "$CACHE_CLASSIFIED" \
    --taxonomy "$TAXONOMY"
echo ""
echo "✓ Phase 2 complete"
echo ""

# Phase 3: Analyze
echo "[Phase 3/4] Analyzing trends and patterns..."
echo "  Command: python analyze.py --input $CACHE_CLASSIFIED --output $REPORT_DATA --taxonomy $TAXONOMY"
echo ""
python "$SCRIPT_DIR/analyze.py" \
    --input "$CACHE_CLASSIFIED" \
    --output "$REPORT_DATA" \
    --taxonomy "$TAXONOMY"
echo ""
echo "✓ Phase 3 complete"
echo ""

# Phase 4: Report
echo "[Phase 4/4] Generating report..."
echo "  Command: python report.py --input $REPORT_DATA --output $OUTPUT --taxonomy $TAXONOMY"
echo ""
python "$SCRIPT_DIR/report.py" \
    --input "$REPORT_DATA" \
    --output "$OUTPUT" \
    --taxonomy "$TAXONOMY"
echo ""
echo "✓ Phase 4 complete"
echo ""

# Print completion summary
echo "================================================================================"
echo "✓ Pipeline complete!"
echo "================================================================================"
echo ""
echo "Output report: $OUTPUT"
echo ""
echo "Next steps:"
echo "  1. Review the report: cat $OUTPUT"
echo "  2. For quarterly updates: ./run.sh --no-collect"
echo "  3. To add new categories: edit $TAXONOMY and re-run phases 2-4"
echo ""
