#!/bin/bash
# Split a large log file into compressed chunks efficiently.
# Usage: log_archive.sh <file> <chunk_size_bytes> <archive_name>
#
# Uses split for single-pass file splitting, then compresses chunks in parallel.
# Disk-efficient: removes each raw chunk immediately after compression.

set -euo pipefail

FILE="$1"
CHUNK_SIZE="$2"
ARCHIVE_NAME="$3"

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

split --line-bytes="$CHUNK_SIZE" --numeric-suffixes=1 --additional-suffix=".part" "$FILE" "$WORK_DIR/chunk_"

seq=0
for chunk in "$WORK_DIR"/chunk_*.part; do
    [ -f "$chunk" ] || continue
    seq=$((seq + 1))
    timestamp=$(head -1 "$chunk" | awk '{print $2,$3}' | sed -e 's/t://g;s/ /__/g;s/-/_/g;s/:/_/g;s/,/_/g;')
    if [ -z "$timestamp" ] || [ "$timestamp" = "__" ]; then
        timestamp="part_$(printf '%04d' $seq)"
    fi
    out_name="${timestamp}.${ARCHIVE_NAME}.zst"
    zstd --rm -q "$chunk" -o "$out_name" &
done

wait
