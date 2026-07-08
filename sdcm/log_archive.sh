#!/usr/bin/env bash
# Usage: log_archive.sh <file> <chunk_size_bytes> <archive_name>

set -euo pipefail

INPUT_FILE="$1"
CHUNK_SIZE="$2"
ARCHIVE_NAME="$3"

WORK_DIR=${LOG_ARCHIVE_WORK_DIR:-$(mktemp -d --tmpdir=/var/tmp)}
trap 'rm -rf "$WORK_DIR"' EXIT

export WORK_DIR ARCHIVE_NAME

# Injecting 'set -eu' into the subshell ensures failures abort the split process.
# (Note: 'pipefail' is omitted as split defaults to /bin/sh, which is 'dash' on many distros).
split --line-bytes="$CHUNK_SIZE" --numeric-suffixes=1 --additional-suffix=".part" \
    --filter='
        set -eu
        if IFS= read -r first_line; then
            chunk_seq=$(basename "$FILE" .part)

            # Extract timestamp completely in-memory
            timestamp=$(printf "%s\n" "$first_line" | awk "{print \$2,\$3}" | sed -e "s/t://g;s/ /__/g;s/-/_/g;s/:/_/g;s/,/_/g;")

            if [ -z "$timestamp" ] || [ "$timestamp" = "__" ]; then
                out_name="${chunk_seq}.${ARCHIVE_NAME}.zst"
            else
                out_name="${timestamp}.${chunk_seq}.${ARCHIVE_NAME}.zst"
            fi

            # Stream the first line + the remaining stdin directly into zstd
            { printf "%s\n" "$first_line"; cat; } | zstd -q -o "$out_name"
        fi
    ' "$INPUT_FILE" "$WORK_DIR/chunk_"
