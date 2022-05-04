#!/bin/sh
set -eu
LOGFILE="/tmp/argus-install.log"
echo "Installing Argus pip package..."
echo "[TODO] Remove this once Argus stabilizes"
sudo /usr/local/bin/pip3 install --force https://github.com/scylladb/argus/archive/refs/tags/v0.5.0.zip >"$LOGFILE" 2>&1 && (
  echo "Package installed."
  cat "$LOGFILE"
  if [ -d "./src" ]; then sudo chown "$(whoami):$(whoami)" -R "./src"; fi
  exit 0
) || (
  env
  echo "Argus failed to install!"
  cat "$LOGFILE"
  exit 1
)
