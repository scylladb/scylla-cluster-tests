#!/bin/sh
set -eu
LOGFILE="/tmp/argus-install.log"
echo "Installing Argus pip package..."
echo "[TODO] Remove this once Argus stabilizes"
sudo /usr/local/bin/pip3 install --force -e git+https://github.com/bentsi/argus#egg=argus >"$LOGFILE" 2>&1 && (
  echo "Package installed."
  if [ -d "./src" ]; then sudo chown "$(whoami):$(whoami)" -R "./src"; fi
  exit 0
) || (
  env
  echo "Argus failed to install!"
  cat "$LOGFILE"
  exit 1
)
