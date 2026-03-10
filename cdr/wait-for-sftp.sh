#!/bin/sh
# Wait for SFTP host:port to be reachable (resolves DNS and TCP connect).
# Usage: the script is intended as an ENTRYPOINT that then execs the original CMD.

set -e

SFTP_ENABLED=${SFTP_ENABLED:-1}
if [ "$SFTP_ENABLED" = "0" ]; then
  echo "SFTP disabled via SFTP_ENABLED=0, skipping wait"
  exec "$@"
fi

HOST=${SFTP_HOSTNAME:-sftp}
PORT=${SFTP_PORT:-22}
TIMEOUT=${WAIT_TIMEOUT:-60}

start_ts=$(date +%s)
echo "Waiting for ${HOST}:${PORT} (timeout ${TIMEOUT}s)"
while true; do
  # Try DNS resolution and TCP connect using Python (available in image)
  python3 - <<PY || true
import socket, sys
try:
    # try to resolve
    addr = socket.gethostbyname("${HOST}")
    s = socket.create_connection((addr, ${PORT}), timeout=5)
    s.close()
    sys.exit(0)
except Exception as e:
    # non-zero to indicate failure
    sys.exit(1)
PY

  if [ "$?" -eq 0 ]; then
    echo "${HOST}:${PORT} reachable"
    break
  fi

  now_ts=$(date +%s)
  elapsed=$((now_ts - start_ts))
  if [ "$elapsed" -ge "$TIMEOUT" ]; then
    echo "Timeout waiting for ${HOST}:${PORT} after ${elapsed}s"
    break
  fi
  echo "Still waiting for ${HOST}:${PORT}... (${elapsed}s elapsed)"
  sleep 2
done

# Exec the original command
exec "$@"
