#!/bin/sh
set -eu

CPU_COUNT="$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || echo 2)"

if [ "${GUNICORN_WORKERS:-}" = "" ]; then
  WORKERS=$((CPU_COUNT * 2 + 1))
else
  WORKERS="${GUNICORN_WORKERS}"
fi

if [ "${GUNICORN_THREADS:-}" = "" ]; then
  if [ "$CPU_COUNT" -ge 16 ]; then
    THREADS=8
  elif [ "$CPU_COUNT" -ge 8 ]; then
    THREADS=6
  elif [ "$CPU_COUNT" -ge 4 ]; then
    THREADS=4
  else
    THREADS=2
  fi
else
  THREADS="${GUNICORN_THREADS}"
fi

TIMEOUT="${GUNICORN_TIMEOUT:-120}"
GRACEFUL_TIMEOUT="${GUNICORN_GRACEFUL_TIMEOUT:-30}"
KEEPALIVE="${GUNICORN_KEEPALIVE:-5}"
MAX_REQUESTS="${GUNICORN_MAX_REQUESTS:-1000}"
MAX_REQUESTS_JITTER="${GUNICORN_MAX_REQUESTS_JITTER:-100}"
BIND="${GUNICORN_BIND:-0.0.0.0:1234}"
LOG_LEVEL="${GUNICORN_LOG_LEVEL:-info}"

exec gunicorn \
  --worker-class gthread \
  --workers "$WORKERS" \
  --threads "$THREADS" \
  --bind "$BIND" \
  --timeout "$TIMEOUT" \
  --graceful-timeout "$GRACEFUL_TIMEOUT" \
  --keep-alive "$KEEPALIVE" \
  --max-requests "$MAX_REQUESTS" \
  --max-requests-jitter "$MAX_REQUESTS_JITTER" \
  --log-level "$LOG_LEVEL" \
  app.app:app
