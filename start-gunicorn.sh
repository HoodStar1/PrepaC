#!/bin/sh
set -eu

export PREPAC_CONFIG_DIR="${PREPAC_CONFIG_DIR:-/config}"
exec python -m prepac --server gunicorn
