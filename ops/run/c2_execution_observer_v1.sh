#!/usr/bin/env bash
set -euo pipefail

cd /home/node/constellation_2_runtime

PY="/home/node/constellation_2_runtime/.venv_c2/bin/python"

exec "$PY" ops/ib/c2_execution_observer_v1.py \
  --host 127.0.0.1 \
  --port 4002 \
  --client-id 79 \
  --poll-seconds 10 \
  --environment PAPER
