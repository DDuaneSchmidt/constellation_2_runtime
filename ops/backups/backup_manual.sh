#!/usr/bin/env bash
set -euo pipefail
cd /home/node/constellation_2_runtime
exec /home/node/constellation_2_runtime/ops/backups/tools/create_backup_v1.py
