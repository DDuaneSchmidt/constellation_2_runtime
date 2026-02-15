#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/home/node/constellation_2_runtime"
BACKUP_ROOT="/home/node/constellation_2_backups"
MANUAL_ROOT="${BACKUP_ROOT}/MANUAL"
EXPORT_ROOT="${BACKUP_ROOT}/_ai_exports"

cd "$REPO_ROOT"

LATEST="$(ls -1dt ${MANUAL_ROOT}/manual_operator_checkpoint_* | head -n 1)"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${EXPORT_ROOT}/${TS}"

mkdir -p "$OUT"
chmod 700 "$OUT"

PART_SIZE="470m"

split -b "$PART_SIZE" -d -a 3 \
  "${LATEST}/repo_snapshot.tar.gz" \
  "${OUT}/repo_snapshot.tar.gz.part."

cp "${LATEST}/backup_manifest.json" "$OUT/"
sha256sum "$OUT"/repo_snapshot.tar.gz.part.* > "$OUT/SHA256SUMS.txt"

chmod 600 "$OUT"/*
echo "OK: AI_EXPORT_CREATED"
ls -lh "$OUT"
