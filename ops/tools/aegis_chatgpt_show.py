#!/usr/bin/env python3
from __future__ import annotations

import re
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.repo_protection_common_v1 import RUNTIME_DATA_ROOT

LATEST_PACKET_PATH = (
    RUNTIME_DATA_ROOT / "exports" / "aegis_state" / "latest" / "chatgpt_aegis_packet.md"
).resolve()
ARCHIVE_ROOT = (RUNTIME_DATA_ROOT / "exports" / "aegis_state" / "archive").resolve()
EXPORT_ID_RE = re.compile(r"^- export_id:\s*(\S+)\s*$", re.MULTILINE)
GENERATED_AT_RE = re.compile(r"^- generated_at_utc:\s*(\S+)\s*$", re.MULTILINE)


def _read_packet_text(path: Path) -> str:
    return path.read_text(encoding="utf-8").rstrip("\n")


def _extract_export_id(text: str) -> str:
    match = EXPORT_ID_RE.search(text)
    return match.group(1).strip() if match else ""


def _extract_generated_at(text: str) -> datetime | None:
    match = GENERATED_AT_RE.search(text)
    if not match:
        return None
    raw = match.group(1).strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw).astimezone(UTC)
    except ValueError:
        return None


def _newest_archive_packet() -> Path | None:
    if not ARCHIVE_ROOT.exists() or not ARCHIVE_ROOT.is_dir():
        return None
    best_path: Path | None = None
    best_mtime = float("-inf")
    for packet_path in ARCHIVE_ROOT.glob("*/chatgpt_aegis_packet.md"):
        if not packet_path.is_file():
            continue
        try:
            mtime = packet_path.stat().st_mtime
        except OSError:
            continue
        if mtime > best_mtime:
            best_mtime = mtime
            best_path = packet_path.resolve()
    return best_path


def _select_packet_path() -> Path:
    latest_exists = LATEST_PACKET_PATH.exists() and LATEST_PACKET_PATH.is_file()
    newest_archive = _newest_archive_packet()
    if latest_exists and newest_archive is None:
        return LATEST_PACKET_PATH
    if not latest_exists and newest_archive is not None:
        return newest_archive
    if not latest_exists:
        raise SystemExit(
            "FAIL: latest Aegis packet not found. Run `npm run aegis:chatgpt:packet` first."
        )

    # Both latest and archive candidates exist. Prefer the fresher artifact by generated_at/export_id.
    latest_text = _read_packet_text(LATEST_PACKET_PATH)
    latest_export_id = _extract_export_id(latest_text)
    latest_generated_at = _extract_generated_at(latest_text)
    latest_mtime = LATEST_PACKET_PATH.stat().st_mtime

    archive_path = newest_archive
    if archive_path is None:
        return LATEST_PACKET_PATH
    archive_text = _read_packet_text(archive_path)
    archive_export_id = _extract_export_id(archive_text)
    archive_generated_at = _extract_generated_at(archive_text)
    archive_mtime = archive_path.stat().st_mtime

    if archive_generated_at is not None and latest_generated_at is not None:
        if archive_generated_at > latest_generated_at and archive_export_id != latest_export_id:
            return archive_path
        return LATEST_PACKET_PATH

    if archive_mtime > latest_mtime and archive_export_id != latest_export_id:
        return archive_path
    return LATEST_PACKET_PATH


def main() -> int:
    if REPO_ROOT != Path("/home/node/constellation").resolve():
        raise SystemExit(f"FAIL: wrong repo root: {REPO_ROOT}")
    packet_path = _select_packet_path()
    text = _read_packet_text(packet_path)
    print("===== BEGIN AEGIS CHATGPT PACKET =====")
    print(text)
    print("===== END AEGIS CHATGPT PACKET =====")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
