#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.repo_protection_common_v1 import RUNTIME_DATA_ROOT

LATEST_PACKET_PATH = (
    RUNTIME_DATA_ROOT / "exports" / "aegis_state" / "latest" / "chatgpt_aegis_packet.md"
).resolve()


def main() -> int:
    if REPO_ROOT != Path("/home/node/constellation").resolve():
        raise SystemExit(f"FAIL: wrong repo root: {REPO_ROOT}")
    if not LATEST_PACKET_PATH.exists() or not LATEST_PACKET_PATH.is_file():
        raise SystemExit(
            "FAIL: latest Aegis packet not found. Run `npm run aegis:chatgpt:packet` first."
        )
    text = LATEST_PACKET_PATH.read_text(encoding="utf-8").rstrip("\n")
    print("===== BEGIN AEGIS CHATGPT PACKET =====")
    print(text)
    print("===== END AEGIS CHATGPT PACKET =====")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
