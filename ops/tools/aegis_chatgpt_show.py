#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
LATEST_PACKET_PATH = (
    REPO_ROOT / "runtime" / "exports" / "aegis_state" / "latest" / "chatgpt_aegis_packet.md"
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
