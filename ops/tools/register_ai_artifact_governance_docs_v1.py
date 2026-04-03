#!/usr/bin/env python3
import hashlib
from pathlib import Path


REPO_ROOT = Path("/home/node/constellation_2_runtime")
INDEX_PATH = REPO_ROOT / "governance" / "00_INDEX.md"
MANIFEST_PATH = REPO_ROOT / "governance" / "00_MANIFEST.yaml"

EXPECTED_INDEX_SHA256 = "c2a3300d6bd2783e86d734ec9ad15ee4c1139e07c38947c3ab1a1b5af5204a26"
EXPECTED_MANIFEST_SHA256 = "076e1e167a43d67d9f21250cc2a64cc0a0dd77190e8fd3ad4a97d0f5f85510a5"


INDEX_INSERT_AFTER = """### Weekly research protocols
- `governance/weekly_engine_diagnostic_review_protocol_v1.md`
"""

INDEX_INSERT_BLOCK = """
### AI governance contracts
- `governance/contracts/constellation_ai_reasoning_contract.v1.md`
- `governance/contracts/constellation_system_invariants.v1.md`
""".lstrip("\n")

MANIFEST_INSERT_AFTER = """  - id: GOVERNANCE_INDEX_C2_V1
    path: governance/00_INDEX.md
    class: GOVERNANCE_INDEX
    status: ACTIVE
"""

MANIFEST_INSERT_BLOCK = """
  - id: C2_CONSTELLATION_AI_REASONING_CONTRACT_V1
    path: governance/contracts/constellation_ai_reasoning_contract.v1.md
    class: C2_CONTRACT
    status: DRAFT
  - id: C2_CONSTELLATION_SYSTEM_INVARIANTS_V1
    path: governance/contracts/constellation_system_invariants.v1.md
    class: C2_CONTRACT
    status: DRAFT
""".lstrip("\n")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def assert_exists(path: Path, label: str) -> None:
    if not path.exists():
        raise SystemExit(f"FAIL_CLOSED: missing {label}: {path}")


def assert_sha(path: Path, expected_sha: str, label: str) -> str:
    text = read_text(path)
    actual = sha256_text(text)
    if actual != expected_sha:
        raise SystemExit(
            f"FAIL_CLOSED: {label} sha mismatch\\n"
            f"expected: {expected_sha}\\n"
            f"actual:   {actual}\\n"
            f"path:     {path}"
        )
    return text


def ensure_absent(text: str, needle: str, label: str) -> None:
    if needle in text:
        raise SystemExit(f"FAIL_CLOSED: {label} already present")


def insert_after_once(text: str, anchor: str, block: str, label: str) -> str:
    if anchor not in text:
        raise SystemExit(f"FAIL_CLOSED: anchor not found for {label}")
    if text.count(anchor) != 1:
        raise SystemExit(f"FAIL_CLOSED: anchor not unique for {label}")
    return text.replace(anchor, anchor + "\n" + block, 1)


def main() -> None:
    assert_exists(INDEX_PATH, "governance index")
    assert_exists(MANIFEST_PATH, "governance manifest")
    assert_exists(REPO_ROOT / "governance" / "contracts" / "constellation_ai_reasoning_contract.v1.md", "AI reasoning contract")
    assert_exists(REPO_ROOT / "governance" / "contracts" / "constellation_system_invariants.v1.md", "system invariants contract")

    index_text = assert_sha(INDEX_PATH, EXPECTED_INDEX_SHA256, "governance/00_INDEX.md")
    manifest_text = assert_sha(MANIFEST_PATH, EXPECTED_MANIFEST_SHA256, "governance/00_MANIFEST.yaml")

    ensure_absent(index_text, "governance/contracts/constellation_ai_reasoning_contract.v1.md", "index AI contract path")
    ensure_absent(index_text, "governance/contracts/constellation_system_invariants.v1.md", "index invariant contract path")
    ensure_absent(manifest_text, "governance/contracts/constellation_ai_reasoning_contract.v1.md", "manifest AI contract path")
    ensure_absent(manifest_text, "governance/contracts/constellation_system_invariants.v1.md", "manifest invariant contract path")

    new_index = insert_after_once(
        index_text,
        INDEX_INSERT_AFTER,
        INDEX_INSERT_BLOCK,
        "governance index insertion",
    )
    new_manifest = insert_after_once(
        manifest_text,
        MANIFEST_INSERT_AFTER,
        MANIFEST_INSERT_BLOCK,
        "governance manifest insertion",
    )

    write_text(INDEX_PATH, new_index)
    write_text(MANIFEST_PATH, new_manifest)

    print("UPDATED:")
    print(INDEX_PATH)
    print(MANIFEST_PATH)
    print("")
    print("NEW SHA256:")
    print(f"{sha256_text(new_index)}  {INDEX_PATH}")
    print(f"{sha256_text(new_manifest)}  {MANIFEST_PATH}")


if __name__ == "__main__":
    main()
