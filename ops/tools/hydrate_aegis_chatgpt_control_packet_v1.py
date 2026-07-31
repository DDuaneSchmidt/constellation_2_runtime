#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_portfolio_control_packet_v1 import (  # noqa: E402
    AUDIT_RESULT_JSON,
    CONTROL_PACKET_JSON,
    CONTROL_PACKET_MD,
    REPORT_DIR,
    TERMINAL_PASS,
    audit_control_packet,
    write_control_packet,
)

HYDRATE_DIR = REPO_ROOT / 'reports' / 'aegis_chatgpt_hydrate_v1'
HYDRATE_RESULT_JSON = HYDRATE_DIR / 'hydrate_result.json'
HYDRATE_SUMMARY_MD = HYDRATE_DIR / 'hydrate_summary.md'
HYDRATE_PASS = 'PASS_AEGIS_CHATGPT_HYDRATE_COMMAND_V1'


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='hydrate_aegis_chatgpt_control_packet_v1')
    parser.add_argument('--debug-print-packet-on-fail', action='store_true')
    args = parser.parse_args(argv)

    HYDRATE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = _timestamp()

    try:
        paths = write_control_packet()
        packet = _load_packet(paths['json'])
    except Exception as exc:
        result = _result(
            status='FAIL',
            packet_version='',
            git_commit='',
            printed_markdown=False,
            timestamp=timestamp,
            error=f'Control packet build failed: {exc}',
        )
        _write_artifacts(result)
        print('Aegis ChatGPT Hydrate')
        print('')
        print('Control packet: FAIL')
        print(f'Error: {result["error"]}')
        return 1

    audit = audit_control_packet()
    metadata = packet.get('packet_metadata') or {}
    packet_version = str(metadata.get('packet_version') or '')
    git_commit = str(metadata.get('git_commit_hash') or '')
    audit_passed = audit.get('terminal_status') == TERMINAL_PASS

    if not audit_passed:
        result = _result(
            status='FAIL',
            packet_version=packet_version,
            git_commit=git_commit,
            printed_markdown=False,
            timestamp=timestamp,
            error='Audit failed',
            audit_terminal_status=str(audit.get('terminal_status') or ''),
            failed_checks=[row for row in audit.get('checks', []) if not row.get('passed')],
        )
        _write_artifacts(result)
        print('Aegis ChatGPT Hydrate')
        print('')
        print('Control packet: PASS')
        print('Audit: FAIL')
        print(f'Packet Version: {packet_version}')
        print(f'Git Commit: {git_commit}')
        print('')
        print(f'Audit failure summary: {result["audit_terminal_status"]}')
        for check in result['failed_checks'][:8]:
            print(f"- {check.get('name')}: {check.get('detail')}")
        if args.debug_print_packet_on_fail and CONTROL_PACKET_MD.exists():
            print('')
            print('--- BEGIN DEBUG AEGIS CONTROL PACKET ---')
            print(CONTROL_PACKET_MD.read_text(encoding='utf-8'), end='')
            print('--- END DEBUG AEGIS CONTROL PACKET ---')
        return 1

    markdown = CONTROL_PACKET_MD.read_text(encoding='utf-8')
    result = _result(
        status='PASS',
        packet_version=packet_version,
        git_commit=git_commit,
        printed_markdown=True,
        timestamp=timestamp,
        terminal_status=HYDRATE_PASS,
    )
    _write_artifacts(result)

    print('Aegis ChatGPT Hydrate')
    print('')
    print('Control packet: PASS')
    print('Audit: PASS')
    print(f'Packet Version: {packet_version}')
    print(f'Git Commit: {git_commit}')
    print('')
    print('--- BEGIN AEGIS CONTROL PACKET ---')
    print('')
    print(markdown, end='' if markdown.endswith('\n') else '\n')
    print('--- END AEGIS CONTROL PACKET ---')
    return 0


def _result(
    *,
    status: str,
    packet_version: str,
    git_commit: str,
    printed_markdown: bool,
    timestamp: str,
    **extra: Any,
) -> dict[str, Any]:
    result = {
        'status': status,
        'packet_version': packet_version,
        'git_commit': git_commit,
        'control_packet_path': str(CONTROL_PACKET_MD),
        'audit_result_path': str(AUDIT_RESULT_JSON),
        'printed_markdown': printed_markdown,
        'timestamp': timestamp,
    }
    result.update(extra)
    return result


def _write_artifacts(result: dict[str, Any]) -> None:
    HYDRATE_RESULT_JSON.write_text(json.dumps(result, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    status = result.get('terminal_status') or result.get('status')
    lines = [
        '# Aegis ChatGPT Hydrate V1 Summary',
        '',
        f"Status: {status}",
        f"Packet Version: {result.get('packet_version') or ''}",
        f"Git Commit: {result.get('git_commit') or ''}",
        f"Control Packet: `{result.get('control_packet_path')}`",
        f"Audit Result: `{result.get('audit_result_path')}`",
        f"Printed Markdown: {str(result.get('printed_markdown')).lower()}",
        f"Timestamp: {result.get('timestamp')}",
        '',
    ]
    if result.get('error'):
        lines.extend(['## Error', '', str(result['error']), ''])
    HYDRATE_SUMMARY_MD.write_text('\n'.join(lines), encoding='utf-8')


def _load_packet(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(data, dict):
        raise ValueError('control packet JSON root is not an object')
    return data


def _timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


if __name__ == '__main__':
    raise SystemExit(main())
