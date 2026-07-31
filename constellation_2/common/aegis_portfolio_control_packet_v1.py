from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_REPO_ROOT = Path('/home/node/constellation_2_runtime')
REPORT_DIR = REPO_ROOT / 'reports' / 'aegis_chatgpt_control_packet_v2'
CONTROL_PACKET_JSON = REPORT_DIR / 'control_packet.json'
CONTROL_PACKET_MD = REPORT_DIR / 'control_packet.md'
PACKET_DIFF_MD = REPORT_DIR / 'packet_diff.md'
AUDIT_RESULT_JSON = REPORT_DIR / 'audit_result.json'
SUMMARY_MD = REPORT_DIR / 'summary.md'
PACKET_VERSION = '2.1'
PACKET_SCHEMA_VERSION = 'aegis_portfolio_control_packet.v2'
TERMINAL_PASS = 'PASS_AEGIS_PORTFOLIO_CONTROL_PACKET_VERSIONED_V2'
CORE_APPS = ['Production Analytics', 'Trade Operations', 'Research Engine']
FORBIDDEN_HOUSEHOLD_TERMS = [
    'medical records',
    'carolyn portal',
    'travel/rental',
    'travel tools',
    'rental tools',
    'language app',
    'gmail',
    'household command center',
    'family control center project',
    'launcher polish',
]
UNSAFE_EXECUTION_PHRASES = [
    'transmit automatically',
    'automatic trade transmission is allowed',
    'automatically transmit',
    'auto-transmit',
    'execution authorization granted',
    'approval to transmit',
]


def build_control_packet(previous_packet: dict[str, Any] | None = None) -> dict[str, Any]:
    generated = datetime.now(UTC).replace(microsecond=0)
    generated_iso = generated.isoformat().replace('+00:00', 'Z')
    metadata = {
        'packet_version': PACKET_VERSION,
        'generated_timestamp': generated_iso,
        'git_commit_hash': _git(['rev-parse', 'HEAD']),
        'git_branch': _git(['rev-parse', '--abbrev-ref', 'HEAD']),
        'repo_path': str(REPO_ROOT),
        'packet_schema_version': PACKET_SCHEMA_VERSION,
    }
    runtime = _runtime_context()
    sleeves = _sleeve_architecture(runtime)
    blockers = _current_blockers(runtime)
    milestones = _latest_validated_milestones()
    packet: dict[str, Any] = {
        'schema_id': 'aegis_portfolio_control_packet',
        'schema_version': 'v2',
        'artifact_id': 'aegis_chatgpt_control_packet_v2',
        'packet_metadata': metadata,
        'current_reality': _current_reality(runtime, sleeves, blockers, milestones),
        'aegis_objective': {
            'statement': 'Aegis exists to discover and validate a Strategy Book demonstrably better than the current production Strategy Book.',
            'principles': [
                'Production reliability matters more than research-lab complexity.',
                'Proposed features are judged by whether they improve finding or operating a better production Strategy Book.',
            ],
        },
        'active_portfolio_applications': [
            {'name': 'Production Analytics', 'purpose': 'Monitor production book and sleeve performance.'},
            {'name': 'Trade Operations', 'purpose': 'Weekly rebalance and capital deployment workflows.'},
            {'name': 'Research Engine', 'purpose': 'Validate challengers and improve the Strategy Book.'},
        ],
        'production_book_and_sleeves': sleeves,
        'trade_operations': _trade_operations(),
        'broker_safety_rules': [
            'Broker Safety rules must not be weakened casually.',
            'Broker Safety separates package validation from IBKR snapshot readiness.',
            'Fresh IBKR holdings are required.',
            'Unexpected post-trade tickers block transmission.',
            'Unchanged sleeves must remain in the expected post-trade universe.',
            'No manual ticker whitelist is allowed unless explicitly approved.',
        ],
        'ibkr_paper_status': _ibkr_status(runtime),
        'current_blockers': blockers,
        'latest_validated_milestones': milestones,
        'runtime_file_conventions': _runtime_file_conventions(),
        'public_portal_launcher_architecture': _public_portal_launcher_architecture(),
        'post_trade_verification': runtime.get('post_trade_verification') or {},
        'canonical_documents': _canonical_documents(),
        'engineering_constitution': [
            'Discovery before implementation for unclear failures.',
            'Implementation only after explicit approval when required.',
            'Use forensic mode for regressions and UI failures.',
            'User-visible evidence is required before PASS.',
            'Validators must try to falsify, not just confirm.',
            'Public portal validation is required for public UI work.',
            'Do not hide assumptions about runtime paths.',
            'Do not provide time estimates unless explicitly requested.',
        ],
        'how_to_use_this_packet': {
            'this_packet_is': ['Context', 'Project memory', 'Architecture summary', 'Current state'],
            'this_packet_is_not': ['Trade instruction', 'Approval', 'Execution authorization', 'Transmission request'],
        },
        'latest_known_run_state': runtime.get('latest_known_run_state') or {},
        'source_evidence': runtime.get('source_evidence') or [],
        'safety_assertions': {
            'context_only': True,
            'not_an_execution_command': True,
            'automatic_transmission_allowed': False,
            'broker_safety_changed': False,
            'trade_generation_changed': False,
            'ui_changed': False,
        },
    }
    packet['changes_since_previous_packet'] = _packet_diff(previous_packet or {}, packet)
    return packet


def write_control_packet(packet: dict[str, Any] | None = None) -> dict[str, Path]:
    previous = _load_existing_packet()
    packet = packet or build_control_packet(previous)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    CONTROL_PACKET_JSON.write_text(json.dumps(packet, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    CONTROL_PACKET_MD.write_text(render_markdown(packet), encoding='utf-8')
    PACKET_DIFF_MD.write_text(render_packet_diff(packet), encoding='utf-8')
    SUMMARY_MD.write_text(render_summary(packet), encoding='utf-8')
    return {'json': CONTROL_PACKET_JSON, 'markdown': CONTROL_PACKET_MD, 'diff': PACKET_DIFF_MD, 'summary': SUMMARY_MD}


def render_markdown(packet: dict[str, Any]) -> str:
    meta = packet['packet_metadata']
    lines = [
        '# Aegis Portfolio Control Packet',
        '',
        f"Packet Version: {meta['packet_version']}",
        f"Generation Timestamp: {_human_timestamp(meta['generated_timestamp'])}",
        f"Git Commit: {meta['git_commit_hash']}",
        f"Git Branch: {meta['git_branch']}",
        f"Repository: {meta['repo_path']}",
        f"Packet Schema Version: {meta['packet_schema_version']}",
        '',
        '## Current Reality',
        '',
    ]
    for item in packet['current_reality']:
        lines.append(f"- **{item['title']}:** {item['value']}")
    lines.extend(['', '## Aegis Objective', '', packet['aegis_objective']['statement'], ''])
    lines.extend(f"- {item}" for item in packet['aegis_objective']['principles'])
    lines.extend(['', '## Active Portfolio Applications', ''])
    for app in packet['active_portfolio_applications']:
        lines.append(f"- **{app['name']}**: {app['purpose']}")
    lines.extend(['', '## Current Production Book / Sleeve Architecture', ''])
    lines.append(f"Source status: {packet['production_book_and_sleeves']['source_status']}")
    lines.append('')
    for sleeve in packet['production_book_and_sleeves']['sleeves']:
        lines.append(
            f"- **{sleeve['name']}**: target weight {sleeve['target_weight']}; "
            f"input source {sleeve['input_source']}; status {sleeve['operating_status']}."
        )
    lines.extend(['', '## Trade Operations', ''])
    lines.append('Trade Operations replaced Weekly Rebalance as the portal tile/page label. The current public route remains `/financial-command-center/weekly-rebalance` for continuity.')
    lines.append('')
    for op in packet['trade_operations']:
        lines.append(f"### {op['name']}")
        lines.append(f"- Operation type: `{op['operation_type']}`")
        for item in op['rules']:
            lines.append(f"- {item}")
        lines.append('')
    lines.extend(['## Post-Trade Verification', ''])
    ptv = packet.get('post_trade_verification') or {}
    if ptv:
        lines.append(f"- **Status:** {ptv.get('status', 'UNKNOWN')}")
        lines.append(f"- **Requirement:** {ptv.get('requirement', 'Post-Trade Verification is required after IBKR transmission and before archive.')}")
        lines.append(f"- **Evidence:** {ptv.get('evidence_path', 'Unavailable')}")
        if ptv.get('blocking_issues'):
            lines.append('- **Blocking Issues:**')
            lines.extend(f"  - {item}" for item in ptv['blocking_issues'])
        lines.append(f"- **UI Panel Evidence:** {ptv.get('ui_panel_evidence', 'Not checked')}")
    else:
        lines.append('- No post-trade verification artifact found.')
    lines.extend(['', '## Broker Safety Rules', ''])
    lines.extend(f"- {item}" for item in packet['broker_safety_rules'])
    lines.extend(['', '## IBKR Paper Status', ''])
    for key, value in packet['ibkr_paper_status'].items():
        label = key.replace('_', ' ').title()
        if isinstance(value, list):
            lines.append(f"- **{label}:**")
            lines.extend(f"  - {item}" for item in value)
        else:
            lines.append(f"- **{label}:** {value}")
    lines.extend(['', '## Current Blockers', ''])
    blockers = packet['current_blockers']
    if blockers:
        for blocker in blockers:
            lines.extend([
                f"### {blocker['title']}",
                f"- Status: {blocker['status']}",
                f"- Impact: {blocker['impact']}",
                f"- Owner: {blocker['owner']}",
                f"- Evidence: {blocker['evidence']}",
                f"- Recommended Next Ticket: {blocker['recommended_next_ticket']}",
                '',
            ])
    else:
        lines.extend(['No active blocker identified.', '', 'No stale blockers.', ''])
    lines.extend(['## Latest Validated Milestones', ''])
    milestones = packet['latest_validated_milestones']
    if milestones:
        for row in milestones:
            lines.append(f"- **{row['status']}** | Date: {row['date']} | Evidence report: `{row['evidence_report']}`")
    else:
        lines.append('- No latest PASS milestone evidence found.')
    lines.extend(['', '## Runtime File/Folder Conventions', ''])
    for row in packet.get('runtime_file_conventions', []):
        lines.append(f"- **{row['name']}**: `{row['pattern']}` ({row['status']})")
    lines.extend(['', '## Public Portal / Launcher Architecture', ''])
    portal = packet.get('public_portal_launcher_architecture') or {}
    for key, value in portal.items():
        label = key.replace('_', ' ').title()
        if isinstance(value, list):
            lines.append(f"- **{label}:** {', '.join(value)}")
        else:
            lines.append(f"- **{label}:** {value}")
    lines.extend(['', '## Canonical Documents', ''])
    for row in packet['canonical_documents']:
        if row['status'] == 'EXISTS':
            lines.append(f"- **{row['name']}**: `{row['path']}`")
        else:
            lines.append(f"- **{row['name']}**: MISSING")
    lines.extend(['', '## Engineering Constitution', ''])
    lines.extend(f"- {item}" for item in packet['engineering_constitution'])
    lines.extend(['', '## Latest Known Run State', ''])
    state = packet.get('latest_known_run_state') or {}
    if state:
        for key, value in state.items():
            lines.append(f"- {key}: {value}")
    else:
        lines.append('- No current run state found.')
    lines.extend(['', '## Changes Since Previous Packet', ''])
    diff = packet['changes_since_previous_packet']
    for key in ['added', 'removed', 'updated']:
        lines.append(f"### {key.title()}")
        values = diff.get(key) or []
        if values:
            lines.extend(f"- {item}" for item in values)
        else:
            lines.append('- None')
        lines.append('')
    lines.extend(['## How To Use This Packet', ''])
    lines.append('This packet is:')
    lines.extend(f"- {item}" for item in packet['how_to_use_this_packet']['this_packet_is'])
    lines.append('')
    lines.append('This packet is NOT:')
    lines.extend(f"- {item}" for item in packet['how_to_use_this_packet']['this_packet_is_not'])
    lines.extend(['', 'It is not an instruction to create, approve, submit, or transmit trades. Transmission must not be automatic.', ''])
    return '\n'.join(lines)


def render_packet_diff(packet: dict[str, Any]) -> str:
    diff = packet.get('changes_since_previous_packet') or {}
    lines = ['# Changes Since Previous Packet', '']
    for key in ['added', 'removed', 'updated']:
        lines.append(f"## {key.title()}")
        values = diff.get(key) or []
        if values:
            lines.extend(f"- {item}" for item in values)
        else:
            lines.append('- None')
        lines.append('')
    return '\n'.join(lines)


def render_summary(packet: dict[str, Any]) -> str:
    return '\n'.join([
        '# Aegis ChatGPT Control Packet V2 Summary',
        '',
        f"Completion marker: `{TERMINAL_PASS}`",
        '',
        'Generated artifacts:',
        '',
        f"- `{CONTROL_PACKET_MD}`",
        f"- `{CONTROL_PACKET_JSON}`",
        f"- `{PACKET_DIFF_MD}`",
        f"- `{AUDIT_RESULT_JSON}`",
        f"- `{SUMMARY_MD}`",
        '',
        'The packet is the canonical AI handoff document for Aegis portfolio context. It is context only and does not authorize trade execution.',
        '',
    ])


def audit_control_packet() -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    if not CONTROL_PACKET_JSON.exists() or not CONTROL_PACKET_MD.exists() or not PACKET_DIFF_MD.exists():
        write_control_packet()
    packet = json.loads(CONTROL_PACKET_JSON.read_text(encoding='utf-8'))
    markdown = CONTROL_PACKET_MD.read_text(encoding='utf-8')
    text = markdown.lower()

    def add(name: str, passed: bool, detail: str = '') -> None:
        checks.append({'name': name, 'passed': bool(passed), 'detail': detail})

    meta = packet.get('packet_metadata') or {}
    required_meta = ['packet_version', 'generated_timestamp', 'git_commit_hash', 'git_branch', 'repo_path', 'packet_schema_version']
    current_reality = packet.get('current_reality') or []
    apps = [row.get('name') for row in packet.get('active_portfolio_applications', [])]
    op_types = {row.get('operation_type') for row in packet.get('trade_operations', [])}
    sections = [
        '## Current Reality',
        '## Current Blockers',
        '## Latest Validated Milestones',
        '## Runtime File/Folder Conventions',
        '## Public Portal / Launcher Architecture',
        '## Post-Trade Verification',
        '## Canonical Documents',
        '## Changes Since Previous Packet',
        '## How To Use This Packet',
    ]
    add('metadata_present', all(meta.get(k) for k in required_meta), json.dumps(meta, sort_keys=True))
    add('current_reality_present', bool(current_reality) and '## Current Reality' in markdown, str(len(current_reality)))
    add('current_reality_maximum_10_bullets', len(current_reality) <= 10, str(len(current_reality)))
    add('blocker_section_present', 'current_blockers' in packet and '## Current Blockers' in markdown, '')
    add('pass_milestones_present', 'latest_validated_milestones' in packet and '## Latest Validated Milestones' in markdown, '')
    add('canonical_document_section_present', 'canonical_documents' in packet and '## Canonical Documents' in markdown, '')
    add('required_sections_present', all(section in markdown for section in sections), ', '.join(sections))
    add('no_household_applications', not any(term in text for term in FORBIDDEN_HOUSEHOLD_TERMS), 'household/FCC exclusions checked')
    add('no_unrelated_fcc_projects', 'family control center' not in text and 'non-portfolio launcher' not in text, '')
    add('no_trade_execution_instructions', not any(phrase in text for phrase in UNSAFE_EXECUTION_PHRASES), 'unsafe execution phrases checked')
    add('version_present', bool(meta.get('packet_version')) and f"Packet Version: {meta.get('packet_version')}" in markdown, str(meta.get('packet_version') or ''))
    add('git_commit_present', len(str(meta.get('git_commit_hash') or '')) == 40, str(meta.get('git_commit_hash') or ''))
    add('repository_path_present', meta.get('repo_path') == str(REPO_ROOT) and f"Repository: {REPO_ROOT}" in markdown, str(meta.get('repo_path') or ''))
    add('active_apps_exact', apps == CORE_APPS, json.dumps(apps))
    add('operation_types_present', {'WEEKLY_REBALANCE', 'CAPITAL_DEPLOYMENT'} <= op_types, json.dumps(sorted(op_types)))
    add('packet_diff_present', PACKET_DIFF_MD.exists() and bool(packet.get('changes_since_previous_packet')), str(PACKET_DIFF_MD))
    add('trade_operations_tile_replacement_present', 'Trade Operations replaced Weekly Rebalance as the portal tile/page label' in markdown, '')
    add('post_trade_verification_required_present', 'Post-Trade Verification is required after IBKR transmission and before archive' in markdown, '')
    add('runtime_file_conventions_present', bool(packet.get('runtime_file_conventions')) and 'REB-*' in markdown and 'CAP-*' in markdown, '')
    add('public_portal_architecture_present', 'portal.schmidtvault.com' in markdown and 'static FCC V2 launcher' in markdown, '')
    add('canonical_runtime_docs_resolved', any((row.get('status') == 'EXISTS' and '/home/node/constellation_2_runtime/docs/' in row.get('path', '')) for row in packet.get('canonical_documents', [])), '')
    add('stale_ibkr_preflight_blocker_not_reported_when_current_pass', not ('ibkr_connectivity_preflight failed' in text and 'Ibkr Preflight Status: PASS'.lower() in text), '')
    add('markdown_only_show_ready', markdown.startswith('# Aegis Portfolio Control Packet'), 'control packet markdown begins with packet title')
    terminal_status = TERMINAL_PASS if all(row['passed'] for row in checks) else 'BLOCKED_AEGIS_PORTFOLIO_CONTROL_PACKET_VERSIONED_V2'
    result = {
        'terminal_status': terminal_status,
        'generated_timestamp': datetime.now(UTC).replace(microsecond=0).isoformat().replace('+00:00', 'Z'),
        'control_packet_json': str(CONTROL_PACKET_JSON),
        'control_packet_markdown': str(CONTROL_PACKET_MD),
        'packet_diff_markdown': str(PACKET_DIFF_MD),
        'checks': checks,
    }
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    AUDIT_RESULT_JSON.write_text(json.dumps(result, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    return result


def _current_reality(
    runtime: dict[str, Any],
    sleeves: dict[str, Any],
    blockers: list[dict[str, str]],
    milestones: list[dict[str, str]],
) -> list[dict[str, str]]:
    latest = milestones[0]['status'] if milestones else 'No current PASS milestone evidence found'
    highest_blocker = blockers[0]['title'] if blockers else 'No active blocker identified'
    production_state = runtime.get('latest_known_run_state') or {}
    health = 'BLOCKED: highest priority blocker requires resolution' if blockers else 'No active blocker found in latest audit'
    if production_state.get('weekly_rebalance_overall_status'):
        health = f"{health}; weekly rebalance status {production_state['weekly_rebalance_overall_status']}"
    return [
        {'title': 'Current Objective', 'value': 'Find and operate a production Strategy Book superior to the current one.'},
        {'title': 'Current Production Architecture', 'value': 'Four-sleeve production book with dynamic/static inputs governed by current sleeve configs and workbook source-of-truth where applicable.'},
        {'title': 'Current Applications', 'value': ', '.join(CORE_APPS)},
        {'title': 'Current Production Sleeve Allocation', 'value': ', '.join(f"{row['name']} {row['target_weight']}" for row in sleeves['sleeves'])},
        {'title': 'Trade Operations Supported', 'value': 'Trade Operations portal tile/page supports Weekly Rebalance and Capital Deployment.'},
        {'title': 'Latest Successfully Validated Workflow', 'value': latest},
        {'title': 'Highest Priority Blocker', 'value': highest_blocker},
        {'title': 'Current Engineering Mode', 'value': 'Implementation'},
        {'title': 'Next Planned Milestone', 'value': 'Paper trade transmission validation after package validation, Broker Safety, fresh IBKR snapshot, IBKR preflight, and typed approval are all satisfied.'},
        {'title': 'Current Project Health', 'value': health},
    ]


def _runtime_context() -> dict[str, Any]:
    evidence: list[str] = []
    latest_state: dict[str, Any] = {}
    blockers: list[str] = []
    source_status = 'local target repo lacks some current portfolio runtime artifacts; scanned external runtime repo when available'

    prod_path = RUNTIME_REPO_ROOT / 'site/aegis-portfolios/data/production-analytics.json'
    prod = _load_json(prod_path)
    if prod:
        evidence.append(str(prod_path))
        latest_state['production_analytics_as_of'] = prod.get('as_of_date') or prod.get('last_updated') or ''
        latest_state['production_book_value'] = ((prod.get('book') or {}).get('book_value') or {}).get('display_value') or ''
        latest_state['production_book_since_funding_return'] = ((prod.get('book') or {}).get('since_funding_return') or {}).get('display_value') or ''
    rebalance_path = RUNTIME_REPO_ROOT / 'data/aegis_rebalance/current_rebalance_summary.json'
    visible_rebalance_path = RUNTIME_REPO_ROOT / 'site/financial-command-center/data/aegis_rebalance/current_rebalance_summary.json'
    rebalance = _load_json(visible_rebalance_path) or _load_json(rebalance_path)
    if rebalance:
        evidence.append(str(visible_rebalance_path if visible_rebalance_path.exists() else rebalance_path))
        latest_state['weekly_rebalance_overall_status'] = rebalance.get('overall_status') or rebalance.get('approval_status') or ''
        latest_state['weekly_rebalance_primary_next_action'] = rebalance.get('primary_next_action') or ''
        blockers.extend(str(item) for item in rebalance.get('blocking_issues', []) if str(item))
    ibkr_path = RUNTIME_REPO_ROOT / 'site/financial-command-center/data/aegis_rebalance/ibkr_connectivity_status.json'
    ibkr = _load_json(ibkr_path)
    if ibkr:
        evidence.append(str(ibkr_path))
        latest_state['ibkr_preflight_status'] = ibkr.get('status') or 'UNKNOWN'
        latest_state['ibkr_preflight_checked_at'] = ibkr.get('checked_at') or ''
        if ibkr.get('status') != 'PASS' and ibkr.get('blocking_issue'):
            blockers.append(str(ibkr['blocking_issue']))
    if ibkr.get('status') == 'PASS':
        blockers = [item for item in blockers if 'ibkr_connectivity_preflight failed' not in item.lower() and 'ibkr paper api not reachable' not in item.lower()]

    post_trade_path = RUNTIME_REPO_ROOT / 'site/financial-command-center/data/aegis_rebalance/post_trade_verification.json'
    post_trade = _load_json(post_trade_path)
    post_trade_info = {}
    if post_trade:
        evidence.append(str(post_trade_path))
        latest_state['post_trade_verification_status'] = post_trade.get('status') or 'UNKNOWN'
        post_trade_info = {
            'status': post_trade.get('status') or 'UNKNOWN',
            'requirement': 'Post-Trade Verification is required after IBKR transmission and before archive.',
            'evidence_path': str(post_trade_path),
            'blocking_issues': post_trade.get('blocking_issues') or [],
            'ui_panel_evidence': 'frontend/app/financial-command-center/weekly-rebalance/page.tsx contains OperatorCard id=post-trade-verification; panel missing is not confirmed by current runtime evidence.',
        }
        if post_trade.get('status') == 'FAIL':
            for issue in post_trade.get('blocking_issues') or []:
                blockers.append(f'Post-Trade Verification FAIL: {issue}')
    cap_dir = RUNTIME_REPO_ROOT / 'data/aegis/operations/capital_deployment'
    if cap_dir.exists():
        evidence.append(str(cap_dir))
        latest_state['capital_deployment_workbooks_found'] = len(list(cap_dir.glob('*.xlsx')))
    return {'source_evidence': evidence, 'latest_known_run_state': latest_state, 'blockers': blockers, 'source_status': source_status, 'ibkr_preflight': ibkr, 'post_trade_verification': post_trade_info}


def _sleeve_architecture(runtime: dict[str, Any]) -> dict[str, Any]:
    return {
        'source_status': runtime.get('source_status') or 'fallback four-sleeve contract',
        'sleeves': [
            {'name': 'AI Factor', 'target_weight': '35%', 'input_source': 'dynamic sleeve CSV/config inputs', 'operating_status': 'active'},
            {'name': 'Protection', 'target_weight': '40%', 'input_source': 'dynamic sleeve CSV/config inputs', 'operating_status': 'active'},
            {'name': 'High Octane', 'target_weight': '5%', 'input_source': 'dynamic sleeve CSV/config inputs', 'operating_status': 'active'},
            {'name': 'Defensive', 'target_weight': '20%', 'input_source': 'static/workbook-driven defensive input source where configured', 'operating_status': 'workbook-driven / paper candidate unless current repo evidence proves active'},
        ],
    }


def _trade_operations() -> list[dict[str, Any]]:
    return [
        {
            'name': 'Weekly Rebalance',
            'operation_type': 'WEEKLY_REBALANCE',
            'rules': [
                'Uses current approved sleeve CSV/config inputs.',
                'Creates REB run IDs.',
                'May generate BUY and SELL trades.',
                'Must pass package validation, Broker Safety, IBKR snapshot, IBKR preflight, and explicit approval before any transmission step.',
                'After IBKR transmission, Post-Trade Verification is required before archive.',
            ],
        },
        {
            'name': 'Capital Deployment',
            'operation_type': 'CAPITAL_DEPLOYMENT',
            'rules': [
                'Workbook is source of truth for deployment amount and sleeve weights.',
                'Uses current sleeve CSV/configs for tickers only.',
                'Creates CAP run IDs.',
                'BUY-only.',
                'No sells.',
                'No drift correction.',
                'No automatic transmission.',
            ],
        },
    ]


def _ibkr_status(runtime: dict[str, Any]) -> dict[str, Any]:
    evidence_text = ' '.join(runtime.get('blockers') or []).lower()
    completed = 'transmission complete' in evidence_text or 'electronic_execution_complete' in evidence_text
    return {
        'integration_mode': 'Paper-first',
        'paper_order_transmission_test_status': 'Completion marker found; verify latest run evidence before relying on it.' if completed else 'No completed live/paper order transmission test found in scanned current evidence.',
        'final_execution_requirements': [
            'package validation PASS',
            'Broker Safety PASS',
            'IBKR snapshot Fresh',
            'IBKR preflight PASS',
            'exact typed approval phrase',
            'transmission must not be automatic',
        ],
    }


def _current_blockers(runtime: dict[str, Any]) -> list[dict[str, str]]:
    raw_items: list[str] = []
    for item in runtime.get('blockers') or []:
        lower = str(item).lower()
        if any(key in lower for key in ['ibkr', 'broker safety', 'snapshot', 'preflight', 'post-trade', 'workbook', 'parser', 'source-of-truth', 'price']):
            raw_items.append(str(item))
    deduped: list[str] = []
    seen = set()
    for item in raw_items:
        if item not in seen:
            deduped.append(item)
            seen.add(item)
    blockers = []
    for item in deduped[:8]:
        blockers.append({
            'title': _blocker_title(item),
            'status': 'ACTIVE',
            'impact': _blocker_impact(item),
            'owner': 'Aegis operator / engineering',
            'evidence': item,
            'recommended_next_ticket': _recommended_ticket(item),
        })
    return blockers


def _blocker_title(text: str) -> str:
    lower = text.lower()
    if 'post-trade verification fail' in lower or 'post-trade' in lower:
        return 'Post-Trade Verification failing or incomplete'
    if 'preflight' in lower:
        return 'IBKR preflight not complete'
    if 'holdings' in lower or 'snapshot' in lower:
        return 'Fresh IBKR holdings snapshot not ready'
    if 'workbook' in lower:
        return 'Workbook source-of-truth validation incomplete'
    if 'price' in lower:
        return 'Price snapshot/input validation incomplete'
    if 'post-trade verification fail' in lower or 'post-trade' in lower:
        return 'Post-Trade Verification failing or incomplete'
    return text[:96]


def _blocker_impact(text: str) -> str:
    lower = text.lower()
    if 'post-trade' in lower:
        return 'Blocks archive/completion after any IBKR transmission until actual holdings are exported and verified.'
    if 'ibkr' in lower or 'preflight' in lower or 'holdings' in lower:
        return 'Blocks any order transmission readiness claim until Paper-first validation gates pass.'
    if 'workbook' in lower:
        return 'Blocks capital deployment source-of-truth confidence.'
    if 'price' in lower:
        return 'Blocks reliable package validation and trade package readiness.'
    if 'post-trade' in lower:
        return 'Blocks archive/completion after any IBKR transmission until actual holdings are exported and verified.'
    return 'Blocks a clean PASS until the cited evidence is resolved.'


def _recommended_ticket(text: str) -> str:
    lower = text.lower()
    if 'post-trade' in lower:
        return 'Post-Trade Verification Actual Holdings Export and Reconciliation'
    if 'preflight' in lower or 'ibkr' in lower or 'holdings' in lower:
        return 'IBKR Paper Preflight Completion and Fresh Snapshot Validation'
    if 'workbook' in lower:
        return 'Capital Deployment Workbook Source-of-Truth Validation'
    if 'price' in lower:
        return 'Price Snapshot Validation Repair'
    if 'post-trade' in lower:
        return 'Post-Trade Verification Actual Holdings Export and Reconciliation'
    return 'Forensic blocker resolution ticket with current evidence refresh'


def _latest_validated_milestones() -> list[dict[str, str]]:
    candidates = [
        'PASS_TRADE_OPERATIONS_CAPITAL_DEPLOYMENT_V1',
        'PASS_S1_053_CURRENT_INPUTS_PROPAGATE_TO_PACKAGE_V1',
        'PASS_RUN_AI_FACTOR_REBALANCE_BUTTON_CREATES_RUN_V1',
        'PASS_TRADE_OPERATIONS_PORTAL_TILE_V1',
        'PASS_TRADE_OPERATIONS_PAGE_RENAMED_V1',
        'PASS_CAPITAL_DEPLOYMENT_BROKER_SAFETY_V1',
        'PASS_CAPITAL_DEPLOYMENT_END_TO_END_V1',
    ]
    rows: list[dict[str, str]] = []
    for marker in candidates:
        evidence = _find_newest_report_containing(marker)
        if evidence:
            rows.append({'status': marker, 'date': _file_date(evidence), 'evidence_report': str(evidence)})
    rows.sort(key=lambda row: (row['date'], row['status']), reverse=True)
    return rows[:5]


def _canonical_documents() -> list[dict[str, str]]:
    specs = [
        ('Engineering Constitution', ['docs/aegis_engineering_constitution_v1.md']),
        ('Workflow Modes', ['docs/aegis_workflow_modes.md']),
        ('Review Protocol', ['docs/aegis_review_protocol.md']),
        ('Investment Architecture', ['docs/aegis_investment_application_architecture_v1.md']),
        ('Production Analytics Design', ['docs/design/production_analytics_design_spec_v1.md']),
        ('Trade Operations Design', ['reports/trade_operations_v1_design/implementation_design.md', 'docs/aegis_trade_operations_design_v1.md']),
        ('Broker Safety', ['docs/aegis_2_broker_configuration_v1.md', 'docs/aegis_foundation/IBKR_BASKETTRADER_IMPORT_SAFETY.md', 'docs/aegis_broker_safety_v1.md']),
        ('Current Production Book', ['site/aegis-portfolios/data/production-analytics.json', 'data/aegis/operations/weekly_rebalance/current_production_book.json']),
    ]
    rows = []
    for name, rels in specs:
        found = ''
        for root in [REPO_ROOT, RUNTIME_REPO_ROOT]:
            for rel in rels:
                candidate = root / rel
                if candidate.exists():
                    found = str(candidate)
                    break
            if found:
                break
        rows.append({'name': name, 'path': found, 'status': 'EXISTS' if found else 'MISSING'})
    return rows


def _runtime_file_conventions() -> list[dict[str, str]]:
    weekly = RUNTIME_REPO_ROOT / 'data/aegis/operations/weekly_rebalance/runs'
    capital = RUNTIME_REPO_ROOT / 'data/aegis/operations/capital_deployment/runs'
    return [
        {'name': 'Weekly Rebalance Runs', 'pattern': str(weekly / 'REB-*'), 'status': 'EXISTS' if weekly.exists() else 'MISSING'},
        {'name': 'Capital Deployment Runs', 'pattern': str(capital / 'CAP-*'), 'status': 'EXISTS' if capital.exists() else 'MISSING'},
    ]


def _public_portal_launcher_architecture() -> dict[str, Any]:
    return {
        'launcher': 'static FCC V2 launcher',
        'public_root': 'https://portal.schmidtvault.com',
        'route_stack': 'Cloudflare -> Caddy -> Next 3012 for portfolio application routes',
        'core_aegis_apps': CORE_APPS,
        'trade_operations_route': '/financial-command-center/weekly-rebalance',
        'trade_operations_tile_status': 'Trade Operations replaced Weekly Rebalance as the portal tile/page label; route retained for continuity.',
        'evidence': '/home/node/constellation_2_runtime/reports/fcc_v2_approved_aegis_logo_header_v1/validation_summary.md',
    }


def _packet_diff(previous: dict[str, Any], current: dict[str, Any]) -> dict[str, list[str]]:
    if not previous:
        return {
            'added': ['Initial v2 packet generated or no previous v2 packet was available.'],
            'removed': [],
            'updated': [],
        }
    added: list[str] = []
    removed: list[str] = []
    updated: list[str] = []
    ignored = {'packet_metadata', 'changes_since_previous_packet'}
    for key in sorted((set(current) | set(previous)) - ignored):
        if key not in previous:
            added.append(key)
        elif key not in current:
            removed.append(key)
        elif current.get(key) != previous.get(key):
            updated.append(key)
    old_meta = previous.get('packet_metadata') or {}
    new_meta = current.get('packet_metadata') or {}
    for key in ['packet_version', 'git_commit_hash', 'git_branch', 'repo_path', 'packet_schema_version']:
        if old_meta.get(key) != new_meta.get(key):
            updated.append(f'packet_metadata.{key}')
    return {'added': added[:20], 'removed': removed[:20], 'updated': updated[:30]}


def _load_existing_packet() -> dict[str, Any]:
    if not CONTROL_PACKET_JSON.exists():
        return {}
    try:
        data = json.loads(CONTROL_PACKET_JSON.read_text(encoding='utf-8'))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _find_newest_report_containing(marker: str) -> Path | None:
    newest: Path | None = None
    newest_mtime = -1.0
    for root in [REPO_ROOT / 'reports', RUNTIME_REPO_ROOT / 'reports']:
        if not root.exists():
            continue
        for path in root.rglob('*'):
            if any(part.startswith('aegis_chatgpt_control_packet_') or part.startswith('aegis_chatgpt_hydrate_') for part in path.parts):
                continue
            if not path.is_file() or path.stat().st_size > 2_000_000:
                continue
            try:
                text = path.read_text(encoding='utf-8', errors='ignore')
            except OSError:
                continue
            if marker in text and path.stat().st_mtime > newest_mtime:
                newest = path
                newest_mtime = path.stat().st_mtime
    return newest


def _file_date(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, UTC).strftime('%Y-%m-%d')


def _human_timestamp(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return value
    return parsed.strftime('%Y-%m-%d %H:%M UTC')


def _git(args: list[str]) -> str:
    try:
        return subprocess.check_output(['git', '-C', str(REPO_ROOT), *args], text=True).strip()
    except Exception:
        return ''


def _load_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}
