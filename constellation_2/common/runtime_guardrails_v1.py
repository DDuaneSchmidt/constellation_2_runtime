from __future__ import annotations


def classify_failure(error: object) -> str:
    text = str(error)
    upper = text.upper()
    if 'INVALID --TRUTH_ROOT' in upper or 'TRUTH_ROOT' in upper and ('INVALID' in upper or 'MISSING OR NOT DIR' in upper or 'MUST BE ABSOLUTE' in upper):
        return 'runtime_root_invalid'
    if 'SCHEMA' in upper or 'VALIDATION' in upper or 'TOP_LEVEL_NOT_OBJECT' in upper or 'JSON_PARSE_FAILED' in upper:
        return 'invalid_schema'
    if 'REFUSE_OVERWRITE' in upper or 'ATTEMPTED_REWRITE' in upper or 'IMMUTABLE' in upper or 'OUT_DIR_NOT_EMPTY' in upper:
        return 'immutable_conflict'
    if 'IB_INSYNC' in upper or 'BROKER_CONNECT' in upper or 'BROKER_PULL' in upper or 'TIMEOUT' in upper or 'CONNECTIVITY' in upper:
        return 'live_connectivity_unavailable'
    if 'KILL_SWITCH' in upper or 'NOT_AUTHORIZED' in upper or 'FAIL_CLOSED' in upper or 'REJECTED' in upper or 'GATE' in upper or 'READINESS' in upper:
        return 'blocked_policy_gate'
    if 'MISSING' in upper or 'NO_AUTHORITATIVE' in upper or 'LINEAGE_MISSING' in upper or 'NOT FILE' in upper:
        return 'missing_input'
    return 'runtime_error'


def format_failure_line(tool: str, classification: str, **fields: object) -> str:
    parts = [f'FAIL: {tool}', f'classification={classification}']
    for key in sorted(fields):
        value = fields[key]
        parts.append(f'{key}={value!r}')
    return ' '.join(parts)
