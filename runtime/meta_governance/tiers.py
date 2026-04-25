TIER_0 = "tier_0"
TIER_1 = "tier_1"
TIER_2 = "tier_2"
TIER_3 = "tier_3"

ALL_TIERS = (TIER_0, TIER_1, TIER_2, TIER_3)

TIER_LABELS = {
    TIER_0: "Constitutional Invariants",
    TIER_1: "Governance Protocols",
    TIER_2: "Policy Modules",
    TIER_3: "Tunable Parameters",
}


def validate_tier(value: str) -> str:
    if value not in ALL_TIERS:
        raise ValueError(f"UNKNOWN_TIER:{value}")
    return value


def tier_rank(value: str) -> int:
    validate_tier(value)
    return ALL_TIERS.index(value)
