from typing import Any, Dict, List


def score_decisions(
    decisions: List[Dict[str, Any]], policy: Dict[str, Any]
) -> List[Dict[str, Any]]:
    scored = []

    for d in decisions:
        total = (
            d["scores"]["risk"] * policy["weights"]["risk"]
            + d["scores"]["tax"] * policy["weights"]["tax"]
            + d["scores"]["allocation"] * policy["weights"]["allocation"]
        )

        scored_decision = dict(d)
        scored_decision["total_score"] = total
        scored.append(scored_decision)

    scored.sort(key=lambda x: x["total_score"], reverse=True)

    return scored
