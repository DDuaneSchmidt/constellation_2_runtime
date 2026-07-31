from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]


def test_ai_operations_assistant_requirements_document_defines_grounded_contract():
    doc_path = REPO_ROOT / "AEGIS_AI_OPERATIONS_ASSISTANT_REQUIREMENTS.md"
    assert doc_path.exists()

    text = doc_path.read_text(encoding="utf-8")
    required_terms = [
        "aegis_ai_operations_context_v1",
        "aegis_ai_operations_response_v1",
        "npm run aegis:ai-operations-self-check",
        "unsupported_claims",
        "Sources Used",
        "Context Builder",
        "Evidence Selection",
        "Grounded Response",
        "It does not operate the system.",
    ]

    for term in required_terms:
        assert term in text


def test_ai_operations_assistant_manifest_references_product_authority():
    manifest_path = REPO_ROOT / "aegis/modules/operator_portal/aegis.module.yaml"
    text = manifest_path.read_text(encoding="utf-8")

    assert "AEGIS_AI_OPERATIONS_ASSISTANT_REQUIREMENTS.md" in text
    assert "product authority for Ask Aegis / AI Operations Assistant" in text
    assert "aegis_ai_operations_context_v1" in text
    assert "aegis_ai_operations_response_v1" in text
    assert "no trade advice, broker execution, live trading, autonomous execution" in text
