from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]


def test_ask_aegis_ui_uses_free_form_operational_assistant():
    page_text = (REPO_ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    main_text = (REPO_ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")
    client_text = (REPO_ROOT / "constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js").read_text(encoding="utf-8")

    assert "data-ask-aegis-form" in page_text
    assert "data-ask-aegis-question" in page_text
    assert "Grounded response" in page_text
    assert "What should be fixed first?" in page_text
    assert "askAegisAiOperations" in main_text
    assert "/api/aegis/ai-operations/ask" in client_text
    assert "renderAskAegisQuestionCard" not in page_text


def test_ai_operations_endpoints_are_registered():
    server_text = (REPO_ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py").read_text(encoding="utf-8")

    assert "/api/aegis/ai-operations/context/latest" in server_text
    assert "/api/aegis/ai-operations/response/latest" in server_text
    assert "/api/aegis/ai-operations/ask" in server_text
    assert "build_ai_operations_response_v1" in server_text
