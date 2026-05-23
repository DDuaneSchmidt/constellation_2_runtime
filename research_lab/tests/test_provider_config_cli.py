from __future__ import annotations

from pathlib import Path

from research_lab.cli import main


def test_create_provider_env_template_refuses_overwrite_by_default(tmp_path: Path) -> None:
    output = tmp_path / ".env.local"
    output.write_text("existing=true\n", encoding="utf-8")

    code = main(["create-provider-env-template", "--output", str(output)])

    assert code == 3
    assert output.read_text(encoding="utf-8") == "existing=true\n"


def test_create_provider_env_template_confirms_output_is_gitignored(tmp_path: Path, capsys) -> None:
    (tmp_path / ".gitignore").write_text(".env.local\n", encoding="utf-8")
    output = tmp_path / ".env.local"

    code = main(["create-provider-env-template", "--output", str(output)])
    captured = capsys.readouterr()

    assert code == 0
    assert output.exists()
    assert '"gitignored": true' in captured.out


def test_provider_config_status_never_prints_full_secret(tmp_path: Path, capsys) -> None:
    secret = "tiingo-secret-abcdef"
    env_file = tmp_path / ".env.local"
    env_file.write_text(f"TIINGO_API_KEY={secret}\n", encoding="utf-8")

    code = main(["provider-config-status", "--env-file", str(env_file)])
    captured = capsys.readouterr()

    assert code == 0
    assert secret not in captured.out
    assert "ti***ef" in captured.out


def test_provider_diagnostics_accepts_env_file_and_passes_secret(monkeypatch, tmp_path: Path, capsys) -> None:
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)
    (tmp_path / ".gitignore").write_text(".env.local\n", encoding="utf-8")
    env_file = tmp_path / ".env.local"
    env_file.write_text("TIINGO_API_KEY=tiingo-secret-abcdef\n", encoding="utf-8")

    code = main(["provider-diagnostics", "--provider", "tiingo", "--env-file", str(env_file)])
    captured = capsys.readouterr()

    assert code == 0
    assert '"api_key_present": true' in captured.out
    assert "tiingo-secret-abcdef" not in captured.out


def test_first_api_dataset_status_exact_commands_include_env_file(monkeypatch, tmp_path: Path, capsys) -> None:
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)
    (tmp_path / ".gitignore").write_text(".env.local\n", encoding="utf-8")
    env_file = tmp_path / ".env.local"
    env_file.write_text("TIINGO_API_KEY=tiingo-secret-abcdef\n", encoding="utf-8")

    code = main(
        [
            "first-api-dataset-status",
            "--provider",
            "tiingo",
            "--universe",
            "local_etf_minimum_viable_v1",
            "--env-file",
            str(env_file),
        ]
    )
    captured = capsys.readouterr()

    assert code == 0
    assert f"--env-file {env_file.resolve()}" in captured.out
