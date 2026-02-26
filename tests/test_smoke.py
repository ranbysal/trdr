from __future__ import annotations

import pytest


def test_package_import() -> None:
    import futures_bot  # noqa: F401


def test_cli_help_runs(capsys: pytest.CaptureFixture[str]) -> None:
    from futures_bot.cli import main

    with pytest.raises(SystemExit) as exc_info:
        main(["--help"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "futures-bot" in output
    assert "backtest" in output
    assert "paper" in output
    assert "validate-config" in output
