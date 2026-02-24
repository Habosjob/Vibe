from __future__ import annotations

import scripts.run as run_script


def test_run_script_executes_all_steps(monkeypatch, capsys) -> None:
    monkeypatch.setattr(run_script, "parse_args", lambda: type("Args", (), {
        "base_dir": run_script.PROJECT_ROOT,
        "skip_universe": False,
        "skip_cashflows": False,
        "skip_screen": False,
    })())

    calls: list[str] = []
    monkeypatch.setattr(run_script.sync_moex_universe, "main", lambda: calls.append("universe") or 0)
    monkeypatch.setattr(run_script.sync_moex_cashflows, "main", lambda: calls.append("cashflows") or 0)
    monkeypatch.setattr(run_script.screen_basic, "main", lambda: calls.append("screen") or 0)

    code = run_script.main()
    out = capsys.readouterr().out

    assert code == 0
    assert calls == ["universe", "cashflows", "screen"]
    assert "Сводка: обработано этапов=3" in out
