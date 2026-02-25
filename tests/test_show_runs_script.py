from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path


def test_show_runs_script_prints_json(tmp_path) -> None:
    project_root = Path(__file__).resolve().parents[1]
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True)
    db_path = state_dir / "screener_state.db"

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE runs(id INTEGER PRIMARY KEY AUTOINCREMENT, started_at TEXT, finished_at TEXT, elapsed_seconds REAL, bonds_processed INTEGER, bonds_filtered INTEGER, errors_count INTEGER, backend TEXT, notes TEXT)"
        )
        conn.execute(
            "INSERT INTO runs(started_at, finished_at, elapsed_seconds, bonds_processed, bonds_filtered, errors_count, backend, notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:05+00:00",
                5.0,
                100,
                20,
                0,
                "sqlite",
                '{"mode":"full"}',
            ),
        )
        conn.commit()

    (tmp_path / "config.yml").write_text(
        "\n".join(
            [
                "storage_backend: sqlite",
                "exclusions_state_dir: state",
                "sqlite_db_path: screener_state.db",
            ]
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [sys.executable, str(project_root / "scripts" / "show_runs.py"), "--format", "json", "--limit", "1"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )

    assert '"bonds_processed": 100' in result.stdout
    assert '"mode": "full"' in result.stdout
