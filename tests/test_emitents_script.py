from __future__ import annotations

import json

from scripts.run_emitents import _load_eligible_bonds


def test_load_eligible_bonds_reads_state_payload(tmp_path):
    path = tmp_path / "eligible_bonds.json"
    path.write_text(
        json.dumps({"bonds": {"A": {"SECID": "A"}, "B": {"SECID": "B", "EMITTER_ID": "2"}}}),
        encoding="utf-8",
    )

    rows = _load_eligible_bonds(path)

    assert rows == [{"SECID": "A"}, {"SECID": "B", "EMITTER_ID": "2"}]
