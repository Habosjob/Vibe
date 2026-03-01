from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


class CheckpointStore:
    def __init__(self, checkpoint_path: Path):
        self.path = checkpoint_path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            self.state = json.loads(self.path.read_text(encoding="utf-8"))
        else:
            self.state = {}

    def get(self, key: str) -> Dict[str, Any]:
        return self.state.get(key, {})

    def set(self, key: str, value: Dict[str, Any]) -> None:
        self.state[key] = value
        self.path.write_text(json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf-8")
