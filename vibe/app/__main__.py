from __future__ import annotations

import sys

from vibe.app.cli import main

if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.argv.append("moex-bond-rates")
    raise SystemExit(main())
