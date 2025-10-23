from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def append_jsonl(path: str | Path, record: dict[str, Any]) -> None:
    """Append a single JSON object to a JSONL file.

    Ensures the parent directory exists. Failures are silently ignored
    to avoid impacting trading flow.
    """

    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    except Exception:
        # Logging or persistence must not block execution
        return
