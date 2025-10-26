from __future__ import annotations

import json
from datetime import timedelta, timezone
from pathlib import Path
from typing import Any

from bot.core.time import parse_exchange_ts, utc_now


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


def append_jsonl_daily(
    out_dir: str | Path,
    prefix: str,
    record: dict[str, Any],
    *,
    ts: object | None = None,
) -> None:
    """Append to a daily-rotated JSONL file like logs/{prefix}-YYYY-MM-DD.jsonl.

    - If ts is not provided, tries record["ts"].
    - Falls back to current UTC time.
    - Silently ignores failures to avoid impacting trading flow.
    """

    try:
        when = ts if ts not in (None, "") else record.get("ts")
        try:
            dt = parse_exchange_ts(when) if when not in (None, "") else utc_now()
        except Exception:
            dt = utc_now()
        # Use Japan Standard Time (UTC+9) for daily file boundary
        jst = timezone(timedelta(hours=9))
        date_tag = dt.astimezone(jst).date().isoformat()
        p = Path(out_dir) / f"{prefix}-{date_tag}.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    except Exception:
        return
