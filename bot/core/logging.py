from __future__ import annotations

import sys
from pathlib import Path

from loguru import logger


def setup_logging(
    *,
    level: str = "INFO",
    log_dir: str = "logs",
    human_filename: str = "app.log",
    json_filename: str = "app.jsonl",
) -> None:
    """Initialize logging files and console.

    Adds two rotating file sinks under `log_dir`:
      1) Human-readable: logs/app.log (rotated daily at UTC midnight)
      2) JSON structured: logs/app.jsonl (rotated daily at UTC midnight)
    """
    logger.remove()

    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    normalized_level = level.upper()

    human_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | {name}:{function}:{line} | {message}"
    )

    # Human-readable log (rotate daily at UTC midnight)
    logger.add(
        str(log_path / human_filename),
        level=normalized_level,
        rotation="00:00",
        retention="14 days",
        enqueue=True,
        backtrace=True,
        diagnose=False,
        format=human_format,
    )

    # JSON structured log (rotate daily at UTC midnight)
    logger.add(
        str(log_path / json_filename),
        level=normalized_level,
        rotation="00:00",
        retention="30 days",
        enqueue=True,
        backtrace=True,
        diagnose=False,
        serialize=True,
    )

    # Console sink (stdout)
    logger.add(
        sys.stdout,
        level=normalized_level,
        backtrace=True,
        diagnose=False,
        format=human_format,
    )

    logger.info("logging initialized: level={}, dir={}", normalized_level, log_dir)
