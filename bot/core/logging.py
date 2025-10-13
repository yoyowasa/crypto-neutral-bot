from __future__ import annotations

import sys
from pathlib import Path

from loguru import logger


def setup_logging(
    log_file: str | Path = "logs/app.log",
    level: str = "INFO",
    rotation: str = "10 MB",
    retention: str = "14 days",
    enqueue: bool = True,
) -> None:
    """
    ログ出力の初期化。
    - コンソール: human readable
    - ファイル   : rotation / retention 付き

    Windows / Linux 双方で動作。log_dir がなければ作成します。
    """
    # 既存ハンドラを全解除
    logger.remove()

    # Console
    logger.add(sys.stderr, level=level)

    # File
    p = Path(log_file)
    p.parent.mkdir(parents=True, exist_ok=True)
    logger.add(
        p.as_posix(),
        level=level,
        rotation=rotation,
        retention=retention,
        enqueue=enqueue,  # マルチスレッド/プロセスでも詰まらない
        backtrace=False,
        diagnose=False,
    )


__all__ = ["logger", "setup_logging"]
