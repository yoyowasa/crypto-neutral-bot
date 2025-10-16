# これは「読みやすいログとJSONログを回転ファイルで出力する」設定を行うファイルです。
from __future__ import annotations

from pathlib import Path

from loguru import logger


def setup_logging(
    *,
    level: str = "INFO",
    log_dir: str = "logs",
    human_filename: str = "app.log",
    json_filename: str = "app.jsonl",
) -> None:
    """これは何をする関数？
    → ログ出力を初期化し、以下の2ハンドラを登録します。
      1) 人向けの読みやすいファイルログ（logs/app.log）
      2) JSON 構造化ログ（logs/app.jsonl）
    どちらもファイルサイズでローテーションし、一定期間で保持します。
    """
    # 既存ハンドラをすべて外してから付け直す
    logger.remove()

    # ログディレクトリを作る
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    human_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | {name}:{function}:{line} | {message}"
    )

    # 人向けの読みやすいログ（回転ファイル）
    logger.add(
        str(log_path / human_filename),
        level=level,
        rotation="20 MB",  # ファイルサイズで回転
        retention="14 days",  # 14日保持
        compression="zip",  # 古いログをzip圧縮
        enqueue=True,  # マルチスレッド/プロセスで安全
        backtrace=True,
        diagnose=False,
        format=human_format,
    )

    # JSON構造化ログ（1行1JSON）
    logger.add(
        str(log_path / json_filename),
        level=level,
        rotation="20 MB",
        retention="14 days",
        compression="zip",
        enqueue=True,
        backtrace=True,
        diagnose=False,
        serialize=True,  # ← JSON出力
    )

    logger.info("logging initialized: level={}, dir={}", level, log_dir)
