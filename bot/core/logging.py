from __future__ import annotations

import logging  # 標準logging→loguru(JSONL)ブリッジ用
import os
import re
import sys
from pathlib import Path
from types import FrameType
from typing import Iterable

from loguru import logger

ORDER_INFO_PREFIXES = (
    "order.submit",
    "order.placed",
    "order.ack",
    "order.reject",
    "order.cancel",
    "order.amend",
    "order.amended",
    "fill.",
    "position.",
    "trade.",
)  # 注文/約定/ポジ系イベントをINFOへ昇格させる対象


def _loguru_origin_patcher(record: dict) -> None:
    # loguru直書きログにも origin* を自動付与する
    extra = record["extra"]
    if "origin" in extra:
        return
    extra["origin"] = record["name"]  # ロガー名
    extra["origin_module"] = record["module"]  # モジュール名
    extra["origin_func"] = record["function"]  # 関数名
    extra["origin_file"] = record["file"].name  # ファイル名
    extra["origin_line"] = record["line"]  # 行番号
    # loguru直書きでDEBUGの注文系イベントはINFOへ昇格（JSONLのINFOシンクで確実に可視化）
    if record["level"].name == "DEBUG" and record["message"].startswith(ORDER_INFO_PREFIXES):
        record["level"].name = "INFO"
        record["level"].no = logger.level("INFO").no


def apply_loguru_patcher() -> None:
    # loguru全体に patcher を適用して origin* メタを保証する
    logger.configure(patcher=_loguru_origin_patcher)  # type: ignore[arg-type]


# 注文イベントを検出してINFOへ昇格させるためのパターン群
ORDER_PROMOTION_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"order\.(submit|place|placed|ack|reject|cancel|amend)", re.IGNORECASE),
    re.compile(r"position\.(open|increase|decrease|close|update)", re.IGNORECASE),
    re.compile(r"(fill|executed|trade_id|約定|成交)", re.IGNORECASE),
)


def _parse_debug_modules(raw: str | Iterable[str] | None) -> tuple[str, ...]:
    """LOG_DEBUG_MODULES �ȂǂőΏۃ��W���[�����w�肷��p�b�`�B"""

    if raw is None:
        return ()
    if isinstance(raw, str):
        items = [x.strip() for x in raw.split(",")]
    else:
        items = [str(x).strip() for x in raw]
    return tuple(x for x in items if x)


def _level_filter_factory(base_level_no: int, debug_modules: tuple[str, ...]):
    """�f�t�H���gINFO�ł�debug_modules�̂�DEBUG��A�b�v���A���̕ς݂͏���B"""

    debug_no = logger.level("DEBUG").no

    def _filter(record: dict) -> bool:
        level_no = record["level"].no
        if level_no >= base_level_no:
            return True
        if level_no == debug_no and debug_modules:
            name = record["extra"].get("origin") or record.get("name")
            return any(name and name.startswith(m) for m in debug_modules)
        return False

    return _filter


class InterceptHandler(logging.Handler):
    """標準loggingのレコードをloguruへ転送する中継ハンドラ。"""

    def emit(self, record: logging.LogRecord) -> None:
        msg = record.getMessage()
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        if record.levelno < logging.INFO and any(p.search(msg) for p in ORDER_PROMOTION_PATTERNS):
            level = "INFO"
        frame: FrameType | None = logging.currentframe()
        depth = 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        if record.levelno == logging.DEBUG and msg.startswith(ORDER_INFO_PREFIXES):
            level = "INFO"

        logger.bind(
            origin=record.name,
            origin_module=record.module,
            origin_func=record.funcName,
            origin_file=record.filename,
            origin_line=record.lineno,
        ).opt(depth=depth, exception=record.exc_info).log(level, msg)


def _inject_origin(record: dict) -> None:
    """loguru直書きログにもorigin系メタ情報を付与するパッチャ。"""

    extra = record.setdefault("extra", {})
    if "origin" in extra:
        return None

    extra["origin"] = record.get("name")
    extra["origin_module"] = record.get("module")
    extra["origin_func"] = record.get("function")

    file_info = record.get("file", {})
    if isinstance(file_info, dict):
        extra["origin_file"] = file_info.get("name")
    else:
        extra["origin_file"] = getattr(file_info, "name", None)

    extra["origin_line"] = record.get("line")
    return None


def setup_std_logging_bridge() -> None:
    """標準loggingのrootをInterceptHandlerに置き換えてloguruへ橋渡しする。"""

    root = logging.getLogger()
    if not any(isinstance(h, InterceptHandler) for h in root.handlers):
        root.handlers.append(InterceptHandler())  # 既存ハンドラを保持したままブリッジを追加
    root.setLevel(logging.NOTSET)
    logging.captureWarnings(True)


def setup_logging(
    *,
    level: str = "INFO",
    log_dir: str = "logs",
    human_filename: str = "app.log",
    json_filename: str = "app.jsonl",
    debug_modules: Iterable[str] | None = None,
) -> None:
    """Initialize logging files and console.

    Adds two rotating file sinks under `log_dir`:
      1) Human-readable: logs/app.log (rotated by size, keep ~10 files)
      2) JSON structured: logs/app.jsonl (rotated by size, keep ~10 files)
    """
    logger.configure(patcher=_inject_origin)  # type: ignore[arg-type]
    logger.remove()

    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    normalized_level = level.upper()
    try:
        base_level_no = logger.level(normalized_level).no
    except ValueError:
        base_level_no = logger.level("INFO").no
    debug_modules_raw = debug_modules if debug_modules is not None else os.getenv("LOG_DEBUG_MODULES")
    debug_modules_tuple = _parse_debug_modules(debug_modules_raw)
    level_filter = _level_filter_factory(base_level_no, debug_modules_tuple)

    human_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | {name}:{function}:{line} | {message}"
    )

    # JSTで日次ローテーション
    human_rotation = "00:00"
    json_rotation = "00:00"
    human_retention = 10  # �ő�10�t�@�C���ʂ�ho���Ȃ��߂̃��[�e�B�V����
    json_retention = 10

    # Human-readable log (size rotation)
    logger.add(
        str(log_path / human_filename),
        level="DEBUG",
        rotation=human_rotation,
        retention=human_retention,
        enqueue=True,
        backtrace=True,
        diagnose=False,
        encoding="utf-8",
        format=human_format,
        filter=level_filter,
    )

    # JSON structured log (size rotation)
    logger.add(
        str(log_path / json_filename),
        level="DEBUG",
        rotation=json_rotation,
        retention=json_retention,
        enqueue=True,
        backtrace=True,
        diagnose=False,
        encoding="utf-8",
        serialize=True,
        filter=level_filter,
    )
    apply_loguru_patcher()  # JSONLシンク設定直後にorigin*自動付与を有効化

    setup_std_logging_bridge()  # 標準logging→loguru(JSONL)ブリッジを起動

    # Console sink (stdout)
    logger.add(
        sys.stdout,
        level="DEBUG",
        backtrace=True,
        diagnose=False,
        format=human_format,
        filter=level_filter,
    )

    logger.info(
        "logging init level={} dir={} mods={} rot_human={} rot_json={} ret_human={} ret_json={}",
        normalized_level,
        log_dir,
        debug_modules_tuple,
        human_rotation,
        json_rotation,
        human_retention,
        json_retention,
    )
