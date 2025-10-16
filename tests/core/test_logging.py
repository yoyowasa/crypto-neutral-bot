from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("loguru")

from bot.core.logging import setup_logging


def test_setup_logging_creates_files(tmp_path: Path) -> None:
    """ログ初期化で回転ファイル（人向け/JSON）が作られること"""
    setup_logging(level="INFO", log_dir=str(tmp_path / "logs"))
    assert (tmp_path / "logs" / "app.log").exists()
    assert (tmp_path / "logs" / "app.jsonl").exists()
