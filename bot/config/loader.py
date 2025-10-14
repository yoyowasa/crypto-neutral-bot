"""設定ローダ。
このファイルは「.env と YAML を読み込み、環境変数優先でマージして AppConfig を返す」ことをする。
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, MutableMapping

try:
    import yaml  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover - 実行時に例外へ委譲
    yaml = None

# YAMLを読むための外部ライブラリ（PyYAML が無い場合は load_config 内で例外を出す）
from dotenv import find_dotenv, load_dotenv  # .env を環境変数に反映

from .models import AppConfig


def _deep_set(dic: MutableMapping[str, Any], path: list[str], value: Any) -> None:
    """何をする関数：'A__B__C' のようなENVキーをネストdictに差し込む（小文字キー前提）"""

    cur = dic
    for key in path[:-1]:
        if key not in cur or not isinstance(cur[key], dict):
            cur[key] = {}
        cur = cur[key]  # 次の階層へ潜る
    cur[path[-1]] = value  # 最終キーに値を設定


def _merge_env_over_yaml(base: dict[str, Any], env: dict[str, str]) -> dict[str, Any]:
    """何をする関数：ENVの値をYAMLで読んだdictに上書きする（ENVが優先）"""

    # 対象となるトップレベル名（AppConfig のフィールド名と合わせる）
    allowed_roots = {"keys", "exchange", "risk", "strategy"}
    out: dict[str, Any] = dict(base)  # まずYAMLの内容をコピー

    for raw_key, val in env.items():
        # 大文字ENVを小文字に変換し、__ でネストとみなす
        key = raw_key.lower()
        if "__" in key:
            parts = key.split("__")
            root = parts[0]
            if root in allowed_roots:
                _deep_set(out, parts, val)
        elif key == "db_url":
            out["db_url"] = val
        elif key == "timezone":
            out["timezone"] = val
        # それ以外のENVは無視（extra="ignore" のため既知以外は落とす）
    return out


def load_config(config_path: str | Path | None = None) -> AppConfig:
    """何をする関数：.env と YAML を読み込み、型検証した AppConfig を返す"""

    # 1) .env を環境に取り込む（find_dotenv はCWDから上位を探索）
    load_dotenv(find_dotenv(usecwd=True), override=False)

    # 2) YAML 読み込み（無ければ空dict）
    #    既定はプロジェクトルートの 'config/app.yaml' を想定
    path = Path(config_path) if config_path is not None else Path("config/app.yaml")
    yaml_data: dict[str, Any] = {}
    if path.exists():
        if yaml is None:
            msg = "PyYAML がインストールされていないため YAML を読み込めません。"
            raise RuntimeError(msg)

        with path.open("r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f)
            if isinstance(loaded, dict):
                # YAMLは小文字キーで書く前提（サンプルも小文字）
                yaml_data = loaded

    # 3) ENV で上書き（ENVがあれば優先）
    merged = _merge_env_over_yaml(yaml_data, dict(os.environ))

    # 4) Pydantic で型検証しつつ構築して返す
    #    （unknown keys は extra="ignore" で無視）
    return AppConfig(**merged)


def redact_secrets(cfg: AppConfig) -> dict[str, Any]:
    """何をする関数：設定ダンプからAPIキーを伏せる"""

    dumped = cfg.model_dump()
    if "keys" in dumped:
        dumped["keys"] = {"api_key": "***", "api_secret": "***"}
    return dumped
