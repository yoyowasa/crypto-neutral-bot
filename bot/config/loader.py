# これは「.env と YAML を読み、型付き設定(AppConfig)を返す関数」を置くファイルです。
# 環境変数 > .env > YAML > デフォルト の優先度でマージします（実装では .env を環境に取り込み、環境変数として扱います）。
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping

import yaml  # type: ignore[import-untyped]  # YAML を dict に読み込む
from dotenv import load_dotenv  # .env を環境変数に取り込む

from .models import AppConfig


def _set_nested(d: dict[str, Any], keys: list[str], value: Any) -> None:
    """ユーティリティ：['risk','max_total_notional'] のようなキー列でネスト辞書に値を入れる"""

    cur = d
    for k in keys[:-1]:
        if k not in cur or not isinstance(cur[k], dict):
            cur[k] = {}
        cur = cur[k]
    cur[keys[-1]] = value


def _env_to_nested_dict(environ: Mapping[str, str]) -> dict[str, Any]:
    """ユーティリティ：ENVを __ で分割し、AppConfig に対応するネスト辞書へ整形する"""

    result: dict[str, Any] = {}
    allowed_roots = {"KEYS", "EXCHANGE", "RISK", "STRATEGY"}
    passthrough_roots = {"DB_URL", "TIMEZONE"}  # ルート直下のキー

    for raw_key, raw_val in environ.items():
        if raw_key in passthrough_roots or any(raw_key.startswith(root + "__") for root in allowed_roots):
            parts = raw_key.lower().split("__")  # 大文字でも小文字でもOKにするため lower
            _set_nested(result, parts, raw_val)
    return result


def _deep_update(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """ユーティリティ：辞書の深いマージ。override を base に上書き反映して返す"""

    for k, v in override.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_update(base[k], v)
        else:
            base[k] = v
    return base


# ◎ 関数：load_config —— 何をする関数？
# 「.env を読み込み → YAML（config/app.yaml）を読み込み → 環境変数で上書き → AppConfig 型にして返す」
def load_config(config_path: str | os.PathLike[str] | None = None) -> AppConfig:
    # 1) .env を読み込み（存在しなくてもOK）
    load_dotenv(dotenv_path=Path(".env"), override=False)

    # 2) YAML のパスは引数 > 環境変数 APP_CONFIG_FILE > 'config/app.yaml' の優先度で決定
    if config_path is not None:
        cfg_path = Path(config_path)
    else:
        cfg_path = Path(os.environ.get("APP_CONFIG_FILE", "config/app.yaml"))

    # 3) YAML を読む（無ければ空の dict）
    yaml_data: dict[str, Any] = {}
    if cfg_path.exists():
        with cfg_path.open("r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f)
            if isinstance(loaded, dict):
                yaml_data = loaded

    # 4) 環境変数をネスト辞書に整形（.env で取り込んだ値も含まれる）
    env_data = _env_to_nested_dict(os.environ)

    # 5) YAML をベースに、環境変数で上書き
    merged = _deep_update(dict(yaml_data), env_data)

    # 6) 最後に AppConfig 型としてバリデーションしながら構築して返す
    return AppConfig(**merged)


# ◎ 関数：redact_secrets —— 何をする関数？
# 「AppConfig から秘密情報（API鍵など）を伏せた辞書を作り、表示用に返す」
def redact_secrets(config: AppConfig) -> dict[str, Any]:
    safe = config.model_dump(mode="python")
    keys = safe.get("keys")
    if isinstance(keys, dict):
        if "api_key" in keys and keys["api_key"]:
            keys["api_key"] = "***"
        if "api_secret" in keys and keys["api_secret"]:
            keys["api_secret"] = "***"
    return safe
