from __future__ import annotations
from pathlib import Path
import os
import yaml
from typing import Any, Callable
from pydantic_settings import SettingsConfigDict
from .models import AppConfig

def make_yaml_source(yaml_path: Path) -> Callable[[], dict[str, Any]]:
    """
    YAML を読み取り dict を返す関数ソース（引数なし）を生成。
    後ろに置くほど優先度が低くなる（先に並べたソースが勝つ）。
    """
    path = Path(yaml_path)

    def _source() -> dict[str, Any]:
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        # AppConfig のフィールド(keys, exchange, risk, strategy, db_url, timezone)に対応
        return data

    return _source

def load_config(config_path: str | None = None) -> AppConfig:
    """
    優先順位: init引数 > 環境変数 > .env > YAML(最低)
    config_path 未指定時は APP_CONFIG or 'config/app.yaml'
    """
    yaml_path = Path(config_path) if config_path else Path(os.environ.get("APP_CONFIG", "config/app.yaml"))

    class _AppConfig(AppConfig):
        model_config = SettingsConfigDict(
            env_file=".env",
            env_nested_delimiter="__",
            extra="ignore",
        )

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls,      # v2 では最初に settings_cls が来る
            init_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
        ):
            # YAML は最後（最低優先）に追加
            yaml_source = make_yaml_source(yaml_path)
            return (
                init_settings,
                env_settings,
                dotenv_settings,
                file_secret_settings,
                yaml_source,
            )

    return _AppConfig()

def redact_secrets(cfg: AppConfig) -> dict:
    d = cfg.model_dump()
    if "keys" in d:
        d["keys"] = {"api_key": "***", "api_secret": "***"}
    return d
