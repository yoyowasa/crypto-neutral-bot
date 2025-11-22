# このモジュールは「.env + YAML から AppConfig を構築する」ためのヘルパーです。
# 優先順位は「環境変数 > .env > YAML > デフォルト値」となります。
from __future__ import annotations

import os
import re  # .env内の 'export ' や 'KEY: value' を正規化するために使用
from io import StringIO  # テキストをストリーム化して python-dotenv に渡すために使用
from pathlib import Path
from typing import Any, Mapping

import yaml  # type: ignore[import-untyped]  # YAML を dict に読み込むだけなので型はゆるく扱う
from dotenv import dotenv_values  # .env を環境変数として読み込むためのライブラリ

from .models import AppConfig


def _set_nested(d: dict[str, Any], keys: list[str], value: Any) -> None:
    """ネスト辞書用ヘルパー: ['risk','max_total_notional'] のようなキー列で入れ子に値を設定する。"""

    cur = d
    for k in keys[:-1]:
        if k not in cur or not isinstance(cur[k], dict):
            cur[k] = {}
        cur = cur[k]
    cur[keys[-1]] = value


def _env_to_nested_dict(environ: Mapping[str, str]) -> dict[str, Any]:
    """ENV を AppConfig 互換のネスト辞書に変換する。

    - KEYS__/EXCHANGE__/RISK__/STRATEGY__ のプレフィックスを持つキーだけを取り込む。
    - DB_URL / TIMEZONE はトップレベルのキーとして素通しする。
    """

    result: dict[str, Any] = {}
    allowed_roots = {"KEYS", "EXCHANGE", "RISK", "STRATEGY"}
    passthrough_roots = {"DB_URL", "TIMEZONE"}  # ルート直下に置くキー

    for raw_key, raw_val in environ.items():
        if raw_key in passthrough_roots or any(raw_key.startswith(root + "__") for root in allowed_roots):
            # 大文字小文字は区別しない運用にするため lower に寄せておく
            parts = raw_key.lower().split("__")
            _set_nested(result, parts, raw_val)
    return result


def _deep_update(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """dict の深いマージ: override の内容で base を上書きして返す。"""

    for k, v in override.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_update(base[k], v)
        else:
            base[k] = v
    return base


def load_env_robust(dotenv_path: Path, override: bool = True) -> dict:
    """
    .env をエンコーディング自動判別付きで安全に読み込み、
    python-dotenv を使ってキー/値をパースし、必要に応じて os.environ に反映する。

    - UTF-8 / UTF-16 / BOM 付き / ゼロ幅スペース混入などにできるだけ耐える。
    - 'export KEY=VAL' や 'KEY: VAL' も 'KEY=VAL' として扱う。
    - override=False の場合は「既存の環境変数を上書きしない」（環境変数 > .env の優先順位）。
    """

    try:
        # バイナリで開いて BOM やエンコーディングを自前で判別する
        with open(dotenv_path, "rb") as f:
            data = f.read()
    except FileNotFoundError:
        return {}

    # --- エンコーディングの当たりを付ける（簡易 BOM 判定） ---
    encoding = "utf-8"
    if data.startswith(b"\xff\xfe"):
        encoding = "utf-16-le"
    elif data.startswith(b"\xfe\xff"):
        encoding = "utf-16-be"
    elif data.startswith(b"\xef\xbb\xbf"):
        encoding = "utf-8-sig"

    # --- デコード（失敗したら別候補でリトライ） ---
    candidates = (encoding, "utf-8", "utf-8-sig", "utf-16", "cp932")
    text: str | None = None
    for enc in candidates:
        try:
            text = data.decode(enc)
            break
        except Exception:
            continue
    if text is None:
        # どのエンコーディングでも読めない場合はあきらめる
        return {}

    # --- ゼロ幅スペースや BOM を除去してクリーンなテキストにする ---
    cleaned = text.replace("\ufeff", "").replace("\u200b", "").replace("\u200c", "").replace("\u200d", "")

    # シェル由来の 'export KEY=VAL' を 'KEY=VAL' に正規化
    cleaned = re.sub(r"^\s*export\s+", "", cleaned, flags=re.MULTILINE)
    # 'KEY: VAL' 形式も 'KEY=VAL' に正規化
    cleaned = re.sub(
        r"^([A-Za-z_][A-Za-z0-9_]*)\s*:\s*",
        r"\1=",
        cleaned,
        flags=re.MULTILINE,
    )

    # --- python-dotenv でパースし、必要なら os.environ に反映 ---
    values = dotenv_values(stream=StringIO(cleaned))
    for k, v in values.items():
        if v is None:
            continue
        # 既に OS 側で設定されている環境変数は優先する。
        # 「環境変数 > .env > YAML」という優先順位を守るため、
        # override=True のときだけ上書きし、それ以外は未設定のキーだけ補完する。
        if override or k not in os.environ:
            os.environ[k] = v

    return values


def load_config(config_path: str | os.PathLike[str] | None = None) -> AppConfig:
    """`.env` と YAML を読み込み、AppConfig インスタンスを構築して返す。

    優先順位:
        1. 既存の環境変数（pytest の monkeypatch などを含む）
        2. .env ファイル（既存の環境変数を上書きしない）
        3. YAML (`config/app.yaml` など)
        4. AppConfig のデフォルト値
    """

    # 1) .env から環境変数を補完する
    #    既に os.environ に存在するキーは .env で上書きしない（override=False）。
    load_env_robust(Path(".env"), override=False)

    # 2) YAML のパス:
    #    明示指定 (config_path) > 環境変数 APP_CONFIG_FILE > デフォルト 'config/app.yaml'
    if config_path is not None:
        cfg_path = Path(config_path)
    else:
        cfg_path = Path(os.environ.get("APP_CONFIG_FILE", "config/app.yaml"))

    # 3) YAML を dict として読み込む（なければ空 dict）
    yaml_data: dict[str, Any] = {}
    if cfg_path.exists():
        with cfg_path.open("r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f)
            if isinstance(loaded, dict):
                yaml_data = loaded

    # 4) 環境変数をネスト辞書に変換
    env_data = _env_to_nested_dict(os.environ)

    # 5) YAML ベースに環境変数をマージ
    merged = _deep_update(dict(yaml_data), env_data)

    # 6) AppConfig としてバリデーションしつつインスタンス化
    return AppConfig.from_dict(merged)


def redact_secrets(config: AppConfig) -> dict[str, Any]:
    """AppConfig から API キーなどの機密情報をマスクした dict を返す。"""

    safe = config.to_dict()
    keys = safe.get("keys")
    if isinstance(keys, dict):
        if "api_key" in keys and keys["api_key"]:
            keys["api_key"] = "***"
        if "api_secret" in keys and keys["api_secret"]:
            keys["api_secret"] = "***"
        if "passphrase" in keys and keys["passphrase"]:
            keys["passphrase"] = "***"
    return safe
