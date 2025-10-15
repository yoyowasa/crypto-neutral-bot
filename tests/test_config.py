from __future__ import annotations

import importlib

import pytest

pytest.importorskip("yaml")

load_config = importlib.import_module("bot.config.loader").load_config


def test_load_config_env_overrides(tmp_path, monkeypatch):
    # 一時YAML
    yaml_text = """
exchange:
  environment: testnet
risk:
  max_total_notional: 10000
  max_symbol_notional: 5000
  max_net_delta: 0.001
  max_slippage_bps: 10
  loss_cut_daily_jpy: 20000
"""
    p = tmp_path / "app.yaml"
    p.write_text(yaml_text, encoding="utf-8")

    # 環境変数で上書き
    monkeypatch.setenv("KEYS__API_KEY", "abc")
    monkeypatch.setenv("KEYS__API_SECRET", "xyz")
    monkeypatch.setenv("EXCHANGE__ENVIRONMENT", "mainnet")

    cfg = load_config(str(p))
    assert cfg.keys.api_key == "abc"
    assert cfg.keys.api_secret == "xyz"
    assert cfg.exchange.environment == "mainnet"
