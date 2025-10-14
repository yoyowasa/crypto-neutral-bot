"""設定ローダの最小テスト。
このテストは「YAMLを読み、.envで上書きできる」ことを検証する。
"""

import os
from pathlib import Path

from bot.config.loader import load_config


def test_load_config_env_overrides_yaml(tmp_path: Path, monkeypatch):
    # 1) テスト用の作業ディレクトリを作る
    work = tmp_path
    (work / "config").mkdir(parents=True, exist_ok=True)

    # 2) YAML を用意（APIキーはダミー。ENVで上書きされる想定）
    (work / "config" / "app.yaml").write_text(
        "\n".join(
            [
                "keys:",
                "  api_key: YAML_KEY",
                "  api_secret: YAML_SECRET",
                "exchange:",
                "  name: bybit",
                '  environment: "testnet"',
                "risk:",
                "  max_total_notional: 10000",
                "  max_symbol_notional: 5000",
                "  max_net_delta: 0.001",
                "  max_slippage_bps: 10",
                "  loss_cut_daily_jpy: 20000",
                "strategy:",
                "  symbols: [\"BTCUSDT\", \"ETHUSDT\"]",
                "  min_expected_apr: 0.05",
                "  pre_event_open_minutes: 60",
                "  hold_across_events: true",
                "  rebalance_band_bps: 5.0",
                'db_url: "sqlite+aiosqlite:///./db/trading.db"',
                'timezone: "UTC"',
            ]
        ),
        encoding="utf-8",
    )

    # 3) .env の代わりに、環境変数を直接設定（ENVがYAMLを上書きすることを確認）
    monkeypatch.setenv("KEYS__API_KEY", "ENV_KEY")
    monkeypatch.setenv("KEYS__API_SECRET", "ENV_SECRET")
    assert os.environ["KEYS__API_KEY"] == "ENV_KEY"

    # 4) CWD をテスト用ディレクトリに変更してローダを呼ぶ
    monkeypatch.chdir(work)
    cfg = load_config()

    # 5) ENV が YAML を上書きしていることの確認
    assert cfg.keys.api_key == "ENV_KEY"
    assert cfg.keys.api_secret == "ENV_SECRET"
    assert cfg.exchange.environment == "testnet"
    assert "sqlite+aiosqlite" in cfg.db_url
    assert cfg.strategy.symbols == ["BTCUSDT", "ETHUSDT"]
