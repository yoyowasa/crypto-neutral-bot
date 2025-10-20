これは何をするファイル？

いまの進行状況スナップショットをそのまま履歴化します（以降も追記して使えます）。

# Progress Log

### 7. STEP13–25 実装ログ（Bybit v5 安定化と安全化）

- STEP13: Funding情報の最終確定（Bybit v5）
  - 予測レートと次回時刻を Ticker 優先で取得し、欠落時は history / instruments-info で補完。
  - 変更: `bot/exchanges/bybit.py: get_funding_info`
  - 型: `bot/exchanges/types.py: FundingInfo` に `funding_interval_hours` を保持。

- STEP14: client_order_id（orderLinkId）の往復配線
  - OMS 未指定時に `bot-<uuid32hex>` を自動採番して発注へ通す。
  - 変更: `bot/oms/engine.py: submit()`／`bot/exchanges/bybit.py: place_order()`（orderLinkId を必ず送る）
  - 型: `bot/exchanges/types.py: OrderRequest / Order` に `client_order_id` を追加。

- STEP15: Private WS イベントへ client_order_id を確実に載せる
  - 変更: `bot/app/live_runner.py` の private order/execution コールバックで `client_order_id` を設定。

- STEP16: client_order_id での取消（orderId 不明でも安全）
  - 変更: `bot/exchanges/base.py: cancel_order`、`bot/exchanges/bybit.py: cancel_order`（orderId or orderLinkId）
  - OMS 側: `bot/oms/engine.py` は `client_order_id` を優先で渡す／Paper はシグネチャ合わせ。

- STEP17: 同一 client_order_id の二重発注ブロック
  - 変更: `bot/oms/engine.py` に `_inflight_client_ids` を導入。`submit()` で重複チェック→登録、`on_execution_event()` で終端時に掃除。

- STEP18: 再起動/再接続で open 注文を再構築（reconcile）
  - 変更: `bot/exchanges/bybit.py: get_open_orders`（/v5/order/realtime）
  - OMS: `reconcile_inflight_open_orders(symbols)` を追加し、Private WS 接続直後に一度呼ぶ（`bot/app/live_runner.py`）。

- STEP19: REST に指数バックオフ再試行（@retryable）
  - 変更: `bot/exchanges/bybit.py` の `place_order / cancel_order / get_funding_info / get_open_orders` に `@retryable()` を付与。

- STEP20: シンボル正規化＆カテゴリ自動判定（spot/linear/inverse）
  - 変更: `bot/exchanges/bybit.py: _resolve_category` を実装、instruments-info で正しく判定＆キャッシュ。既存の endswith 判定は置換。

- STEP21: 価格/数量の正規化（tickSize/qtyStep/min-max）
  - 追加: `bybit.py: _get_instrument_info / _quantize_step / _dec_to_str / _normalize_price_qty`
  - `place_order` 送信直前に丸め・範囲保証・指数表記なし化を実施。

- STEP22: WS ステータス正規化＋部分約定の進捗をイベントに載せる
  - 変更: `bot/app/live_runner.py`（order/execution）で標準化ステータスと cumExecQty/avgPrice/execQty/execPrice を搭載。

- STEP23: PostOnly / ReduceOnly / TIF をエンドツーエンド配線
  - 型: `OrderRequest` の `time_in_force/post_only/reduce_only` を活用。
  - 変更: `bybit.py: place_order` で `timeInForce="PostOnly"` 優先、先物系のみ `reduceOnly=True`。戦略のクローズ系注文は `reduce_only=True`。

- STEP24: PostOnly 価格を「非クロス」に自動微調整（BBO 参照）
  - 追加: `bybit.py: _ensure_post_only_price`（BUY→ask-1tick / SELL→bid+1tick、安全側で再丸め）
  - `place_order` で PostOnly の Limit 時のみ適用。

- STEP25: 公開 WS の BBO をゲートウェイでキャッシュ（低遅延化）
  - 追加: `bybit.py: update_bbo / get_bbo` と `_bbo_cache`。`_ensure_post_only_price` は WS キャッシュ優先、無ければ REST 補完。
  - `bot/app/live_runner.py` で orderbook トピックから BBO を `update_bbo()` に反映。

— 総合効果 —

- 安定性: @retryable、カテゴリ/丸めの正規化、WS/REST の両輪で落ちにくく復帰しやすい。
- 安全性: reduce_only、二重発注ブロック、再接続時の open 再構築で事故を未然に防止。
- 充実データ: Funding・BBO・部分約定進捗が正しく取得・伝搬され、戦略/レポートの精度が向上。
- コスト最適化: PostOnly のメイカー維持と非クロス自動補正で手数料と失敗を削減。

## 2025-10-19 — Manual Snapshot
（このスナップショットはChatの要約を手動登録したものです）

### 1. 全体サマリ（何が入っていて何が動くか）

**実装済み（コード／テスト／動作確認まで）**

- STEP0 仕様文書：`docs/spec_mvp.md`
- STEP2 設定と型：`bot/config/models.py`, `bot/config/loader.py`, `.env.example`, `config/app.yaml`
- STEP3 コア：`bot/core/logging.py`, `bot/core/time.py`, `bot/core/retry.py`, `bot/core/errors.py`（コアのpytest通過）
- STEP4 Exchange抽象＋Bybit実装 雛形（REST: ccxt、WS: v5）
- STEP5 DB永続化（SQLite/SQLAlchemy）：スキーマ＋Repo
- STEP6 OMS（状態機械、タイムアウト、再送、ヘッジ命令）
- STEP7 リスク・キルスイッチ（事前チェック／事後監視／flatten_all）
- STEP8 戦略MVP（Funding／ベーシス捕捉：評価→実行→再ヘッジ→全クローズ）
- STEP9 Paper取引（疑似約定：ベストBBOでMarket即時、Limitは板内成立）＋`paper_runner`
- STEP10 Liveランナー（Testnet/Mainnet切替、Private WS→OMSイベント、`--dry-run`）
- STEP11 監視・レポート（心拍メトリクス30秒／日次Markdown）
- STEP12 バックテスト基盤（CSV/Parquet再生＋Funding適用、簡易コスト）

**品質ゲート**

- `ruff` と `mypy` が成功（`ruff check .`／`mypy` 実行OK）。
- `pytest` でコアテスト通過（他テストは段階的に有効化想定）。

**Paper モード稼働**

- 起動・心拍ログ出力を確認（`metrics heartbeat`）。ログは `logs/app.log`／`logs/app.jsonl`。

---

### 2. 実装の要点（主要モジュールと関数の「これは何をする？」）

#### 2.1 コア（STEP3）

- `bot/core/logging.py`  
  - `setup_logging()`：人向けログ（`logs/app.log`）＋JSON行（`logs/app.jsonl`）の2ハンドラを回転出力。
- `bot/core/time.py`  
  - `utc_now()`：UTC aware datetime  
  - `sleep_until(dt)`：指定時刻まで非同期スリープ  
  - `parse_exchange_ts(x)`：取引所TS（epoch秒/ms/ISO/naive）→UTC正規化
- `bot/core/retry.py`  
  - `@retryable`：指数バックオフ付き再試行（同期/非同期どちらも）
- `bot/core/errors.py`  
  - `ExchangeError / RateLimitError / WsDisconnected / RiskBreach / ConfigError / DataError`

#### 2.2 設定（STEP2）

- `bot/config/models.py`  
  - `AppConfig`：`keys/exchange/risk/strategy/db_url/timezone` を一括管理  
  - `StrategyFundingConfig`：`symbols/min_expected_apr/pre_event_open_minutes/hold_across_events/rebalance_band_bps`
- `bot/config/loader.py`  
  - `load_config()`：`.env` と `config/app.yaml` を読み、**環境変数が優先**でマージ（ネストは `__` 区切り）

#### 2.3 取引所ゲートウェイ（STEP4）

- `bot/exchanges/types.py`：`Balance/Position/OrderRequest/Order/FundingInfo`
- `bot/exchanges/base.py`：抽象 `ExchangeGateway`（残高・ポジ・注文・Ticker・Funding・WS購読）
- `bot/exchanges/bybit.py`（雛形＋v5コメント写経）  
  - `subscribe_public/subscribe_private`：Bybit v5のPublic/Private WSを購読してコールバック  
  - `get_funding_info`：predicted/nextの受け渡し（※フィールド名はTODO:要API確認）

#### 2.4 データ永続化（STEP5）

- `bot/data/schema.py`：`TradeLog / OrderLog / PositionSnap / FundingEvent / DailyPnl`
- `bot/data/repo.py`：`create_all()` と CRUD 群

#### 2.5 OMS（STEP6）

- `bot/oms/engine.py`  
  - `submit()`：注文発行  
  - `cancel()`：取消  
  - `submit_hedge(symbol, delta_to_neutral)`：デルタをゼロ付近へ  
  - `on_execution_event(event)`：注文状態更新（NEW→SENT→PARTIAL→FILLED/CANCELED/REJECTED）

#### 2.6 リスク（STEP7）

- `bot/risk/limits.py`：名目/スリッページ/デルタの事前チェック  
- `bot/risk/guards.py`：WS遅延・APIエラーバースト・日次損失・Funding符号反転の監視→`flatten_all`

#### 2.7 戦略（STEP8）

- `bot/strategy/funding_basis/models.py`  
  - `Decision`, `annualize_rate`, `basis`, `net_delta_(bps)`, `notional_candidate`
- `bot/strategy/funding_basis/engine.py`  
  - `evaluate()`：Funding予想・APR閾値・next funding・ベーシスから `OPEN/CLOSE/HOLD`  
  - `execute()`：`predicted>0 → perp sell + spot buy`／`predicted<0 → perp buy + spot sell(在庫のみ)`  
  - `flatten_all()`：全銘柄クローズ

#### 2.8 Paper（STEP9）

- `bot/oms/fill_sim.py`（`PaperExchange`）  
  - Market→BBOで即時約定／Limit→板内成立  
  - `handle_public_msg()`：orderbook の `data` が list/dict どちらでも処理できるよう補強（修正点）  
  - OMSへ `on_execution_event()` を発火
- `bot/app/paper_runner.py`  
  - Public WS購読 → PaperExchangeにBBO/Tradeを転送 → 戦略を数秒ステップ  
  - `finally`でタスク停止＆データ源の`close`呼出（修正点）

#### 2.9 Live（STEP10）

- `bot/app/live_runner.py`  
  - `--env testnet|mainnet`／`--dry-run`（Paperでフル経路）／`--flatten-on-exit`  
  - Private WSの `order/execution/position` を OMSイベントへマップ  
  - `@retryable` で WS/step の自動復帰

#### 2.10 監視・レポート（STEP11）

- `bot/monitor/metrics.py`（`MetricsLogger`）  
  - 30秒ごとに **ネットデルタbps／名目／価格／日次PnL概算** をログ出力
- `bot/monitor/report.py`（`generate_daily_report`／`ReportScheduler`）  
  - 日次Markdownレポートを `reports/YYYY-MM-DD.md` に出力（UTC 00:05）

#### 2.11 バックテスト（STEP12）

- `bot/backtest/costs.py`：手数料・スリッページの簡易モデル  
- `bot/backtest/replay.py`（`BacktestRunner`）  
  - CSV/Parquetの価格とFunding CSVで1日再生 → Paper約定 → Funding到来で実現PnL記録  
  - CLI：`--prices/--funding/--date`

---

### 3. この期間に入れた修正点（重要度順）

- **WS orderbook 互換性（Paper）**  
  `fill_sim.py::handle_public_msg()` を修正：`data`がlist/dict双方に対応、`b/a`が無い時は `bp/ap` 等の代替キーを参照。  
  効果：`KeyError: 0` が解消、Public WSのフォーマット揺れで落ちない。

- **停止時のリソース解放**  
  `paper_runner.py`：`finally` で全タスクキャンセル＋データ源の `close()` を `await`（Unclosed connector 抑止）。  
  `bybit.py`：Public WS `subscribe_public()` を `finally`で必ず `ws.close()`。  
  `BybitGateway.close()`：ccxt async クライアントやWSを明示Close（新規追加）。  
  （補足）REST呼び出しで ccxtインスタンスを都度生成しない運用に統一（単一 `self._ccxt` を共有）。

- **ツールチェーンの警告解消＆実行方法統一**  
  `pyproject.toml`：旧 `[tool.poetry.dev-dependencies]` を削除、新形式 `[tool.poetry.group.dev.dependencies]` に統一。  
  `ruff`：コマンドを `ruff check .` に更新（新仕様）。  
  `mypy`：`files=["bot"]` かつ `tests.*` を ignore（MVP段階はテストの厳密型を要求しない）— 設定は `pyproject.toml` に反映済み。

---

### 4. 動作確認の事実（ログ／実行結果）

- **コアテスト**：`poetry run pytest -q tests/core` → 合格  
- **静的検査**：`poetry run ruff check .`／`poetry run mypy` → **All checks passed!**  
- **Paper起動**：`poetry run python -m bot.app.paper_runner --config config/app.yaml`  
  - `logs/app.log` に `logging initialized ...` → **OK**  
  - `metrics heartbeat: sym=BTCUSDT / ETHUSDT ...` が30秒ごとに出力 → **OK**  
  - 停止時（Ctrl+C）に `Unclosed connector` が出ないことを確認（出る場合は `BybitGateway.close()` と単一 `self._ccxt` が未適用の可能性あり）

---

### 5. 運用・引継ぎのポイント（実務で困らないために）

- **秘密情報**：`.env` に `KEYS__API_KEY`／`KEYS__API_SECRET` を設定（コミット禁止）  
- **環境切替**：Liveランナーの `--env testnet|mainnet` と `config/app.yaml` の `exchange.environment` は整合させる  
- **ログの見方**：  
  - テキスト：`Get-Content .\logs\app.log -Wait`  
  - JSON：`Get-Content .\logs\app.jsonl | % { $_ | ConvertFrom-Json } | Select time,level,message`
- **紙上検証**：  
  - バックテストで **取引ログ（opened/closed）とFunding適用**を必ず確認  
  - コマンド例：  
    `poetry run python -m bot.backtest.replay --prices tmp\prices.csv --funding tmp\funding.csv --date 2024-01-01`
- **レポート**：毎日UTC 00:05に `reports/YYYY-MM-DD.md` 生成（`ReportScheduler`）  
- **キルスイッチ**：`RiskManager` が異常時に `flatten_all()` と `disable_new_orders=True`  
- **エラーハンドリング**：  
  - 一過性は `@retryable` が指数バックオフ  
  - 恒常エラーは `ExchangeError / RateLimitError / WsDisconnected / RiskBreach` で把握

---

### 6. 既知のTODO／最終確認が必要な点

- **Bybit v5 の最終フィールド確認（公式ドキュメントで照合）**  
  `orderLinkId`（client id）、`predicted funding rate`／`next funding time` の正確なキー名  
  Public/Private WSのトピック名・認証負荷、URL
- **Funding取得の安定化**：`predicted_rate` が無い場合のフォールバック戦略（APR閾値に届かずHOLDのまま）
- **手数料・スリッページの実値**：MVPの係数を実績で補正（設定化して日次見直し）
- **シンボル正規化**：Bybitのインストルメント情報APIでSpot/Perpの正式表記を取得し内部マップに反映
- **Live 安全稼働テスト**：Testnetで最小ロット／WS断・429の自動復帰の実機確認
- **pre-commit**：`ruff check`／`mypy` をフックに設定（導入済みなら見直し）

置き替えるコードと必要なコード（説明つきで出して）

新規追加：docs/progress.md（上のコードブロックをそのまま保存）。

既存ファイルの変更はありません。
### 7. STEP13–25 実装ログ（Bybit v5 安定化と安全化）

- STEP13: Funding情報の最終確定（Bybit v5）
  - 予測レートと次回時刻を Ticker 優先で取得し、欠落時は history / instruments-info で補完。
  - 変更: `bot/exchanges/bybit.py: get_funding_info`
  - 型: `bot/exchanges/types.py: FundingInfo` に `funding_interval_hours` を保持。

- STEP14: client_order_id（orderLinkId）の往復配線
  - OMS 未指定時に `bot-<uuid32hex>` を自動採番して発注へ通す。
  - 変更: `bot/oms/engine.py: submit()`／`bot/exchanges/bybit.py: place_order()`（orderLinkId を必ず送る）
  - 型: `bot/exchanges/types.py: OrderRequest / Order` に `client_order_id` を追加。

- STEP15: Private WS イベントへ client_order_id を確実に載せる
  - 変更: `bot/app/live_runner.py` の private order/execution コールバックで `client_order_id` を設定。

- STEP16: client_order_id での取消（orderId 不明でも安全）
  - 変更: `bot/exchanges/base.py: cancel_order`、`bot/exchanges/bybit.py: cancel_order`（orderId or orderLinkId）
  - OMS 側: `bot/oms/engine.py` は `client_order_id` を優先で渡す／Paper はシグネチャ合わせ。

- STEP17: 同一 client_order_id の二重発注ブロック
  - 変更: `bot/oms/engine.py` に `_inflight_client_ids` を導入。`submit()` で重複チェック→登録、`on_execution_event()` で終端時に掃除。

- STEP18: 再起動/再接続で open 注文を再構築（reconcile）
  - 変更: `bot/exchanges/bybit.py: get_open_orders`（/v5/order/realtime）
  - OMS: `reconcile_inflight_open_orders(symbols)` を追加し、Private WS 接続直後に一度呼ぶ（`bot/app/live_runner.py`）。

- STEP19: REST に指数バックオフ再試行（@retryable）
  - 変更: `bot/exchanges/bybit.py` の `place_order / cancel_order / get_funding_info / get_open_orders` に `@retryable()` を付与。

- STEP20: シンボル正規化＆カテゴリ自動判定（spot/linear/inverse）
  - 変更: `bot/exchanges/bybit.py: _resolve_category` を実装、instruments-info で正しく判定＆キャッシュ。既存の endswith 判定は置換。

- STEP21: 価格/数量の正規化（tickSize/qtyStep/min-max）
  - 追加: `bybit.py: _get_instrument_info / _quantize_step / _dec_to_str / _normalize_price_qty`
  - `place_order` 送信直前に丸め・範囲保証・指数表記なし化を実施。

- STEP22: WS ステータス正規化＋部分約定の進捗をイベントに載せる
  - 変更: `bot/app/live_runner.py`（order/execution）で標準化ステータスと cumExecQty/avgPrice/execQty/execPrice を搭載。

- STEP23: PostOnly / ReduceOnly / TIF をエンドツーエンド配線
  - 型: `OrderRequest` の `time_in_force/post_only/reduce_only` を活用。
  - 変更: `bybit.py: place_order` で `timeInForce="PostOnly"` 優先、先物系のみ `reduceOnly=True`。戦略のクローズ系注文は `reduce_only=True`。

- STEP24: PostOnly 価格を「非クロス」に自動微調整（BBO 参照）
  - 追加: `bybit.py: _ensure_post_only_price`（BUY→ask-1tick / SELL→bid+1tick、安全側で再丸め）
  - `place_order` で PostOnly の Limit 時のみ適用。

- STEP25: 公開 WS の BBO をゲートウェイでキャッシュ（低遅延化）
  - 追加: `bybit.py: update_bbo / get_bbo` と `_bbo_cache`。`_ensure_post_only_price` は WS キャッシュ優先、無ければ REST 補完。
  - `bot/app/live_runner.py` で orderbook トピックから BBO を `update_bbo()` に反映。

— 総合効果 —

- 安定性: @retryable、カテゴリ/丸めの正規化、WS/REST の両輪で落ちにくく復帰しやすい。
- 安全性: reduce_only、二重発注ブロック、再接続時の open 再構築で事故を未然に防止。
- 充実データ: Funding・BBO・部分約定進捗が正しく取得・伝搬され、戦略/レポートの精度が向上。
- コスト最適化: PostOnly のメイカー維持と非クロス自動補正で手数料と失敗を削減。
