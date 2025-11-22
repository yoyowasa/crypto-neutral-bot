# Bitget 移行メモ（Bitget → Bitget）

> 注意: これは開発用メモです。実際の運用・法令・税務は必ずご自身の責任で最新の公式ドキュメント・規約を確認してください。

## 1. コード側の前提

- `ExchangeGateway` 抽象はそのまま維持し、`BitgetGateway` に加えて `BitgetGateway` を追加した。
  - 新規ファイル: `bot/exchanges/bitget.py`
  - REST は ccxt の `bitget` を利用（UTA 前提、`options["uta"]=True`）。
  - 実装済みメソッド:
    - 情報系: `get_balances`, `get_positions`, `get_open_orders`, `get_ticker`, `get_bbo`, `get_funding_info`
    - 注文系: `place_order`, `cancel_order`, `amend_order`
    - 後始末: `close`
  - 未実装（今後対応予定）:
    - `subscribe_public` / `subscribe_private`（WS は Bitget のみ利用する想定）

- 共通設定モデル `ExchangeConfig` はそのまま利用しつつ、ドキュメント上は以下の方針とする:
  - `exchange.name` が `"bitget"` → `BitgetGateway` を使う
  - `exchange.name` が `"bitget"` → `BitgetGateway` を使う想定（runner 側の切替は今後のステップ）
  - `exchange.environment` は Bitget ではメモ用途（文字列は `"mainnet"` 固定でよい）

## 2. BitgetGateway の仕様メモ

### 2.1 シンボル表記

- 内部表記:
  - パーペチュアル: `BTCUSDT`, `ETHUSDT` …（既存 Bitget と同じ）
  - 現物: `BTCUSDT_SPOT`, `ETHUSDT_SPOT` …（末尾 `_SPOT`）
- ccxt へのマッピング（`BitgetGateway._to_ccxt_symbol`）:
  - perp: `"BTCUSDT"` → `"BTC/USDT:USDT"`
  - spot: `"BTCUSDT_SPOT"` → `"BTC/USDT"`

### 2.2 スケール情報（_scale_cache）

- `load_markets()` の結果から以下を抽出し `self._scale_cache[core_symbol]` に保存:
  - `priceScale` / `tickSize`（perp or spot の `precision.price` ベース）
  - perp 用: `qtyStep_perp`, `minQty_perp`, `minNotional_perp`
  - spot 用: `qtyStep_spot`, `minQty_spot`, `minNotional_spot`
- これにより既存の `ops-check` ヘルパ（`_qty_steps_for_symbol`, `_min_limits_for_symbol` など）が Bitget でも利用可能になる。

### 2.3 BBO / Funding

- `get_bbo(symbol)`:
  - `fetch_order_book(ccxt_symbol, limit=1)` から bid/ask を取得。
  - 結果を `self._bbo_cache` にキャッシュし、`_price_state[symbol]` を `"READY"` に更新。
  - 失敗時は `(None, None)` を返す。

- `get_ticker(symbol)`:
  - まず `get_bbo` の mid を利用。
  - BBO が取れない場合は `fetch_ticker` の `last` をフォールバックとして使用。

- `get_funding_info(symbol)`:
  - `fetch_funding_rate("BTC/USDT:USDT")` 等を呼び出し、
    - `fundingRate` → `current_rate` および `predicted_rate`（Bitget は next レートを個別には返さないため current を流用）
    - `fundingTimestamp` / `nextFundingTimestamp` → `next_funding_time`
    - `interval`（例: `"8h"`）→ `funding_interval_hours`

## 3. ランナー側との統合方針（このコミット時点のステータス）

- 現状の `live_runner.py` / `paper_runner.py` は引き続き `BitgetGateway` 前提で動作している。
  - Private/Public WS は Bitget v5 専用のため、Bitget 用にはまだ差し替えていない。
  - `BitgetGateway.subscribe_private` / `subscribe_public` は `WsDisconnected` を投げるスタブ実装。

- 今後の統合ステップ案:
  1. `live_runner` / `paper_runner` に「取引所ファクトリ」を追加し、
     - `exchange.name` に応じて `BitgetGateway` / `BitgetGateway` を選択する。
  2. Bitget を選んだ場合の挙動:
     - 短期的: REST ベースで `get_bbo` / `get_funding_info` を呼び出し、WS のないモードで戦略を動かす（約定反映は遅延前提）。
     - 中期的: Bitget WebSocket API から
       - Public: order book / trades
       - Private: orders / positions / account
       を購読し、既存の OMS イベント (`on_execution_event`) に流す。
  3. `ops-check` と `metrics` に Bitget 特有のレート制限・エラーコードを反映。

## 4. 設定ファイル側の変更ポイント

> このコミットでは **設定ファイルそのものは書き換えていません**。移行時に手動で編集する前提です。

- `.env` / `.env.example`
  - これまでと同じキー名で Bitget の API キーを設定する（ラベルだけ Bitget から Bitget に読み替え）。
    - `KEYS__API_KEY=your_bitget_key`
    - `KEYS__API_SECRET=your_bitget_secret`

- `config/app.yaml` / `config/app.mainnet.yaml`
  - `exchange.name` を `"bitget"` → `"bitget"` に変更。
  - `exchange.environment` は `"mainnet"` のままでよい（Bitget では記録用の文字列）。

## 5. 今後の TODO（コード側）

- `BitgetGateway`:
  - Private WS（orders/positions/account）の実装と、`_handle_private_order` 相当のマッピング。
  - Public WS（orderbook/trades）の実装と、`PaperExchange.handle_public_msg` 互換のメッセージ整形。
  - レートリミット・429 時の指数バックオフ（Bitget の `_RestWrapper` 相当）。

- ランナー:
  - `bot/app/live_runner.py` / `bot/app/paper_runner.py` に Bitget 対応の分岐を追加。
  - Bitget 使用時に WS タスクを起動しない / REST ポーリングで代替するかどうかの設計。

---

このファイルは「Bitget 対応の進捗」と「設計上の前提」をまとめるメモとして、実装を進めながら随時更新していく想定です。

