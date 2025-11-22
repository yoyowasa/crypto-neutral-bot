# Bitget デルタ・ニュートラルBOT — MVP仕様（STEP0）
（この文書は開発の「よりどころ」です。あとで迷わないように、MVPの範囲・安全装置・KPI・停止/撤退条件をここで固定します）

## 前提
- 開発環境：Python 3.11 / VSCode / Poetry（Docker 不使用）
- 取引所：Bitget  
  - 口座：UTA（Unified Trading Account）を使う想定。**必須かどうかは口座設定に依存（不明）**
  - APIの細かいフィールド名・エンドポイント等は **正式ドキュメントで最終確認** し、実装ではラッパー層で吸収  
    （例：`TODO: 要API確認` とコメントを付ける）

---

## 戦略MVP（やること）
- **スポット買い＋USDT建てパーペ売り**（または逆）で **デルタ≒0** に保ち、
  - **資金調達（Funding）** と **先物-現物ベーシス** を収益源にする
- **予想Funding** と **取引コスト** から名目最適化  
  まずは **閾値超えで建てる簡易版** から開始
- 取扱銘柄：**BTCUSDT, ETHUSDT** から開始
- 注：**ショート現物は不可**。負のFunding想定時は先物ロング＋現物売却に限定

---

## 運用KPI（毎日見る数字）
- **日次PnL**
- **累積PnL**
- **最大ドローダウン（最大DD）**
- **β（BTC/ETHとの相関）**
- **実現Funding寄与**（Fundingで実際にどれだけ稼げたか）

---

## 停止条件（止めるスイッチ）
次のどれかが起きたら **自動または手動で停止** します。
- **WS断 > 30s**
- **ヘッジ遅延 p95 > 2s**
- **日次損失 > 資本 × X%**（X は設定値）
- **ネットデルタ > 上限**（設定値）
- **API異常の繰り返し**
- **資金調達（Funding）の符号反転**

> 停止時は **キルスイッチ `flatten_all()`** を呼んでポジションをゼロに戻す（市場成行で全クローズ）。  
> 新規発注も **disable_new_orders** で止める。

---

## 上限（リスクのふた）
- **総名目上限**
- **銘柄別名目上限**
- **取引所ハードリミット**
- **手数料テーブル**（VIPティアで変動。MVPでは設定ファイルで手数料率を入力し、日次で実績補正）

---

## Funding / ベーシス — 評価と実行の考え方（MVP）
- **データ取得**
  - FundingInfo（`predicted_rate`, `next_funding_time`）  
    `TODO: 要API確認（正式フィールド名/単位/期間）`
  - 価格：`Px_spot`, `Px_perp`  
    ベーシス = `Px_perp / Px_spot - 1`（**内部で正規化**：現物/パーペ記号はAPIから取得しマッピング）
- **期間長**：銘柄/取引所依存（多くは 8h。**断言しない**）
- **期待収益**
  - `expected_pnl = (predicted_rate_per_period) * notional`
  - 年率換算APRから **手数料往復・金利・推定スリッページ** を引いて **閾値比較**
- **建玉決定とヘッジ**
  - `predicted_rate > 0` → **パーペをショート**、**スポットをロング**
  - `< 0` → 逆（ただし **ショート現物は不可**。**先物ロング＋現物売却**）
  - **ネットデルタ=0** を目指してサイズ計算（**ベース数量基準**）
- **実行ガード**
  - **流動性の厚い側**を先に発注し、もう一方でヘッジ
  - **全体スリッページ bps** で保護（超えたら両方キャンセル→やり直し）
- **保有・解消**
  - `pre_event_open_minutes` 以内に建てる
  - イベント跨ぎは `hold_across_events` で制御
  - `predicted_rate` が閾値を下回る／**符号反転**／**リスク超過** で解消
- **再ヘッジ**
  - 在庫帯 `rebalance_band_bps` を超えたら `submit_hedge()` 実行

---

## 名目とコスト（簡易式 / MVP）
- `notional_candidate = min(max_symbol_notional, max_total_notional - used_notional)`
- `expected_gain = predicted_rate_per_period * notional_candidate`
- `expected_cost ≈ (taker_fee_roundtrip + estimated_slippage_bps) * notional_candidate`
- **建てる条件**：`expected_gain - expected_cost > 0` かつ `predicted_rate_apr >= min_expected_apr`
- **ヘッジ再入**：`|net_delta_bps| > rebalance_band_bps`

> 注意：**Fundingレートの単位（期間/年率）や次回時刻** は APIで要確認。  
> 変換は `utils.funding.normalize_rate()` に寄せる（`TODO: 要API確認` をコメントで残す）。

---

## Bitget 特有の注意（MVP時点の扱い）
- **API種別**：Bitget v5 REST/WS を使う想定  
  - **pybit の v5対応が不十分な場合**  
    - REST：`ccxt.bitget` を利用  
    - WS：`websockets` で自前実装（チャネル名・署名方式は **必ず公式ドキュメント確認**）
- **クライアント注文ID**：Bitget の `orderLinkId` を利用予定（**正式名/制約は要確認**）
- **Funding情報**：`predicted funding rate` と `next funding time` を提供するAPIがある想定（**要確認**）
- **記号正規化**：v5の正式シンボルは **インストルメント情報APIから取得して保存**  
  例：`SymbolMap{ "BTC": {"spot":"BTCUSDT", "perp":"BTCUSDT"} }`（**実際の表記はAPIで要確認**）
- **口座モード**：UTAにより証拠金が統合される場合あり。詳細不明のため、MVPでは**十分なUSDT現金残高**を置いて回避
- **手数料**：MVPでは**設定ファイルへ入力**し、**日次で実績補正**

---

## 停止後の動作（安全装置）
- **`flatten_all()`**：市場成行で全クローズ（ポジション=0）
- **`disable_new_orders`**：新規発注を止める
- **監視**：WS遅延/切断、APIエラー連発、日次損失、連続損失、Funding符号反転・急変

---

## 撤退基準（プロダクト継続の線引き）
> この基準は「続ける／やめる」をはっきり決めるためのものです。初期は保守的に設定し、後で見直せます。
- **3週間連続**で **週次ネット損失**（手数料込み）
- **最大DD** が **資本の Y% を超過**（Y は設定値）
- **Funding機会の枯渇**（対象銘柄の平均予想APRが **min_expected_apr をN日連続で下回る**）
- **技術的安定性に問題**（WS断>30sやヘッジ遅延p95>2sが **営業日あたりM回以上**）

*上記の X, Y, N, M は設定ファイルで管理（初期値は運用開始前に決定）。*

---

## 受入条件（STEP0 完了の判定）
- `docs/spec_mvp.md` が **作成済み** で、以下が **記述されている**：
  - 取引所（Bitget/UTA前提の注意）
  - 戦略MVPの方針（Funding/ベーシス、デルタ中立、建て/解消/再ヘッジ）
  - 運用KPI（PnL、DD、β、Funding寄与）
  - 停止条件（WS断、遅延、損失、デルタ上限、API異常、Funding符号反転）
  - 取扱銘柄（BTCUSDT, ETHUSDT）
  - 上限（総名目/銘柄別/ハードリミット/手数料）
  - Bitget特有の注意・`TODO: 要API確認` の明示
  - 撤退基準（継続/中止の線引き）


以上で STEP0 は終わりです。ここまでで「作るのは docs/spec_mvp.md だけ」です。
準備ができたら、次に進みますので 「次」 とだけ送ってください。
