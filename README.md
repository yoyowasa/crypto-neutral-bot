# crypto-neutral-bot

最小骨格（STEP1）。Poetry で依存を入れ、pytest/ruff/mypy が通ることを目標にしています。

## セットアップ（目安）
1) Poetry をインストール
2) `poetry install` で依存導入
3) `poetry run pre-commit install` でコミットフックをセット
4) 動作確認
   - `poetry run pytest`
   - `poetry run ruff .`
   - `poetry run mypy .`

## 構成
- `bot/` … プロダクトのPythonパッケージ（import対象）
- `tests/` … テスト
- `.env.example` … APIキーのテンプレート（秘密は .env にのみ）
- `.pre-commit-config.yaml` … コミット時チェック
