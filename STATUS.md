# 現状サマリ (2025-10-12)

- GitHub Actions は `.github/workflows/ci.yml` に定義されたパイプラインで `ruff`、`black --check`、`mypy bot --ignore-missing-imports`、`pytest -q` を順に実行する。
- `requirements-ci.txt` には CI で必要な開発／設定依存 (PyYAML / types-PyYAML / pydantic / pydantic-settings 等) が含まれている。
- 設定ローダー `bot/config/loader.py` は Pydantic Settings を利用し、`.env`／環境変数／YAML の優先順位でマージした `AppConfig` を返す。
- `tests/test_config.py` では YAML ファイルと環境変数の優先度検証、`tests/test_smoke.py` では `version_info` が文字列を返すことを確認する。

## 手元検証結果

- `ruff check .`
- `black --check .`
- `pytest -q`
- `mypy bot --ignore-missing-imports` (types-PyYAML 未導入環境では `yaml` のスタブ欠如で失敗する可能性あり)
