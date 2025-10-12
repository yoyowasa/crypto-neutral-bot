from __future__ import annotations

import argparse
import json

from bot.config.loader import load_config, redact_secrets

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config", type=str, default=None, help="YAML設定ファイルのパス(省略可)"

    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    safe = redact_secrets(cfg)
    print(json.dumps(safe, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
