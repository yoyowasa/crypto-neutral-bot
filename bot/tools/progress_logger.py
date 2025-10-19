# これは「直近のコミット(HEAD)の変更概要を docs/progress.md に追記する」スクリプトです。
from __future__ import annotations

import subprocess
from pathlib import Path
from datetime import datetime, timezone


def _git(args: list[str]) -> str:
    """これは何をする関数？
    → git コマンドを実行し、標準出力を文字列で返します（失敗時は空文字）。
    """
    try:
        return subprocess.check_output(["git", *args], text=True, encoding="utf-8").strip()
    except Exception:
        return ""


def _parse_numstat(s: str) -> tuple[int, int, int]:
    """これは何をする関数？
    → `git show --numstat` の出力から、追加/削除行数と対象ファイル数を集計します。
    """
    adds = dels = files = 0
    for line in s.splitlines():
        parts = line.split("\t")
        if len(parts) == 3:
            a, d, _ = parts
            try:
                adds += int(a) if a.isdigit() else 0
                dels += int(d) if d.isdigit() else 0
                files += 1
            except Exception:
                pass
    return adds, dels, files


def main() -> None:
    """これは何をする関数？
    → HEAD コミットの概要（ハッシュ・件名・著者・変更ファイル・行数集計）を Markdown で追記します。
    """
    short_hash = _git(["rev-parse", "--short", "HEAD"])
    subject = _git(["log", "-1", "--pretty=%s"])
    author = _git(["log", "-1", "--pretty=%an <%ae>"])
    date_iso = _git(["log", "-1", "--date=iso-strict", "--pretty=%ad"]) or datetime.now(timezone.utc).isoformat()

    # 変更ファイル（ステータス）と行数集計
    status = _git(["diff-tree", "--no-commit-id", "--name-status", "-r", "HEAD"])
    numstat = _git(["show", "--numstat", "--format=", "HEAD"])
    adds, dels, files = _parse_numstat(numstat)

    out_path = Path("docs") / "progress.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not out_path.exists():
        out_path.write_text("# Progress Log\n\n", encoding="utf-8")

    lines: list[str] = []
    lines.append(f"## {date_iso} — {short_hash} — {subject}")
    lines.append("")
    lines.append(f"Author: {author}")
    lines.append(f"Summary: +{adds} / -{dels} across {files} files")
    lines.append("")
    if status:
        lines.append("Changed files:")
        for line in status.splitlines():
            lines.append(f"- {line}")
        lines.append("")

    # 追記
    prev = out_path.read_text(encoding="utf-8")
    out_path.write_text(prev + "\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
