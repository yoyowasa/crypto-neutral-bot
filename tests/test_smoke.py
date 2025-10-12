from __future__ import annotations

from bot import version_info


def test_smoke_version() -> None:
    assert isinstance(version_info(), str)
