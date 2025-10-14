from __future__ import annotations

from bot import __doc__ as bot_doc


def test_bot_package_has_docstring() -> None:
    assert bot_doc is not None
    assert "bot" in bot_doc
