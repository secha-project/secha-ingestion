from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def landing(tmp_path: Path) -> str:
    """A fresh local landing root per test (does not need to pre-exist)."""
    return str(tmp_path / "landing")
