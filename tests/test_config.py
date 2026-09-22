from __future__ import annotations

from pathlib import Path

import pytest

from secha_ingestion.config import Settings


def test_env_file_is_read_as_utf8(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A Finnish path in .env survives: the file is read as UTF-8, never the platform default."""
    value = "C:/Jaetut tiedostot/Sähkötalo/export"
    (tmp_path / ".env").write_bytes(f'SECHA_KEMPOWER_SOURCE_URL="{value}"\n'.encode())
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("SECHA_KEMPOWER_SOURCE_URL", raising=False)

    assert Settings().kempower_source_url == value
