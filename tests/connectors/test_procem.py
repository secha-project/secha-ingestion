"""ProCem file-connector tests: offline, against a real (tiny) 7z built per test."""

from __future__ import annotations

import json
from pathlib import Path

import py7zr
import pytest

from secha_ingestion.connectors.procem import ProcemConnector, _parse_id_ranges
from secha_ingestion.core.runner import run
from secha_ingestion.core.sink import RawSink

DATE = "2026-06-15"
# two EVCharging lines, one out-of-scope id, one malformed line
LINES = [
    b"23501\t49.987724\t1781470800246\n",
    b"23502\t-45.110664\t1781470800246\n",
    b"99999\t1.0\t1781470800246\n",
    b"not a triple\n",
]


def _make_archive(source_dir: Path) -> None:
    csv_path = source_dir / f"{DATE}_procem.csv"
    csv_path.write_bytes(b"".join(LINES))
    with py7zr.SevenZipFile(source_dir / f"{DATE}_procem.7z", "w") as archive:
        archive.write(csv_path, arcname=f"{DATE}_procem.csv")
    csv_path.unlink()  # only the archive remains, as on the group drive


def _land(source_dir: Path, landing: Path, ids: str | None):
    connector = ProcemConnector(source_url=str(source_dir), ids=ids)
    return run(connector, RawSink(str(landing)), run_params={"date": DATE})


def test_filtered_landing_is_verbatim_line_selection(tmp_path: Path) -> None:
    source, landing = tmp_path / "src", tmp_path / "landing"
    source.mkdir()
    _make_archive(source)

    results = _land(source, landing, ids="23501-23949")

    assert len(results) == 1 and results[0].written
    landed = Path(results[0].payload_path).read_bytes()
    assert landed == LINES[0] + LINES[1]  # selected lines byte-identical, others absent
    assert "vendor=procem_kampusareena_pq/source=daily_dump/date=2026-06-15" in results[
        0
    ].payload_path.replace("\\", "/")


def test_envelope_records_filter_and_selection_counts(tmp_path: Path) -> None:
    source, landing = tmp_path / "src", tmp_path / "landing"
    source.mkdir()
    _make_archive(source)

    results = _land(source, landing, ids="23501-23949")

    envelope = json.loads(Path(results[0].envelope_path).read_text(encoding="utf-8"))
    params = envelope["request_params"]
    assert params["id_filter"] == "23501-23949"
    assert params["source_file"] == f"{DATE}_procem.7z"
    assert (params["lines_selected"], params["lines_total"], params["lines_malformed"]) == (
        "2",
        "4",
        "1",
    )
    assert envelope["sensitivity"] == "project-internal"
    assert envelope["fetched_at"]  # transform's snapshot selection depends on this


def test_second_run_is_idempotent_skip(tmp_path: Path) -> None:
    source, landing = tmp_path / "src", tmp_path / "landing"
    source.mkdir()
    _make_archive(source)

    first = _land(source, landing, ids="23501-23949")
    second = _land(source, landing, ids="23501-23949")

    assert first[0].written and not second[0].written
    assert first[0].content_sha256 == second[0].content_sha256


def test_plain_csv_source_is_supported(tmp_path: Path) -> None:
    """A pre-extracted .csv day file (no archive) works identically."""
    source, landing = tmp_path / "src", tmp_path / "landing"
    source.mkdir()
    (source / f"{DATE}_procem.csv").write_bytes(b"".join(LINES))

    results = _land(source, landing, ids="23501-23949")

    assert Path(results[0].payload_path).read_bytes() == LINES[0] + LINES[1]


def test_unfiltered_lands_whole_file_verbatim(tmp_path: Path) -> None:
    source, landing = tmp_path / "src", tmp_path / "landing"
    source.mkdir()
    _make_archive(source)

    results = _land(source, landing, ids=None)

    assert Path(results[0].payload_path).read_bytes() == b"".join(LINES)  # incl. malformed line


def test_missing_day_file_raises_clearly(tmp_path: Path) -> None:
    source = tmp_path / "src"
    source.mkdir()
    connector = ProcemConnector(source_url=str(source), ids="23501-23949")
    with pytest.raises(FileNotFoundError, match="2026-06-15"):
        list(connector.fetch(next(iter(connector.list_partitions(date=DATE)))))


def test_bad_id_filter_syntax_raises() -> None:
    with pytest.raises(ValueError, match="23z"):
        ProcemConnector(source_url="somewhere", ids="23501-23z")


def test_id_range_parsing() -> None:
    assert _parse_id_ranges("23501-23503,24001") == {23501, 23502, 23503, 24001}
