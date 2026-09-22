"""Kempower connector tests: offline, against a tiny Spark-style export built per test."""

from __future__ import annotations

import json
import struct
import zlib
from pathlib import Path

import pytest
from typer.testing import CliRunner

from secha_ingestion.cli import app
from secha_ingestion.connectors.kempower import DamagedPartError, KempowerConnector, verify_part
from secha_ingestion.core.runner import run
from secha_ingestion.core.sink import RawSink

JOB = "95d29330-4a32-4f58-93a9-ed0eaa1fe611"
OTHER_JOB = "0a1b2c3d-1111-2222-3333-444455556666"


def _crc(body: bytes, chunk: int = 512) -> bytes:
    """A Hadoop .crc sidecar as Spark writes it: header, chunk size, one CRC32 per chunk."""
    sums = b"".join(
        struct.pack(">I", zlib.crc32(body[i : i + chunk])) for i in range(0, len(body), chunk)
    )
    return b"crc\x00" + struct.pack(">i", chunk) + sums


def _body(task: int) -> bytes:
    # 1,208 bytes, so three checksum chunks; only the magic at both ends makes it Parquet
    return b"PAR1" + bytes([task]) * 1200 + b"PAR1"


def _name(task: int, job: str = JOB) -> str:
    return f"part-{task:05d}-{job}-c000.snappy.parquet"


def _write_part(export_dir: Path, task: int, *, job: str = JOB, crc: bool = True) -> None:
    (export_dir / _name(task, job)).write_bytes(_body(task))
    if crc:
        (export_dir / f".{_name(task, job)}.crc").write_bytes(_crc(_body(task)))


def _export(tmp_path: Path, tasks: int = 3) -> Path:
    export_dir = tmp_path / "public_passenger_dataset.parquet"
    export_dir.mkdir()
    for task in range(tasks):
        _write_part(export_dir, task)
    (export_dir / "_SUCCESS").write_bytes(b"")
    return export_dir


def _land(export_dir: Path, landing: str):
    connector = KempowerConnector(source_url=str(export_dir))
    return connector, run(connector, RawSink(landing), run_params={})


def test_every_verified_part_lands_verbatim_as_parquet(tmp_path: Path, landing: str) -> None:
    connector, results = _land(_export(tmp_path), landing)

    assert len(results) == 3 and all(result.written for result in results)
    assert connector.rejected == []
    for task, result in enumerate(results):
        assert Path(result.payload_path).read_bytes() == _body(task)  # byte for byte
        path = result.payload_path.replace("\\", "/")
        assert path.endswith(".parquet")
        assert (
            f"vendor=kempower/source=public_passenger_dataset/export=95d29330/part={task:05d}-c000/"
            in path
        )


def test_envelope_records_verification_and_export_provenance(tmp_path: Path, landing: str) -> None:
    _, results = _land(_export(tmp_path), landing)

    envelope = json.loads(Path(results[0].envelope_path).read_text(encoding="utf-8"))
    assert envelope["content_type"] == "application/vnd.apache.parquet"
    assert envelope["partition"] == {"export": "95d29330", "part": "00000-c000"}
    assert envelope["request_params"] == {
        "source_file": _name(0),
        "source_bytes": "1208",
        "spark_job_id": JOB,
        "checksum": "hadoop crc32, 3 chunks of 512 bytes, all match",
        "export_parts": "3",
        "export_success_marker": "present",
    }
    assert envelope["sensitivity"] == "partner-confidential"
    assert envelope["fetched_at"]  # transform's snapshot selection depends on this


def test_truncated_part_is_not_landed_and_the_rest_are(tmp_path: Path, landing: str) -> None:
    export_dir = _export(tmp_path)
    damaged = export_dir / _name(1)
    damaged.write_bytes(damaged.read_bytes()[:512])  # cut at a chunk boundary, like the real copy

    connector, results = _land(export_dir, landing)

    assert [Path(result.payload_path).read_bytes() for result in results] == [_body(0), _body(2)]
    assert [rejection.source_file for rejection in connector.rejected] == [_name(1)]
    assert connector.rejected[0].reason == (
        "truncated: 512 bytes, but the checksum covers 1,025 to 1,536"
    )
    assert not list(Path(landing).rglob("part=00001-c000"))


def test_flipped_byte_fails_the_checksum_of_its_chunk() -> None:
    body = bytearray(_body(0))
    body[700] ^= 0xFF

    with pytest.raises(DamagedPartError, match="chunk 1 "):
        verify_part(bytes(body), _crc(_body(0)))


def test_checksummed_file_that_is_not_parquet_is_refused() -> None:
    with pytest.raises(DamagedPartError, match="not Parquet"):
        verify_part(b"hello world", _crc(b"hello world"))


@pytest.mark.parametrize(
    ("crc_file", "reason"),
    [
        (b"xyz", "not a Hadoop checksum file"),
        (b"crc\x00" + struct.pack(">i", 0), "chunk size of 0"),
    ],
)
def test_malformed_sidecar_is_refused(crc_file: bytes, reason: str) -> None:
    with pytest.raises(DamagedPartError, match=reason):
        verify_part(_body(0), crc_file)


def test_part_without_crc_sidecar_is_not_landed(tmp_path: Path, landing: str) -> None:
    export_dir = tmp_path / "export"
    export_dir.mkdir()
    _write_part(export_dir, 0, crc=False)

    connector, results = _land(export_dir, landing)

    assert results == []
    assert connector.rejected[0].reason == "no .crc sidecar, so the bytes cannot be verified"


def test_second_run_is_idempotent_skip(tmp_path: Path, landing: str) -> None:
    export_dir = _export(tmp_path)

    _, first = _land(export_dir, landing)
    _, second = _land(export_dir, landing)

    assert all(result.written for result in first)
    assert not any(result.written for result in second)


def test_only_part_files_are_data(tmp_path: Path, landing: str) -> None:
    export_dir = _export(tmp_path, tasks=1)
    (export_dir / "_SUCCESS_Error.txt").write_text("OneDrive could not download _SUCCESS")

    _, results = _land(export_dir, landing)

    assert len(results) == 1


def test_two_spark_writes_land_as_two_exports(tmp_path: Path, landing: str) -> None:
    export_dir = _export(tmp_path, tasks=2)
    _write_part(export_dir, 0, job=OTHER_JOB)

    _, results = _land(export_dir, landing)

    envelopes = [json.loads(Path(r.envelope_path).read_text(encoding="utf-8")) for r in results]
    assert [(e["partition"]["export"], e["request_params"]["export_parts"]) for e in envelopes] == [
        ("0a1b2c3d", "1"),
        ("95d29330", "2"),
        ("95d29330", "2"),
    ]


def test_missing_or_empty_directory_raises_clearly(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="not found"):
        KempowerConnector(source_url=str(tmp_path / "absent")).list_partitions()
    with pytest.raises(FileNotFoundError, match="no Spark part files"):
        KempowerConnector(source_url=str(tmp_path)).list_partitions()


def test_a_part_file_spark_did_not_name_is_refused(tmp_path: Path) -> None:
    (tmp_path / "part-00000.parquet").write_bytes(_body(0))  # pandas-style name

    with pytest.raises(ValueError, match="does not follow Spark's part file naming"):
        KempowerConnector(source_url=str(tmp_path)).list_partitions()


def test_two_jobs_sharing_a_short_export_id_are_refused(tmp_path: Path) -> None:
    _write_part(tmp_path, 0)
    _write_part(tmp_path, 1, job="95d29330-0000-0000-0000-000000000000")

    with pytest.raises(ValueError, match="share the export id '95d29330'"):
        KempowerConnector(source_url=str(tmp_path)).list_partitions()


def test_run_parameters_are_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="no run parameters"):
        KempowerConnector(source_url=str(tmp_path)).list_partitions(date="2026-06-15")


def test_cli_fails_loudly_but_lands_the_verified_parts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    export_dir = _export(tmp_path)
    damaged = export_dir / _name(2)
    damaged.write_bytes(damaged.read_bytes()[:600])
    monkeypatch.chdir(tmp_path)  # no .env here, so only the variables below apply
    monkeypatch.setenv("SECHA_KEMPOWER_SOURCE_URL", str(export_dir))
    monkeypatch.setenv("SECHA_LANDING_ROOT", str(tmp_path / "landing"))

    result = CliRunner().invoke(app, ["kempower"])

    assert result.exit_code == 1
    assert "Landed 2 payload(s)" in result.output
    assert f"Not landed: {_name(2)}: truncated" in result.output
    assert len(list((tmp_path / "landing").rglob("*.parquet"))) == 2


@pytest.mark.parametrize(
    ("source_url", "message"),
    [("", "Set SECHA_KEMPOWER_SOURCE_URL"), ("absent", "export directory not found")],
)
def test_cli_explains_a_missing_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source_url: str, message: str
) -> None:
    monkeypatch.chdir(tmp_path)  # no .env here, so only the variables below apply
    monkeypatch.setenv("SECHA_KEMPOWER_SOURCE_URL", source_url)
    monkeypatch.setenv("SECHA_LANDING_ROOT", str(tmp_path / "landing"))

    result = CliRunner().invoke(app, ["kempower"])

    assert result.exit_code == 1
    assert message in result.output
    assert result.exception is None or isinstance(result.exception, SystemExit)  # no traceback
