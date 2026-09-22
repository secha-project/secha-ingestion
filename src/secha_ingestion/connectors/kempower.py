"""Kempower connector: lands a Spark Parquet export of the passenger charging dataset.

A FILE source. Kempower shares the dataset over Delta Sharing; TAU fetched it once and wrote it
with Spark as a directory of snappy Parquet part files, each with the Hadoop `.crc` sidecar that
Spark writes next to it. That directory is what we hold, so one part file is one partition and
lands byte for byte. Parquet stays Parquet: it is typed, compressed and self-describing, and
rewriting it as CSV or JSON here would be a transformation.

Every part is verified before it lands: each 512-byte chunk against the CRC32 that Spark
recorded when it wrote the file, then the Parquet magic at both ends. A part that fails never
lands. It is listed in `rejected` so the CLI can fail loudly, while the verified parts still
land. Landing is idempotent, so a re-run after a clean copy arrives adds only what was missing.

Partition identity: `export` is the first 8 hex digits of the Spark write job's UUID, which
every file of one write carries, so two exports never mix (the full UUID is in the envelope;
the short form keeps landing paths inside the Windows 260-character limit). `part` is the task
number plus Spark's file counter, e.g. `00000-c000`.
"""

from __future__ import annotations

import re
import struct
import zlib
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from functools import cached_property

import fsspec
import structlog

from secha_ingestion.core.models import RawPayload, SourcePartition

_log = structlog.get_logger(__name__)

_SOURCE = "public_passenger_dataset"
_PARQUET = "application/vnd.apache.parquet"  # IANA media type
_MAGIC = b"PAR1"
_CRC_HEADER = b"crc\x00"
_EXPORT_ID_LEN = 8
# Spark names every file of one write `part-<task>-<job uuid>-c<counter>[.<codec>].parquet`
_PART_NAME = re.compile(
    r"^part-(?P<task>\d{5})-(?P<job>[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})"
    r"-c(?P<counter>\d{3})(?:\.[a-z0-9]+)?\.parquet$"
)


class DamagedPartError(Exception):
    """A part file whose bytes are not the bytes Spark wrote."""


@dataclass(frozen=True)
class Rejection:
    """A part that failed verification and was not landed."""

    source_file: str
    reason: str


@dataclass(frozen=True)
class _Part:
    path: str
    name: str
    job: str


def verify_part(body: bytes, crc_file: bytes) -> tuple[int, int]:
    """Check a part against its Hadoop `.crc` sidecar; return (chunks, chunk size).

    The sidecar is `crc\\0`, a big-endian chunk size, then one big-endian CRC32 per chunk.
    Raises DamagedPartError, naming what is wrong, if the bytes differ from what was written.
    """
    if len(crc_file) < 8 or crc_file[:4] != _CRC_HEADER or (len(crc_file) - 8) % 4:
        raise DamagedPartError("the .crc sidecar is not a Hadoop checksum file")
    (chunk,) = struct.unpack(">i", crc_file[4:8])
    if chunk <= 0:
        raise DamagedPartError(f"the .crc sidecar declares a chunk size of {chunk}")
    chunks = (len(crc_file) - 8) // 4
    low, high = (chunks - 1) * chunk + 1, chunks * chunk
    if not low <= len(body) <= high:
        state = "truncated" if len(body) < low else "longer than written"
        raise DamagedPartError(
            f"{state}: {len(body):,} bytes, but the checksum covers {low:,} to {high:,}"
        )
    view = memoryview(body)
    for index in range(chunks):
        (stored,) = struct.unpack_from(">I", crc_file, 8 + 4 * index)
        if zlib.crc32(view[index * chunk : (index + 1) * chunk]) != stored:
            raise DamagedPartError(f"chunk {index} (from byte {index * chunk:,}) fails its CRC32")
    if body[:4] != _MAGIC or body[-4:] != _MAGIC:
        raise DamagedPartError("checksums match, but the file is not Parquet (no PAR1 magic)")
    return chunks, chunk


class KempowerConnector:
    """File connector for a Spark Parquet export; verifies each part, never transforms it.

    One instance per run: the directory listing is read once and cached, and `rejected`
    collects the parts that failed during that run.
    """

    name = "kempower"
    version = "0.1.0"

    def __init__(self, source_url: str) -> None:
        if not source_url:
            raise ValueError("Kempower source URL is required")
        self._fs, base = fsspec.core.url_to_fs(source_url)
        self._base: str = str(base).rstrip("/")
        self.rejected: list[Rejection] = []

    def list_partitions(self, **run_params: str) -> Iterable[SourcePartition]:
        if run_params:
            raise ValueError(f"kempower takes no run parameters, got {sorted(run_params)}")
        return [
            SourcePartition(vendor=self.name, source=_SOURCE, identity=identity)
            for identity in (dict(key) for key in sorted(self._parts))
        ]

    def fetch(self, partition: SourcePartition) -> Iterator[tuple[SourcePartition, RawPayload]]:
        part = self._parts[tuple(sorted(partition.identity.items()))]
        with self._fs.open(part.path, "rb") as handle:
            body = handle.read()
        try:
            chunks, chunk = verify_part(body, self._read_crc(part))
        except DamagedPartError as exc:
            self.rejected.append(Rejection(part.name, str(exc)))
            _log.warning("part_rejected", source_file=part.name, reason=str(exc))
            return
        yield (
            partition,
            RawPayload(
                body=body,
                content_type=_PARQUET,
                request_url=part.path,
                request_params={
                    "source_file": part.name,
                    "source_bytes": str(len(body)),
                    "spark_job_id": part.job,
                    "checksum": f"hadoop crc32, {chunks} chunks of {chunk} bytes, all match",
                    "export_parts": str(self._export_sizes[part.job]),
                    "export_success_marker": "present" if self._has_success else "absent",
                },
                # the Delta table version was not recorded when the export was written
                source_version=None,
                # Kempower's data; "public" in the table name is unconfirmed (open question)
                sensitivity="partner-confidential",
            ),
        )

    def _read_crc(self, part: _Part) -> bytes:
        crc_path = f"{self._base}/.{part.name}.crc"
        if crc_path not in self._listing:
            raise DamagedPartError("no .crc sidecar, so the bytes cannot be verified")
        with self._fs.open(crc_path, "rb") as handle:
            data: bytes = handle.read()
        return data

    @cached_property
    def _listing(self) -> set[str]:
        if not self._fs.isdir(self._base):
            raise FileNotFoundError(f"Kempower export directory not found: {self._base}")
        return {str(path).rstrip("/") for path in self._fs.ls(self._base, detail=False)}

    @cached_property
    def _has_success(self) -> bool:
        return f"{self._base}/_SUCCESS" in self._listing

    @cached_property
    def _parts(self) -> dict[tuple[tuple[str, str], ...], _Part]:
        parts: dict[tuple[tuple[str, str], ...], _Part] = {}
        jobs: dict[str, str] = {}
        for path in sorted(self._listing):
            name = path.rsplit("/", 1)[-1]
            if not (name.startswith("part-") and name.endswith(".parquet")):
                continue  # .crc sidecars, _SUCCESS and anything else that is not data
            match = _PART_NAME.match(name)
            if match is None:
                raise ValueError(f"{name!r} does not follow Spark's part file naming")
            job = match["job"]
            export = job[:_EXPORT_ID_LEN]
            if jobs.setdefault(export, job) != job:
                raise ValueError(f"two Spark jobs share the export id {export!r}: {job}")
            identity = {"export": export, "part": f"{match['task']}-c{match['counter']}"}
            parts[tuple(sorted(identity.items()))] = _Part(path=path, name=name, job=job)
        if not parts:
            raise FileNotFoundError(f"no Spark part files (part-*.parquet) under {self._base}")
        return parts

    @cached_property
    def _export_sizes(self) -> dict[str, int]:
        sizes: dict[str, int] = {}
        for part in self._parts.values():
            sizes[part.job] = sizes.get(part.job, 0) + 1
        return sizes
