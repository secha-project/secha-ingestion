"""ProCem daily-dump file connector (Kampusareena platform archives).

A FILE source, not an API: one 7-Zip archive per LOCAL (Europe/Helsinki) day, each holding a
single whole-platform CSV of tab-separated (rtl_id, value, epoch_ms) triples. The connector
optionally narrows to a declared rtl_id subset: line SELECTION only, selected bytes land
verbatim (the file-source equivalent of requesting a field subset from an API). The filter,
selection counts, and source-file provenance are recorded in the envelope's request params.

7-Zip extraction is envelope unpacking (like HTTP gzip), not transformation: the CSV member's
bytes are what the platform produced.
"""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import IO

import fsspec
import py7zr

from secha_ingestion.core.models import RawPayload, SourcePartition

_SOURCE = "daily_dump"
_DAY_SUFFIXES = (".7z", ".csv")
_COPY_CHUNK = 8 * 1024 * 1024


def _parse_id_ranges(spec: str) -> set[int]:
    """Parse the platform's considered-ids syntax: "23501-23949,24001" -> set of rtl_ids."""
    ids: set[int] = set()
    for part in spec.split(","):
        text = part.strip()
        if not text:
            continue
        try:
            if "-" in text:
                start_text, end_text = text.split("-", 1)
                start, end = int(start_text), int(end_text)
                if end < start:
                    raise ValueError(text)
                ids.update(range(start, end + 1))
            else:
                ids.add(int(text))
        except ValueError as exc:
            raise ValueError(
                f"invalid rtl_id filter segment {text!r} (use e.g. '23501-23949,24001')"
            ) from exc
    if not ids:
        raise ValueError("rtl_id filter is empty")
    return ids


class ProcemConnector:
    """File connector for ProCem daily dumps; selects lines, never transforms them."""

    name = "procem_kampusareena_pq"
    version = "0.1.0"

    def __init__(self, source_url: str, ids: str | None = None) -> None:
        if not source_url:
            raise ValueError("ProCem source URL is required")
        self._fs, base = fsspec.core.url_to_fs(source_url)
        self._base: str = str(base).rstrip("/")
        self._ids: set[int] | None = _parse_id_ranges(ids) if ids else None
        self._ids_label: str = ids if ids else "all"

    def list_partitions(self, **run_params: str) -> Iterable[SourcePartition]:
        # {date} is the archive's LOCAL Helsinki day; canonical event_date is derived from
        # ts_utc downstream and legitimately differs near midnight (see secha-metadata).
        date = run_params["date"]
        return [SourcePartition(vendor=self.name, source=_SOURCE, identity={"date": date})]

    def fetch(self, partition: SourcePartition) -> Iterator[tuple[SourcePartition, RawPayload]]:
        date = partition.identity["date"]
        source_path = self._day_file(date)
        body, selection_stats = self._read_day(source_path)
        params = {
            "id_filter": self._ids_label,
            "source_file": source_path.rsplit("/", 1)[-1],
            "source_bytes": str(self._fs.size(source_path)),
        }
        params.update(selection_stats)
        yield (
            partition,
            RawPayload(
                body=body,
                content_type="text/csv",  # the platform's own naming: tab-separated *.csv
                request_url=source_path,
                request_params=params,
                source_version="Procem_IDs_v1.2",
                sensitivity="project-internal",
            ),
        )

    def _day_file(self, date: str) -> str:
        candidates = sorted(
            str(path)
            for path in self._fs.glob(f"{self._base}/{date}*")
            if str(path).endswith(_DAY_SUFFIXES)
        )
        if not candidates:
            raise FileNotFoundError(f"no {date}*.7z / {date}*.csv day file under {self._base}")
        return candidates[0]

    def _read_day(self, source_path: str) -> tuple[bytes, dict[str, str]]:
        if source_path.endswith(".csv"):
            with self._fs.open(source_path, "rb") as handle:
                return self._select(handle)
        # .7z: copy locally (network reads are sequential-friendly), extract the CSV member,
        # then stream-select from disk; the whole day is never held in memory when filtering
        with tempfile.TemporaryDirectory(prefix="secha-procem-") as tmp:
            tmp_dir = Path(tmp)
            local_archive = tmp_dir / source_path.rsplit("/", 1)[-1]
            with self._fs.open(source_path, "rb") as src, local_archive.open("wb") as dst:
                shutil.copyfileobj(src, dst, length=_COPY_CHUNK)
            with py7zr.SevenZipFile(local_archive) as archive:
                members = [name for name in archive.getnames() if name.endswith(".csv")]
                if not members:
                    raise FileNotFoundError(f"no CSV member inside {source_path}")
                archive.extract(path=tmp_dir, targets=members[:1])
            with (tmp_dir / members[0]).open("rb") as handle:
                return self._select(handle)

    def _select(self, handle: IO[bytes]) -> tuple[bytes, dict[str, str]]:
        """Stream-select lines whose rtl_id is in the filter; line bytes are never modified."""
        if self._ids is None:
            return handle.read(), {}
        selected = bytearray()
        total = kept = malformed = 0
        for line in handle:
            total += 1
            try:
                rtl_id = int(line.split(b"\t", 1)[0])
            except ValueError:
                malformed += 1  # counted in the envelope; excluded lines are never invisible
                continue
            if rtl_id in self._ids:
                selected += line
                kept += 1
        return bytes(selected), {
            "lines_selected": str(kept),
            "lines_total": str(total),
            "lines_malformed": str(malformed),
        }
