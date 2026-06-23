"""Raw, immutable, format-agnostic landing sink.

Path scheme (Hive-partitioned, WORM):
    <root>/vendor=<v>/source=<s>/<k=v sorted>/<sha16>.<ext>   # payload, verbatim
    <root>/vendor=<v>/source=<s>/<k=v sorted>/<sha16>.meta.json  # IngestionEnvelope

Identity = (vendor, source, partition.identity). The content SHA-256 only *detects change*:
identical content is an idempotent skip; changed content lands as a new snapshot. Nothing is
ever overwritten.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

import fsspec

from secha_ingestion.core.envelope import IngestionEnvelope
from secha_ingestion.core.models import RawPayload, SourcePartition

_UNSAFE = re.compile(r"[^A-Za-z0-9._=-]+")
_EXT_BY_CONTENT_TYPE = {
    "application/json": "json",
    "text/csv": "csv",
    "application/xml": "xml",
    "text/xml": "xml",
}


def _slug(value: str) -> str:
    return _UNSAFE.sub("-", value)


@dataclass(frozen=True)
class LandedResult:
    """Outcome of landing one payload."""

    payload_path: str
    envelope_path: str
    content_sha256: str
    written: bool  # False => identical content already present (idempotent skip)


class RawSink:
    """Writes payload bytes verbatim plus a sidecar envelope, idempotently, via fsspec."""

    def __init__(self, landing_root: str, storage_options: dict[str, object] | None = None) -> None:
        self._fs, base = fsspec.core.url_to_fs(landing_root, **(storage_options or {}))
        self._base: str = str(base).rstrip("/")

    def _partition_dir(self, partition: SourcePartition) -> str:
        parts = [
            self._base,
            f"vendor={_slug(partition.vendor)}",
            f"source={_slug(partition.source)}",
        ]
        for key in sorted(partition.identity):
            parts.append(f"{_slug(key)}={_slug(partition.identity[key])}")
        return "/".join(parts)

    @staticmethod
    def _extension(content_type: str) -> str:
        base = content_type.split(";", 1)[0].strip().lower()
        return _EXT_BY_CONTENT_TYPE.get(base, "bin")

    def land(
        self,
        partition: SourcePartition,
        payload: RawPayload,
        *,
        connector_name: str,
        connector_version: str,
    ) -> LandedResult:
        sha = hashlib.sha256(payload.body).hexdigest()
        directory = self._partition_dir(partition)
        stem = f"{directory}/{sha[:16]}"
        payload_path = f"{stem}.{self._extension(payload.content_type)}"
        envelope_path = f"{stem}.meta.json"

        if self._fs.exists(payload_path):
            return LandedResult(payload_path, envelope_path, sha, written=False)

        envelope = IngestionEnvelope(
            vendor=partition.vendor,
            source=partition.source,
            partition=dict(partition.identity),
            content_type=payload.content_type,
            content_sha256=sha,
            byte_size=len(payload.body),
            connector_name=connector_name,
            connector_version=connector_version,
            http_status=payload.http_status,
            request_url=payload.request_url,
            request_params=dict(payload.request_params),
            source_version=payload.source_version,
            sensitivity=payload.sensitivity,
        )

        self._fs.makedirs(directory, exist_ok=True)
        with self._fs.open(payload_path, "wb") as handle:
            handle.write(payload.body)
        with self._fs.open(envelope_path, "w") as handle:
            handle.write(envelope.model_dump_json(indent=2))

        return LandedResult(payload_path, envelope_path, sha, written=True)
