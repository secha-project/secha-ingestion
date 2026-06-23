"""Sidecar metadata describing a landed raw payload. Provenance, never transformation."""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(UTC)


class IngestionEnvelope(BaseModel):
    """Ingestion metadata written beside each raw payload (the payload is never mutated)."""

    vendor: str
    source: str
    partition: dict[str, str]
    content_type: str
    content_sha256: str
    byte_size: int
    connector_name: str
    connector_version: str
    http_status: int | None = None
    request_url: str | None = None
    request_params: dict[str, str] = Field(default_factory=dict)
    source_version: str | None = None
    sensitivity: str = "partner-confidential"
    fetched_at: datetime = Field(default_factory=_utcnow)
    envelope_schema_version: str = "1.0.0"
