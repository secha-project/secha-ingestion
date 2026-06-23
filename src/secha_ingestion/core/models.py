"""Vendor-agnostic value objects. No vendor logic lives here."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True)
class SourcePartition:
    """The smallest independently-fetchable unit of a source.

    Example: one MX Electrix meter on one day -> identity={"meter": "21", "date": "2025-05-21"}.
    The identity uniquely defines the logical partition; it is what the landing path is keyed on.
    """

    vendor: str
    source: str
    identity: Mapping[str, str]


@dataclass(frozen=True)
class RawPayload:
    """Bytes exactly as received from a source; never parsed or re-serialised here."""

    body: bytes
    content_type: str
    http_status: int | None = None
    request_url: str | None = None
    request_params: Mapping[str, str] = field(default_factory=dict)
    source_version: str | None = None
    sensitivity: str = "partner-confidential"
