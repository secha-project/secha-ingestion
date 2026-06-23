"""The contract every vendor connector implements. The core depends on this, not on vendors."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Protocol, runtime_checkable

from secha_ingestion.core.models import RawPayload, SourcePartition


@runtime_checkable
class SourceConnector(Protocol):
    """A vendor adapter: enumerate partitions, then fetch raw bytes. Performs no transformation.

    NOTE: this interface is deliberately minimal and may be refactored once a second
    connector (Kempower) exists. Do not over-generalise it on a single example.
    """

    name: str
    version: str

    def list_partitions(self, **run_params: str) -> Iterable[SourcePartition]:
        """Enumerate the partitions to ingest for the given run parameters (e.g. date, meter)."""
        ...

    def fetch(self, partition: SourcePartition) -> Iterator[tuple[SourcePartition, RawPayload]]:
        """Fetch raw bytes for a partition. May yield more than one payload (fan-out)."""
        ...
