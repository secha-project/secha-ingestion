"""Generic, vendor-blind ingestion loop."""

from __future__ import annotations

import structlog

from secha_ingestion.core.connector import SourceConnector
from secha_ingestion.core.sink import LandedResult, RawSink

_log = structlog.get_logger(__name__)


def run(
    connector: SourceConnector,
    sink: RawSink,
    *,
    run_params: dict[str, str],
) -> list[LandedResult]:
    """Enumerate partitions, fetch raw bytes, and land them. No transformation."""
    results: list[LandedResult] = []
    for partition in connector.list_partitions(**run_params):
        for part, payload in connector.fetch(partition):
            result = sink.land(
                part,
                payload,
                connector_name=connector.name,
                connector_version=connector.version,
            )
            _log.info(
                "landed",
                vendor=part.vendor,
                source=part.source,
                identity=dict(part.identity),
                path=result.payload_path,
                written=result.written,
                bytes=len(payload.body),
            )
            results.append(result)
    return results
