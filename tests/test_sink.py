from __future__ import annotations

import json
from pathlib import Path

from secha_ingestion.core.models import RawPayload, SourcePartition
from secha_ingestion.core.sink import RawSink


def test_lands_payload_verbatim_and_envelope(landing: str) -> None:
    sink = RawSink(landing)
    part = SourcePartition("mx_electrix", "measurements", {"meter": "21", "date": "2025-05-21"})
    payload = RawPayload(
        body=b'{"a": 1}',
        content_type="application/json",
        http_status=200,
        request_url="https://host/api/v2/measurements/",
        source_version="v2",
    )

    result = sink.land(part, payload, connector_name="mx_electrix", connector_version="0.1.0")

    assert result.written is True
    assert Path(result.payload_path).read_bytes() == b'{"a": 1}'  # verbatim
    assert result.payload_path.endswith(".json")
    assert "vendor=mx_electrix" in result.payload_path
    assert "source=measurements" in result.payload_path
    assert "meter=21" in result.payload_path
    assert "date=2025-05-21" in result.payload_path

    meta = json.loads(Path(result.envelope_path).read_text())
    assert meta["content_sha256"] == result.content_sha256
    assert meta["vendor"] == "mx_electrix"
    assert meta["partition"] == {"meter": "21", "date": "2025-05-21"}
    assert meta["byte_size"] == len(b'{"a": 1}')
    assert meta["sensitivity"] == "partner-confidential"


def test_idempotent_skip_on_identical_content(landing: str) -> None:
    sink = RawSink(landing)
    part = SourcePartition("mx_electrix", "meters", {"date": "2025-05-21"})
    payload = RawPayload(body=b"[]", content_type="application/json")

    first = sink.land(part, payload, connector_name="mx_electrix", connector_version="0.1.0")
    second = sink.land(part, payload, connector_name="mx_electrix", connector_version="0.1.0")

    assert first.written is True
    assert second.written is False  # idempotent: identical content already present
    assert first.payload_path == second.payload_path


def test_changed_content_lands_new_snapshot(landing: str) -> None:
    sink = RawSink(landing)
    part = SourcePartition("mx_electrix", "meters", {"date": "2025-05-21"})

    first = sink.land(
        part,
        RawPayload(b"[1]", "application/json"),
        connector_name="mx_electrix",
        connector_version="0.1.0",
    )
    second = sink.land(
        part,
        RawPayload(b"[1, 2]", "application/json"),
        connector_name="mx_electrix",
        connector_version="0.1.0",
    )

    assert first.written is True
    assert second.written is True
    assert first.payload_path != second.payload_path  # change detected -> new snapshot


def test_run_count_invariant_via_sink(landing: str) -> None:
    """Determinism: re-landing the same payloads is fully idempotent."""
    sink = RawSink(landing)
    part = SourcePartition("mx_electrix", "meters", {"date": "2025-05-21"})
    payload = RawPayload(body=b'{"x": 1}', content_type="application/json")
    sink.land(part, payload, connector_name="mx_electrix", connector_version="0.1.0")
    again = sink.land(part, payload, connector_name="mx_electrix", connector_version="0.1.0")
    assert again.written is False
