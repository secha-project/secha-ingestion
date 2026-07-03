from __future__ import annotations

import json

import httpx
import pytest
import respx

from secha_ingestion.connectors.mx_electrix import MxElectrixConnector
from secha_ingestion.core.runner import run
from secha_ingestion.core.sink import RawSink

BASE = "https://electrix.example.test"
DEVICES = [{"id": 21, "location": "ABC 400V", "serialnumber": "0902", "ik": 1.0, "uk": 1.0}]
MEASUREMENTS = [{"id": 1, "meter": 21, "timestamp": "2025-05-21T00:00:00", "fhz": 50.0}]


@respx.mock
def test_lists_and_fetches_raw_json() -> None:
    respx.get(url__startswith=f"{BASE}/api/v2/meters/").mock(
        return_value=httpx.Response(200, json=DEVICES)
    )
    respx.get(url__startswith=f"{BASE}/api/v2/measurements/").mock(
        return_value=httpx.Response(200, json=MEASUREMENTS)
    )
    conn = MxElectrixConnector(BASE, "secret-token")
    try:
        partitions = list(conn.list_partitions(date="2025-05-21"))
        assert sorted(p.source for p in partitions) == ["measurements", "meters"]

        by_source = {}
        for partition in partitions:
            for part, payload in conn.fetch(partition):
                by_source[part.source] = payload

        assert json.loads(by_source["meters"].body) == DEVICES  # bytes preserved
        assert by_source["meters"].source_version == "v2"
        assert by_source["measurements"].content_type.startswith("application/json")
        assert by_source["measurements"].http_status == 200
    finally:
        conn.close()


@respx.mock
def test_end_to_end_lands_files(landing: str) -> None:
    respx.get(url__startswith=f"{BASE}/api/v2/meters/").mock(
        return_value=httpx.Response(200, json=DEVICES)
    )
    respx.get(url__startswith=f"{BASE}/api/v2/measurements/").mock(
        return_value=httpx.Response(200, json=MEASUREMENTS)
    )
    conn = MxElectrixConnector(BASE, "secret-token")
    sink = RawSink(landing)
    try:
        results = run(conn, sink, run_params={"date": "2025-05-21", "meter": "21"})
    finally:
        conn.close()

    assert len(results) == 2  # meters + measurements
    assert all(r.written for r in results)


@respx.mock
def test_requires_credentials() -> None:
    try:
        MxElectrixConnector("", "")
    except ValueError:
        return
    raise AssertionError("expected ValueError for missing credentials")


@respx.mock
def test_4xx_fails_fast_without_retry() -> None:
    """Client errors (e.g. 404 = bad meter id / permission denied) are not transient: no retry."""
    route = respx.get(url__startswith=f"{BASE}/api/v2/measurements/").mock(
        return_value=httpx.Response(404, text="Incorrect meter id or permission denied")
    )
    conn = MxElectrixConnector(BASE, "secret-token")
    try:
        partition = next(
            p
            for p in conn.list_partitions(date="2025-05-21", meter="99")
            if p.source == "measurements"
        )
        with pytest.raises(RuntimeError, match="404"):
            list(conn.fetch(partition))
        assert route.call_count == 1  # failed fast, no retries
    finally:
        conn.close()


@respx.mock
def test_5xx_is_retried_then_succeeds(monkeypatch) -> None:
    """Server errors are transient: retried with backoff until success."""
    monkeypatch.setattr("secha_ingestion.connectors.mx_electrix.time.sleep", lambda _s: None)
    route = respx.get(url__startswith=f"{BASE}/api/v2/measurements/").mock(
        side_effect=[httpx.Response(500), httpx.Response(200, json=MEASUREMENTS)]
    )
    conn = MxElectrixConnector(BASE, "secret-token")
    try:
        partition = next(
            p
            for p in conn.list_partitions(date="2025-05-21", meter="21")
            if p.source == "measurements"
        )
        payloads = list(conn.fetch(partition))
        assert payloads[0][1].http_status == 200
        assert route.call_count == 2  # one failure, one successful retry
    finally:
        conn.close()


@respx.mock
def test_meters_fetched_once_in_discovery_mode(landing: str) -> None:
    """All-meters mode lands the SAME device-list bytes it used for discovery: one API call."""
    meters_route = respx.get(url__startswith=f"{BASE}/api/v2/meters/").mock(
        return_value=httpx.Response(200, json=DEVICES)
    )
    respx.get(url__startswith=f"{BASE}/api/v2/measurements/").mock(
        return_value=httpx.Response(200, json=MEASUREMENTS)
    )
    conn = MxElectrixConnector(BASE, "secret-token")
    sink = RawSink(landing)
    try:
        results = run(conn, sink, run_params={"date": "2025-05-21"})
    finally:
        conn.close()

    assert meters_route.call_count == 1  # landed + discovery share one fetch
    assert len(results) == 2  # meters + measurements for the one discovered meter
