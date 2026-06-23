"""MX Electrix connector: lands raw JSON from /meters/ and /measurements/.

No transformation: the API already returns JSON; we persist the response bytes verbatim. Coefficient
scaling, timestamp normalisation and field selection are the transform engine's job, not ingestion's
. Only /meters/ + /measurements/ are implemented for the first vertical slice; /events/,
/events/{id}/ and /ssstamps/ are deliberately out of scope here.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterable, Iterator

import httpx

from secha_ingestion.core.models import RawPayload, SourcePartition

VENDOR = "mx_electrix"
SOURCE_API_VERSION = "v2"
_METERS_PATH = "/api/v2/meters/"
_MEASUREMENTS_PATH = "/api/v2/measurements/"


class MxElectrixConnector:
    """Implements `core.connector.SourceConnector` for the MX Electrix API."""

    name: str = VENDOR
    version: str = "0.1.0"

    def __init__(
        self,
        host_url: str,
        access_token: str,
        *,
        allow_invalid_certs: bool = False,
        timeout_s: int = 20,
        max_retries: int = 3,
        fields: str | None = None,
    ) -> None:
        if not host_url or not access_token:
            raise ValueError("MxElectrixConnector requires host_url and access_token")
        self._fields = fields
        self._max_retries = max(1, max_retries)
        self._client = httpx.Client(
            base_url=host_url.rstrip("/"),
            headers={
                "Authorization": f"Api-Key {access_token}",
                "Content-Type": "application/json",
            },
            verify=not allow_invalid_certs,
            timeout=timeout_s,
        )

    # -- SourceConnector protocol ---------------------------------------------------------------
    def list_partitions(self, **run_params: str) -> Iterable[SourcePartition]:
        date = run_params["date"]
        meter = run_params.get("meter")
        # the device list is itself a source, landed once per run
        yield SourcePartition(VENDOR, "meters", {"date": date})
        if meter:
            yield SourcePartition(VENDOR, "measurements", {"meter": meter, "date": date})
            return
        # discovery only: enumerate device ids (landed bytes still come verbatim from fetch())
        for device_id in self._discover_meter_ids():
            yield SourcePartition(VENDOR, "measurements", {"meter": device_id, "date": date})

    def fetch(self, partition: SourcePartition) -> Iterator[tuple[SourcePartition, RawPayload]]:
        if partition.source == "meters":
            yield partition, self._get(_METERS_PATH, {})
        elif partition.source == "measurements":
            date = partition.identity["date"]
            params: dict[str, str] = {
                "meter": partition.identity["meter"],
                "start": f"{date}T00:00:00",
                "end": f"{date}T23:59:59",
            }
            if self._fields:
                params["fields"] = self._fields
            yield partition, self._get(_MEASUREMENTS_PATH, params)
        else:  # pragma: no cover - guard against an unknown partition source
            raise ValueError(f"Unknown source for {VENDOR}: {partition.source}")

    def close(self) -> None:
        self._client.close()

    # -- internals ------------------------------------------------------------------------------
    def _discover_meter_ids(self) -> list[str]:
        payload = self._get(_METERS_PATH, {})
        devices = json.loads(payload.body)
        return [str(device["id"]) for device in devices]

    def _get(self, path: str, params: dict[str, str]) -> RawPayload:
        last_exc: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                response = self._client.get(path, params=params)
                response.raise_for_status()
            except httpx.HTTPError as exc:
                last_exc = exc
                time.sleep(min(2**attempt, 8))
                continue
            return RawPayload(
                body=response.content,  # verbatim bytes; never parsed/re-serialised
                content_type=response.headers.get("content-type", "application/json"),
                http_status=response.status_code,
                request_url=str(response.request.url),
                request_params=dict(params),
                source_version=SOURCE_API_VERSION,
                sensitivity="partner-confidential",
            )
        raise RuntimeError(
            f"MX Electrix GET {path} failed after {self._max_retries} attempts: {last_exc}"
        )
