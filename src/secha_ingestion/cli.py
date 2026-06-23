"""CLI entrypoint: `secha-ingest <connector> ...`."""

from __future__ import annotations

from typing import Annotated

import typer

from secha_ingestion import logging as ingest_logging
from secha_ingestion.config import Settings
from secha_ingestion.connectors.mx_electrix import MxElectrixConnector
from secha_ingestion.core.runner import run
from secha_ingestion.core.sink import RawSink

app = typer.Typer(help="SECHA vendor-agnostic raw ingestion layer.", no_args_is_help=True)


@app.callback()
def _main() -> None:
    """SECHA vendor-agnostic raw ingestion layer (one subcommand per vendor connector)."""


@app.command("mx-electrix")
def mx_electrix(
    date: Annotated[str, typer.Option(help="Data date to fetch, YYYY-MM-DD.")],
    meter: Annotated[
        str | None, typer.Option(help="Single meter id; omit to fetch all meters.")
    ] = None,
) -> None:
    """Ingest raw MX Electrix /meters/ and /measurements/ for a date into the landing zone."""
    ingest_logging.configure()
    settings = Settings()
    connector = MxElectrixConnector(
        host_url=settings.electrix_host_url,
        access_token=settings.electrix_access_token,
        allow_invalid_certs=settings.electrix_allow_invalid_certs,
        timeout_s=settings.request_timeout_s,
        max_retries=settings.max_retries,
        fields=settings.electrix_fields,
    )
    try:
        sink = RawSink(settings.landing_root)
        run_params = {"date": date} if meter is None else {"date": date, "meter": meter}
        results = run(connector, sink, run_params=run_params)
    finally:
        connector.close()

    written = sum(1 for result in results if result.written)
    typer.echo(
        f"Landed {len(results)} payload(s) to {settings.landing_root}: "
        f"{written} new, {len(results) - written} skipped."
    )


if __name__ == "__main__":
    app()
