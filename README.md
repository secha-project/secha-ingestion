# secha-ingestion

> Vendor-agnostic **raw ingestion layer** for the SECHA EV-charging data interoperability framework.

`secha-ingestion` fetches partner data and lands it **verbatim** (no parsing, scaling, renaming, or
re-serialisation) into an immutable, replayable landing zone: the medallion **Bronze** layer. All
transformation happens *later*, in the separate `secha-transform` engine, driven by `secha-metadata`
configs. Keeping ingestion "dumb on purpose" guarantees a pristine source of truth you can always
re-process.

## Architecture at a glance

![secha-ingestion data flow](docs/secha-ingestion-data-flow.svg)

Read it top to bottom: a **command** starts the **CLI + config**, the **runner** drives the loop, and a
**vendor connector** fetches the raw bytes. For MX Electrix (shown) that means calling the external
**API** and receiving **raw JSON**; for ProCem it means reading daily 7-Zip archives from a file share.
Either way the connector wraps the exact bytes as a **raw payload**, and the **sink** fingerprints,
labels (envelope), de-duplicates, and writes them **verbatim to the landing zone**. The grey, dashed
boxes (the external API and the transform engine) are outside this repo; processing happens later,
reading from the pristine shelf.

## Where this fits in the SECHA system
```
secha-ingestion  →  raw data (Bronze)         ← this repo
secha-metadata   →  the transformation rulebook (config-as-code)
secha-transform  →  reads raw + rulebook → canonical data (Delta / Unity Catalog)
```

## Principles (what makes this "raw, no processing")
1. **Bytes verbatim.** The fetched body is written exactly as received, never parsed or re-serialised.
2. **Envelope, not edit.** Provenance (vendor, endpoint, params, fetch time, content hash, source
   version, sensitivity) goes in a sidecar `*.meta.json`; the payload stays pristine.
3. **Idempotent + immutable.** The path is keyed on `(vendor, source, partition)`; the content SHA-256
   only *detects change*: identical content is skipped, changed content lands as a new snapshot.
   Nothing is ever overwritten.
4. **Format-agnostic sink.** Stores any bytes (JSON and CSV today; Parquet/XML later) under one envelope.
5. **Resilience ≠ transformation.** Retries, backoff, timeouts, and TLS handling live here; field logic
   does not.

`core/` is **vendor-blind**: it must never import from `connectors/` or contain `if vendor == ...`.
A new vendor is a new file in `connectors/` + a CLI command, with **zero changes to `core/`**.

## Design decisions (the load-bearing ones)
- **Deterministic, no transformation in ingestion.** Scaling, timestamp normalisation, and field
  selection are deliberately the transform engine's job.
- **Raw, immutable landing storing bytes verbatim.** Higher fidelity than the legacy CSV, with full
  auditability and replayability.
- **Vendor-blind core + per-vendor connectors, abstraction deferred.** The `SourceConnector` interface
  is intentionally minimal to avoid a wrong abstraction designed on one example. It has now absorbed
  two very different connectors (an authenticated API and a file-archive share) unchanged.
- **Narrowing at source is declared, never silent.** MX Electrix can narrow via the API `fields`
  parameter; ProCem can narrow to a declared rtl_id subset (line selection, bytes untouched). Both
  filters are recorded in the landing envelope, so provenance always says exactly what was requested.

## Quickstart

Using **uv** (recommended):
```bash
uv sync --all-extras --dev
cp .env.template .env          # fill in SECHA_ELECTRIX_HOST_URL + SECHA_ELECTRIX_ACCESS_TOKEN
uv run secha-ingest --help
uv run secha-ingest mx-electrix --date 2025-08-15 --meter 21
uv run secha-ingest procem --date 2026-06-15        # needs the ProCem archive share (see below)
```

Without uv:
```bash
python -m venv .venv
.venv/Scripts/pip install -e .          # Linux/macOS: .venv/bin/pip
.venv/Scripts/secha-ingest mx-electrix --date 2025-08-15 --meter 21
```

> A real MX Electrix fetch needs the host URL + token and network access to that host (typically the
> TUNI network/VPN). A real ProCem run needs the group-drive archives (`SECHA_PROCEM_SOURCE_URL`).
> Neither at hand? `pytest` exercises both pipelines offline (mocked API, fixture archives).

## Landing-zone layout (Hive-partitioned, WORM)
```
<SECHA_LANDING_ROOT>/vendor=mx_electrix/source=measurements/date=2025-08-15/meter=21/
    <sha16>.json        # raw response body, verbatim
    <sha16>.meta.json   # IngestionEnvelope (provenance only)
```
`source=meters` (the device list) lands with no `meter=` key. ProCem days land as
`vendor=procem_kampusareena_pq/source=daily_dump/date=<local-day>/<sha16>.csv` (+ envelope); the
`date=` is ProCem's **Helsinki-local** day, faithful to the archive boundary, while canonical
`event_date` is derived from timestamps downstream. Hive-style partitioning is read natively
by Spark and Databricks Autoloader. The landing root is configurable via `SECHA_LANDING_ROOT`
(`data/landing` locally; `s3://…`, `abfs://…`, or a Unity Catalog Volume in production) through
`fsspec`, with no code change.

## Adding a new vendor connector
1. Add `src/secha_ingestion/connectors/<vendor>.py` implementing the `SourceConnector` protocol
   (`name`, `version`, `list_partitions`, `fetch`). Return raw bytes; **never** transform.
2. Add a CLI subcommand in `cli.py`.
3. Add a connector test under `tests/connectors/`: mock the API with `respx` (API vendors) or build
   tiny fixture archives in the test (file vendors, see `test_procem.py`).
   No change to `core/`. That property is the ingestion-side proof of the framework's decoupling
   claim, and it has held for both connectors so far.

## Project layout
```
src/secha_ingestion/
  core/        # vendor-blind: models, envelope, connector Protocol, sink, runner
  connectors/  # one module per vendor: mx_electrix (API), procem (file archives)
  config.py    # pydantic-settings (env-prefixed SECHA_)
  cli.py       # typer entrypoint
  logging.py   # structlog setup
tests/         # sink + connector tests (offline: mocked API, fixture archives)
docs/          # architecture diagram, open questions
```

## Configuration
All settings are environment variables prefixed `SECHA_` (read from `.env`; secrets never in code).

| Variable | Default | Purpose |
|---|---|---|
| `SECHA_LANDING_ROOT` | `data/landing` | where raw data lands (local path or `s3://…`/`abfs://…`) |
| `SECHA_REQUEST_TIMEOUT_S` | `20` | per-request timeout |
| `SECHA_MAX_RETRIES` | `3` | retry attempts (resilience, not transformation) |
| `SECHA_ELECTRIX_HOST_URL` | (required) | MX Electrix API base URL |
| `SECHA_ELECTRIX_ACCESS_TOKEN` | (required) | API key (kept in `.env`, gitignored) |
| `SECHA_ELECTRIX_ALLOW_INVALID_CERTS` | `false` | set `true` if the host uses a self-signed cert |
| `SECHA_ELECTRIX_FIELDS` | unset | leave unset to request the server default (all fields) |
| `SECHA_PROCEM_SOURCE_URL` | (required for procem) | directory of `YYYY-MM-DD_procem.7z` archives (path or fsspec URL) |
| `SECHA_PROCEM_IDS` | unset | rtl_id subset to land (e.g. `23501-23949` = EV charging meter); unset lands the whole day file |

## Quality gates
```bash
uv run ruff check . && uv run ruff format --check .
uv run mypy src
uv run pytest
```
All four run in CI on every push (`.github/workflows/ci.yml`).

## Status / open items
- **Scope:** two vendors. (1) MX Electrix `/meters/` + `/measurements/` (API; `/events/`,
  `/events/{id}/`, `/ssstamps/` deliberately out of slice). (2) **ProCem daily dumps**
  (`secha-ingest procem`: 7z file archives, optional rtl_id subset, Kampusareena EV-charging
  meter by default). The second connector required **zero `core/` changes**.
- **Open questions (data platform):** see [docs/open-questions.md](docs/open-questions.md).
  The once-blocking MX Electrix questions (pagination, `fields`, timezone, scaling) are resolved;
  the remaining items are confirmations, not blockers.
- **`SourceConnector` interface:** absorbed the file-based ProCem connector unchanged; revisit
  only if Kempower pinches.
- **License:** TBD.
