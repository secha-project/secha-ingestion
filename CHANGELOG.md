# Changelog

All notable changes to `secha-ingestion` are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning: [SemVer](https://semver.org/).

## [Unreleased]
### Added (vendor #3: Kempower)
- **Kempower file connector** (`connectors/kempower.py`), the third vendor. It lands the
  Spark Parquet export of Kempower's passenger charging dataset (`public_passenger_dataset`,
  written by TAU from the Delta Sharing table) one part file per partition, byte for byte, as
  `.parquet`. Identity is `export` (first 8 hex digits of the Spark write job UUID, so two
  exports never mix) and `part` (Spark's task number and file counter).
- **Verification before landing:** every 512-byte chunk is checked against the CRC32 in Spark's
  `.crc` sidecar, then the Parquet magic at both ends. A part that fails is never landed; the
  CLI names it with the reason and exits 1, and the verified parts land regardless. The
  envelope records the check, the full job UUID, the number of parts in the export, and
  whether Spark's `_SUCCESS` marker was present.
- CLI `secha-ingest kempower`; setting `SECHA_KEMPOWER_SOURCE_URL`. No new dependency: the
  check uses `zlib` and `struct` only.
- First real run: 99 of 100 parts landed and verified (606,521,747 bytes, 71,793,566 rows);
  `part-00099` is refused because every copy we hold is truncated at 1,556,480 bytes. See
  `docs/open-questions.md`.
- The `SourceConnector` protocol, runner and landing logic absorbed a third, very different
  source unchanged.
### Changed
- The sink names `application/vnd.apache.parquet` payloads `.parquet` instead of `.bin`, so
  landed Parquet is recognisable by name, to people and to tools that pick files by extension.
  The one `core/` edit Kempower needed: a format, not vendor logic.
- Settings state the `.env` encoding (UTF-8) explicitly instead of relying on the
  pydantic-settings default, and a test pins that a Finnish path survives.
- License: proprietary (SECHA project). The LICENSE file said MIT while `pyproject.toml` and
  the README said TBD; all three now agree.
- Pre-commit hooks pinned to the tool versions CI resolves today (ruff 0.16.8, mypy 2.3.1),
  up from ruff 0.6.9 and mypy 1.11.2, so a local hook cannot format differently from CI. The
  mypy hook installs the typed libraries the code imports (httpx, typer) instead of the unused
  `types-requests`.
### Added (vendor #2: ProCem)
- **ProCem file connector** (`connectors/procem.py`), the second vendor and the first FILE
  source: one 7-Zip archive per LOCAL Helsinki day (Kampusareena platform dumps), extracted and
  landed as tab-separated CSV. Supports an optional declared rtl_id subset (**line selection,
  bytes verbatim**, the file-source analogue of an API field filter); the filter, selection
  counts (incl. malformed lines), and source-file provenance go in the envelope. Plain `.csv`
  day files are also accepted. Filtering streams from disk (never holds 3.8 GB in RAM).
- CLI `secha-ingest procem --date … [--ids …]`; settings `SECHA_PROCEM_SOURCE_URL` /
  `SECHA_PROCEM_IDS`; dependency `py7zr`.
- **Zero changes to `core/`**: the `SourceConnector` protocol, sink, and runner absorbed a
  file-based vendor unchanged (the ingestion-side decoupling claim, now demonstrated).
### Fixed
- Envelope sidecars are now written as explicit UTF-8 bytes. Text-mode writes used the platform
  default encoding (cp1252 on Windows), which would mangle or crash on non-ASCII content such as
  Finnish site names.
- Landing is now atomic (payload to `.tmp` → envelope → rename payload last). Previously a crash
  mid-write could leave a partial payload at the content-hash path, which every later run would
  mistake for an already-landed file and skip forever.
### Changed
- HTTP retry policy: only transport errors and 5xx responses are retried. 4xx client errors
  (e.g. 404 = incorrect meter id / permission denied per the Swagger) now fail immediately with
  a clear message; no pointless backoff sleep after the final attempt.
- `/meters/` is fetched once per run: the landed device list and meter discovery share the same
  payload (consistency + one fewer API call in all-meters mode).
- CLI: `--date` is validated (YYYY-MM-DD) up front; missing credentials produce a clean error
  message instead of a traceback.

## [0.1.0] - 2026-06-18
### Added
- Vendor-agnostic ingestion core: `models`, `envelope`, `SourceConnector` protocol, `RawSink`, `runner`.
- Raw, immutable, idempotent landing sink over `fsspec` (local → S3/ADLS/Volumes with no code change).
- `IngestionEnvelope` sidecar carrying provenance + a `sensitivity` tag.
- MX Electrix connector for `/meters/` + `/measurements/` (lands raw JSON verbatim).
- Typer CLI (`secha-ingest mx-electrix`), pydantic-settings config, structlog logging.
- Tests (sink determinism/idempotency + mocked connector), ruff + mypy(strict) + pytest, pre-commit, CI.
- Architecture diagram (`docs/secha-ingestion-data-flow.svg`) and open-questions log.
