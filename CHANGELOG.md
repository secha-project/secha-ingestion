# Changelog

All notable changes to `secha-ingestion` are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning: [SemVer](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-06-18
### Added
- Vendor-agnostic ingestion core: `models`, `envelope`, `SourceConnector` protocol, `RawSink`, `runner`.
- Raw, immutable, idempotent landing sink over `fsspec` (local → S3/ADLS/Volumes with no code change).
- `IngestionEnvelope` sidecar carrying provenance + a `sensitivity` tag.
- MX Electrix connector for `/meters/` + `/measurements/` (lands raw JSON verbatim).
- Typer CLI (`secha-ingest mx-electrix`), pydantic-settings config, structlog logging.
- Tests (sink determinism/idempotency + mocked connector), ruff + mypy(strict) + pytest, pre-commit, CI.
- Architecture diagram (`docs/secha-ingestion-data-flow.svg`) and open-questions log.
