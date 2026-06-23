"""Runtime configuration via environment / .env (12-factor; secrets never in code)."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Environment-driven settings. All keys are prefixed `SECHA_` (e.g. SECHA_LANDING_ROOT)."""

    model_config = SettingsConfigDict(env_prefix="SECHA_", env_file=".env", extra="ignore")

    landing_root: str = "data/landing"
    request_timeout_s: int = 20
    max_retries: int = 3

    # --- MX Electrix connector ---
    electrix_host_url: str = ""
    electrix_access_token: str = ""
    electrix_allow_invalid_certs: bool = False
    electrix_fields: str | None = None  # None => request server default (all fields)
