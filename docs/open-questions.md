# Open questions — for (SECHA data platform)

Ingestion-scoped clarifications. The first three are **blocking** for trusting the raw we land; the rest
are operational. (A broader list spanning transform/next-vendors is tracked at the project level.)

## Blocking — affect whether the landed raw is complete and correctly interpretable
1. **Pagination.** Does `/api/v2/measurements/` (or any endpoint) ever paginate or cap rows for a full
   day? Our fetcher assumes one request returns the whole day — if it pages, we silently lose data.
2. **`fields` parameter.** If we omit `fields`, does the API return *all* available measurements? Is
   there a definitive full field list? We want a faithful raw payload (the engine selects later), not the
   legacy curated whitelist.
3. **Timestamps & timezone.** Are measurement timestamps UTC (the raw has no `Z`), and are the
   `start`/`end` query params interpreted as UTC or Helsinki local? A local/DST interpretation would fetch
   the wrong day and mislabel every reading.

## Vendor facts to confirm (interpretation, used by the transform engine)
4. **Scaling.** We read in the electrix-api code that raw JSON values are *unscaled* and the CSV
   multiplies voltages by `uk`, currents by `ik`, powers by `uk·ik`. Confirm raw is unscaled, multiply is
   correct, and what `ik`/`uk` physically are (CT/VT ratios?) and that they are static per device.
5. **Energy units.** Counter columns end in `wh`/`varh` but the spec lists kWh/kvarh — which is correct?

## Operational
6. **Revisions/late data.** Does the API ever revise past values (re-fetching an old day returns
   different numbers)? Determines whether to expect multiple landed snapshots per day.
7. **Access.** Is the host URL/port stable, and reachable only from the TUNI network/VPN?
8. **Token & TLS.** Token lifetime/rotation and renewal; rate limits; is the endpoint self-signed
   (needs `allow-invalid-certs`) or is there a proper certificate to verify?
9. **Meter inventory.** Are meters 21/22 the ABC-station and Plugit-charger meters? Are IDs stable, and
   will planned relocation/new meters change or reuse IDs?
