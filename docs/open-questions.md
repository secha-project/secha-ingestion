# Open questions: SECHA data sources

Updated after the meeting. Sources: the eQL DBAPI **v2 Swagger** (`MX Electrix API endpoints`) and
`data-platform-documentation`; for Kempower, the scripts in `kempower-dataset` and our own checks of
the export (2026-09-22).

## Resolved
1. **Pagination.** ✅ No pagination. The Swagger defines no `page`/`limit`/`offset`/cursor on any
   endpoint, so one request returns the whole day. Our single-request-per-day assumption is correct.
2. **`fields` parameter.** ✅ Optional. Omitting it returns the full `Measurement` object (all fields);
   `fields` only *narrows* the selection. So leaving `SECHA_ELECTRIX_FIELDS` unset lands everything, which is correct.
3. **Timestamps & timezone.** ✅ Timestamps are **UTC**; the engine attaching UTC is correct.
4. **Scaling (uk/ik).** ✅ Scale per device: multiply voltages by `uk`, currents by `ik`, powers by
   `uk·ik`. Raw JSON is unscaled; scaling is the transform engine's job (config rule). Direction confirmed.
5. **Energy units.** ✅ Values are **kWh / kvarh** (cumulative) even though attribute names end in
   `wh`/`varh`; confirmed by comparing `pw` vs `ep10wh`. Canonical units updated accordingly.
6. **Auth.** ✅ `Api-Key <key>` in the `Authorization` header (Swagger `securityDefinitions`). Matches our connector.
7. **ProCem source (vendor #2, now onboarded).** ✅ Verified first-hand on the group drive
   (`S:\81404_ProCem\kampusareena\data`, also reachable via SSH `rd-file-transfer.tuni.fi`,
   `procem_81404_server`): one 7-Zip archive per **Helsinki-local** day, each holding a single
   whole-day tab-separated CSV of `(rtl_id, value, epoch_ms)` triples (the `_part_*.csv` split seen
   earlier is post-processing, not the raw form). The `procem` connector lands a declared rtl_id
   subset verbatim; the official reading guide (`Kampusareenan datan lukeminen.pdf`) confirms the
   local-midnight file boundary and that rows are in collection order, not time order.
8. **Events form.** ✅ `/events/{id}/` `map_variable_names` defaults to **false**. Store the default
   (false) form as the faithful raw; the name mapping depends on per-meter settings and is a transform concern.
9. **Kempower source (vendor #3, now onboarded).** ✅ Both folders (`kempower-dataset` and the
   shared OneDrive folder) hold the same export, byte for byte. Kempower shares the dataset over
   Delta Sharing. TAU fetched the table with `delta_sharing.load_as_pandas`, saved it as CSV, and
   rewrote it with Spark 4.0.1 (`to_parquet.py`: schema inferred from the CSV, the pandas `index`
   column dropped, rows sorted by country, year, month, weekday, transactionId and
   sampleTime10sIncrement) into 100 snappy Parquet parts from one write job
   (`95d29330-4a32-4f58-93a9-ed0eaa1fe611`). The `kempower` connector lands those parts verbatim
   after checking Spark's own checksums.
   **What the table holds:** one row per 10-second step of a charging session, 13 columns:
   `transactionId` (a 64-hex pseudonym), `country`, `EVModel` (free text, sometimes two
   candidate models), `year`, `month`, `quarter`, `weekday`, `sampleTime10sIncrement` (seconds
   since the session started), `soc`, `tempC`, `avgPowerW`, `avgCurrentA`, `avgVoltageV`. Nine
   countries, January 2024 to June 2025, 72,546,284 rows and 400,926 sessions by the count in `test_results.txt`.
   There is **no absolute timestamp**: only month, weekday and the offset into the session.
   `test_results.txt` compares months as text, so two of its first months are off: Belgium
   starts in 2024-06 and Denmark in 2024-04, not 2024-10.

## Still open / confirm later
- **Data revisions / late arrivals.** ProCem measurements near midnight can land in the next day's
  file (confirmed by the official guide); whether the MX Electrix API revises past values is
  unconfirmed. (Our immutable-snapshot design handles it either way.)
- **Token lifetime / rotation & rate limits:** not explicitly specified for the MX Electrix API.
- **TLS:** the host is `https://213.186.239.132:25847` (an IP address), likely with a self-signed
  cert, so `SECHA_ELECTRIX_ALLOW_INVALID_CERTS=true` is probably required. Confirm / obtain a proper cert.
- **Meter inventory:** confirm which meter id is the ABC station vs the Plugit charger, and ID
  stability across the planned relocation.
- **ProCem 1 Hz semantics:** are the per-second values instantaneous samples or 1-second
  aggregates? Are values final engineering units (no scaling factors)? Also flag: the EVCharging
  feed stopped logging on 2026-06-28.

## Kempower: still open
- **A clean `part-00099`.** Every copy we hold (the shared OneDrive folder, the `kempower-dataset`
  folder and `P:`) comes from the same download, cut at 1,556,480 bytes where Spark's checksum
  covers 6,331,905 to 6,332,416. The bytes that are there match their checksums, so the file was
  cut in transfer, not corrupted. It holds 752,718 rows, the end of the sort order: United
  Kingdom, June 2025; 4,078 sessions appear nowhere else. Only the original file completes this
  export. Re-running Spark would write a new export (new job UUID, new part boundaries) that
  lands separately and would have to replace this one whole. We can ask for the original tar or
  the file itself; the next `secha-ingest kempower` run then lands only that part.
- **`_SUCCESS` is missing.** OneDrive could not download Spark's zero-byte commit marker
  (`_SUCCESS_Error.txt`), so completeness rests on other evidence: parts 00000 to 00099 with no
  gap, one write job, and 752,718 missing rows being one part's worth (the other parts hold
  658,681 to 802,836). The envelopes record `export_success_marker: absent`.
- **Direct Delta Sharing access.** The export is a second-hand copy: Kempower's table went
  through pandas, CSV and Spark's schema inference, so the column types are Spark's guesses
  rather than Kempower's schema, and the Delta table version was not recorded
  (`source_version` is null). A Delta Sharing profile would let a connector land Kempower's own
  files at a known table version. Who holds the profile, and may we use it?
- **Sensitivity.** The table is called `public_passenger_dataset`. Is it public, or partner data
  under the SECHA agreement? It lands as `partner-confidential` until someone confirms.
- **Semantics, for the metadata step** (questions for the data provider):
  - 12,784,066 rows (17.8%) share their `(transactionId, sampleTime10sIncrement)` with another
    row that has different readings, e.g. two rows at 140 s with SoC 24 and 25. Two samples
    inside one 10-second step? The canonical row identity has to account for it.
  - 709 rows have a negative `avgVoltageV` (as low as -58 V) and 8 have zero.
  - `tempC`: ambient, battery or charger temperature?
  - `avgPowerW`, `avgCurrentA`, `avgVoltageV`: averaged over which window, and on the DC side?
  - `sampleTime10sIncrement` reaches 69,450 s (19.3 hours): long sessions, or time left plugged in?
