# Open questions: SECHA data platform (MX Electrix slice)

Updated after the meeting. Sources: the eQL DBAPI **v2 Swagger** (`MX Electrix API endpoints`) and `data-platform-documentation`.

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
