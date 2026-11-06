# Data Sources

Endpoints, licenses and limits below were checked on the providers' own pages and by calling the endpoints on **2026-11-06**. Re-check before relying on them; providers change terms without notice.

## Summary

| Source | Endpoint | License / terms | Updates | Limits | Used for | Ingested by |
|---|---|---|---|---|---|---|
| LAMP subway on-time performance | `https://performancedata.mbta.com/lamp/subway-on-time-performance-v1/index.csv` → one Parquet per service date | MassDOT Developers License Agreement | Daily (files for 2019-09-15 onward; old dates are sometimes republished) | None published; we use 4 parallel downloads | **Historical backbone:** travel times, headways, dwell times, scheduled vs actual | `transitiq-ingest lamp-subway` |
| LAMP alerts history | `https://performancedata.mbta.com/lamp/tableau/alerts/LAMP_RT_ALERTS.parquet` (~134 MB) | MassDOT Developers License Agreement | Replaced in place (daily at time of checking) | None published; we fetch at most weekly | Alert history, elevator/escalator outage history (Accessibility Watch) | `transitiq-ingest lamp-alerts` |
| MBTA GTFS static | `https://cdn.mbta.com/MBTA_GTFS.zip` (~23 MB) | MassDOT Developers License Agreement | When the schedule changes | n/a (file); we skip unchanged ETags | Schedule baseline, routes/stops/trips dimensions | `transitiq-ingest gtfs-static` |
| MBTA V3 API | `https://api-v3.mbta.com` (`/alerts`, `/facilities`, later `/predictions`, `/vehicles`) | MassDOT Developers License Agreement | Real time | **1,000 req/min with a free key**; responses without a key reported `x-ratelimit-limit: 20` (per minute) | Live alert and facility snapshots; live app views (Phase 8) | `transitiq-ingest v3 …` and the ingest Lambda |
| MBTA GTFS-Realtime | `https://cdn.mbta.com/realtime/…` (protobuf) | MassDOT Developers License Agreement | Every few seconds | Not yet checked | Ghost trips, bunching | Phase 6 |
| Open-Meteo Historical Weather API | `https://archive-api.open-meteo.com/v1/archive` | Free for **non-commercial** use only; data under **CC BY 4.0** (attribution required) | Hourly; the archive lags real time by several days | < 10,000 calls/day, 5,000/hour, 600/minute | Weather features and weather-impact analysis | `transitiq-ingest weather` (one call per month) |
| Bluebikes GBFS | `https://gbfs.bluebikes.com/gbfs/gbfs.json` | Bluebikes Data License Agreement (free; no standalone redistribution; no Bluebikes name/logo without permission) | ~1 minute | Not published | Backup options in Commute Guard alerts | Phase 10 |
| US federal holidays + MBTA service calendar | GTFS `calendar.txt` / `calendar_dates.txt`; holiday list in code | Public domain (federal holidays); MassDOT license (GTFS) | Yearly / per schedule | n/a | Prediction features | Phase 3–4 |

## License notes

**MassDOT Developers License Agreement** (covers MBTA V3, GTFS, GTFS-Realtime and the LAMP data published for developers): <https://cdn.mbta.com/sites/default/files/2023-08/mbta-massdot-develop-license-agreement.pdf>
- Grants non-exclusive, limited, revocable rights to **use, reproduce and redistribute** the data.
- We must **clearly acknowledge MassDOT as the provider of the data**.
- We must not use MassDOT/MBTA logos or trademarks, imply we are MassDOT or its partner, misrepresent the data, or claim ownership of it.
- Data is "as is"; MassDOT may change or withdraw it at any time.
- The LAMP site (performancedata.mbta.com) does not state its own license; we treat LAMP data as covered by the agreement above, since it is MBTA data published for developers. **This is our interpretation, not a statement from the MBTA.**

**Open-Meteo:** free API for non-commercial use only. TransitIQ is a non-commercial portfolio project. Weather data is CC BY 4.0, so we credit Open-Meteo.

**Bluebikes:** free license; no publishing the data as a standalone dataset and no Bluebikes name/logo use without written permission. We will only show live nearby availability inside alerts.

## Bronze layout

Raw files are stored unchanged under `data/bronze/` (durable, on the laptop) or the LocalStack bronze bucket (Lambda path, ephemeral). Each file has a `<file>.meta.json` sidecar with source URL, fetch time, sha256, bytes and row count.

```
source=lamp_subway_otp/service_date=YYYY-MM-DD/<file>_v<upstream last_modified>.parquet
source=lamp_alerts/snapshot_date=YYYY-MM-DD/LAMP_RT_ALERTS_v<last_modified>.parquet
source=gtfs_static/fetched_date=YYYY-MM-DD/MBTA_GTFS_<etag>.zip
source=open_meteo_hourly/month=YYYY-MM/open_meteo_boston_YYYY-MM[_partial_YYYYMMDD].json
source=mbta_v3_alerts/snapshot_date=YYYY-MM-DD/alerts_<timestamp>.json
source=mbta_v3_facilities/snapshot_date=YYYY-MM-DD/facilities_<timestamp>.json
```

## Not yet available / known gaps

- **Bus history:** LAMP publishes subway on-time performance per day, but we found no equivalent public per-day bus file (`lamp/bus_vehicle_events/index.csv` returned 403). Bus reliability will come from our own GTFS-Realtime sampling (Phase 6), so bus history will be shorter than subway history.
- **Commuter rail** history is not in the LAMP subway files.

## Attribution (app footer, Phase 9)

> Transit data provided by the Massachusetts Department of Transportation (MassDOT) and the MBTA. TransitIQ is not affiliated with or endorsed by MassDOT or the MBTA. Weather data by [Open-Meteo.com](https://open-meteo.com/) (CC BY 4.0).
