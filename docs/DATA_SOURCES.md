# Data Sources

Status: **Phase 0 draft.** Each row marked _verify in Phase 2_ will be checked against the provider's own page (license, endpoint, rate limit) before any ingestion code is written, and this file will be updated with the date checked.

| Source | Provides | License / terms | Update frequency | Rate limits | Used for | Status |
|---|---|---|---|---|---|---|
| MBTA V3 API (`api-v3.mbta.com`) | Routes, stops, schedules, predictions, vehicles, alerts, facilities; SSE streaming | MBTA developer license; free API key from the MBTA developer portal | Real time | Higher with a key than without; exact numbers _verify in Phase 2_ | Live app views, alerts, facility status | _verify in Phase 2_ |
| MBTA GTFS static | Routes, trips, stop_times, calendars, shapes | MBTA developer license | When the schedule changes | n/a (file download) | Schedule baseline, dimension tables | _verify in Phase 2_ |
| MBTA GTFS-Realtime | Vehicle positions, trip updates, alerts (protobuf) | MBTA developer license | About every few seconds | _verify in Phase 2_ | Live map, ghost trips, bunching | _verify in Phase 2_ |
| MBTA historical performance data (Open Data Portal, LAMP) | Historical travel times, headways, dwell times, ridership | MBTA open data terms | Daily or monthly, depending on dataset | n/a (file download) | Historical backbone for reliability and ML | _verify availability in Phase 2_ |
| Open-Meteo | Historical and forecast weather for Boston | Free for non-commercial use, attribution required (CC BY 4.0 data) | Hourly | Daily request cap on the free API; _verify in Phase 2_ | Weather features | _verify in Phase 2_ |
| Bluebikes GBFS | Station locations and live availability | Bluebikes data license | About every minute | _verify in Phase 2_ | Backup options in alerts | _verify in Phase 2_ |
| US federal holidays + MBTA service calendar | Holiday and service-change dates | Public domain (federal holidays); MBTA license (calendar) | Yearly / per schedule | n/a | Prediction features | _verify in Phase 2_ |

## Attribution

The app footer will credit: "Data provided by the Massachusetts Bay Transportation Authority (MBTA). Weather data by Open-Meteo.com. Bike data by Bluebikes." Exact wording follows each provider's terms once verified.
