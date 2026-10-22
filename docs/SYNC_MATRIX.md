# Stream sync matrix

`visualization_exports` is the export path that has been run against the live API. These exports are **beta** until a live run confirms them: `payout_summaries`, `payout_worksheet_exports`, and `time_series_exports`.

| Stream | Sync mode | State / window | Expected requests / typical run |
|--------|-----------|----------------|---------------------------------|
| `employees` | Full refresh | none | `ceil(n/100)` list pages |
| `plans` | Full refresh | `is_active=true` by default | 1–few pages |
| `period_groups` | Full refresh | none | 1–few pages |
| `employee_assumptions` | Windowed | `end_date_after` / `start_date_before` from `start_date` or lookback | pages for current window only |
| `payout_dates` | Full refresh | none | 1 page (catalog) |
| `payout_worksheets_catalog` | Windowed | up to 5 recent payout dates | 1 list call per date |
| `payout_summaries` (**beta**) | Date bookmark + always latest | `synced_payout_summary_dates`; re-pull the latest open snapshot | 1 export job per ≤5 dates + poll GETs |
| `payout_worksheet_exports` (**beta**) | Same as summaries | requires `worksheet_names` | 1 job × worksheet × date batch |
| `report_models` | Full refresh | none | 1–few pages |
| `<stream_name>` from `visualization_exports` | Full refresh every run | records export by `worksheet_uuid`; no period. One model GET stamps the current model and worksheet names | 1 GET + 1 job per configured sheet |
| `<stream_name>` from `time_series_exports` (**beta**) | Full refresh of configured `period_date` | export-worksheet by `worksheet_uuid` + `period_date`. One model GET stamps the current model and worksheet names | 1 GET + 1 job per configured sheet |

## Rate budget

- Default `request_delay_ms=250` (~4 rps max)
- Honour HTTP 429 with `Retry-After`
- Prefer async CSV over paginating large fact tables
- Do not re-export closed locked payout dates every run

## Open periods

- Path A (**beta**): latest `payout_date` + `locked_only=false` → `payout_summaries` / worksheet exports
- Path B: `visualization_exports` (current visualization worksheets) or `time_series_exports` (**beta**, a chosen period)
- Path C: `employee_assumptions.data` for targets only

Warehouse: overwrite open-period rows by natural key; append closed locked history once.
