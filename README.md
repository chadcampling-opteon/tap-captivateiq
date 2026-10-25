# tap-captivateiq

Singer tap for [CaptivateIQ](https://www.captivateiq.com/) commissions data, built with the [Meltano Singer SDK](https://sdk.meltano.com/).

Extracts employees, hierarchy groups and reporting edges, plans, period groups, plan payout dates, employee assumptions (targets), payout snapshot exports, and report model worksheets — including unlocked mid-period data when available (`locked_only=false`).

## Auth

1. In CaptivateIQ: user profile → **API Tokens** → create token.
2. Copy [`.env.example`](.env.example) to `.env` and set:

```bash
TAP_CAPTIVATEIQ_AUTH_TOKEN=...
```

Header sent: `Authorization: Token <value>` against `https://api.captivateiq.com`.

## Streams & sync matrix

`visualization_exports` is the export path that has been run against the live API. `payout_summaries`, `payout_worksheet_exports`, and `time_series_exports` are **beta**: the tap implements them, and they have not been validated with a live export.

| Stream | Mode | Window / state | Notes |
|--------|------|----------------|-------|
| `employees` | Full refresh | — | Small roster; has `updated_at` but no server `updated_after` filter |
| `hierarchy_groups` | Full refresh | — | Groups that contain reporting edges. The `default` field is the API name |
| `hierarchies` | Full refresh | one partition per group | Reporting edges. Join `managing_employee` and `reporting_employee` to `employees.id`. Includes ended edges (`end_date` set). Quote `hierarchy_groups.default` in SQL |
| `plans` | Full refresh | `active_plans_only` (default true) | Config dimension |
| `period_groups` | Full refresh | — | Config dimension |
| `employee_assumptions` | Windowed | `start_date` or lookback/lookahead date filters | Targets/quotas in `data` |
| `payout_dates` | Full refresh | — | Plan payout dates. Join `plans.payout_dates[].id` to `id`. Export selection still calls the snapshot endpoint inside the tap |
| `payout_worksheets_catalog` | Windowed | Recent payout dates (max 5 queried) | Sheet names for calc exports |
| `payout_summaries` | Date-windowed + state | Bookmark synced dates; **always re-export latest** (MTD mutates) | **Beta.** Async CSV; max 5 dates/request |
| `payout_worksheet_exports` | Date-windowed + state | Same as summaries; requires `worksheet_names` | **Beta.** Calc detail / attainment columns |
| `report_models` | Full refresh | — | Metadata + period_dates |
| one stream per `visualization_exports` entry | Full refresh every run | `stream_name`; records export, no period | Visualization report worksheets |
| one stream per `time_series_exports` entry | Full refresh of the configured `period_date` | `stream_name` + `worksheet_uuid` + `period_date` | **Beta.** Period report worksheets |

Rate limits (CaptivateIQ FAQs): Standard 5 rps / 1500 req/hr. Default `request_delay_ms=250`. Prefer async CSV exports over huge paginated dumps.

**Warehouse tip:** Treat unlocked/open period rows as overwrite-by-natural-key (employee + plan + period), not append-only.

## Quick start

```bash
uv sync
cp .env.example .env   # set token
```

See [docs/SYNC_MATRIX.md](docs/SYNC_MATRIX.md).

```bash
uv run tap-captivateiq --config .secrets/config.json --discover
uv run tap-captivateiq --config .secrets/config.json
```

Copy [`.secrets/config.json.template`](.secrets/config.json.template) to `.secrets/config.json` for CLI config (gitignored via `config.json` pattern — prefer `.secrets/` locally).

### Meltano

```bash
meltano install
meltano config tap-captivateiq set auth_token "$TAP_CAPTIVATEIQ_AUTH_TOKEN"
meltano run tap-captivateiq target-jsonl
```

## Config highlights

| Setting | Purpose |
|---------|---------|
| `auth_token` | Required API token |
| `api_url` | Default `https://api.captivateiq.com` |
| `start_date` | Window start for assumptions/payouts/reports |
| `payout_date_lookback_days` | Alternate window if `start_date` unset (default 120) |
| `locked_only` | `false`/omit to include unlocked payout snapshots |
| `worksheet_names` | Required for `payout_worksheet_exports` (beta) |
| `visualization_exports` | Dashboard (`type=visualization`) sheets. Each item: `stream_name`, `report_model_uuid`, `worksheet_uuid` |
| `time_series_exports` | **Beta.** Time-series sheets. Same fields plus required `period_date` |
| `request_delay_ms` | Client pacing (default 250) |

`stream_name` must match `^[a-z][a-z0-9_]*$`. Report exports select the sheet by `worksheet_uuid` only.

```json
{
  "visualization_exports": [
    {
      "stream_name": "fee_summary",
      "report_model_uuid": "00000000-0000-0000-0000-000000000001",
      "worksheet_uuid": "00000000-0000-0000-0000-000000000002"
    }
  ],
  "time_series_exports": [
    {
      "stream_name": "period_commission",
      "report_model_uuid": "00000000-0000-0000-0000-000000000003",
      "worksheet_uuid": "00000000-0000-0000-0000-000000000004",
      "period_date": "2026-08-31"
    }
  ]
}
```

`discover` emits one `tap_stream_id` per configured export, using that entry's `stream_name`. Each report export loads the report model once and stamps `_export_report_model_name` and `_export_worksheet_name` from that response, so a rename shows up on the next sync. Fields that stream does not set are omitted.

Run `uv run tap-captivateiq --about` for the full schema.

## Development

```bash
uv sync
uv run pytest
```
