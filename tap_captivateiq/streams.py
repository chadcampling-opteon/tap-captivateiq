"""Stream definitions for tap-captivateiq."""

from __future__ import annotations

import re
import sys
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from singer_sdk import typing as th

from tap_captivateiq.client import API_PREFIX, CaptivateIQStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context


def _parse_start_date(raw: str | None) -> date | None:
    if not raw:
        return None
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        return date.fromisoformat(text[:10])


_STREAM_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


def _chunked(items: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


_EXPORT_FIELD_DESCRIPTIONS = {
    "_export_payout_date": "Payout dates requested for this export batch",
    "_export_worksheet_name": "Worksheet name at extract time",
    "_export_report_model_id": "Report model UUID for this export",
    "_export_report_model_name": "Report model name at extract time",
    "_export_period_date": "Report model period date for this export",
}

# _export_payout_date is a comma-separated batch, so it stays a string.
_EXPORT_FIELD_TYPES: dict[str, th.JSONTypeHelper] = {
    "_export_payout_date": th.StringType,
    "_export_worksheet_name": th.StringType,
    "_export_report_model_id": th.StringType,
    "_export_report_model_name": th.StringType,
    "_export_period_date": th.DateType,
}


def _csv_row_schema(*export_fields: str) -> dict:
    """Schema for a CSV export stream.

    Only fields this stream can populate are included.
    """
    return th.PropertiesList(
        th.Property(
            "_row_id",
            th.StringType,
            description="Synthetic primary key for the CSV row",
        ),
        *[
            th.Property(
                name,
                _EXPORT_FIELD_TYPES[name],
                description=_EXPORT_FIELD_DESCRIPTIONS[name],
            )
            for name in export_fields
        ],
        th.Property(
            "row",
            th.ObjectType(additional_properties=True),
            description="CSV row columns as key/value pairs",
        ),
    ).to_dict()


def _export_metadata(**fields: str | None) -> dict[str, str]:
    """Drop export fields that have no value so they stay off the record."""
    return {key: value for key, value in fields.items() if value}


class EmployeesStream(CaptivateIQStream):
    """Employee (payee) dimension."""

    name = "employees"
    path = f"{API_PREFIX}/employees/"
    primary_keys = ("id",)
    replication_key = None
    schema = th.PropertiesList(
        th.Property("object", th.StringType),
        th.Property("id", th.StringType),
        th.Property("employee_id", th.StringType),
        th.Property("email", th.StringType),
        th.Property("eid", th.StringType),
        th.Property("name", th.StringType),
        th.Property("display_value", th.StringType),
        th.Property("first_name", th.StringType),
        th.Property("last_name", th.StringType),
        th.Property(
            "additional_notification_emails",
            th.ArrayType(th.StringType),
        ),
        th.Property("final_process_date", th.DateType),
        th.Property("is_active", th.BooleanType),
        th.Property("linked_user", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
    ).to_dict()


class PlansStream(CaptivateIQStream):
    """Commission plans."""

    name = "plans"
    path = f"{API_PREFIX}/plans/"
    primary_keys = ("id",)
    replication_key = None
    schema = th.PropertiesList(
        th.Property("object", th.StringType),
        th.Property("id", th.StringType),
        th.Property(
            "period_groups",
            th.ArrayType(th.ObjectType(th.Property("id", th.StringType))),
        ),
        th.Property(
            "payout_dates",
            th.ArrayType(th.ObjectType(th.Property("id", th.StringType))),
        ),
        th.Property("name", th.StringType),
        th.Property("confetti_enabled", th.BooleanType),
        th.Property("is_active", th.BooleanType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("preview_config", th.ObjectType(additional_properties=True)),
        th.Property("attainment_display_setting", th.StringType),
    ).to_dict()

    @override
    def get_http_request(self, *, page):  # noqa: ANN001
        request = super().get_http_request(page=page)
        if page.next_page_token is None and self.config.get("active_plans_only", True):
            params = dict(request.params or {})
            params["is_active"] = "true"
            request.params = params
        return request


class PeriodGroupsStream(CaptivateIQStream):
    """Period groups under commission plans."""

    name = "period_groups"
    path = f"{API_PREFIX}/period-groups/"
    primary_keys = ("id",)
    replication_key = None
    schema = th.PropertiesList(
        th.Property("object", th.StringType),
        th.Property("id", th.StringType),
        th.Property("plan", th.StringType),
        th.Property("plan_name", th.StringType),
        th.Property("workbook", th.StringType),
        th.Property("name", th.StringType),
        th.Property("default_plan_logic_view", th.StringType),
        th.Property("is_active", th.BooleanType),
        th.Property("start_date", th.DateType),
        th.Property("end_date", th.DateType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
    ).to_dict()


class EmployeeAssumptionsStream(CaptivateIQStream):
    """Live employee assumptions (targets/quotas), date-windowed."""

    name = "employee_assumptions"
    path = f"{API_PREFIX}/employee-assumptions/"
    primary_keys = ("id",)
    replication_key = None
    schema = th.PropertiesList(
        th.Property("object", th.StringType),
        th.Property("id", th.StringType),
        th.Property("employee", th.StringType),
        th.Property("eid", th.StringType),
        th.Property("display_value", th.StringType),
        th.Property("period_group", th.StringType),
        th.Property("start_date", th.DateType),
        th.Property("end_date", th.DateType),
        th.Property("payout_currency", th.StringType),
        th.Property("hierarchy_group_override", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("statement_template", th.StringType),
        th.Property("data", th.ObjectType(additional_properties=True)),
    ).to_dict()

    @override
    def get_http_request(self, *, page):  # noqa: ANN001
        request = super().get_http_request(page=page)
        if page.next_page_token is not None:
            return request

        params = dict(request.params or {})
        lookback_days = int(self.config.get("assumption_lookback_days") or 90)
        lookahead_days = int(self.config.get("assumption_lookahead_days") or 31)
        today = datetime.now(timezone.utc).date()
        start_date = _parse_start_date(self.config.get("start_date"))
        window_start = start_date or (today - timedelta(days=lookback_days))
        # Assumptions overlapping the window: end_date_after window_start,
        # start_date_before window_end.
        params["end_date_after"] = window_start.isoformat()
        params["start_date_before"] = (today + timedelta(days=lookahead_days)).isoformat()
        plan_ids = self.config.get("plan_ids") or []
        if plan_ids:
            params["plan_ids[]"] = list(plan_ids)
        request.params = params
        return request


class PayoutDatesStream(CaptivateIQStream):
    """Known payout snapshot dates (catalog for export selection)."""

    name = "payout_dates"
    path = f"{API_PREFIX}/payouts/payout-dates/"
    primary_keys = ("payout_date",)
    replication_key = None
    schema = th.PropertiesList(
        th.Property("payout_date", th.DateType),
    ).to_dict()


class ReportModelsStream(CaptivateIQStream):
    """Report model catalog including worksheets and period dates."""

    name = "report_models"
    path = f"{API_PREFIX}/report-models/"
    primary_keys = ("id",)
    replication_key = None
    # Nested list quirks: some responses nest data; parse_response handles flat $.data[*]
    schema = th.PropertiesList(
        th.Property("object", th.StringType),
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("type", th.StringType),
        th.Property(
            "worksheets",
            th.ArrayType(
                th.ObjectType(
                    th.Property("object", th.StringType),
                    th.Property("id", th.StringType),
                    th.Property("name", th.StringType),
                    th.Property("type", th.StringType),
                    th.Property("is_hidden", th.BooleanType),
                ),
            ),
        ),
        th.Property(
            "period_dates",
            th.ArrayType(
                th.ObjectType(
                    th.Property("object", th.StringType),
                    th.Property("id", th.StringType),
                    th.Property("date", th.DateType),
                ),
            ),
        ),
    ).to_dict()

    @override
    def get_http_request(self, *, page):  # noqa: ANN001
        request = super().get_http_request(page=page)
        if page.next_page_token is None:
            params = dict(request.params or {})
            params.setdefault("type", self.config.get("report_model_type") or "time_series")
            request.params = params
        return request

    @override
    def parse_response(self, response):  # noqa: ANN001
        """Normalize nested report-model list payloads."""
        self._respect_rate_limit()
        payload = response.json()
        data = payload.get("data")
        # Recipe samples sometimes nest { data: { data: [...] } }
        if isinstance(data, dict) and "data" in data:
            records = data["data"]
        elif isinstance(data, list):
            records = data
        else:
            records = []
        yield from records


class PayoutWorksheetsStream(CaptivateIQStream):
    """Names of calculation workbook worksheets available for a payout date."""

    name = "payout_worksheets_catalog"
    path = f"{API_PREFIX}/payouts/worksheets/"
    primary_keys = ("name", "payout_date")
    replication_key = None
    schema = th.PropertiesList(
        th.Property("object", th.StringType),
        th.Property("name", th.StringType),
        th.Property(
            "payout_date",
            th.DateType,
            description="Payout date used when listing worksheets",
        ),
    ).to_dict()

    @override
    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        dates = self._select_payout_dates()
        if not dates:
            self.logger.warning("No payout dates available for worksheet catalog")
            return
        # API accepts payout_dates[] filter; query recent dates only
        for payout_date in dates[:5]:
            path = f"{API_PREFIX}/payouts/worksheets/"
            offset = 0
            while True:
                payload = self.request_json(
                    "GET",
                    path,
                    params={
                        "limit": self.page_size,
                        "offset": offset,
                        "payout_dates[]": payout_date,
                    },
                )
                for row in payload.get("data") or []:
                    yield {**row, "payout_date": payout_date}
                next_link = payload.get("next")
                if not next_link:
                    break
                offset += self.page_size

    def _select_payout_dates(self) -> list[str]:
        lookback = int(self.config.get("payout_date_lookback_days") or 120)
        start_date = _parse_start_date(self.config.get("start_date"))
        cutoff = start_date or (
            datetime.now(timezone.utc).date() - timedelta(days=lookback)
        )
        payload = self.request_json(
            "GET",
            f"{API_PREFIX}/payouts/payout-dates/",
            params={"limit": 1000, "ordering": "-payout_date"},
        )
        dates: list[str] = []
        for row in payload.get("data") or []:
            value = row.get("payout_date")
            if not value:
                continue
            if date.fromisoformat(value) >= cutoff:
                dates.append(value)
        return sorted(dates)


class _PayoutExportStream(CaptivateIQStream):
    """Shared async payout CSV export behaviour."""

    primary_keys = ("_row_id",)
    replication_key = None
    schema = _csv_row_schema("_export_payout_date")
    path = ""  # unused; exports use request_json
    export_path: str = ""
    state_key: str = "synced_payout_dates"

    @override
    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        dates = self._dates_to_export(context)
        if not dates:
            self.logger.info("%s: no payout dates to export this run", self.name)
            return

        locked_only = self.config.get("locked_only")
        plan_ids = self.config.get("plan_ids") or []
        period_group_ids = self.config.get("period_group_ids") or []
        synced: list[str] = []

        for batch in _chunked(dates, 5):
            body: dict[str, Any] = {"payout_dates": batch}
            if plan_ids:
                body["plan_ids"] = plan_ids
            if period_group_ids:
                body["period_group_ids"] = period_group_ids
            if locked_only is not None:
                body["locked_only"] = locked_only
            body.update(self._extra_export_body())

            self.logger.info("Starting export %s for dates %s", self.export_path, batch)
            job = self.request_json("POST", self.export_path, json_body=body)
            finished = self.poll_job(job)
            download_url = (finished.get("details") or {}).get("download_url")
            if not download_url:
                msg = f"Job {finished.get('id')} missing download_url"
                raise RuntimeError(msg)

            for idx, row in enumerate(self.download_csv_rows(download_url)):
                record = self._record_from_csv_row(row, batch)
                record["_row_id"] = (
                    f"{'-'.join(batch)}:{idx}:"
                    f"{row.get('Employee ID') or row.get('employee_id') or idx}"
                )
                yield record
            synced.extend(batch)

        self._bookmark_dates(context, synced)

    def _extra_export_body(self) -> dict[str, Any]:
        return {}

    def _record_from_csv_row(
        self,
        row: dict[str, str],
        batch: list[str],
    ) -> dict[str, Any]:
        return {
            **_export_metadata(_export_payout_date=",".join(batch)),
            "row": row,
        }

    def _dates_to_export(self, context: Context | None) -> list[str]:
        lookback = int(self.config.get("payout_date_lookback_days") or 120)
        start_date = _parse_start_date(self.config.get("start_date"))
        cutoff = start_date or (
            datetime.now(timezone.utc).date() - timedelta(days=lookback)
        )
        payload = self.request_json(
            "GET",
            f"{API_PREFIX}/payouts/payout-dates/",
            params={"limit": 1000, "ordering": "-payout_date"},
        )
        all_dates = [
            row["payout_date"]
            for row in (payload.get("data") or [])
            if row.get("payout_date")
            and date.fromisoformat(row["payout_date"]) >= cutoff
        ]
        all_dates = sorted(set(all_dates))
        if not all_dates:
            return []

        state = self.get_context_state(context)
        already = set(state.get(self.state_key) or [])
        # Always re-export the latest (likely open/unlocked) date for MTD.
        latest = all_dates[-1]
        new_dates = [d for d in all_dates if d not in already or d == latest]
        # Optional: only sync current open + newly seen (skip re-pull of all lookback
        # on first run after empty state — first run intentionally seeds history
        # within lookback window once).
        return new_dates

    def _bookmark_dates(self, context: Context | None, dates: list[str]) -> None:
        state = self.get_context_state(context)
        existing = set(state.get(self.state_key) or [])
        existing.update(dates)
        state[self.state_key] = sorted(existing)


class PayoutSummariesStream(_PayoutExportStream):
    """Payout summary CSV exports (earnings), windowed by payout date."""

    name = "payout_summaries"
    export_path = f"{API_PREFIX}/payouts/payout-summaries/export/"
    state_key = "synced_payout_summary_dates"


class PayoutWorksheetExportsStream(_PayoutExportStream):
    """Named calculation worksheet CSV exports."""

    name = "payout_worksheet_exports"
    export_path = f"{API_PREFIX}/payouts/worksheets/export/"
    state_key = "synced_payout_worksheet_dates"
    schema = _csv_row_schema("_export_payout_date", "_export_worksheet_name")

    def _extra_export_body(self) -> dict[str, Any]:
        names = self.config.get("worksheet_names") or []
        if not names:
            msg = (
                "payout_worksheet_exports requires config worksheet_names "
                "(list of calc workbook sheet names)"
            )
            raise ValueError(msg)
        # API exports one worksheet_name per request; use first configured name
        # per batch. Multiple names are handled in get_records override.
        return {"worksheet_name": names[0]}

    @override
    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        names = self.config.get("worksheet_names") or []
        if not names:
            self.logger.warning(
                "Skipping payout_worksheet_exports: set worksheet_names in config",
            )
            return

        dates = self._dates_to_export(context)
        if not dates:
            return

        locked_only = self.config.get("locked_only")
        plan_ids = self.config.get("plan_ids") or []
        period_group_ids = self.config.get("period_group_ids") or []
        synced: list[str] = []

        for worksheet_name in names:
            for batch in _chunked(dates, 5):
                body: dict[str, Any] = {
                    "payout_dates": batch,
                    "worksheet_name": worksheet_name,
                }
                if plan_ids:
                    body["plan_ids"] = plan_ids
                if period_group_ids:
                    body["period_group_ids"] = period_group_ids
                if locked_only is not None:
                    body["locked_only"] = locked_only

                job = self.request_json("POST", self.export_path, json_body=body)
                finished = self.poll_job(job)
                download_url = (finished.get("details") or {}).get("download_url")
                if not download_url:
                    msg = f"Job {finished.get('id')} missing download_url"
                    raise RuntimeError(msg)
                for idx, row in enumerate(self.download_csv_rows(download_url)):
                    yield {
                        **_export_metadata(
                            _export_payout_date=",".join(batch),
                            _export_worksheet_name=worksheet_name,
                        ),
                        "_row_id": f"{worksheet_name}:{'-'.join(batch)}:{idx}",
                        "row": row,
                    }
                synced.extend(batch)

        self._bookmark_dates(context, synced)


def configured_report_streams(tap: Any) -> list[CaptivateIQStream]:
    """Build one stream per visualization or time-series export entry."""
    streams: list[CaptivateIQStream] = []
    seen: set[str] = set()
    for item in tap.config.get("visualization_exports") or []:
        streams.append(_report_export_stream(tap, item, mode="visualization", seen=seen))
    for item in tap.config.get("time_series_exports") or []:
        streams.append(_report_export_stream(tap, item, mode="time_series", seen=seen))
    return streams


def _report_export_stream(
    tap: Any,
    item: dict[str, Any],
    *,
    mode: str,
    seen: set[str],
) -> CaptivateIQStream:
    stream_name = item.get("stream_name") or ""
    if not _STREAM_NAME.fullmatch(stream_name):
        msg = (
            f"stream_name {stream_name!r} must match {_STREAM_NAME.pattern} "
            "(lowercase, start with a letter)"
        )
        raise ValueError(msg)
    if stream_name in seen:
        msg = f"Duplicate report export stream_name {stream_name!r}"
        raise ValueError(msg)
    seen.add(stream_name)

    model_id = item["report_model_uuid"]
    worksheet_id = item["worksheet_uuid"]
    period_date = item.get("period_date")
    if mode == "time_series" and not period_date:
        msg = f"time_series export {stream_name!r} requires period_date"
        raise ValueError(msg)

    class ConfiguredReportExportStream(CaptivateIQStream):
        """CSV export for one configured report-model worksheet."""

        name = stream_name
        path = ""
        primary_keys = ("_row_id",)
        replication_key = None
        schema = _csv_row_schema(
            "_export_report_model_id",
            "_export_report_model_name",
            "_export_worksheet_name",
            *(("_export_period_date",) if mode == "time_series" else ()),
        )

        def _labels(self) -> tuple[str, str]:
            """Read the current model and worksheet names before export."""
            payload = self.request_json(
                "GET",
                f"{API_PREFIX}/report-models/{model_id}/",
            )
            model = payload.get("data", payload)
            if (
                isinstance(model, dict)
                and "worksheets" not in model
                and isinstance(model.get("data"), dict)
            ):
                model = model["data"]
            if not isinstance(model, dict):
                msg = f"Report model {model_id} response was not an object"
                raise RuntimeError(msg)
            model_name = model.get("name")
            worksheet_name = next(
                (
                    sheet.get("name")
                    for sheet in model.get("worksheets") or []
                    if sheet.get("id") == worksheet_id and sheet.get("name")
                ),
                None,
            )
            if not model_name or not worksheet_name:
                msg = (
                    f"Report model {model_id} did not include a name for "
                    f"worksheet {worksheet_id}"
                )
                raise RuntimeError(msg)
            return str(model_name), str(worksheet_name)

        @override
        def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
            model_name, worksheet_name = self._labels()
            if mode == "visualization":
                job = self.request_json(
                    "POST",
                    f"{API_PREFIX}/report-models/{model_id}/records/export/",
                    json_body={"worksheet_uuid": worksheet_id},
                )
                export_period = None
            else:
                body: dict[str, Any] = {
                    "period_date": period_date,
                    "report_model_uuid": model_id,
                    "worksheet_uuid": worksheet_id,
                }
                if self.config.get("report_reprocess"):
                    body["reprocess"] = True
                job = self.request_json(
                    "POST",
                    f"{API_PREFIX}/report-models/export-worksheet/",
                    json_body=body,
                )
                export_period = period_date

            finished = self.poll_job(job)
            download_url = (finished.get("details") or {}).get("download_url")
            if not download_url:
                msg = f"Job {finished.get('id')} missing download_url"
                raise RuntimeError(msg)

            # Re-export every run. Visualization snapshots and the selected
            # time-series period both change between syncs.
            for idx, row in enumerate(self.download_csv_rows(download_url)):
                yield {
                    **_export_metadata(
                        _export_report_model_id=model_id,
                        _export_report_model_name=model_name,
                        _export_worksheet_name=worksheet_name,
                        _export_period_date=export_period,
                    ),
                    "_row_id": f"{model_id}:{worksheet_id}:{export_period or ''}:{idx}",
                    "row": row,
                }
            state = self.get_context_state(context)
            state["report_model_uuid"] = model_id
            state["worksheet_uuid"] = worksheet_id
            state["period_date"] = export_period

    return ConfiguredReportExportStream(tap)
