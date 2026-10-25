"""CaptivateIQ tap class."""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_captivateiq import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapCaptivateIQ(Tap):
    """Singer tap for CaptivateIQ."""

    name = "tap-captivateiq"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="Auth Token",
            description=(
                "CaptivateIQ API token from user profile → API Tokens. "
                "Sent as `Authorization: Token <value>`."
            ),
        ),
        th.Property(
            "api_url",
            th.StringType(nullable=False),
            title="API URL",
            default="https://api.captivateiq.com",
            description="CaptivateIQ API host (no path suffix)",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="Earliest date for windowed streams (assumptions, payouts, reports)",
        ),
        th.Property(
            "active_plans_only",
            th.BooleanType,
            default=True,
            description="When true, plans stream requests is_active=true",
        ),
        th.Property(
            "plan_ids",
            th.ArrayType(th.StringType),
            description="Optional commission plan UUID filter for assumptions/exports",
        ),
        th.Property(
            "period_group_ids",
            th.ArrayType(th.StringType),
            description="Optional period group UUID filter for payout exports",
        ),
        th.Property(
            "assumption_lookback_days",
            th.IntegerType,
            default=90,
            description="Employee assumptions window lookback when start_date unset",
        ),
        th.Property(
            "assumption_lookahead_days",
            th.IntegerType,
            default=31,
            description="Employee assumptions window lookahead from today",
        ),
        th.Property(
            "payout_date_lookback_days",
            th.IntegerType,
            default=120,
            description="Only consider payout dates on/after today - lookback",
        ),
        th.Property(
            "locked_only",
            th.BooleanType,
            description=(
                "Payout export locked_only flag. Omit/false includes unlocked "
                "(needed for mid-period MTD)."
            ),
        ),
        th.Property(
            "worksheet_names",
            th.ArrayType(th.StringType),
            description="Calc workbook worksheet names for payout_worksheet_exports",
        ),
        th.Property(
            "report_model_type",
            th.StringType,
            default="time_series",
            description="report-models list type filter (time_series|visualization)",
        ),
        th.Property(
            "visualization_exports",
            th.ArrayType(
                th.ObjectType(
                    th.Property("stream_name", th.StringType, required=True),
                    th.Property("report_model_uuid", th.StringType, required=True),
                    th.Property("worksheet_uuid", th.StringType, required=True),
                ),
            ),
            description=(
                "Visualization report worksheets (API type=visualization). "
                "Each entry is its own stream. Exported via records/export "
                "with worksheet_uuid and no period_date."
            ),
        ),
        th.Property(
            "time_series_exports",
            th.ArrayType(
                th.ObjectType(
                    th.Property("stream_name", th.StringType, required=True),
                    th.Property("report_model_uuid", th.StringType, required=True),
                    th.Property("worksheet_uuid", th.StringType, required=True),
                    th.Property("period_date", th.DateType, required=True),
                ),
            ),
            description=(
                "Time-series report worksheets. Each entry is its own stream. "
                "Exported via export-worksheet with worksheet_uuid and period_date."
            ),
        ),
        th.Property(
            "report_reprocess",
            th.BooleanType,
            default=False,
            description="Pass reprocess=true on report worksheet exports",
        ),
        th.Property(
            "request_delay_ms",
            th.IntegerType,
            default=250,
            description="Sleep between API calls to stay under rate limits",
        ),
        th.Property(
            "job_poll_seconds",
            th.NumberType,
            default=5,
            description="Seconds between async job status polls",
        ),
        th.Property(
            "job_timeout_seconds",
            th.NumberType,
            default=900,
            description="Max seconds to wait for an async export job",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.CaptivateIQStream]:
        """Return a list of discovered streams."""
        return [
            streams.EmployeesStream(self),
            streams.HierarchyGroupsStream(self),
            streams.HierarchiesStream(self),
            streams.PlansStream(self),
            streams.PeriodGroupsStream(self),
            streams.EmployeeAssumptionsStream(self),
            streams.PayoutDatesStream(self),
            streams.PayoutWorksheetsStream(self),
            streams.PayoutSummariesStream(self),
            streams.PayoutWorksheetExportsStream(self),
            streams.ReportModelsStream(self),
            *streams.configured_report_streams(self),
        ]


if __name__ == "__main__":
    TapCaptivateIQ.cli()
