"""Unit tests for tap-captivateiq (no live API required)."""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest
from singer_sdk.exceptions import ConfigValidationError

from tap_captivateiq.client import CaptivateIQPaginator
from tap_captivateiq.streams import (
    EmployeesStream,
    PayoutDatesStream,
    PayoutSummariesStream,
    _parse_start_date,
)
from tap_captivateiq.tap import TapCaptivateIQ

SAMPLE_CONFIG = {
    "auth_token": "test-token",
    "api_url": "https://api.captivateiq.com",
    "request_delay_ms": 0,
    "payout_date_lookback_days": 90,
}


def test_parse_start_date() -> None:
    assert _parse_start_date("2025-01-15") == date(2025, 1, 15)
    assert _parse_start_date("2025-01-15T12:00:00Z") == date(2025, 1, 15)
    assert _parse_start_date(None) is None


def test_payout_dates_stream_is_plan_level() -> None:
    tap = TapCaptivateIQ(config=SAMPLE_CONFIG)
    stream = PayoutDatesStream(tap)
    assert stream.path == "/ciq/v1/payout-dates/"
    assert stream.primary_keys == ("id",)
    assert set(stream.schema["properties"]) == {
        "object",
        "id",
        "period_group",
        "plan",
        "date",
        "created_at",
        "updated_at",
    }


def test_discover_stream_names() -> None:
    tap = TapCaptivateIQ(config=SAMPLE_CONFIG)
    names = sorted(s.name for s in tap.discover_streams())
    assert "employees" in names
    assert "payout_summaries" in names
    assert "report_models" in names
    assert "employee_assumptions" in names
    assert "fee_summary" not in names


def test_paginator_relative_next() -> None:
    paginator = CaptivateIQPaginator()
    response = MagicMock()
    response.json.return_value = {"next": "/ciq/v1/employees/?limit=50&offset=50"}
    response.url = "https://api.captivateiq.com/ciq/v1/employees/?limit=50"
    url = paginator.get_next_url(response)
    assert url == "https://api.captivateiq.com/ciq/v1/employees/?limit=50&offset=50"


def test_employees_url_base() -> None:
    tap = TapCaptivateIQ(config=SAMPLE_CONFIG)
    stream = EmployeesStream(tap)
    assert stream.url_base == "https://api.captivateiq.com"
    assert stream.path.endswith("/employees/")


def test_stream_session_uses_corporate_ca_bundle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
    monkeypatch.delenv("CURL_CA_BUNDLE", raising=False)
    monkeypatch.setenv("SSL_CERT_FILE", "corp-ca.pem")
    tap = TapCaptivateIQ(config=SAMPLE_CONFIG)
    stream = EmployeesStream(tap)
    assert stream.requests_session.verify == "corp-ca.pem"

    monkeypatch.setenv("CURL_CA_BUNDLE", "curl-ca.pem")
    stream = EmployeesStream(tap)
    assert stream.requests_session.verify == "curl-ca.pem"

    monkeypatch.setenv("REQUESTS_CA_BUNDLE", "requests-ca.pem")
    stream = EmployeesStream(tap)
    assert stream.requests_session.verify == "requests-ca.pem"


def test_payout_summaries_dates_to_export_always_includes_latest() -> None:
    tap = TapCaptivateIQ(config=SAMPLE_CONFIG)
    stream = PayoutSummariesStream(tap)
    today = date.today()
    d1 = (today - timedelta(days=60)).isoformat()
    d2 = (today - timedelta(days=30)).isoformat()
    d3 = today.isoformat()
    with patch.object(
        stream,
        "request_json",
        return_value={
            "data": [
                {"payout_date": d1},
                {"payout_date": d2},
                {"payout_date": d3},
            ],
        },
    ):
        with patch.object(
            stream,
            "get_context_state",
            return_value={"synced_payout_summary_dates": [d1, d2]},
        ):
            dates = stream._dates_to_export(None)
    assert dates == [d3]


def test_visualization_export_stream_is_discovered() -> None:
    tap = TapCaptivateIQ(
        config={
            **SAMPLE_CONFIG,
            "visualization_exports": [
                {
                    "stream_name": "fee_summary",
                    "report_model_uuid": "00000000-0000-0000-0000-000000000001",
                    "worksheet_uuid": "00000000-0000-0000-0000-000000000002",
                },
            ],
        },
    )
    stream = next(s for s in tap.discover_streams() if s.name == "fee_summary")
    with (
        patch.object(
            stream,
            "request_json",
            side_effect=[
                {
                    "name": "Example Reports",
                    "worksheets": [
                        {
                            "id": "00000000-0000-0000-0000-000000000002",
                            "name": "Example Worksheet",
                        },
                    ],
                },
                {"id": "job-1", "url": "/ciq/v1/jobs/job-1/", "status": "SUCCESS"},
            ],
        ) as request_json,
        patch.object(
            stream,
            "poll_job",
            return_value={"id": "job-1", "status": "SUCCESS", "details": {"download_url": "http://x"}},
        ),
        patch.object(
            stream,
            "download_csv_rows",
            return_value=iter([{"Company": "Example Co", "Cost Centre": "100"}]),
        ),
    ):
        records = list(stream.get_records(None))
    assert request_json.call_count == 2
    assert request_json.call_args_list[0].args[1].endswith(
        "/report-models/00000000-0000-0000-0000-000000000001/",
    )
    assert request_json.call_args_list[1].args[1].endswith(
        "/report-models/00000000-0000-0000-0000-000000000001/records/export/",
    )
    assert request_json.call_args.kwargs["json_body"] == {
        "worksheet_uuid": "00000000-0000-0000-0000-000000000002",
    }
    assert records[0]["row"]["Company"] == "Example Co"
    assert "_export_payout_date" not in records[0]
    assert "_export_period_date" not in records[0]
    assert "_export_period_date" not in stream.schema["properties"]
    assert records[0]["_export_report_model_id"] == "00000000-0000-0000-0000-000000000001"
    assert records[0]["_export_report_model_name"] == "Example Reports"
    assert records[0]["_export_worksheet_name"] == "Example Worksheet"


def test_time_series_export_includes_period_date() -> None:
    tap = TapCaptivateIQ(
        config={
            **SAMPLE_CONFIG,
            "time_series_exports": [
                {
                    "stream_name": "period_commission",
                    "report_model_uuid": "00000000-0000-0000-0000-000000000003",
                    "worksheet_uuid": "00000000-0000-0000-0000-000000000004",
                    "period_date": "2026-08-31",
                },
            ],
        },
    )
    stream = next(s for s in tap.discover_streams() if s.name == "period_commission")
    with (
        patch.object(
            stream,
            "request_json",
            side_effect=[
                {
                    "name": "Example Model",
                    "worksheets": [
                        {
                            "id": "00000000-0000-0000-0000-000000000004",
                            "name": "Example Worksheet",
                        },
                    ],
                },
                {"id": "job-2", "url": "/j"},
            ],
        ) as request_json,
        patch.object(
            stream,
            "poll_job",
            return_value={"details": {"download_url": "http://x"}},
        ),
        patch.object(stream, "download_csv_rows", return_value=iter([{"Plan Name": "Example"}])),
    ):
        records = list(stream.get_records(None))
    body = request_json.call_args.kwargs["json_body"]
    assert body["period_date"] == "2026-08-31"
    assert body["worksheet_uuid"] == "00000000-0000-0000-0000-000000000004"
    assert records[0]["_export_period_date"] == "2026-08-31"
    assert records[0]["_export_report_model_name"] == "Example Model"
    assert records[0]["_export_worksheet_name"] == "Example Worksheet"
    assert "_export_payout_date" not in records[0]


def test_time_series_export_requires_period_date() -> None:
    with pytest.raises(ConfigValidationError) as raised:
        TapCaptivateIQ(
            config={
                **SAMPLE_CONFIG,
                "time_series_exports": [
                    {
                        "stream_name": "period_commission",
                        "report_model_uuid": "00000000-0000-0000-0000-000000000003",
                        "worksheet_uuid": "00000000-0000-0000-0000-000000000004",
                    },
                ],
            },
        )
    assert any("period_date" in error for error in raised.value.errors)


def test_authenticator_token_prefix() -> None:
    tap = TapCaptivateIQ(config=SAMPLE_CONFIG)
    stream = EmployeesStream(tap)
    headers = stream.authenticator.auth_headers
    assert headers["Authorization"] == "Token test-token"
