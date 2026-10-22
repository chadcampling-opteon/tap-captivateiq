"""REST client handling for CaptivateIQ."""

from __future__ import annotations

import csv
import decimal
import io
import sys
import time
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin, urlparse

import requests
from singer_sdk.authenticators import SimpleAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseHATEOASPaginator
from singer_sdk.streams import RESTStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context
    from singer_sdk.pagination import BaseAPIPaginator
    from singer_sdk.streams.rest import HTTPRequest, PageContext


DEFAULT_API_URL = "https://api.captivateiq.com"
API_PREFIX = "/ciq/v1"
JOB_TERMINAL = frozenset(
    {"SUCCESS", "FAILURE", "REVOKED", "REJECTED", "IGNORED"},
)
JOB_SUCCESS = "SUCCESS"


class CaptivateIQPaginator(BaseHATEOASPaginator):
    """Follow CaptivateIQ list `next` links (absolute or relative)."""

    @override
    def get_next_url(self, response: requests.Response) -> str | None:
        payload = response.json()
        next_link = payload.get("next")
        if not next_link:
            return None
        if next_link.startswith("http"):
            return next_link
        # Relative paths are rooted at the API host.
        parsed = urlparse(response.url)
        return urljoin(f"{parsed.scheme}://{parsed.netloc}", next_link)


class CaptivateIQStream(RESTStream):
    """Base REST stream for CaptivateIQ list endpoints."""

    records_jsonpath = "$.data[*]"
    page_size: int = 100

    @override
    @property
    def url_base(self) -> str:
        return self.config.get("api_url", DEFAULT_API_URL).rstrip("/")

    @override
    @property
    def authenticator(self) -> SimpleAuthenticator:
        token = self.config["auth_token"]
        return SimpleAuthenticator(
            auth_headers={"Authorization": f"Token {token}"},
        )

    @property
    @override
    def http_headers(self) -> dict[str, str]:
        return {"Accept": "application/json"}

    @override
    def get_new_paginator(self) -> BaseAPIPaginator:
        return CaptivateIQPaginator()

    @override
    def get_http_request(self, *, page: PageContext[Any]) -> HTTPRequest:
        request = super().get_http_request(page=page)
        if page.next_page_token is not None:
            request.url = page.next_page_token.geturl()
            request.params = {}
            return request

        params = dict(request.params or {})
        params.setdefault("limit", self.page_size)
        params.setdefault("offset", 0)
        request.params = params
        return request

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        self._respect_rate_limit()
        yield from extract_jsonpath(
            self.records_jsonpath,
            input=response.json(parse_float=decimal.Decimal),
        )

    def _respect_rate_limit(self) -> None:
        delay_ms = self.config.get("request_delay_ms") or 0
        if delay_ms:
            time.sleep(delay_ms / 1000.0)

    def request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Authenticated JSON request helper for export/job flows."""
        self._respect_rate_limit()
        url = path if path.startswith("http") else f"{self.url_base}{path}"
        response = self.requests_session.request(
            method,
            url,
            headers={
                **self.http_headers,
                **self.authenticator.auth_headers,
            },
            params=params,
            json=json_body,
            timeout=60,
        )
        if response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", "5"))
            self.logger.warning("Rate limited (429); sleeping %ss", retry_after)
            time.sleep(retry_after)
            response = self.requests_session.request(
                method,
                url,
                headers={
                    **self.http_headers,
                    **self.authenticator.auth_headers,
                },
                params=params,
                json=json_body,
                timeout=60,
            )
        response.raise_for_status()
        if not response.content:
            return {}
        return response.json()

    def poll_job(self, job: dict[str, Any]) -> dict[str, Any]:
        """Poll an async job until terminal status."""
        job_path = job.get("url") or f"{API_PREFIX}/jobs/{job['id']}/"
        poll_seconds = float(self.config.get("job_poll_seconds") or 5)
        max_wait = float(self.config.get("job_timeout_seconds") or 900)
        elapsed = 0.0
        details = job
        while details.get("status") not in JOB_TERMINAL:
            if elapsed >= max_wait:
                msg = f"Job {job.get('id')} timed out after {max_wait}s"
                raise TimeoutError(msg)
            time.sleep(poll_seconds)
            elapsed += poll_seconds
            details = self.request_json("GET", job_path)
            self.logger.info(
                "Job %s status=%s (%ss)",
                details.get("id"),
                details.get("status"),
                int(elapsed),
            )
        if details.get("status") != JOB_SUCCESS:
            msg = (
                f"Job {details.get('id')} failed with status "
                f"{details.get('status')}: {details.get('details')}"
            )
            raise RuntimeError(msg)
        return details

    def download_csv_rows(self, download_url: str) -> Iterable[dict[str, str]]:
        """Download a CSV export and yield row dicts.

        Presigned S3 URLs must be fetched without the CaptivateIQ Authorization
        header (S3 rejects dual auth mechanisms).
        """
        self._respect_rate_limit()
        response = requests.get(download_url, timeout=120)
        response.raise_for_status()
        text = response.content.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            yield {k: (v if v is not None else "") for k, v in row.items()}
