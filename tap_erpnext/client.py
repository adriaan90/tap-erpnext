"""REST client handling, including ErpNextStream base class."""

from __future__ import annotations

import decimal
import json
import sys
from typing import TYPE_CHECKING, Any

import requests
from singer_sdk import StreamSchema
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import OffsetPaginator
from singer_sdk.streams import RESTStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context
    from singer_sdk.streams.rest import HTTPRequest, PageContext


class ErpNextAuthenticator(APIKeyAuthenticator):
    """Custom authenticator for ERPNext token-based authentication."""

    def __init__(self, stream: RESTStream, api_key: str, api_secret: str):
        super().__init__(
            stream=stream,
            key="Authorization",
            value=f"token {api_key}:{api_secret}",
            location="header",
        )


class ErpNextStream(RESTStream):
    """ErpNext stream class."""

    # ERPNext wraps records in {"data": [...]}
    records_jsonpath = "$.data[*]"

    # schema: ClassVar[StreamSchema] = StreamSchema(SCHEMAS_DIR)

    @override
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["api_url"]

    @override
    @property
    def authenticator(self) -> ErpNextAuthenticator:
        """Return a new authenticator object."""
        return ErpNextAuthenticator(
            stream=self,
            api_key=self.config["api_key"],
            api_secret=self.config["api_secret"],
        )

    @override
    def get_new_paginator(self) -> OffsetPaginator:
        """Create a pagination helper using offset-based pagination."""
        return OffsetPaginator(
            start_value=0,
            page_size=self.config.get("limit_page_length", 200),
        )

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, Any]:
        """Return URL params for the request.

        Args:
            context: Stream context.
            next_page_token: The next page token (offset value).

        Returns:
            A dictionary of URL parameters.
        """
        params: dict[str, Any] = {}

        # Request all fields (ERPNext defaults to only "name")
        params["fields"] = json.dumps(["*"])

        # Set page size
        params["limit_page_length"] = self.config.get("limit_page_length", 200)

        # Set offset for pagination
        if next_page_token is not None:
            params["limit_start"] = next_page_token

        # Add order_by for incremental sync
        if self.replication_key:
            params["order_by"] = f"{self.replication_key} asc"

        # Add start_date filter for incremental sync
        starting = self.get_starting_timestamp(context)
        if starting:
            filter_condition = ["modified", ">=", starting.strftime("%Y-%m-%d %H:%M:%S")]
            params["filters"] = json.dumps([filter_condition])

        return params

    @override
    def parse_response(self, response: requests.Response) -> list[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # Parse response body and return a set of records.
        return list(extract_jsonpath(
            self.records_jsonpath,
            input=response.json(parse_float=decimal.Decimal),
        ))

    @override
    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        return row
