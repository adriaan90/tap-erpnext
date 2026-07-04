"""REST client handling, including ErpNextStream base class."""

from __future__ import annotations

import datetime
import decimal
import json
import sys
from typing import TYPE_CHECKING, Any

import requests
from singer_sdk import StreamSchema, typing as th
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

    def __init__(self, api_key: str, api_secret: str):
        super().__init__(
            key="Authorization",
            value=f"token {api_key}:{api_secret}",
            location="header",
        )


class ErpNextStream(RESTStream):
    """ErpNext stream class."""

    # ERPNext wraps records in {"data": [...]}
    records_jsonpath = "$.data[*]"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the stream with a lazy schema cache."""
        super().__init__(*args, **kwargs)
        self._schema: dict | None = None

    @override
    @property
    def schema(self) -> dict:
        """Return the stream schema, dynamically discovered from the API.

        On first access, fetches a sample record to infer field names and types.
        Falls back to a minimal schema if no records exist or the API is unreachable.
        """
        if self._schema is not None:
            return self._schema

        sample = self._fetch_sample_record()
        if sample:
            self._schema = self._infer_schema_from_record(sample)
        else:
            self._schema = self._minimal_schema()

        return self._schema

    def _minimal_schema(self) -> dict:
        """Return a minimal fallback schema (name + modified only)."""
        return th.PropertiesList(
            th.Property("name", th.StringType, required=True),
            th.Property("modified", th.DateTimeType),
        ).to_dict()

    def _fetch_sample_record(self) -> dict | None:
        """Fetch a single record from the API to infer the schema.

        Returns:
            A sample record dict, or None if no records exist or the request fails.
        """
        url = f"{self.url_base}{self.path}"
        headers = self.authenticator.auth_headers or {}
        params: dict[str, str] = {
            "fields": json.dumps(["*"]),
            "limit_page_length": "1",
        }
        try:
            response = requests.get(
                url, headers=headers, params=params, timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            records = data.get("data", [])
            if records:
                return records[0]
            self.logger.warning(
                f"No records found for {self.name} — using minimal schema.",
            )
        except Exception as e:
            self.logger.warning(
                f"Failed to fetch sample record for {self.name}: {e} — "
                f"using minimal schema.",
            )
        return None

    @staticmethod
    def _infer_schema_from_record(record: dict) -> dict:
        """Build a Singer schema by inferring types from a sample record.

        Args:
            record: A single record dict from the ERPNext API.

        Returns:
            A Singer-compatible JSON Schema dict with all discovered fields.
        """

        def _singer_type(value: Any) -> th.JSONTypeHelper[Any] | type[th.JSONTypeHelper[Any]]:
            """Map a Python value to the best Singer type."""
            if value is None:
                return th.StringType
            if isinstance(value, bool):
                return th.BooleanType
            if isinstance(value, int):
                return th.IntegerType
            if isinstance(value, float):
                return th.NumberType
            if isinstance(value, (datetime.date, datetime.datetime)):
                return th.DateTimeType
            if isinstance(value, list):
                return th.ArrayType(th.StringType)
            if isinstance(value, dict):
                return th.ObjectType()
            return th.StringType

        schema = th.PropertiesList(
            *[th.Property(key, _singer_type(value)) for key, value in record.items()],
        ).to_dict()

        # Force known datetime fields to DateTimeType.
        # ERPNext returns datetimes as JSON strings, so type inference
        # defaults to StringType — but the Singer SDK requires DateTimeType
        # for replication keys like "modified".
        _DATETIME_FIELDS = frozenset({"modified", "creation"})
        for prop_name, prop_schema in schema.get("properties", {}).items():
            if prop_name in _DATETIME_FIELDS:
                prop_schema["type"] = ["string", "null"]
                prop_schema["format"] = "date-time"

        return schema

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

        # Request all known fields from the discovered schema.
        # ERPNext's fields=["*"] is unreliable — it only returns a default
        # subset of fields. Instead, explicitly list every field we know about.
        # Use _schema (not self.schema) to avoid triggering lazy discovery
        # inside the request-building path (would recurse via _fetch_sample_record).
        if self._schema is not None:
            field_names = list(self._schema.get("properties", {}).keys())
            params["fields"] = json.dumps(field_names)
        else:
            # Fallback for initial discovery (schema not yet inferred)
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


class ChildTableStream(ErpNextStream):
    """Stream for ERPNext child tables (istable=1).

    ERPNext's list endpoint for child doctypes only returns stubs
    (name, owner, creation, modified, etc.) — the actual data fields
    (account, debit, credit, etc.) are only available embedded inside
    the parent document.

    This stream paginates through the **parent** endpoint and extracts
    child records from the embedded table field (e.g. the ``accounts``
    field on a Journal Entry).
    """

    # Child tables don't have a modified-based replication key in practice
    # because the list endpoint doesn't return meaningful modified values
    # for filtering. We use the parent's sync as the driver instead.
    replication_key = None

    def __init__(
        self,
        *args: Any,
        parent_doctype: str | None = None,
        parent_field: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize a child table stream.

        Args:
            parent_doctype: The parent DocType name (e.g. "Journal Entry").
            parent_field: The field name on the parent that holds this table
                (e.g. "accounts").
        """
        super().__init__(*args, **kwargs)
        self._parent_doctype = parent_doctype
        self._parent_field = parent_field

        # Override path to point to the PARENT endpoint — child data
        # is only available embedded inside parent documents.
        if parent_doctype:
            self.path = f"/api/resource/{parent_doctype}"

    @override
    def _fetch_sample_record(self) -> dict | None:
        """Fetch a parent record and extract the first child for schema inference.

        The child doctype's own endpoint returns stubs, so we must go through
        the parent to see the real child fields (account, debit, credit, etc.).
        """
        url = f"{self.url_base}{self.path}"
        headers = self.authenticator.auth_headers or {}
        params: dict[str, str] = {
            "fields": json.dumps(["*"]),
            "limit_page_length": "1",
        }
        try:
            response = requests.get(
                url, headers=headers, params=params, timeout=30,
            )
            response.raise_for_status()
            records = response.json().get("data", [])
            if records:
                parent = records[0]
                children = parent.get(self._parent_field, [])
                if isinstance(children, list) and children:
                    return children[0]
            self.logger.warning(
                f"No child records found in parent field "
                f"'{self._parent_field}' for {self.name} — using minimal schema.",
            )
        except Exception as e:
            self.logger.warning(
                f"Failed to fetch sample child record for {self.name}: {e}",
            )
        return None

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, Any]:
        """Return URL params for the parent endpoint.

        Uses ``fields=["*"]`` to ensure the embedded child table field
        is included in the response.
        """
        params: dict[str, Any] = {
            "fields": json.dumps(["*"]),
            "limit_page_length": self.config.get("limit_page_length", 200),
        }
        if next_page_token is not None:
            params["limit_start"] = next_page_token
        return params

    @override
    def parse_response(self, response: requests.Response) -> list[dict]:
        """Extract child records from parent documents.

        Each parent document contains an embedded list of child records
        under ``self._parent_field`` (e.g. ``accounts`` on a Journal Entry).
        We extract and flatten all children across all parents in the page.
        """
        parents = list(extract_jsonpath(
            self.records_jsonpath,
            input=response.json(parse_float=decimal.Decimal),
        ))

        child_records: list[dict] = []
        for parent in parents:
            children = parent.get(self._parent_field, [])
            if isinstance(children, list):
                child_records.extend(children)

        return child_records
