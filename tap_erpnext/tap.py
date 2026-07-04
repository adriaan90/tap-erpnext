"""ErpNext tap class."""

from __future__ import annotations

import json
import sys

import requests
from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_erpnext import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapErpNext(Tap):
    """Singer tap for ErpNext."""

    name = "tap-erpnext"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType(nullable=False),
            required=True,
            title="API URL",
            description="The base URL of the ERPNext instance (e.g. https://erp.example.com)",
        ),
        th.Property(
            "api_key",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="API Key",
            description="ERPNext API key",
        ),
        th.Property(
            "api_secret",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="API Secret",
            description="ERPNext API secret",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="The earliest record date to sync",
        ),
        th.Property(
            "limit_page_length",
            th.IntegerType(nullable=True),
            default=200,
            title="Page Length",
            description="Number of records to fetch per API request",
        ),
        th.Property(
            "doctypes",
            th.ArrayType(th.StringType),
            description="Specific DocType names to sync. If omitted, all accessible DocTypes are discovered.",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.ErpNextStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        api_url = self.config["api_url"]
        api_key = self.config["api_key"]
        api_secret = self.config["api_secret"]

        headers = {
            "Authorization": f"token {api_key}:{api_secret}",
            "Accept": "application/json",
        }
        params: dict[str, str] = {
            "fields": json.dumps(["name"]),
            "filters": json.dumps([["istable", "=", 0], ["issingle", "=", 0]]),
            "limit_page_length": "1000",
        }

        try:
            response = requests.get(
                f"{api_url}/api/resource/DocType",
                headers=headers,
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            doctypes = [d["name"] for d in data.get("data", [])]
        except requests.RequestException as e:
            self.logger.error(f"Failed to discover DocTypes: {e}")
            raise RuntimeError(f"DocType discovery failed: {e}") from e

        # Filter to user-specified doctypes if configured
        config_doctypes = self.config.get("doctypes")
        if config_doctypes:
            config_set = set(config_doctypes)
            doctypes = [d for d in doctypes if d in config_set]
            missing = config_set - set(doctypes)
            if missing:
                self.logger.warning(f"Configured DocTypes not found: {missing}")

        # Create one stream per doctype using the factory function from streams.py
        return [streams.create_doctype_stream(doctype, self) for doctype in doctypes]


if __name__ == "__main__":
    TapErpNext.cli()
