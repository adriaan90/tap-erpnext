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

        # Fetch all DocTypes (excluding singles) with istable flag
        params: dict[str, str] = {
            "fields": json.dumps(["name", "istable"]),
            "filters": json.dumps([["issingle", "=", 0]]),
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
            all_doctypes = data.get("data", [])
        except requests.RequestException as e:
            self.logger.error(f"Failed to discover DocTypes: {e}")
            raise RuntimeError(f"DocType discovery failed: {e}") from e

        # Separate parent and child doctypes
        parent_doctypes = []
        child_doctypes = []
        for dt in all_doctypes:
            if dt.get("istable", 0):
                child_doctypes.append(dt["name"])
            else:
                parent_doctypes.append(dt["name"])

        # Filter to user-specified doctypes if configured
        config_doctypes = self.config.get("doctypes")
        if config_doctypes:
            config_set = set(config_doctypes)
            parent_doctypes = [d for d in parent_doctypes if d in config_set]
            child_doctypes = [d for d in child_doctypes if d in config_set]
            missing = config_set - set(parent_doctypes) - set(child_doctypes)
            if missing:
                self.logger.warning(f"Configured DocTypes not found: {missing}")

        # Build child → (parent, fieldname) map from DocField
        child_parent_map: dict[str, tuple[str, str]] = {}
        if child_doctypes:
            try:
                fields_response = requests.get(
                    f"{api_url}/api/resource/DocField",
                    headers=headers,
                    params={
                        "fields": json.dumps(["parent", "fieldname", "options"]),
                        "filters": json.dumps([["fieldtype", "=", "Table"]]),
                        "limit_page_length": "1000",
                    },
                    timeout=30,
                )
                fields_response.raise_for_status()
                for field in fields_response.json().get("data", []):
                    child_parent_map[field["options"]] = (
                        field["parent"],
                        field["fieldname"],
                    )
            except requests.RequestException as e:
                self.logger.warning(f"Failed to discover child table parents: {e}")

        # Create streams: parent doctypes first, then child tables
        result = []
        for doctype in parent_doctypes:
            result.append(streams.create_doctype_stream(doctype, self))

        for doctype in child_doctypes:
            parent_info = child_parent_map.get(doctype)
            if parent_info is None:
                self.logger.warning(
                    f"Child table '{doctype}' has no discoverable parent — skipping.",
                )
                continue
            parent_doctype, parent_field = parent_info
            self.logger.info(
                f"Child table '{doctype}' → parent '{parent_doctype}.{parent_field}'",
            )
            result.append(
                streams.create_doctype_stream(
                    doctype,
                    self,
                    is_child=True,
                    parent_doctype=parent_doctype,
                    parent_field=parent_field,
                ),
            )

        return result


if __name__ == "__main__":
    TapErpNext.cli()
