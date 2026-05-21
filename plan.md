# Implementation Plan: tap-erpnext for ERPNext Data Extraction

## Objective

Transform this template tap into a working Singer tap that extracts data from
ERPNext via its Frappe REST API, supporting **dynamic DocType discovery** and
**incremental sync**.

## Decisions Made (User Input)

Before implementation, these decisions were confirmed by the user:

1. **Timezone handling**: Accept drift — pass `start_date` as-is without timezone
   conversion. ERPNext stores `modified` in server local time; minor drift is
   acceptable.
2. **Stream naming**: Allow spaces — DocType names like "Sales Invoice" are used
   as-is for Singer stream names. The `name` class attribute preserves the
   original name.
3. **Schema approach**: Flexible schema — use `additionalProperties: true` for
   all dynamic streams. Do NOT build strict JSON Schemas per DocType.

## ERPNext API Reference

- **List endpoint**: `GET /api/resource/{doctype_name}`
- **List all DocTypes**: `GET /api/resource/DocType?fields=["name"]&filters=[["istable","=",0],["issingle","=",0]]`
- **Auth**: `Authorization: token {api_key}:{api_secret}` header
- **Pagination**: offset-based via `limit_start` and `limit_page_length` query params
- **Response format**: `{"data": [...]}` — records are under `data`
- **Sorting**: `order_by=modified%20asc` for incremental sync
- **Primary key**: `name` (present on all DocTypes)
- **Replication key**: `modified` (DateTime, present on all DocTypes)

---

## Step 1: Fix `tap_erpnext/tap.py` — Configuration (Syntax Fix)

**CRITICAL BUG**: The file has a **syntax error** — `config_jsonschema = ` is
entirely missing. Lines 25-79 are orphaned expressions that will cause a
`SyntaxError` on import.

**Actions:**

1. **Fix the syntax** — The entire property block (lines 25-79) must be wrapped
   inside `config_jsonschema = th.PropertiesList(...).to_dict()`:

   ```python
   class TapErpNext(Tap):
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
   ```

2. **Remove duplicates** that existed in the original:
   - `api_url` appeared twice (lines 25-31 and 72-78) — keep the one with `required=True`
   - `start_date` appeared twice (lines 48-52 and 67-71) — keep one
   - `project_ids` (lines 60-66) — **remove entirely**, not relevant to ERPNext

---

## Step 2: Implement `discover_streams()` with Authenticated API Call

**Challenge**: `discover_streams()` must make an authenticated API call to list
DocTypes, but no stream instances exist yet (so no `authenticator` is available).

**Solution**: Manually construct the auth header from `self.config` values:

```python
@override
def discover_streams(self) -> list[streams.ErpNextStream]:
    import json
    import requests

    api_url = self.config["api_url"]
    api_key = self.config["api_key"]
    api_secret = self.config["api_secret"]

    headers = {
        "Authorization": f"token {api_key}:{api_secret}",
        "Accept": "application/json",
    }
    params = {
        "fields": json.dumps(["name"]),
        "filters": json.dumps([["istable", "=", 0], ["issingle", "=", 0]]),
        "limit_page_length": 1000,
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
```

> **Why a factory function?** The Singer SDK expects `stream.name` to be a
> class-level attribute. We use a factory (`create_doctype_stream`) that creates
> a new class dynamically for each DocType using `type()`. See Step 3.

---

## Step 3: Rewrite `tap_erpnext/streams.py` — Dynamic Stream Factory

**Current state**: Contains placeholder `UsersStream` and `GroupsStream` with
hardcoded schemas that don't match any ERPNext API.

**Actions:**

1. **Delete** the existing `UsersStream` and `GroupsStream` classes entirely.

2. **Create a factory function** that dynamically creates a stream class per DocType:

   ```python
   """Stream type classes for tap-erpnext."""

   from __future__ import annotations

   from singer_sdk import typing as th
   from tap_erpnext.client import ErpNextStream

   # Dynamic schema: allow all properties since we fetch all fields.
   # ERPNext always returns "name" and "modified" at minimum.
   DYNAMIC_SCHEMA = th.PropertiesList(
       th.Property("name", th.StringType, required=True),
       th.Property("modified", th.DateTimeType),
   ).to_dict()
   # Allow additional properties (all other ERPNext fields)
   DYNAMIC_SCHEMA["additionalProperties"] = True


   def create_doctype_stream(doctype: str, tap) -> ErpNextStream:
       """Factory: create a stream instance for a specific DocType.

       Uses `type()` to create a dynamic subclass with `name` as a class attribute,
       which is required by the Singer SDK.

       Args:
           doctype: The ERPNext DocType name (e.g., "Sales Invoice").
           tap: The TapErpNext instance.

       Returns:
           An instance of a dynamically-created ErpNextStream subclass.
       """
       # Sanitize class name (Python identifiers can't have spaces)
       safe_name = doctype.replace(" ", "_").replace("-", "_")
       class_name = f"ErpNext_{safe_name}"

       # Dynamically create a new class for this DocType
       stream_class = type(
           class_name,
           (ErpNextStream,),
           {
               "name": doctype,  # Class-level attribute, required by SDK
               "path": f"/api/resource/{doctype}",
               "primary_keys": ("name",),
               "replication_key": "modified",
               "schema": DYNAMIC_SCHEMA,
               "__doc__": f"ERPNext {doctype} stream.",
           },
       )

       return stream_class(tap)
   ```

3. **Why `type()`?** The SDK reads `stream.name` early (before records are
   processed). Setting `name` as a class attribute (via `type()`) ensures the
   SDK can read it correctly. Setting it as an instance attribute in `__init__`
   can cause issues because the SDK may inspect the class before instantiation.

4. **DocType names with spaces**: ERPNext DocTypes like "Sales Invoice" have
   spaces. The `name` class attribute preserves the original name (used in
   catalogs and state). The `path` property URL-encodes the name via
   `f"/api/resource/{doctype}"` — the `requests` library handles URL encoding
   of path parameters automatically.

---

## Step 4: Fix `tap_erpnext/client.py` — Pagination (Using `OffsetPaginator`)

**Current state**: The client has token auth and JSONPath response parsing
working correctly. Pagination is broken: `get_new_paginator()` returns `None`,
and `get_http_request` has partial (non-working) pagination logic.

**Important**: The SDK (v0.54.x) provides `OffsetPaginator` (current) —
`BaseOffsetPaginator` is **deprecated**. Use `OffsetPaginator`.

**Actions:**

1. **Replace `get_new_paginator`** to return an `OffsetPaginator`:

   ```python
   from singer_sdk.pagination import OffsetPaginator

   class ErpNextStream(RESTStream):
       # ... existing code ...

       @override
       def get_new_paginator(self) -> OffsetPaginator:
           """Create a pagination helper using offset-based pagination."""
           return OffsetPaginator(
               start_value=0,
               page_size=self.config.get("limit_page_length", 200),
           )
   ```

2. **Remove custom pagination from `get_http_request`** — the paginator handles
   `limit_start` automatically. Simplify `get_http_request`:

   ```python
   @override
   def get_http_request(self, *, page: PageContext[Any]) -> HTTPRequest:
       request = super().get_http_request(page=page)

       # Set page size (limit_page_length)
       request.params["limit_page_length"] = self.config.get("limit_page_length", 200)

       # Add order_by for incremental sync
       if self.replication_key:
           request.params["order_by"] = f"{self.replication_key} asc"

       return request
   ```

   > **Note**: The `OffsetPaginator` automatically adds `limit_start` (or the
   > equivalent offset param) to the request via the SDK's pagination machinery.
   > We don't need to manually set `limit_start` anymore.

3. **How `OffsetPaginator` works with ERPNext**:
   - The paginator tracks `current_value` (the offset)
   - The SDK passes this value as `next_page_token` to `get_url_params`
   - We need to override `get_url_params` to map the token to `limit_start`:

   ```python
   @override
   def get_url_params(
       self,
       context: Context | None,
       next_page_token: int | None,
   ) -> dict[str, Any]:
       params = super().get_url_params(context, next_page_token)
       if next_page_token is not None:
           params["limit_start"] = next_page_token
       return params
   ```

4. **Stop condition**: `OffsetPaginator` stops when a page returns fewer
   records than `page_size`. This works correctly with ERPNext because:
   - ERPNext returns up to `limit_page_length` records per page
   - If the result has fewer records, we've reached the end

---

## Step 5: Incremental Sync — `start_date` Filter

**Current state**: `replication_key = "modified"` is set on streams, which
enables incremental sync at the SDK level (bookmarks). However, the
`start_date` config value is NOT being used to filter API results.

**Actions:**

1. **Add `start_date` filter to API requests** by overriding `get_url_params`
   in `ErpNextStream`:

   ```python
   import json

   @override
   def get_url_params(
       self,
       context: Context | None,
       next_page_token: int | None,
   ) -> dict[str, Any]:
       params = super().get_url_params(context, next_page_token)
       if next_page_token is not None:
           params["limit_start"] = next_page_token

       # Add start_date filter for incremental sync
       starting = self.get_starting_timestamp(context)
       if starting:
           # ERPNext filter format: [["modified", ">=", "YYYY-MM-DD HH:MM:SS"]]
           # Use <= to include records modified exactly at the bookmark
           filter_condition = ["modified", ">=", starting.strftime("%Y-%m-%d %H:%M:%S")]
           # Merge with any existing filters (though none are set by default)
           existing = params.get("filters", "[]")
           if existing == "[]":
               params["filters"] = json.dumps([filter_condition])
           else:
               # Parse existing filters and append
               existing_list = json.loads(existing)
               existing_list.append(filter_condition)
               params["filters"] = json.dumps(existing_list)

       return params
   ```

2. **Verify timezone handling**: ERPNext stores `modified` in server local
   time. The `start_date` from config is in UTC. This could cause records
   to be missed or duplicated. **Recommendation**: Document this limitation
   and suggest users set `start_date` in the ERPNext server's timezone, or
   use UTC and accept minor drift.

---

## Step 6: Align `meltano.yml` with Config Schema

**Actions:**

1. **Add `doctypes` setting**:
   ```yaml
   - name: doctypes
     kind: array
     label: DocTypes
     description: Specific DocType names to sync (optional; if empty, all accessible DocTypes are discovered)
   ```

2. **Verify existing settings match** `tap.py` config schema:
   - `api_url` → `kind: string`, required
   - `api_key` → `kind: string`, `sensitive: true`, required
   - `api_secret` → `kind: string`, `sensitive: true`, required
   - `start_date` → `kind: date_iso8601`, optional
   - `limit_page_length` → `kind: integer`, optional, default 200

3. **Remove `project_ids`** from any config if it exists (it shouldn't in
   the current `meltano.yml`).

---

## Step 7: Update `.env.example`

```bash
# ERPNext Configuration
# Copy this file to .env and fill in your actual values

# Required: API URL of the ERPNext instance
TAP_ERPNEXT_API_URL=https://erp.example.com

# Required: ERPNext API key
TAP_ERPNEXT_API_KEY=your_api_key_here

# Required: ERPNext API secret
TAP_ERPNEXT_API_SECRET=your_api_secret_here

# Optional: The earliest record date to sync (ISO format, in ERPNext server timezone)
TAP_ERPNEXT_START_DATE=2023-01-01T00:00:00

# Optional: Number of records to fetch per API request
TAP_ERPNEXT_LIMIT_PAGE_LENGTH=200

# Optional: Comma-separated list of DocType names to sync (leave empty to sync all)
# Example: TAP_ERPNEXT_DOCTYPES=Sales Invoice,Customer,Item
TAP_ERPNEXT_DOCTYPES=
```

---

## Step 8: Update Tests

**File**: `tests/test_core.py`

1. **Update `SAMPLE_CONFIG`** with valid structure (tests that need real API
   can be skipped with `@pytest.mark.skip`):

   ```python
   import datetime
   import os

   from singer_sdk.testing import get_tap_test_class

   from tap_erpnext.tap import TapErpNext

   SAMPLE_CONFIG = {
       "api_url": os.environ.get("TAP_ERPNEXT_API_URL", "https://erp.example.com"),
       "api_key": os.environ.get("TAP_ERPNEXT_API_KEY", "test_key"),
       "api_secret": os.environ.get("TAP_ERPNEXT_API_SECRET", "test_secret"),
       "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
   }

   # Run standard built-in tap tests from the SDK:
   TestTapErpNext = get_tap_test_class(
       tap_class=TapErpNext,
       config=SAMPLE_CONFIG,
   )
   ```

2. **Add unit tests for the factory function** (these don't need API access):

   ```python
   from tap_erpnext.streams import create_doctype_stream

   def test_create_doctype_stream():
       """Test that the factory creates streams with correct attributes."""
       # Mock tap object with minimal interface
       class MockTap:
           config = {"limit_page_length": 200}
           name = "tap-erpnext"

       stream = create_doctype_stream("Sales Invoice", MockTap())

       assert stream.name == "Sales Invoice"
       assert stream.path == "/api/resource/Sales Invoice"
       assert stream.primary_keys == ("name",)
       assert stream.replication_key == "modified"
       assert stream.schema["additionalProperties"] is True

   def test_doctype_stream_schema_allows_additional_properties():
       """Test that the dynamic schema allows all ERPNext fields."""
       class MockTap:
           config = {}
           name = "tap-erpnext"

       stream = create_doctype_stream("Item", MockTap())
       schema = stream.schema

       assert schema["type"] == "object"
       assert "name" in schema["properties"]
       assert "modified" in schema["properties"]
       assert schema["additionalProperties"] is True
   ```

3. **Add integration test markers** for tests that need a real ERPNext instance:

   ```python
   import pytest

   @pytest.mark.integration
   def test_discover_streams_with_real_api():
       """Test DocType discovery against a real ERPNext instance."""
       config = {
           "api_url": os.environ["TAP_ERPNEXT_API_URL"],
           "api_key": os.environ["TAP_ERPNEXT_API_KEY"],
           "api_secret": os.environ["TAP_ERPNEXT_API_SECRET"],
       }
       tap = TapErpNext(config=config)
       streams = tap.discover_streams()
       assert len(streams) > 0
       assert all(s.name for s in streams)
   ```

---

## Step 9: Verify Pagination Works End-to-End

**Critical**: Pagination is the most error-prone part. Verify by:

1. Run `uv run tap-erpnext --config config.json --discover` against a real
   ERPNext instance to confirm streams are discovered.

2. Run a sync against a DocType with >200 records to confirm multiple pages
   are fetched. Check the logs for `limit_start` incrementing: 0, 200, 400, ...

3. Confirm the sync stops when a page returns fewer than `limit_page_length`
   records.

---

## File-by-File Summary

| File | Action |
|------|--------|
| `tap_erpnext/tap.py` | **Fix syntax error** (add `config_jsonschema =`), remove duplicates, add `doctypes`, implement `discover_streams()` with authenticated API call |
| `tap_erpnext/client.py` | Use `OffsetPaginator` (not `BaseOffsetPaginator`), remove broken pagination from `get_http_request`, add `get_url_params` for `limit_start` and `start_date` filter |
| `tap_erpnext/streams.py` | Delete `UsersStream`/`GroupsStream`, create `create_doctype_stream()` factory using `type()` |
| `meltano.yml` | Add `doctypes` setting, verify all settings match `tap.py` |
| `.env.example` | Add `TAP_ERPNEXT_DOCTYPES`, update `START_DATE` format |
| `tests/test_core.py` | Update `SAMPLE_CONFIG`, add unit tests for factory function |

---

## Risks & Open Questions

1. **DocType discovery API**: Confirm that `/api/resource/DocType` returns
   child tables as records. The filters `istable=0` and `issingle=0` should
   exclude child tables and single doctypes. Verify against a real instance.

2. **Rate limiting**: ERPNext doesn't have built-in rate limiting, but add
   a small delay between pages if needed. The SDK's built-in retry logic
   handles transient failures.

3. **Field name conflicts**: ERPNext field names may contain characters that
   are valid in JSON but unexpected in downstream targets. Post-processing
   may be needed for complex field names.

4. **Large DocTypes**: Some DocTypes (like `Version` or `Communication`) can
   have millions of records. Consider adding a `doctypes_denylist` config or
   letting the user specify which DocTypes to sync (already done via `doctypes`).

5. **Modified field timezone**: ERPNext stores `modified` in server local
   time. The `start_date` comparison uses string comparison — ensure the
   format matches what ERPNext expects (`YYYY-MM-DD HH:MM:SS`).

6. **Stream name with spaces**: The Singer spec allows spaces in stream names,
   but some targets may not. Monitor for issues with DocTypes like "Sales Invoice".
   If needed, add a `stream_name_map` config to rename streams.

7. **`additionalProperties: True` in schema**: This tells the SDK that records
   can have arbitrary fields. Some targets (like SQL-based ones) may not handle
   this well. If issues arise, consider generating a strict schema by fetching
   a sample record from each DocType during discovery.
