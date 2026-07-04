"""Tests for tap-erpnext."""

from typing import Any
import datetime
import json
import logging
import os
from unittest.mock import Mock, patch

import pytest

from tap_erpnext.streams import create_doctype_stream
from tap_erpnext.tap import TapErpNext


def _make_mock_tap():
    """Create a mock tap with required attributes."""
    tap = Mock()
    tap.config = {
        "api_url": "https://erp.example.com",
        "api_key": "test_key",
        "api_secret": "test_secret",
        "limit_page_length": 200,
    }
    tap.name = "tap-erpnext"
    tap.logger = logging.getLogger("test")
    return tap


SAMPLE_CONFIG = {
    "api_url": "https://erp.example.com",
    "api_key": "test_key",
    "api_secret": "test_secret",
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
}

# Mock responses
MOCK_DOCTYPES_RESPONSE = {
    "data": [
        {"name": "Sales Invoice", "istable": 0},
        {"name": "Customer", "istable": 0},
        {"name": "Item", "istable": 0},
    ]
}

MOCK_DOCFIELDS_RESPONSE = {
    "data": []  # No child tables in basic test
}

MOCK_CHILD_DOCTYPES_RESPONSE = {
    "data": [
        {"name": "Journal Entry", "istable": 0},
        {"name": "Journal Entry Account", "istable": 1},
        {"name": "Sales Invoice", "istable": 0},
        {"name": "Sales Invoice Item", "istable": 1},
    ]
}

MOCK_CHILD_DOCFIELDS_RESPONSE = {
    "data": [
        {"parent": "Journal Entry", "fieldname": "accounts", "options": "Journal Entry Account"},
        {"parent": "Sales Invoice", "fieldname": "items", "options": "Sales Invoice Item"},
    ]
}

MOCK_STREAM_DATA_RESPONSE = {
    "data": [
        {"name": "INV-001", "modified": "2023-01-01 10:00:00"},
    ]
}


def _make_discovery_mock(doctypes_data, docfields_data):
    """Create a requests.get mock that returns different data based on URL."""
    def _mock_get(url, **kwargs):
        mock_resp = Mock()
        if "/api/resource/DocType" in url:
            mock_resp.json.return_value = doctypes_data
        elif "/api/resource/DocField" in url:
            mock_resp.json.return_value = docfields_data
        else:
            mock_resp.json.return_value = {"data": []}
        mock_resp.raise_for_status.return_value = None
        return mock_resp
    return _mock_get


# --- Basic stream tests ---

def test_create_doctype_stream():
    """Test that the factory creates streams with correct attributes."""
    tap = _make_mock_tap()
    stream = create_doctype_stream("Sales Invoice", tap)

    assert stream.name == "Sales Invoice"
    assert stream.path == "/api/resource/Sales Invoice"
    assert stream.primary_keys == ("name",)
    assert stream.replication_key == "modified"
    assert stream._schema is None


def test_doctype_stream_schema_infers_from_sample():
    """Test that the schema is dynamically inferred from a sample record."""
    tap = _make_mock_tap()
    stream = create_doctype_stream("Item", tap)

    sample = {
        "name": "ITEM-001",
        "modified": "2023-01-01 10:00:00",
        "item_name": "Widget",
        "item_group": "Products",
        "standard_rate": 99.95,
        "disabled": False,
        "has_variants": 0,
    }
    schema = stream._infer_schema_from_record(sample)

    assert schema["type"] == "object"
    assert "name" in schema["properties"]
    assert "modified" in schema["properties"]
    assert "item_name" in schema["properties"]
    assert "item_group" in schema["properties"]
    assert "standard_rate" in schema["properties"]
    assert "disabled" in schema["properties"]
    assert "has_variants" in schema["properties"]


def test_doctype_stream_schema_fallback_minimal():
    """Test that the minimal fallback schema is used when no sample is available."""
    tap = _make_mock_tap()
    stream = create_doctype_stream("EmptyDoctype", tap)

    schema = stream._minimal_schema()
    assert schema["type"] == "object"
    assert "name" in schema["properties"]
    assert "modified" in schema["properties"]
    assert len(schema["properties"]) == 2


def test_infer_schema_none_values():
    """Test that None values default to StringType."""
    from tap_erpnext.client import ErpNextStream

    sample = {"name": "TEST", "maybe_null": None}
    schema = ErpNextStream._infer_schema_from_record(sample)
    assert "maybe_null" in schema["properties"]
    assert schema["properties"]["maybe_null"]["type"] == ["string", "null"]


def test_create_doctype_stream_with_spaces():
    """Test that DocType names with spaces are handled correctly."""
    tap = _make_mock_tap()
    stream = create_doctype_stream("Sales Invoice", tap)
    assert stream.name == "Sales Invoice"
    assert "Sales_Invoice" in stream.__class__.__name__


# --- Discovery tests (no child tables) ---

@patch("tap_erpnext.client.ErpNextStream._fetch_sample_record")
@patch("tap_erpnext.tap.requests.get")
def test_discover_streams(mock_requests_get, mock_fetch):
    """Test DocType discovery."""
    mock_requests_get.side_effect = _make_discovery_mock(
        MOCK_DOCTYPES_RESPONSE, MOCK_DOCFIELDS_RESPONSE,
    )
    mock_fetch.return_value = {"name": "TEST", "modified": "2023-01-01"}

    tap = TapErpNext(config=SAMPLE_CONFIG)
    streams = tap.discover_streams()

    assert len(streams) == 3
    assert streams[0].name == "Sales Invoice"
    assert streams[1].name == "Customer"
    assert streams[2].name == "Item"


@patch("tap_erpnext.client.ErpNextStream._fetch_sample_record")
@patch("tap_erpnext.tap.requests.get")
def test_discover_streams_with_config_doctypes(mock_requests_get, mock_fetch):
    """Test DocType discovery with configured doctypes filter."""
    mock_requests_get.side_effect = _make_discovery_mock(
        MOCK_DOCTYPES_RESPONSE, MOCK_DOCFIELDS_RESPONSE,
    )
    mock_fetch.return_value = {"name": "TEST", "modified": "2023-01-01"}

    config: dict[str, Any] = SAMPLE_CONFIG.copy()
    config["doctypes"] = ["Sales Invoice", "Item"]

    tap = TapErpNext(config=config)
    streams = tap.discover_streams()

    assert len(streams) == 2
    assert streams[0].name == "Sales Invoice"
    assert streams[1].name == "Item"


@patch("tap_erpnext.client.ErpNextStream._fetch_sample_record")
@patch("tap_erpnext.tap.requests.get")
def test_discover_streams_missing_doctypes_warns(mock_requests_get, mock_fetch, caplog):
    """Test that missing configured doctypes log a warning."""
    mock_requests_get.side_effect = _make_discovery_mock(
        MOCK_DOCTYPES_RESPONSE, MOCK_DOCFIELDS_RESPONSE,
    )
    mock_fetch.return_value = {"name": "TEST", "modified": "2023-01-01"}

    config: dict[str, Any] = SAMPLE_CONFIG.copy()
    config["doctypes"] = ["NonExistent", "Customer"]

    tap = TapErpNext(config=config)
    streams = tap.discover_streams()

    assert len(streams) == 1
    assert "Configured DocTypes not found" in caplog.text


# --- Child table discovery tests ---

@patch("tap_erpnext.client.ErpNextStream._fetch_sample_record")
@patch("tap_erpnext.client.ChildTableStream._fetch_sample_record")
@patch("tap_erpnext.tap.requests.get")
def test_discover_streams_with_child_tables(
    mock_requests_get, mock_child_fetch, mock_parent_fetch,
):
    """Test that child tables are discovered with parent info."""
    mock_requests_get.side_effect = _make_discovery_mock(
        MOCK_CHILD_DOCTYPES_RESPONSE, MOCK_CHILD_DOCFIELDS_RESPONSE,
    )
    mock_parent_fetch.return_value = {"name": "TEST", "modified": "2023-01-01"}
    mock_child_fetch.return_value = {"name": "TEST", "modified": "2023-01-01"}

    tap = TapErpNext(config=SAMPLE_CONFIG)
    streams = tap.discover_streams()

    # 2 parents + 2 children
    assert len(streams) == 4
    assert streams[0].name == "Journal Entry"
    assert streams[1].name == "Sales Invoice"
    assert streams[2].name == "Journal Entry Account"
    assert streams[3].name == "Sales Invoice Item"

    # Child tables have no replication key
    assert streams[2].replication_key is None
    assert streams[3].replication_key is None


@patch("tap_erpnext.client.ErpNextStream._fetch_sample_record")
@patch("tap_erpnext.client.ChildTableStream._fetch_sample_record")
@patch("tap_erpnext.tap.requests.get")
def test_discover_child_tables_with_config_filter(
    mock_requests_get, mock_child_fetch, mock_parent_fetch,
):
    """Test that config doctypes filter works with child tables."""
    mock_requests_get.side_effect = _make_discovery_mock(
        MOCK_CHILD_DOCTYPES_RESPONSE, MOCK_CHILD_DOCFIELDS_RESPONSE,
    )
    mock_parent_fetch.return_value = {"name": "TEST", "modified": "2023-01-01"}
    mock_child_fetch.return_value = {"name": "TEST", "modified": "2023-01-01"}

    config: dict[str, Any] = SAMPLE_CONFIG.copy()
    config["doctypes"] = ["Journal Entry", "Journal Entry Account"]

    tap = TapErpNext(config=config)
    streams = tap.discover_streams()

    assert len(streams) == 2
    assert streams[0].name == "Journal Entry"
    assert streams[1].name == "Journal Entry Account"


# --- Child table stream behavior ---

def test_create_child_table_stream():
    """Test that child table streams have correct attributes."""
    tap = _make_mock_tap()
    stream = create_doctype_stream(
        "Journal Entry Account",
        tap,
        is_child=True,
        parent_doctype="Journal Entry",
        parent_field="accounts",
    )

    assert stream.name == "Journal Entry Account"
    assert stream.path == "/api/resource/Journal Entry Account"
    assert stream.primary_keys == ("name",)
    assert stream.replication_key is None
    assert stream._parent_doctype == "Journal Entry"
    assert stream._parent_field == "accounts"


@patch("tap_erpnext.client.requests.get")
def test_child_table_parse_response_expands_stubs(mock_requests_get):
    """Test that ChildTableStream.parse_response fetches full records."""
    tap = _make_mock_tap()
    stream = create_doctype_stream(
        "Journal Entry Account",
        tap,
        is_child=True,
        parent_doctype="Journal Entry",
        parent_field="accounts",
    )

    # Mock the individual record fetch
    mock_record_resp = Mock()
    mock_record_resp.json.return_value = {
        "data": {
            "name": "abc123",
            "account": "Cash - KCD",
            "debit": 5000.0,
            "credit": 0.0,
            "parent": "ACC-JV-2026-00001",
            "modified": "2026-07-04 19:12:56",
        },
    }
    mock_record_resp.raise_for_status.return_value = None
    mock_requests_get.return_value = mock_record_resp

    # Simulate a list response with stubs
    mock_response = Mock()
    mock_response.json.return_value = {
        "data": [
            {"name": "abc123"},
            {"name": "def456"},
        ],
    }

    records = stream.parse_response(mock_response)
    assert len(records) == 2
    assert records[0]["account"] == "Cash - KCD"
    assert records[0]["debit"] == 5000.0
    assert records[0]["credit"] == 0.0


# --- Integration test ---

@pytest.mark.integration
def test_discover_streams_with_real_api():
    """Test DocType discovery against a real ERPNext instance."""
    api_url = os.environ.get("TAP_ERPNEXT_API_URL")
    api_key = os.environ.get("TAP_ERPNEXT_API_KEY")
    api_secret = os.environ.get("TAP_ERPNEXT_API_SECRET")

    if not all([api_url, api_key, api_secret]):
        pytest.skip("ERPNext API credentials not set in environment")

    config = {
        "api_url": api_url,
        "api_key": api_key,
        "api_secret": api_secret,
    }
    tap = TapErpNext(config=config)
    streams = tap.discover_streams()
    assert len(streams) > 0
    assert all(s.name for s in streams)