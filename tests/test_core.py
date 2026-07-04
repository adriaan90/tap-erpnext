"""Tests for tap-erpnext."""

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
    tap.config = {"limit_page_length": 200}
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
        {"name": "Sales Invoice"},
        {"name": "Customer"},
        {"name": "Item"},
    ]
}

MOCK_STREAM_DATA_RESPONSE = {
    "data": [
        {"name": "INV-001", "modified": "2023-01-01 10:00:00"},
    ]
}


def test_create_doctype_stream():
    """Test that the factory creates streams with correct attributes."""
    tap = _make_mock_tap()
    stream = create_doctype_stream("Sales Invoice", tap)

    assert stream.name == "Sales Invoice"
    assert stream.path == "/api/resource/Sales Invoice"
    assert stream.primary_keys == ("name",)
    assert stream.replication_key == "modified"
    # Schema is lazy — not fetched until first access
    assert stream._schema is None


def test_doctype_stream_schema_infers_from_sample():
    """Test that the schema is dynamically inferred from a sample record."""
    tap = _make_mock_tap()
    stream = create_doctype_stream("Item", tap)

    # Simulate what _fetch_sample_record would return
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
    # No additionalProperties — all fields are explicitly declared
    assert "additionalProperties" not in schema


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

    sample = {
        "name": "TEST",
        "maybe_null": None,
    }
    schema = ErpNextStream._infer_schema_from_record(sample)
    assert "maybe_null" in schema["properties"]
    assert schema["properties"]["maybe_null"]["type"] == ["string", "null"]


def test_create_doctype_stream_with_spaces():
    """Test that DocType names with spaces are handled correctly."""
    tap = _make_mock_tap()
    stream = create_doctype_stream("Sales Invoice", tap)
    assert stream.name == "Sales Invoice"
    assert "Sales_Invoice" in stream.__class__.__name__


@patch("tap_erpnext.client.ErpNextStream._fetch_sample_record")
@patch("tap_erpnext.tap.requests.get")
def test_discover_streams(mock_tap_get, mock_fetch):
    """Test DocType discovery."""
    # Mock the DocType discovery call
    mock_tap_response = Mock()
    mock_tap_response.json.return_value = MOCK_DOCTYPES_RESPONSE
    mock_tap_response.raise_for_status.return_value = None
    mock_tap_get.return_value = mock_tap_response

    # Mock schema discovery — return a valid sample record
    mock_fetch.return_value = {"name": "TEST", "modified": "2023-01-01"}

    tap = TapErpNext(config=SAMPLE_CONFIG)
    streams = tap.discover_streams()

    assert len(streams) == 3
    assert streams[0].name == "Sales Invoice"
    assert streams[1].name == "Customer"
    assert streams[2].name == "Item"


@patch("tap_erpnext.client.ErpNextStream._fetch_sample_record")
@patch("tap_erpnext.tap.requests.get")
def test_discover_streams_with_config_doctypes(mock_tap_get, mock_fetch):
    """Test DocType discovery with configured doctypes filter."""
    mock_tap_response = Mock()
    mock_tap_response.json.return_value = MOCK_DOCTYPES_RESPONSE
    mock_tap_response.raise_for_status.return_value = None
    mock_tap_get.return_value = mock_tap_response

    mock_fetch.return_value = {"name": "TEST", "modified": "2023-01-01"}

    config = SAMPLE_CONFIG.copy()
    config["doctypes"] = ["Sales Invoice", "Item"]

    tap = TapErpNext(config=config)
    streams = tap.discover_streams()

    assert len(streams) == 2
    assert streams[0].name == "Sales Invoice"
    assert streams[1].name == "Item"


@patch("tap_erpnext.client.ErpNextStream._fetch_sample_record")
@patch("tap_erpnext.tap.requests.get")
def test_discover_streams_missing_doctypes_warns(mock_tap_get, mock_fetch, caplog):
    """Test that missing configured doctypes log a warning."""
    mock_tap_response = Mock()
    mock_tap_response.json.return_value = MOCK_DOCTYPES_RESPONSE
    mock_tap_response.raise_for_status.return_value = None
    mock_tap_get.return_value = mock_tap_response

    mock_fetch.return_value = {"name": "TEST", "modified": "2023-01-01"}

    config = SAMPLE_CONFIG.copy()
    config["doctypes"] = ["NonExistent", "Customer"]

    tap = TapErpNext(config=config)
    streams = tap.discover_streams()

    assert len(streams) == 1
    assert "Configured DocTypes not found" in caplog.text


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
