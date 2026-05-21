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
    assert stream.schema["additionalProperties"] is True


def test_doctype_stream_schema_allows_additional_properties():
    """Test that the dynamic schema allows all ERPNext fields."""
    tap = _make_mock_tap()
    stream = create_doctype_stream("Item", tap)
    schema = stream.schema

    assert schema["type"] == "object"
    assert "name" in schema["properties"]
    assert "modified" in schema["properties"]
    assert schema["additionalProperties"] is True


def test_create_doctype_stream_with_spaces():
    """Test that DocType names with spaces are handled correctly."""
    tap = _make_mock_tap()
    stream = create_doctype_stream("Sales Invoice", tap)
    assert stream.name == "Sales Invoice"
    assert "Sales_Invoice" in stream.__class__.__name__


@patch("tap_erpnext.tap.requests.get")
def test_discover_streams(mock_get):
    """Test DocType discovery."""
    mock_response = Mock()
    mock_response.json.return_value = MOCK_DOCTYPES_RESPONSE
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    tap = TapErpNext(config=SAMPLE_CONFIG)
    streams = tap.discover_streams()

    assert len(streams) == 3
    assert streams[0].name == "Sales Invoice"
    assert streams[1].name == "Customer"
    assert streams[2].name == "Item"


@patch("tap_erpnext.tap.requests.get")
def test_discover_streams_with_config_doctypes(mock_get):
    """Test DocType discovery with configured doctypes filter."""
    mock_response = Mock()
    mock_response.json.return_value = MOCK_DOCTYPES_RESPONSE
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    config = SAMPLE_CONFIG.copy()
    config["doctypes"] = ["Sales Invoice", "Item"]

    tap = TapErpNext(config=config)
    streams = tap.discover_streams()

    assert len(streams) == 2
    assert streams[0].name == "Sales Invoice"
    assert streams[1].name == "Item"


@patch("tap_erpnext.tap.requests.get")
def test_discover_streams_missing_doctypes_warns(mock_get, caplog):
    """Test that missing configured doctypes log a warning."""
    mock_response = Mock()
    mock_response.json.return_value = MOCK_DOCTYPES_RESPONSE
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

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
