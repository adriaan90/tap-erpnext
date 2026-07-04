"""Stream type classes for tap-erpnext."""

from __future__ import annotations

from tap_erpnext.client import ErpNextStream


def create_doctype_stream(doctype: str, tap) -> ErpNextStream:
    """Factory: create a stream instance for a specific DocType.

    Uses `type()` to create a dynamic subclass with `name` as a class attribute,
    which is required by the Singer SDK.

    Schema discovery is handled dynamically by ErpNextStream — on first access
    it fetches a sample record from the API and infers field names and types.

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
            "__doc__": f"ERPNext {doctype} stream.",
        },
    )

    return stream_class(tap)
