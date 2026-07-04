"""Stream type classes for tap-erpnext."""

from __future__ import annotations

from tap_erpnext.client import ChildTableStream, ErpNextStream


def create_doctype_stream(
    doctype: str,
    tap,
    is_child: bool = False,
    parent_doctype: str | None = None,
    parent_field: str | None = None,
) -> ErpNextStream:
    """Factory: create a stream instance for a specific DocType.

    Uses `type()` to create a dynamic subclass with `name` as a class attribute,
    which is required by the Singer SDK.

    Schema discovery is handled dynamically by ErpNextStream — on first access
    it fetches a sample record from the API and infers field names and types.

    Args:
        doctype: The ERPNext DocType name (e.g., "Sales Invoice").
        tap: The TapErpNext instance.
        is_child: Whether this is a child table (istable=1).
        parent_doctype: For child tables, the parent DocType name.
        parent_field: For child tables, the field name on the parent.

    Returns:
        An instance of a dynamically-created ErpNextStream subclass.
    """
    # Sanitize class name (Python identifiers can't have spaces)
    safe_name = doctype.replace(" ", "_").replace("-", "_")
    class_name = f"ErpNext_{safe_name}"

    # Choose base class based on whether this is a child table
    base_class = ChildTableStream if is_child else ErpNextStream

    # Class attributes
    class_attrs: dict = {
        "name": doctype,
        "path": f"/api/resource/{doctype}",
        "primary_keys": ("name",),
        "__doc__": f"ERPNext {doctype} stream.",
    }

    # Only parent streams have a replication key — child tables
    # are synced via full refresh (list → individual fetches).
    if not is_child:
        class_attrs["replication_key"] = "modified"

    # Dynamically create a new class for this DocType
    stream_class = type(
        class_name,
        (base_class,),
        class_attrs,
    )

    # Pass parent info for child tables
    if is_child:
        return stream_class(
            tap,
            parent_doctype=parent_doctype,
            parent_field=parent_field,
        )

    return stream_class(tap)
