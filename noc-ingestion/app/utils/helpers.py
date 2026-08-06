import json
from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    """Returns current UTC datetime."""
    return datetime.now(timezone.utc)


def custom_json_serializer(obj: Any) -> Any:
    """Custom serializer for non-standard JSON types like datetime."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")
