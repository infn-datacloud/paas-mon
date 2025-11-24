from __future__ import annotations
from typing import Any
from datetime import datetime
from pydantic import BaseModel, Field, field_validator, model_validator
import logging

logger = logging.getLogger(__name__)

"""
Utilities to parse timestamps, escape values for InfluxDB Line Protocol, and
map a versioned JSON message (v1.3.x) into InfluxDB-friendly payloads.

Main entry point: `json_to_lines(msg, output_format="line_protocol" | "json")`.
"""


# ==========================
#  Timestamp & Line Protocol
# ==========================

def parse_ts_ns(ts_str: str) -> int:
    """
    Convert an ISO 8601 timestamp string to a Unix epoch timestamp in nanoseconds (UTC).

    If the input ends with 'Z', it is treated as UTC and normalized to '+00:00'
    for parsing with `datetime.fromisoformat`.

    Parameters
    ----------
    ts_str : str
        Timestamp string in ISO 8601 format, e.g. '2025-10-03T07:05:33+00:00'
        or '2025-10-03T07:05:33Z'.

    Returns
    -------
    int
        Epoch time in nanoseconds since 1970-01-01T00:00:00Z.
    """
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts_str)
    return int(dt.timestamp() * 1_000_000_000)

def escape_measurement(m: str) -> str:
    """
    Escape an InfluxDB measurement name according to InfluxDB LP rules.
    """
    return m.replace(",", r"\,").replace(" ", r"\ ")

def escape_tag_key(k: str) -> str:
    """
    Escape an InfluxDB tag key according to InfluxDB LP rules.
    """
    return k.replace(",", r"\,").replace(" ", r"\ ").replace("=", r"\=")

def escape_tag_val(v: str) -> str:
    """
    Escape an InfluxDB tag value according to InfluxDB LP rules.

    Notes
    -----
    Tag values are always strings in InfluxDB Line Protocol.
    """
    return str(v).replace(",", r"\,").replace(" ", r"\ ").replace("=", r"\=")

def escape_field_key(k: str) -> str:
    """
    Escape an InfluxDB field key according to InfluxDB LP rules.
    """
    return k.replace(",", r"\,").replace(" ", r"\ ").replace("=", r"\=")

def format_field_val(v):
    """
    Format a Python value as an InfluxDB field value.

    Booleans are formatted as `true`/`false`.
    Integers are suffixed with `i`.
    Floats use `repr` for stable representation.
    Strings are quoted and escaped for LP; newlines and carriage returns are replaced by spaces.

    Parameters
    ----------
    v : Any
        Value to format.

    Returns
    -------
    str
        Value formatted per InfluxDB LP field rules.
    """
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int) and not isinstance(v, bool):
        return f"{v}i"
    if isinstance(v, float):
        return repr(v)
    s = str(v).replace("\\", r"\\").replace('"', r'\"')
    s = s.replace("\n", " ").replace("\r", " ")
    return f"\"{s}\""

# ===================
# InfluxDB message
# ===================

class InfluxdbMessage(BaseModel):
    """
    Container for a single InfluxDB data point.

    Parameters
    ----------
    measurement : str
        Measurement name (non-empty).
    tags : dict[str, str | None], optional
        Tag set; keys and values are sanitized and empty values are dropped.
    fields : dict[str, bool | int | float | str | None]
        Field set; `None` values are dropped. At least one field is required.
    timestamp_ns : int | None, optional
        Explicit timestamp in nanoseconds since epoch. If omitted, InfluxDB will assign
        the server-side timestamp at write time.

    Notes
    -----
    Validation:
      - `measurement` must be non-empty.
      - `fields` must contain at least one non-None entry.
      - `tags` omit `None` and empty-string values.
      - Strings in `fields` are normalized to single-line.
    """

    measurement: str
    tags: dict[str, str | None] = Field(default_factory=dict)
    fields: dict[str, bool | int | float | str | None] = Field(default_factory=dict)
    timestamp_ns: int | None = None

    @field_validator("measurement", mode="before")
    @classmethod
    def _meas_nonempty(cls, v):
        s = str(v).strip()
        if not s:
            raise ValueError("Measurement name must be a non-empty string.")
        return s

    @field_validator("tags", mode="before")
    @classmethod
    def _coerce_tags(cls, v: dict[str, Any]):
        out: dict[str, str] = {}
        for k, val in (v or {}).items():
            if val is None or val == "":
                continue 
            out[str(k)] = str(val)
        return out

    @field_validator("fields", mode="before")
    @classmethod
    def _coerce_fields(cls, v: dict[str, Any]):
        out: dict[str, bool | int | float | str] = {}
        for k, val in (v or {}).items():
            if val is None:
                continue
            if isinstance(val, bool):
                out[str(k)] = val
            elif isinstance(val, int):
                out[str(k)] = int(val)
            elif isinstance(val, float):
                out[str(k)] = float(val)
            else:
                s = str(val).replace("\n", " ").replace("\r", " ")
                out[str(k)] = s
        return out
        
    @field_validator("timestamp_ns", mode="before")
    @classmethod
    def _log_if_timestamp_missing(cls, v):
        if v is None:
            logger.warning("InfluxdbMessage created without 'timestamp_ns'; InfluxDB server time will be used.")
        return v
        
    @model_validator(mode="after")
    def _must_have_at_least_one_field(self):
        if not self.fields:
            raise ValueError("InfluxdbMessage requires at least one field.")
        return self
    
    def to_line_protocol(self) -> str:
        """
        Render the object as a single InfluxDB Line Protocol string.

        Returns
        -------
        str
            Line Protocol string.
        """
        m = escape_measurement(self.measurement)
        
        tag_part = ",".join(
            f"{escape_tag_key(k)}={escape_tag_val(v)}"
            for k, v in sorted(self.tags.items())
            if v is not None and v != ""
        )
        
        field_part = ",".join(
            f"{escape_field_key(k)}={format_field_val(v)}"
            for k, v in sorted(self.fields.items())
            if v is not None
        )
        
        if tag_part:
            base = f"{m},{tag_part} {field_part}"
        else:
            base = f"{m} {field_part}"
        
        if self.timestamp_ns is not None:
            return f"{base} {self.timestamp_ns}"
        return base
    
    def to_json(self) -> dict[str, Any]:
        """
        Render the object as a JSON-friendly dictionary.

        Returns
        -------
        dict[str, Any]
            Dictionary with measurement, tags, fields, and optional timestamp.
        """
        result = {
            "measurement": self.measurement,
            "tags": {k: v for k, v in self.tags.items() if v is not None and v != ""},
            "fields": {k: v for k, v in self.fields.items() if v is not None},
        }
        if self.timestamp_ns is not None:
            result["timestamp"] = self.timestamp_ns
        return result
        
