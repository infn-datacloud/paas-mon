# Influxdb_message.py

from __future__ import annotations
from typing import Any, Dict, Optional
from datetime import datetime
from pydantic import BaseModel, Field, field_validator, model_validator
import logging

logger = logging.getLogger(__name__)


class InfluxdbMessage(BaseModel):
    """
    Representation of a single InfluxDB datapoint produced by the mapper.

    This class contains all logic required to:
    - validate measurement, tags and fields
    - ensure at least one valid field is present
    - normalize and validate timestamps
    - render the datapoint into:
        • InfluxDB Line Protocol
        • JSON_v2-compatible dictionary

    Attributes
    ----------
    measurement : str
        Name of the InfluxDB measurement (must be non-empty).

    tags : dict[str, str | None]
        Dictionary of tag key/value pairs. Invalid, empty, or None tag values
        are automatically removed during validation.

    fields : dict[str, Any]
        Dictionary of field key/value pairs. Fields must contain at least one
        valid (non-None) entry after validation.

    timestamp_ns : int | None
        Timestamp expressed in nanoseconds since Unix epoch. If omitted,
        InfluxDB will apply its server time.
    """

    measurement: str
    tags: Dict[str, Optional[str]] = Field(default_factory=dict)
    fields: Dict[str, Optional[Any]] = Field(default_factory=dict)
    timestamp_ns: Optional[int] = None

    # ================================================================
    #                     TIMESTAMP PARSING
    # ================================================================
    @staticmethod
    def parse_timestamp_ns(timestamp_str: str) -> int:
        """
        Convert an ISO-8601 timestamp string into an epoch timestamp in
        nanoseconds.

        Supports:
        - trailing 'Z' (converted to UTC +00:00)
        - explicit timezone offsets (+HH:MM / -HH:MM)
        """
        if timestamp_str.endswith("Z"):
            timestamp_str = timestamp_str[:-1] + "+00:00"

        dt = datetime.fromisoformat(timestamp_str)
        epoch_seconds = dt.timestamp()
        return int(epoch_seconds * 1_000_000_000)

    # ================================================================
    #                     ESCAPE HELPERS
    # ================================================================
    @staticmethod
    def escape_measurement(measurement_name: str) -> str:
        """Escape measurement name according to LP rules."""
        return (
            measurement_name.replace(",", r"\,")
                            .replace(" ", r"\ ")
        )

    @staticmethod
    def escape_tag_key(tag_key: str) -> str:
        """Escape tag key according to LP rules."""
        return (
            tag_key.replace(",", r"\,")
                   .replace(" ", r"\ ")
                   .replace("=", r"\=")
        )

    @staticmethod
    def escape_tag_value(tag_value: str) -> str:
        """Escape tag value according to LP rules."""
        return (
            str(tag_value)
                .replace(",", r"\,")
                .replace(" ", r"\ ")
                .replace("=", r"\=")
        )

    @staticmethod
    def escape_field_key(field_key: str) -> str:
        """Escape field key according to LP rules."""
        return (
            field_key.replace(",", r"\,")
                     .replace(" ", r"\ ")
                     .replace("=", r"\=")
        )

    @staticmethod
    def format_field_value(value: Any) -> str:
        """
        Convert a Python value into a valid Line Protocol field representation.

        Rules:
        - bool  → "true"/"false"
        - int   → "<value>i"
        - float → standard string representation
        - str   → escaped and wrapped with quotes
        """
        if isinstance(value, bool):
            return "true" if value else "false"

        if isinstance(value, int) and not isinstance(value, bool):
            return f"{value}i"

        if isinstance(value, float):
            return repr(value)

        # Strings: escape and sanitize control characters
        string_value = str(value)
        string_value = string_value.replace("\\", r"\\").replace('"', r'\"')
        string_value = string_value.replace("\n", " ").replace("\r", " ")
        return f"\"{string_value}\""

    # ================================================================
    #                     VALIDATORS
    # ================================================================
    @field_validator("measurement", mode="before")
    @classmethod
    def validate_measurement_name(cls, measurement_name_raw: str):
        """
        Ensure measurement name is a clean, non-empty string.
        """
        measurement_clean = str(measurement_name_raw).strip()
        if not measurement_clean:
            raise ValueError("Measurement name must be a non-empty string.")
        return measurement_clean

    @field_validator("tags", mode="before")
    @classmethod
    def validate_tags(cls, tags_raw: Dict[str, Any]):
        """
        Ensure tag keys/values are strings and remove None or empty entries.
        """
        cleaned_tags: Dict[str, str] = {}
        for key, value in (tags_raw or {}).items():
            if value is None or value == "":
                continue
            cleaned_tags[str(key)] = str(value)
        return cleaned_tags

    @field_validator("fields", mode="before")
    @classmethod
    def validate_fields(cls, fields_raw: Dict[str, Any]):
        """
        Clean and normalize all field values:
        - remove None entries
        - normalize int/float/bool
        - convert other values to sanitized strings
        """
        cleaned_fields: Dict[str, Any] = {}
        for key, value in (fields_raw or {}).items():
            if value is None:
                continue
            field_key = str(key)

            if isinstance(value, bool):
                cleaned_fields[field_key] = value
            elif isinstance(value, int):
                cleaned_fields[field_key] = int(value)
            elif isinstance(value, float):
                cleaned_fields[field_key] = float(value)
            else:
                string_value = str(value).replace("\n", " ").replace("\r", " ")
                cleaned_fields[field_key] = string_value

        return cleaned_fields

    @field_validator("timestamp_ns", mode="before")
    @classmethod
    def warn_if_missing_timestamp(cls, timestamp_ns_raw):
        """
        Warn when no timestamp is provided. InfluxDB will auto-assign server time.
        """
        if timestamp_ns_raw is None:
            logger.warning(
                "InfluxdbMessage created without 'timestamp_ns'; "
                "InfluxDB server time will be assigned."
            )
        return timestamp_ns_raw

    @model_validator(mode="after")
    def ensure_at_least_one_field(self):
        """
        Ensure that at least one field is present.
        Line Protocol does not allow empty field sets.
        """
        if not self.fields:
            raise ValueError("InfluxdbMessage requires at least one field.")
        return self

    # ================================================================
    #                        CONVERSION METHODS
    # ================================================================
    def to_line_protocol(self) -> str:
        """
        Convert the message into a valid InfluxDB Line Protocol string:
            <measurement>[,<tag-set>] <field-set> [timestamp]

        Returns:
            str: LP-formatted representation.
        """
        escaped_measurement = self.escape_measurement(self.measurement)

        # TAG SET
        tag_components = []
        for tag_key, tag_value in sorted(self.tags.items()):
            if tag_value is None or tag_value == "":
                continue
            escaped_key = self.escape_tag_key(tag_key)
            escaped_value = self.escape_tag_value(tag_value)
            tag_components.append(f"{escaped_key}={escaped_value}")

        tags_str = ",".join(tag_components)

        # FIELD SET
        field_components = []
        for field_key, field_value in sorted(self.fields.items()):
            if field_value is None:
                continue
            escaped_key = self.escape_field_key(field_key)
            formatted_value = self.format_field_value(field_value)
            field_components.append(f"{escaped_key}={formatted_value}")

        fields_str = ",".join(field_components)

        if not field_components:
            raise ValueError("Cannot generate Line Protocol: no valid fields present.")
        
        # BASE LINE (measurement + tags + fields)
        if tags_str:
            line = f"{escaped_measurement},{tags_str} {fields_str}"
        else:
            line = f"{escaped_measurement} {fields_str}"

        # TIMESTAMP
        if self.timestamp_ns is not None:
            line = f"{line} {self.timestamp_ns}"

        return line

    def to_json(self) -> Dict[str, Any]:
        """
        Convert the message into a JSON_v2-compatible dictionary,
        suitable for ingestion through the Telegraf JSON input plugin.
        """
        output: Dict[str, Any] = {
            "measurement": self.measurement,
            "tags": {
                key: value
                for key, value in self.tags.items()
                if value is not None and value != ""
            },
            "fields": {
                key: value
                for key, value in self.fields.items()
                if value is not None
            },
        }
        
        if not output["fields"]:
            raise ValueError("Cannot generate JSON: no valid fields present.")

        if self.timestamp_ns is not None:
            output["timestamp"] = self.timestamp_ns

        return output
