# msg_v13_model.py
from __future__ import annotations
from typing import Any, Optional
from pydantic import BaseModel, Field, ValidationError, field_validator
import logging
import json

from modules.feedermonitoring.Influxdb_message import (
    parse_ts_ns,
    InfluxdbMessage,
)

logger = logging.getLogger(__name__)

"""
Schema definitions and mapping logic for versioned feeder messages (v1.3.x).
Builds InfluxdbMessage objects from structured JSON data.
"""

# ==========================
#   Common Pydantic config
# ==========================

class BaseCfg(BaseModel):
    """Base configuration for tolerant JSON parsing."""
    model_config = dict(
        extra="ignore",
        validate_assignment=True,
        populate_by_name=True,
    )


# ==========================
#   Shared Schemas
# ==========================

class Quota(BaseCfg):
    """
    Generic service quota/usage structure.

    Examples of per-service fields:
      - compute: cores, instances, ram
      - block storage: gigabytes, volumes
      - network: networks, ports, public_ips, security_group_rules, security_groups

    Attributes
    ----------
    usage : bool
        True for usage counters, False for quota limits.
    
    Tags
    ----------  
    project: str
        Project identifier
    """
    usage: bool
    project: Optional[str] = None
    
    # compute
    cores: Optional[int] = None
    instances: Optional[int] = None
    ram: Optional[int] = None
    
    # block storage
    gigabytes: Optional[int] = None
    volumes: Optional[int] = None
    
    # network
    networks: Optional[int] = None
    ports: Optional[int] = None
    public_ips: Optional[int] = None
    security_group_rules: Optional[int] = None
    security_groups: Optional[int] = None


class ServiceBase(BaseCfg):
    """
    Base type for services. In the input JSON, each service key maps to a
    single-element list containing the service object with `type` and `quotas`.
    """
    type: Optional[str] = None
    quotas: list[Quota] = Field(default_factory=list)

    def find_quota(self, usage_flag: bool) -> Optional[Quota]:
        """
        Return the first quota object matching the requested usage flag.

        Parameters
        ----------
        usage_flag : bool
            True to get usage values, False to get quota limits.

        Returns
        -------
        Optional[Quota]
            The quota/usage entry if present, otherwise None.
        """
        for q in self.quotas:
            if q.usage == usage_flag:
                return q
        return None


class BlockStorageService(ServiceBase):
    """Block storage service data."""
    pass


class ComputeService(ServiceBase):
    """Compute service data."""
    pass


class NetworkService(ServiceBase):
    """Network service data."""
    pass


class ObjectStoreService(ServiceBase):
    """Object storage service data (placeholder for future extensions)."""
    pass



# ==========================
#  Message model v1.3.x
# ==========================

class MsgV13(BaseCfg):
    """Message schema for version 1.3.x payloads."""

    # common metadata
    provider_name: str | None = None
    provider_type: str | None = None
    region_name: str | None = None
    user_group: str | None = None
    
    timestamp: str

    # general_info-only fields
    overbooking_cpu: Optional[float] = None
    overbooking_ram: Optional[float] = None
    bandwidth_in: Optional[float] = None
    bandwidth_out: Optional[float] = None

    # Services (each is a key with an associated single-element list in the source message)
    block_storage_services: list[BlockStorageService] = Field(default_factory=list)
    compute_services: list[ComputeService] = Field(default_factory=list)
    network_services: list[NetworkService] = Field(default_factory=list)
    object_store_services: list[ObjectStoreService] = Field(default_factory=list)

    @field_validator("timestamp", mode="before")
    @classmethod
    def _normalize_timestamp(cls, v: str) -> str:
        """
        Normalize timestamp to an ISO 8601 string with explicit UTC if missing.

        Accepts:
          - '...' ending with 'Z'
          - '...+HH:MM'/'...-HH:MM'
          - no offset -> appends 'Z' (assume UTC)
        """
        s = str(v).strip()
        
        # Unify possible space-separated date/time into ISO 'T'.
        # Example: "2025-10-03 10:30:00" -> "2025-10-03T10:30:00"
        if " " in s and "T" not in s:
            s = s.replace(" ", "T", 1)
        
        # If the string already ends with 'Z' (Zulu/UTC time), return as is. 
        if s.endswith("Z"):
            return s
            
        # Check for explicit offset (+HH:MM or -HH:MM) which satisfies ISO 8601.
        # We check the 6th position from the end for '+' or '-', and the 3rd for ':'.
        # Example: "2025-10-03T07:05:33+01:00"
        if len(s) >= 6 and (s[-6] in "+-") and s[-3] == ":":
            return s
        # If no timezone offset is found, append 'Z' to assume UTC.
        return s + "Z"

    @property
    def ts_ns(self) -> int:
        """Timestamp as epoch nanoseconds (UTC).""" 
        return parse_ts_ns(self.timestamp)

    @property
    def common_tags(self) -> dict[str, str]:
        """
        Common tags derived from message metadata. Empty strings are used for missing values.
        """
        return {
            "provider_name": self.provider_name or "",
            "provider_type": self.provider_type or "",
            "region_name": self.region_name or "",
            "user_group": self.user_group or "",
        }

    # ---------- Builders ----------

    def _build_general_info(self) -> Optional[InfluxdbMessage]:
        """
        Build the 'general_info' measurement if any field is present.
        """
        fields = {}
        if self.overbooking_cpu is not None:
            fields["overbooking_cpu"] = float(self.overbooking_cpu)
        if self.overbooking_ram is not None:
            fields["overbooking_ram"] = float(self.overbooking_ram)
        if self.bandwidth_in is not None:
            fields["bandwidth_in"] = float(self.bandwidth_in)
        if self.bandwidth_out is not None:
            fields["bandwidth_out"] = float(self.bandwidth_out)
        
        if not fields:
            return None
        
        return InfluxdbMessage(
            measurement="general_info",
            tags=self.common_tags,
            fields=fields,
            timestamp_ns=self.ts_ns,
        )

    def _build_compute(self) -> Optional[InfluxdbMessage]:
        """
        Build the 'compute_services' measurement.
        """
        if not self.compute_services:
            return None
        
        c0 = self.compute_services[0]
        tags = self.common_tags | {"type": c0.type or ""}
        
        q_quota, q_usage = c0.find_quota(False), c0.find_quota(True)
        
        fields: dict[str, Any] = {}
        if q_quota:
            if q_quota.project:
                tags["project_id"] = q_quota.project
            if q_quota.cores is not None:
                fields["cores_quota"] = q_quota.cores
            if q_quota.instances is not None:
                fields["instances_quota"] = q_quota.instances
            if q_quota.ram is not None:
                fields["ram_quota"] = q_quota.ram
        
        if q_usage:
            if q_usage.cores is not None:
                fields["cores_usage"] = q_usage.cores
            if q_usage.instances is not None:
                fields["instances_usage"] = q_usage.instances
            if q_usage.ram is not None:
                fields["ram_usage"] = q_usage.ram
        
        if not fields:
            return None
        
        return InfluxdbMessage(
            measurement="compute_services",
            tags=tags,
            fields=fields,
            timestamp_ns=self.ts_ns,
        )

    def _build_block_storage(self) -> Optional[InfluxdbMessage]:
        """
        Build the 'block_storage_services' measurement.
        """
        if not self.block_storage_services:
            return None
        b0 = self.block_storage_services[0]
        tags = self.common_tags | {"type": b0.type or ""}
        
        q_quota, q_usage = b0.find_quota(False), b0.find_quota(True)
        
        fields: dict[str, Any] = {}
        if q_quota:
            if q_quota.project:
                tags["project_id"] = q_quota.project
            if q_quota.gigabytes is not None:
                fields["storage_gb_quota"] = q_quota.gigabytes
            if q_quota.volumes is not None:
                fields["volume_quota"] = q_quota.volumes
        
        if q_usage:
            if q_usage.gigabytes is not None:
                fields["storage_gb_usage"] = q_usage.gigabytes
            if q_usage.volumes is not None:
                fields["volume_usage"] = q_usage.volumes
        
        if not fields:
            return None
        
        return InfluxdbMessage(
            measurement="block_storage_services",
            tags=tags,
            fields=fields,
            timestamp_ns=self.ts_ns,
        )

    def _build_network(self) -> Optional[InfluxdbMessage]:
        """
        Build the 'network_services' measurement.
        """
        if not self.network_services:
            return None

        n0 = self.network_services[0]
        tags = self.common_tags | {"type": n0.type or ""}

        q_quota, q_usage = n0.find_quota(False), n0.find_quota(True)

        fields: dict[str, Any] = {}

        if q_quota:
            if q_quota.project:
                tags["project_id"] = q_quota.project
            if q_quota.networks is not None:
                fields["networks_quota"] = q_quota.networks
            if q_quota.ports is not None:
                fields["ports_quota"] = q_quota.ports
            if q_quota.public_ips is not None:
                fields["public_ips_quota"] = q_quota.public_ips
            if q_quota.security_group_rules is not None:
                fields["security_group_rules_quota"] = q_quota.security_group_rules
            if q_quota.security_groups is not None:
                fields["security_groups_quota"] = q_quota.security_groups

        if q_usage:
            if q_usage.networks is not None:
                fields["networks_usage"] = q_usage.networks
            if q_usage.ports is not None:
                fields["ports_usage"] = q_usage.ports
            if q_usage.public_ips is not None:
                fields["public_ips_usage"] = q_usage.public_ips
            if q_usage.security_group_rules is not None:
                fields["security_group_rules_usage"] = q_usage.security_group_rules
            if q_usage.security_groups is not None:
                fields["security_groups_usage"] = q_usage.security_groups

        if not fields:
            return None

        return InfluxdbMessage(
            measurement="network_services",
            tags=tags,
            fields=fields,
            timestamp_ns=self.ts_ns
        )

    def _build_object_store(self) -> Optional[InfluxdbMessage]:
        """
        Build the 'object_store_services' measurement.

        Notes
        -----
        Currently there are no object-store fields defined; this is a placeholder
        for future extensions. Returns None unless fields are populated.
        """
        if not self.object_store_services:
            return None
        
        o0 = self.object_store_services[0]
        tags = self.common_tags | {"type": o0.type or ""}
        
        fields: dict[str, Any] = {}
        if not fields:
            return None
        return InfluxdbMessage(
            measurement="object_store_services",
            tags=tags,
            fields=fields,
            timestamp_ns=self.ts_ns,
        )

    def to_influxdb_messages(self) -> list[InfluxdbMessage]:
        """
        Build all available measurement messages from this v1.3 model.

        Returns
        -------
        list[InfluxdbMessage]
            Non-empty list of messages for the present sections.
        """
        msgs: list[InfluxdbMessage] = []
        for builder in [
            self._build_general_info,
            self._build_block_storage,
            self._build_compute,
            self._build_network,
            self._build_object_store,
        ]:
            msg = builder()
            if msg:
                msgs.append(msg)
        return msgs

    def to_lp_format(self) -> list[str]:
        """
        Render all available measurements as Line Protocol strings.

        Returns
        -------
        list[str]
        """
        return [msg.to_line_protocol() for msg in self.to_influxdb_messages()]

    def to_json_format(self) -> list[dict[str, Any]]:
        """
        Render all available measurements as JSON dictionaries.

        Returns
        -------
        list[dict[str, Any]]
        """
        return [msg.to_json() for msg in self.to_influxdb_messages()]


# ==========================
#  Version handling
# ==========================

class Envelope(BaseCfg):
    """
    Envelope schema carrying the `msg_version` discriminator.
    """
    msg_version: str

    @field_validator("msg_version", mode="before")
    @classmethod
    def _msg_version_to_str(cls, v):
        return str(v).strip()


def json_to_lines(msg: dict[str, Any], output_format: str = "influx") -> list[str | dict]:
    """
    Convert a versioned JSON message into InfluxDB-friendly output.

    Parameters
    ----------
    msg : dict[str, Any]
        Input JSON message.
    output_format : {"influx", "json_v2"}, default "influx"
        Output format selector.

    Returns
    -------
    list[str | dict]
        A list of Line Protocol strings (for "influx") or
        a list of JSON dicts (for "json_v2"). Returns an empty list on validation errors.

    Logging
    -------
    - ERROR: envelope or model validation fails.
    - WARNING: unsupported `msg_version`, v1.3.x parser is used as a fallback.
    """
    try:
        env = Envelope.model_validate(msg)
    except ValidationError as e:
        logger.error("Envelope validation failed: %s", e)
        return []
    
    ver = env.msg_version.strip()
    if not ver.startswith("1.3"):
        logger.warning("Unsupported msg_version '%s'. Using v1.3.x parser as fallback.", ver)
    
    try:
        model = MsgV13.model_validate(msg)
    except ValidationError as e:
        logger.error("MsgV13 parsing failed: %s", e)
        return []
    
    return model.to_json_format() if output_format == "json_v2" else model.to_lp_format()


if __name__ == "__main__":
    import json
    
    # Example usage:
    # Requires a local 'example_message.json' in the working directory.
    with open("example_message.json") as f:
        data = json.load(f)
    
    # Output Line Protocol (default)
    lines = json_to_lines(data)
    with open("output.lp", "w") as f:
        for line in lines:
            f.write(line + "\n")
    
    # Output JSON
    json_msgs = json_to_lines(data, output_format="json_v2")
    with open("output.json", "w") as f:
        json.dump(json_msgs, f, indent=2)

