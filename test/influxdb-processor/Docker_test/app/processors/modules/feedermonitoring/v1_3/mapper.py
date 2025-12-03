# mapper.py — FeederMessage → InfluxdbMessage conversion
from __future__ import annotations
from typing import Any, Dict, List, Optional

import modules.feedermonitoring.v1_3.config as cfg
from modules.feedermonitoring.v1_3.parser import parse_feeder_message
from modules.feedermonitoring.Influxdb_message import InfluxdbMessage
from modules.feedermonitoring.v1_3.model import FeederMessage, QuotaModel, GenericServiceModel

import logging
logger = logging.getLogger(__name__)


class Mapper:
    """
    Converter for feeder v1.3.x messages → InfluxDB-compatible structures.

    This class encapsulates all logic required to transform a validated
    FeederMessage into one or more InfluxdbMessage objects. The output is
    suitable for ingestion via InfluxDB Line Protocol or JSON_v2.

    Responsibilities:
    -----------------
    - Validate raw JSON using the parser
    - Convert:
        • general_info → InfluxdbMessage
        • each service block → InfluxdbMessage
    - Build quota/usage fields dynamically (based on config.py)
    - Support multiple output formats:
        • influx  → .to_line_protocol()
        • json_v2 → .to_json()

    The class is intentionally stateless, making it easy to test and reuse
    within the Processor component.
    """

    # =====================================================================
    #                           PUBLIC API
    # =====================================================================

    def map_feeder_influxdb(self, json_msg: Dict[str, Any], output_format: str = "influx"):
        """
        Validate raw feeder JSON and produce InfluxDB-ready structures.

        Workflow:
        1. JSON → FeederMessage (via parser)
        2. FeederMessage → list[InfluxdbMessage]
        3. Convert each InfluxdbMessage to the requested output format:
           - "influx"  → Line Protocol strings
           - "json_v2" → JSON-v2 dictionaries

        Args:
            json_msg (dict): Raw feeder message.
            output_format (str): Output encoding ("influx" | "json_v2").

        Returns:
            list[str] or list[dict]: Encoded InfluxDB messages.
        """
        try:
            feeder_msg = parse_feeder_message(json_msg)
        except Exception as e:
            logger.error("Failed to parse feeder message: %s", e)
            raise

        influx_msgs = self.map_json_lp(feeder_msg)

        if output_format == "influx":
            return [msgs.to_line_protocol() for msgs in influx_msgs]

        if output_format == "json_v2":
            return [msgs.to_json() for msgs in influx_msgs]

        raise ValueError(f"Unsupported output_format '{output_format}'")

    def map_json_lp(self, msg: FeederMessage) -> List[InfluxdbMessage]:
        """
        Convert a validated FeederMessage into a list of InfluxdbMessage objects.

        Steps:
        - Map the general_info block (if present)
        - Map each service block
        """
        messages: List[InfluxdbMessage] = []

        # general_info block
        general_info_msg = self._map_general_info(msg)
        if general_info_msg is not None:
            messages.append(general_info_msg)

        # service_* blocks
        services = msg.services
        if not services:
            return []

        for service_name, service_list in services.items():
            if not service_list:
                continue

            for service in service_list:
                service_msg = self._map_service(msg, service, service_name)
                if service_msg is not None:
                    messages.append(service_msg)

        return messages

    # =====================================================================
    #                     GENERAL INFO → INFLUX MESSAGE
    # =====================================================================

    def _map_general_info(self, msg: FeederMessage) -> Optional[InfluxdbMessage]:
        """
        Map the 'general_info' section of the feeder message into a single
        InfluxdbMessage.

        This block contains aggregated provider-level metadata such as
        overbooking ratios and bandwidth settings.
        """
        fields: Dict[str, Any] = {}

        for field_name in cfg.GENERAL_FIELDS:
            if field_name is not None:
                value = getattr(msg, field_name, None)
            else:
                logger.warning(f"General info field '{field_name}' is missing in Config")
                continue
            if value is not None:
                fields[field_name] = value

        if not fields:
            logger.error("No valid general_info fields found; skipping line message creation.")
            return None
        
        # Convert feeder timestamp → ns epoch for InfluxDB
        timestamp_feeder_msg = getattr(msg, cfg.ROOT_TIMESTAMP)
        try:
            timestamp_ns = InfluxdbMessage.parse_timestamp_ns(timestamp_feeder_msg)
        except Exception as e:
            logger.error("Invalid timestamp '%s': %s", timestamp_feeder_msg, e)
            return None

        return InfluxdbMessage(
            measurement=cfg.MEAS_GENERAL_INFO,
            tags=msg.tags,
            fields=fields,
            timestamp_ns=timestamp_ns,
        )

    # =====================================================================
    #                 SERVICE MODEL → INFLUX MESSAGE
    # =====================================================================

    def _map_service(
        self,
        msg: FeederMessage,
        service: GenericServiceModel,
        service_name: str,
    ) -> Optional[InfluxdbMessage]:
        """
        Convert a single service entry (compute, network, block_storage, ...)
        into an InfluxdbMessage by combining:
        - provider-level tags
        - service_type tag
        - quota + usage values for all fields defined in config.py
        """
        quota_limits = service.get_quota(False)
        quota_usage = service.get_quota(True)

        fields = self._build_quota_fields(quota_limits, quota_usage)
        if not fields:
            logger.debug("Skipping service '%s' because it contains no quota/usage fields.", service_name)
            return None

        # Compose final tag set: root tags + service_type
        tags = dict(msg.tags)
        tags[cfg.TAG_SERVICE_TYPE] = service.service_type

        timestamp_feeder_msg = getattr(msg, cfg.ROOT_TIMESTAMP)
        timestamp_ns = InfluxdbMessage.parse_timestamp_ns(timestamp_feeder_msg)

        return InfluxdbMessage(
            measurement=service_name,
            tags=tags,
            fields=fields,
            timestamp_ns=timestamp_ns,
        )

    # =====================================================================
    #                      BUILD QUOTA+USAGE FIELDS
    # =====================================================================

    def _build_quota_fields(
        self,
        quota_limits: Optional[QuotaModel],
        quota_usage: Optional[QuotaModel],
    ) -> Dict[str, Any]:
        """
        Construct InfluxDB fields for:
        - <field>_quota
        - <field>_usage

        For each quota field defined in config.QUOTA_FIELDS:
        - extract the assigned (quota) value
        - extract the usage (usage) value
        - include only non-null entries
        """
        fields: Dict[str, Any] = {}

        for quota_field in cfg.QUOTA_FIELDS:
            field_quota_name = f"{quota_field}_quota"
            field_usage_name = f"{quota_field}_usage"

            # Quota assigned value
            if quota_limits is not None:
                val = getattr(quota_limits, quota_field, None)
                if val is not None:
                    fields[field_quota_name] = val

            # Usage value
            if quota_usage is not None:
                val = getattr(quota_usage, quota_field, None)
                if val is not None:
                    fields[field_usage_name] = val

        return fields
