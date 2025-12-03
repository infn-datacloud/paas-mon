"""
Configuration module for feeder message format v1.3.x.

This module defines all constant names that describe the structure of the
incoming feeder JSON message. Every component of the ingestion pipeline
(model, parser, mapper, processor) MUST rely exclusively on these constants.

By centralizing the definitions here, any future change to the message
structure (renaming fields, adding new keys, removing obsolete keys, etc.)
requires modification in one place only, ensuring full pipeline consistency.

Content overview:
- Measurement names used in InfluxDB (MEAS_*)
- Service names list (SERVICE_NAMES)
- JSON tag keys (TAG_*)
- Root-level JSON fields (ROOT_*)
- General information fields (GENERAL_FIELDS)
- Quota and usage field names (QUOTA_FIELDS)
- Reserved flags (e.g., QUOTA_USAGE_FLAG)
"""

# ============================================================
#                     MEASUREMENT NAMES
# ============================================================

# Measurement used for general provider-level information
MEAS_GENERAL_INFO = "general_info"

# Measurements for service-level information
MEAS_COMPUTE = "compute_services"
MEAS_BLOCK_STORAGE = "block_storage_services"
MEAS_NETWORK = "network_services"
MEAS_OBJECT_STORE = "object_store_services"

# List of all service measurements expected in the message
SERVICE_NAMES = [
    MEAS_COMPUTE,
    MEAS_BLOCK_STORAGE,
    MEAS_NETWORK,
    MEAS_OBJECT_STORE,
]

# ============================================================
#                     TAG KEYS (JSON TAGS)
# ============================================================

# Provider metadata
TAG_PROVIDER_NAME = "provider_name"
TAG_PROVIDER_TYPE = "provider_type"
TAG_REGION_NAME = "region_name"
TAG_USER_GROUP = "user_group"

# Project identifier
TAG_PROJECT_ID = "project_id"

# Service type (e.g., "compute", "network", ...)
TAG_SERVICE_TYPE = "type"

# JSON keys that must always appear in the root of each message
ROOT_TAGS = [
    TAG_PROVIDER_NAME,
    TAG_PROVIDER_TYPE,
    TAG_REGION_NAME,
    TAG_USER_GROUP,
    TAG_PROJECT_ID,
]

# Additional root-level JSON fields
ROOT_TIMESTAMP = "timestamp"      # Timestamps are expected in ISO8601
ROOT_MSG_VERSION = "msg_version"  # Message schema version (e.g. "1.3.x")

# ============================================================
#                GENERAL INFO FIELDS (JSON KEYS)
# ============================================================

# Provider capacity and network metadata
FIELD_OVERBOOKING_CPU = "overbooking_cpu"
FIELD_OVERBOOKING_RAM = "overbooking_ram"
FIELD_BANDWIDTH_IN = "bandwidth_in"
FIELD_BANDWIDTH_OUT = "bandwidth_out"

# List of all "general_info" fields that belong to the MEAS_GENERAL_INFO measurement
GENERAL_FIELDS = [
    FIELD_OVERBOOKING_CPU,
    FIELD_OVERBOOKING_RAM,
    FIELD_BANDWIDTH_IN,
    FIELD_BANDWIDTH_OUT,
]

# ============================================================
#               QUOTA / USAGE FIELD NAMES (JSON KEYS)
# ============================================================

# JSON key that groups quota information inside each service object
SERVICE_KEY_QUOTAS = "quotas"

# --- Compute quotas ---
QUOTA_CORES = "cores"
QUOTA_INSTANCES = "instances"
QUOTA_RAM = "ram"

# --- Block Storage quotas ---
QUOTA_GIGABYTES = "gigabytes"
QUOTA_VOLUMES = "volumes"

# --- Network quotas ---
QUOTA_NETWORKS = "networks"
QUOTA_PORTS = "ports"
QUOTA_PUBLIC_IPS = "public_ips"
QUOTA_SECURITY_GROUP_RULES = "security_group_rules"
QUOTA_SECURITY_GROUPS = "security_groups"

# List of all quota fields that may appear under SERVICE_KEY_QUOTAS
QUOTA_FIELDS = [
    QUOTA_CORES,
    QUOTA_INSTANCES,
    QUOTA_RAM,
    QUOTA_GIGABYTES,
    QUOTA_VOLUMES,
    QUOTA_NETWORKS,
    QUOTA_PORTS,
    QUOTA_PUBLIC_IPS,
    QUOTA_SECURITY_GROUP_RULES,
    QUOTA_SECURITY_GROUPS,
]

# JSON key used to distinguish between quota and usage values in the message:
QUOTA_USAGE_FLAG = "usage"
