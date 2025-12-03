# model.py 
from __future__ import annotations
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, create_model, field_validator

from modules.feedermonitoring.v1_3 import config as cfg


# ============================================================
#                      BASE MODEL
# ============================================================

class BaseConfigModel(BaseModel):
    """
    Base configuration for all dynamically generated Pydantic models.

    This base class enforces:
    - strict alias usage (aliases always come from config.py)
    - ignore any unexpected extra fields in incoming JSON
    - allow assignment validation on attribute modification

    All models in this module inherit these rules to ensure that the
    feeder message structure remains fully controlled by config.py.
    """
    model_config = dict(
        extra="ignore",
        populate_by_name=True,
        validate_assignment=True,
    )

# ============================================================
#                   QUOTA MODEL (dynamic)
# ============================================================

# Prepare dynamic field definitions for the quota object.
# Every quota field listed in config.QUOTA_FIELDS becomes a field.
quota_fields: Dict[str, tuple] = {
    field_name: (Optional[Any], Field(default=None, alias=field_name))
    for field_name in cfg.QUOTA_FIELDS
}

# Add the "usage" boolean flag (required for identifying quota vs usage)
quota_fields[cfg.QUOTA_USAGE_FLAG] = (
    bool,
    Field(alias=cfg.QUOTA_USAGE_FLAG)
)

# Generate the concrete Pydantic model for a quota entry
QuotaModel = create_model(
    "QuotaModel",
    __base__=BaseConfigModel,
    **quota_fields
)


# ============================================================
#                GENERIC SERVICE MODEL
# ============================================================

class GenericServiceModel(BaseConfigModel):
    """
    Generic representation of a service entry inside the feeder message.
    Every concrete service (compute, block storage, network, ...) will inherit
    from this class through dynamic model creation.

    Fields:
    - service_type: service discriminator (comes from TAG_SERVICE_TYPE)
    - quotas: list of quota objects, dynamically converted into QuotaModel

    The validator ensures that raw quota dictionaries are automatically
    validated as fully structured QuotaModel instances.
    """

    service_type: Optional[str] = Field(
        default=None,
        alias=cfg.TAG_SERVICE_TYPE
    )

    quotas: list = Field(
        default_factory=list,
        alias=cfg.SERVICE_KEY_QUOTAS
    )

    @field_validator(cfg.SERVICE_KEY_QUOTAS, mode="after")
    @classmethod
    def _convert_quotas(cls, raw_quotas):
        """Convert raw quota dictionaries into strongly typed QuotaModel objects."""
        return [QuotaModel.model_validate(item) for item in raw_quotas]

    def get_quota(self, usage: bool):
        """
        Return the quota entry matching the usage flag.

        Args:
            usage (bool): False → assigned quotas, True → usage quotas.

        Returns:
            QuotaModel | None: The matching quota entry if present.
        """
        usage_key = cfg.QUOTA_USAGE_FLAG
        return next(
            (q for q in self.quotas if getattr(q, usage_key) == usage),
            None
        )


# Create one dynamic service model for each service measurement name.
# This keeps the system flexible: adding/removing a service in config.py
# automatically updates the schema.
SERVICE_MODELS: Dict[str, type] = {
    service_name: create_model(
        f"{service_name.replace('_', '').title()}Model",
        __base__=GenericServiceModel
    )
    for service_name in cfg.SERVICE_NAMES
}


# ============================================================
#                FULL FEEDER MESSAGE MODEL
# ============================================================

class FeederMessageBase(BaseConfigModel):
    """
    Base behaviour shared by the dynamically constructed FeederMessage model.

    Responsibilities:
    - normalize timestamp strings into ISO8601-compatible format
    - expose convenient accessors for:
        • tags (provider_name, region_name, ...)
        • service blocks (compute, block_storage, ...)
    """

    @staticmethod
    def _normalize_timestamp(ts: str) -> str:
        """
        Normalize various timestamp formats into a consistent ISO8601 string.

        Ensures:
        - implicit UTC is made explicit by appending 'Z'
        - space-separated formats are converted to 'T'
        - timestamps already containing timezone offsets are left untouched
        """
        ts = str(ts).strip()
        # Convert "YYYY MM DD HH:MM:SS" → "YYYY-MM-DDTHH:MM:SS"
        if " " in ts and "T" not in ts:
            ts = ts.replace(" ", "T", 1)
        # Already timezone-qualified or ending in Z
        if ts.endswith("Z"):
            return ts
        if len(ts) >= 6 and ts[-6] in "+-" and ts[-3] == ":":
            return ts
        return ts + "Z"
    
    @field_validator(cfg.ROOT_TIMESTAMP, mode="before", check_fields=False)
    @classmethod
    def validate_timestamp(cls, value: Any):
        """Apply timestamp normalization before validation."""
        if value is None:
            return value
        return cls._normalize_timestamp(value)

    @property
    def tags(self) -> Dict[str, str]:
        """Return a dictionary of root-level tags (provider, region, project, ...)."""
        return {
            tag: getattr(self, tag)
            for tag in cfg.ROOT_TAGS
            if getattr(self, tag) is not None
        }

    @property
    def services(self) -> Dict[str, List[GenericServiceModel]]:
        """Return the lists of service blocks grouped by service name."""
        return {
            name: getattr(self, name)
            for name in cfg.SERVICE_NAMES
        }


# Build dynamic fields for the FeederMessage model
message_fields: Dict[str, tuple] = {}

# Root tags
for tag in cfg.ROOT_TAGS:
    message_fields[tag] = (Optional[str], Field(default=None, alias=tag))

# General info block
for info in cfg.GENERAL_FIELDS:
    message_fields[info] = (
        Optional[float],
        Field(default=None, alias=info)
    )

# Service blocks
for service_name in cfg.SERVICE_NAMES:
    message_fields[service_name] = (
        List[SERVICE_MODELS[service_name]],
        Field(default_factory=list, alias=service_name)
    )

# Timestamp field (mandatory)
message_fields[cfg.ROOT_TIMESTAMP] = (
    str,
    Field(alias=cfg.ROOT_TIMESTAMP)
)

# Final dynamically generated model for the message
FeederMessage = create_model(
    "FeederMessage",
    __base__=FeederMessageBase,
    **message_fields
)


class Envelope(BaseConfigModel):
    """
    Minimal wrapper model containing the message version discriminator.

    This model is used to identify the schema version of the incoming
    feeder message before selecting the appropriate parser/mapping logic.
    """
    msg_version: str = Field(alias=cfg.ROOT_MSG_VERSION)

    @field_validator("msg_version", mode="before")
    @classmethod
    def _msg_version_to_str(cls, v):
        """Ensure the version field is always stored as a normalized string."""
        return str(v).strip()
