# parser.py 
from __future__ import annotations
import logging
from typing import Any, Dict
from pydantic import ValidationError

from modules.feedermonitoring.v1_3 import config as cfg
from modules.feedermonitoring.v1_3.model import Envelope, FeederMessage

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def parse_feeder_message(msg: Dict[str, Any]):
    """
    Parse and validate a raw feeder message according to schema v1.3.x.

    Parsing steps:
    1. Validate the Envelope to extract `msg_version`.
    2. Emit warnings if the version is missing or incompatible.
    3. Validate the full `FeederMessage` structure using dynamic Pydantic models.

    If validation succeeds:
        → return a fully typed `FeederMessage` instance.
    If validation fails:
        → a Pydantic ValidationError is logged and re-raised.

    This function provides a single entry point for validating all feeder
    messages for version 1.3.x. Any schema modifications should be done in
    config.py or model.py, leaving the parsing logic unchanged.
    """

    # ------------------------------------------------------------
    # 1. Envelope validation (extract version)
    # ------------------------------------------------------------
    try:
        env = Envelope.model_validate(msg)
        version = env.msg_version.strip()
    except ValidationError as e:
        logger.warning(
            "Message does not contain a valid '%s'. Proceeding anyway. Error: %s",
            cfg.ROOT_MSG_VERSION, str(e)
        )
        version = None

    # ------------------------------------------------------------
    # 2. Version compatibility check
    # ------------------------------------------------------------
    if version is None:
        logger.warning(
            "Missing msg_version: expected '1.3.x'. Attempting fallback parsing."
        )
    elif not version.startswith("1.3"):
        logger.warning(
            "Unsupported msg_version '%s'. Expected '1.3.x'. Attempting fallback parsing.",
            version
        )

    # ------------------------------------------------------------
    # 3. Full model validation (strict schema)
    # ------------------------------------------------------------
    try:
        return FeederMessage.model_validate(msg)
    except ValidationError as e:
        logger.error("Feeder message parsing failed: %s", e)
        raise




