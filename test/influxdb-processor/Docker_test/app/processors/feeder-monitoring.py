#!/usr/bin/env python3

import logging

from modules.feedermonitoring.MonitoringConfig import MonitoringConfig
from modules.feedermonitoring.processor import Processor


def build_logger() -> logging.Logger:
    """Initialize and configure a basic logger for the processor module."""
    logger = logging.getLogger("monitoring_processor")
    logger.setLevel(logging.INFO)
    
    # Stream handler for console output
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    
    # Log message format: timestamp, module name, level, and message
    fmt = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s')
    ch.setFormatter(fmt)
    
    # Prevent duplicate handlers in interactive or repeated runs
    if not logger.handlers:
        logger.addHandler(ch)
    return logger



if __name__ == "__main__":
    logger = build_logger()

    configs = MonitoringConfig()

    try:
        processor = Processor(configs, logger)
        processor.run_processor()

    except KeyboardInterrupt:
        logger.info("Shutting down due to KeyboardInterrupt...")

    except Exception as e:
        logger.error("Fatal error in feeder-monitoring: %s", e)







