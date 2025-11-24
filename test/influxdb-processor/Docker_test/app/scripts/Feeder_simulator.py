import json
import logging
from pathlib import Path
from modules.utilities.kafka_client import KafkaClient
from modules.feedermonitoring.MonitoringConfig import MonitoringConfig
import time
import os

# -----------------------------------------------------------------------------
# This script simulates a "feeder" component that publishes JSON messages
# to a Kafka topic using the shared KafkaClient class.
# It reads an input file line by line and sends it.
#
# Target pipeline:
# feeder → Kafka → processor → Kafka → Telegraf → InfluxDB → Grafana
# -----------------------------------------------------------------------------

def build_logger() -> logging.Logger:
    logger = logging.getLogger("feeder")
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    fmt = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s')
    ch.setFormatter(fmt)
    if not logger.handlers:
        logger.addHandler(ch)
    return logger


def main():
    logger = build_logger()
    configs = MonitoringConfig()

    # --- KafkaClient configuration override ---
    client = KafkaClient(logger, **configs.get_values())
    client.output_topic = "paas_mon"
    # --- Input file path ---
    SCRIPT_DIR = Path(__file__).resolve().parent
    filename = os.getenv("FEEDER_INPUT_FILE")

    if not filename:
        raise RuntimeError("Environment variable FEEDER_INPUT_FILE is missing")
    
    input_file = SCRIPT_DIR / "input" / filename

    if not input_file.exists():
        logger.error(f"Input file not found: {input_file}")
        return

    total, success, fail = 0, 0, 0
    logger.info(f"Reading messages from: {input_file}")

    with input_file.open() as f:
        for i, line in enumerate(f, start=1):
            if total==1000:
                time.sleep(30) # wait for 30 seconds after sending 1000 messages
            
            
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                msg = json.loads(line)
            except Exception as e:
                fail += 1
                logger.warning(f"Line {i}: JSON parse error → {e}")
                continue

            try:
                client.send(value=msg)
                success += 1
            except Exception as e:
                fail += 1
                logger.error(f"Line {i}: send error → {e}")

    logger.info("Publishing finished.")
    logger.info(f"Total messages read: {total}")
    logger.info(f"Successfully sent: {success}")
    logger.info(f"Failed: {fail}")


if __name__ == "__main__":
    main()

