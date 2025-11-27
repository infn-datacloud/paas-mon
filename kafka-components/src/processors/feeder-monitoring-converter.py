import logging
from modules.utilities.kafka_client import KafkaClient
from modules.feedermonitoring.msg_v13_model import json_to_lines
from modules.feedermonitoring.MonitoringConfig import MonitoringConfig
import json

# ---------------------------------------------------------------------------------------
# This script represents the "processor" stage in the end-to-end pipeline:
# feeder → Kafka → processor → Kafka → Telegraf → InfluxDB → Grafana
#
# It consumes JSON messages from the Kafka topic produced by the feeder (e.g."paas_mon"),
# converts them into InfluxDB Line Protocol (LP) format using json_to_lines(),
# and republishes the converted data to the output topic (e.g. "paas_mon_processed").
#
# The function json_to_lines() is imported from
# modules.monitoring.msg_v13_model and handles the mapping between
# JSON fields and their LP representation, ensuring compatibility with InfluxDB.
# ----------------------------------------------------------------------------------------


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


def main():
    """Main entry point for the processor module."""
    logger = build_logger()
    
    # Load configuration parameters from environment variables or defaults
    configs = MonitoringConfig()
    
    # Initialize Kafka client 
    client = KafkaClient(logger, **configs.get_values())
    
    logger.info(f"Listening for messages on topics: {client.input_topics}")
    
    try:
        for message in client.consumer:
            msg = message.value
            timestamp = msg.get("timestamp", "unknown")
            
            try:
                parsed = json_to_lines(msg, output_format=configs.OUTPUT_FORMAT)
            except Exception as e:
                logger.warning(f"Failed to parse message with timestamp {timestamp}: {e}")
                continue

            # Format output depending on user configuration
            if configs.OUTPUT_FORMAT == "influx":
                # parsed = list[str]
                output_value = "\n".join(parsed)
                try:
                    client.send(output_value)
                except:
                    logger.error(f"Failed to send InfluxDB Line Protocol message for timestamp {timestamp}")
                    continue  # Move to next message after sending all services 

            elif configs.OUTPUT_FORMAT == "json_v2":
                # parsed = list[dict]
                for service in parsed:
                    try:
                        client.send(service)
                    except Exception as e:
                            logger.error(f"Failed to send JSON message for timestamp {timestamp}: {e}")
                            continue

            else:
                logger.error(
                    f"Unsupported OUTPUT_FORMAT '{configs.OUTPUT_FORMAT}'. "
                    f"Expected 'influx' or 'json_v2'."
                )
                continue
    
    except KeyboardInterrupt:
        logger.info("Shutting down processor...")

    except Exception as e:
        logger.error(f"Fatal error: {e}")

if __name__ == "__main__":
    main()