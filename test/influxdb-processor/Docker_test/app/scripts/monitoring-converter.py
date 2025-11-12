import logging
from modules.utilities.kafka_client import KafkaClient
from modules.monitoring.msg_v13_model import json_to_lines
from modules.monitoring.config import ProviderSelectorConfig
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
    configs = ProviderSelectorConfig()
    
    # Initialize Kafka client 
    client = KafkaClient(logger, **configs.get_values())
    
    # Retrieve all messages from the configured input topics
    messages = client.collect_all_msgs_from_topics(topics=client.input_topics)

    if not messages:
        logger.warning("No topics found in configuration or no messages collected.")
        return
    
    # Identify topics that are configured but contain no data
    empty_topics = [t for t, msgs in messages.items() if not msgs]
    if empty_topics:
        logger.info(f"The following topics are empty: {', '.join(empty_topics)}")
        return

    logger.info("All input topics contain messages.")
    total, success, fail = 0, 0, 0

    # Process each Kafka message and convert it to InfluxDB Line Protocol
    for topic, msg_list in messages.items():
        logger.info(f"Processing {len(msg_list)} messages from topic '{topic}'.")
        for message in msg_list:
            total += 1
            msg = message.value
            timestamp = msg.get("timestamp", "unknown")
            
            try:
                # Convert JSON message to InfluxDB Line Protocol format
                msg_lp = "\n".join(json_to_lines(msg))
            except Exception as e:
                logger.warning(f"Failed to parse message with timestamp {timestamp}: {e}")
                fail += 1
                continue

            try:
                client.send(msg_lp)
                success += 1
                logger.debug(f"Message with timestamp {timestamp} successfully sent to '{client.output_topic}'.")
            except Exception as e:
                logger.error(f"Failed to send message with timestamp {timestamp}: {e}")
                fail += 1

    # --- Summary and graceful shutdown ---
    logger.info(f"Processing summary — Total: {total}, Success: {success}, Failed: {fail}")

    try:
        if client.consumer is not None:
            client.consumer.close()
            logger.info("Consumer closed successfully.")
    except Exception as e:
        logger.warning(f"Error during consumer closure: {e}")

if __name__ == "__main__":
    main()
