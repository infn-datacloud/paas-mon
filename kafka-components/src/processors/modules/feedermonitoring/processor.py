from typing import Any, Dict, List
import logging

from modules.utilities.kafka_client import KafkaClient
from modules.feedermonitoring.v1_3.mapper import Mapper
from modules.feedermonitoring.v1_3 import config as cfg


class Processor:
    """
    Main processing engine for the feeder monitoring pipeline.

    This component connects Kafka input → transformation → Kafka output.
    It consumes raw JSON messages from one or more Kafka topics, converts
    them into InfluxDB-compatible structures (Line Protocol or JSON_v2),
    and republishes the results to an output topic.

    Responsibilities:
    -----------------
    - Initialize Kafka consumer and producer via KafkaClient
    - Continuously consume feeder messages
    - Validate + convert messages through Mapper
    - Publish mapped output to Kafka
    - Commit offsets only after successful processing to ensure at-least-once
      delivery semantics

    The Processor is intentionally thin: it delegates all parsing and
    mapping responsibilities to the Mapper and focuses solely on orchestration.
    """

    def __init__(self, configs, logger: logging.Logger):
        """
        Initialize Processor with:
        - configs: MonitoringConfig instance
        - logger: configured Logger from main script

        Creates:
        - KafkaClient (consumer + producer)
        - Mapper for JSON → InfluxDB conversion
        """
        self.configs = configs
        self.logger = logger

        # Initialize Kafka client using user-defined configs
        self.client = KafkaClient(logger, **configs.get_values())

        # Mapping component (stateless)
        self.mapper = Mapper()

        self.logger.info(
            f"Processor initialized. Listening on topics: {self.client.input_topics}"
        )

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------
    def run_processor(self) -> None:
        """
        Main processing loop.

        Continuously:
        - read Kafka messages (blocking iterator)
        - process them via `process_message`
        - if successful, publish the output and commit the offset

        Offset commit is skipped when processing fails, allowing the consumer
        to retry the same message depending on auto-commit configuration.
        """
        for message in self.client.consumer:
            msg = message.value
            timestamp = msg.get(cfg.ROOT_TIMESTAMP, "unknown")

            parsed = self.process_message(msg, timestamp)
            if parsed is None:
                # Processing failed → do not commit offset
                continue

            self.send_output(parsed, timestamp)

    # ------------------------------------------------------------------
    # Parse → Map → Convert
    # ------------------------------------------------------------------
    def process_message(self, msg: Dict[str, Any], timestamp: str):
        """
        Parse and convert a raw feeder JSON message using Mapper.

        Returns:
            - list[str]  → for OUTPUT_FORMAT='influx'
            - list[dict] → for OUTPUT_FORMAT='json_v2'
            - None       → if parsing or mapping fails
        """
        try:
            parsed = self.mapper.map_feeder_influxdb(
                msg, output_format=self.configs.OUTPUT_FORMAT
            )
            return parsed

        except Exception as e:
            self.logger.warning(
                f"Failed to parse feeder message with timestamp {timestamp}: {e}"
            )
            return None

    # ------------------------------------------------------------------
    # Send converted messages to Kafka output topic
    # ------------------------------------------------------------------
    def send_output(self, parsed, timestamp: str) -> bool:
        """
        Publish converted messages to Kafka.

        Behavior depends on the output format:

        OUTPUT_FORMAT = "influx"
            • parsed is a list[str] containing Line Protocol lines
            • all lines are concatenated into a single Kafka message

        OUTPUT_FORMAT = "json_v2"
            • parsed is a list[dict]
            • each dict is sent as an individual Kafka message

        Returns:
            True  → all messages sent successfully
            False → partial failures (JSON_v2 mode)
            None  → fatal error (LP mode or invalid OUTPUT_FORMAT)
        """

        if self.configs.OUTPUT_FORMAT == "influx":
            # parsed = list[str] (line protocol)
            output_value = "\n".join(parsed)
            try:
                self.client.send(output_value)
                return True
            except Exception as e:
                self.logger.error(
                    f"Failed to send LP message for timestamp {timestamp}: {e}"
                )
                return None

        elif self.configs.OUTPUT_FORMAT == "json_v2":
            # parsed = list[dict]
            errors = 0
            for service in parsed:
                try:
                    self.client.send(service)
                except Exception as e:
                    self.logger.error(
                        f"Failed to send JSON message for timestamp {timestamp}: {e}"
                    )
                    errors += 1

            return errors == 0

        else:
            self.logger.error(
                f"Unsupported OUTPUT_FORMAT '{self.configs.OUTPUT_FORMAT}'. "
                "Expected 'influx' or 'json_v2'."
            )
            return None
