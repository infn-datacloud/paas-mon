from pydantic_settings import BaseSettings
from pydantic import Field, field_validator, ValidationInfo
from typing import Callable, Dict, Any, List, Optional
import json


class MonitoringConfig(BaseSettings):
    """
    Configuration class for the monitoring pipeline.
    Loads Kafka and application parameters from environment variables
    or defaults. Compatible with KafkaClient (both producer and consumer modes).
    """

    # --- Application metadata ---
    APP_NAME: str = Field(default="monitoring_processor", env="APP_NAME")
    CLIENT_ID: str = Field(default="monitoring_processor", env="CLIENT_ID")
    GROUP_ID_BASE: str = Field(default="monitoring_processor-", env="GROUP_ID_BASE")
    LOG_DIR: str = Field(default="./logs", env="APP_LOG_DIR")

    # --- Kafka connection ---
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="kafka-broker:9092", env="KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_SECURITY_PROTOCOL: str = Field(default="PLAINTEXT", env="KAFKA_SECURITY_PROTOCOL")
    KAFKA_ALLOW_AUTO_CREATE_TOPICS: bool = Field(default=False, env="KAFKA_ALLOW_AUTO_CREATE_TOPICS")
    KAFKA_ACKS: str = Field(default="all", env="KAFKA_ACKS")
    KAFKA_ENABLE_IDEMPOTENCE: bool = Field(default=True, env="KAFKA_ENABLE_IDEMPOTENCE")
    KAFKA_AUTO_OFFSET_RESET: str = Field(default="earliest", env="KAFKA_AUTO_OFFSET_RESET")
    KAFKA_ENABLE_AUTO_COMMIT: bool = Field(default=True, env="KAFKA_ENABLE_AUTO_COMMIT")
    KAFKA_FETCH_MAX_BYTES: int = Field(default=104857600, env="KAFKA_FETCH_MAX_BYTES")
    KAFKA_MAX_REQUEST_SIZE: int = Field(default=104857600, env="KAFKA_MAX_REQUEST_SIZE")
    KAFKA_MAX_POLL_RECORDS: int = Field(default=1, env="KAFKA_MAX_POLL_RECORDS")
    KAFKA_GROUP_ID: str = Field(default="", env="KAFKA_GROUP_CONSUMER")

    # --- Kafka topics ---
    KAFKA_INPUT_TOPICS: List[str] = Field(default=["test_input_topic"], env="KAFKA_INPUT_TOPICS")
    KAFKA_OUTPUT_TOPIC: str = Field(default="test_output_topic", env="KAFKA_OUTPUT_TOPIC")
    KAFKA_LOG_TOPIC: Optional[str] = Field(default="logs_test", env="KAFKA_LOG_TOPIC")

    # --- SSL fields (optional, not used in PLAINTEXT but reserved for prod) ---
    KAFKA_SSL_CHECK_HOSTNAME: Optional[bool] = Field(default=None, env="KAFKA_SSL_CHECK_HOSTNAME")
    KAFKA_SSL_PASSWORD: Optional[str] = Field(default=None, env="KAFKA_SSL_PASSWORD")
    KAFKA_SSL_CAFILE: Optional[str] = Field(default=None, env="KAFKA_SSL_CAFILE")
    KAFKA_SSL_CERTFILE: Optional[str] = Field(default=None, env="KAFKA_SSL_CERTFILE")
    KAFKA_SSL_KEYFILE: Optional[str] = Field(default=None, env="KAFKA_SSL_KEYFILE")

    # --- Value serialization ---
    KAFKA_VALUE_SERIALIZER_STR: str = Field(default="json", env="KAFKA_VALUE_SERIALIZER")
    KAFKA_VALUE_DESERIALIZER_STR: str = Field(default="json", env="KAFKA_VALUE_DESERIALIZER")
    value_serializer: Callable = None
    value_deserializer: Callable = None

    # --- Message format ---
    OUTPUT_FORMAT: str = Field(default="line_protocol", env="OUTPUT_FORMAT")


    # --- Validators to generate (de)serializers automatically ---
    @field_validator("value_serializer", mode="before")
    def build_serializer(cls, v, info: ValidationInfo):
        fmt = info.data.get("KAFKA_VALUE_SERIALIZER_STR", "json")
        if fmt == "json":
            return lambda x: (x.encode("utf-8") if isinstance(x, str)
                             else json.dumps(x, sort_keys=True).encode("utf-8"))
        elif fmt == "string":
            return lambda x: x.encode("utf-8")
        else:
            raise ValueError(f"Unsupported serializer: {fmt}")

    @field_validator("value_deserializer", mode="before")
    def build_deserializer(cls, v, info: ValidationInfo):
        fmt = info.data.get("KAFKA_VALUE_DESERIALIZER_STR", "json")
        if fmt == "json":
            return lambda x: json.loads(x.decode("utf-8"))
        elif fmt == "string":
            return lambda x: x.decode("utf-8")
        else:
            raise ValueError(f"Unsupported deserializer: {fmt}")

    @field_validator("KAFKA_INPUT_TOPICS", mode="before")
    def parse_input_topics(cls, v):
        if isinstance(v, str):
            return [t.strip() for t in v.replace(";", ",").replace(" ", ",").split(",") if t.strip()]
        return v
    # --- The function json_to_lines can convert messages only in line protocol or json format
    @field_validator("OUTPUT_FORMAT")
    def validate_output_format(cls, v):
        allowed = {"line_protocol", "json"}
        if v not in allowed:
            raise ValueError(f"OUTPUT_FORMAT must be one of {allowed}")
        return v

    
    
    # --- Utility methods ---
    def get_values(self) -> Dict[str, Any]:
        """
        Return a dictionary of arguments compatible with KafkaClient
        (includes both Kafka and application-specific fields).
        """
        settings_dict = {}
        for key, value in self.model_dump().items():
            key = key.lower()
            if key.endswith('_str'):
                continue
            if key.startswith('kafka_'):
                key = key.replace('kafka_', '')
            settings_dict[key] = value
        return settings_dict

    def show_configs(self) -> str:
        msg = f"\n[CONFIG] Loaded environment variables for {self.APP_NAME}:\n"
        for key, value in self.model_dump().items():
            msg += f"  {key} = {value}\n"
        msg += "}\n"
        return msg



if __name__ == "__main__":
    cfg = MonitoringConfig()
    print(cfg.show_configs())