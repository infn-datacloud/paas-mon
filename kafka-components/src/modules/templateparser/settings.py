from pydantic import Field, validator
from pydantic_settings import BaseSettings
import json
from typing import Callable, Dict, Any


class TemplateParserConfig(BaseSettings):
    
    APP_NAME: str = Field(default = "template-parser",
                          env="APP_NAME",
                          description="Name of the application")
    LOG_DIR: str = Field(default = "./logs",
                         env="APP_LOG_DIR",
                         description="Directory for application logs")
    KAFKA_ACKS: str = Field(default = 'all',
                            env="KAFKA_ACKS",
                            description="Acknowledgment setting for Kafka producer")    
    KAFKA_ALLOW_AUTO_CREATE_TOPICS: bool = Field(default = False,
                                                  env="KAFKA_ALLOW_AUTO_CREATE_TOPICS",
                                                  description="Allow auto creation of topics in Kafka")
    KAFKA_AUTO_OFFSET_RESET: str = Field(default = 'earliest',
                                          env="KAFKA_AUTO_OFFSET_RESET",
                                          description="Auto offset reset policy for Kafka consumer")
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default = 'kafka-1:9095,kafka-2:9095,kafka-3:9095',
                                          env="KAFKA_BOOTSTRAP_SERVERS",
                                          description="Bootstrap servers for Kafka cluster")
    KAFKA_CLIENT_ID: str = Field(default = "prod-template-parser",
                                            env="KAFKA_CLIENT_ID",
                                            description="Client ID for Kafka producer/consumer")
    KAFKA_ENABLE_AUTO_COMMIT: bool = Field(default = True,
                                            env="KAFKA_ENABLE_AUTO_COMMIT",
                                            description="Enable auto commit for Kafka consumer")
    KAFKA_ENABLE_IDEMPOTENCE: bool = Field(default = True,
                                            env="KAFKA_ENABLE_IDEMPOTENCE",
                                            description="Enable idempotence for Kafka producer")
    KAFKA_FETCH_MAX_BYTES: int = Field(default = 104857600,
                                        env="KAFKA_FETCH_MAX_BYTES",
                                        description="Maximum bytes to fetch in a single request for Kafka consumer")
    KAFKA_GROUP_ID_BASE : str = Field(default = "prod-template-parser-group-id",
                                           env="KAFKA_GROUP_ID",
                                           description="Consumer group ID for Kafka consumer")
    KAFKA_INPUT_TOPICS: list[str] = Field(default = [],
                                           env="KAFKA_INPUT_TOPICS",
                                           description="List of input Kafka topics")
    KAFKA_INPUT_TOPIC: str = Field(default = "orchestrator-logs",
                                   env="KAFKA_INPUT_TOPIC",
                                   description="List of input Kafka topics")
    KAFKA_MAX_POLL_RECORDS: int = Field(default = 1,
                                         env="KAFKA_MAX_POLL_RECORDS",
                                         description="Maximum number of records to return in a single poll for Kafka consumer")
    KAFKA_MAX_REQUEST_SIZE: int = Field(default = 104857600,
                                         env="KAFKA_MAX_REQUEST_SIZE",
                                         description="Maximum request size for Kafka producer")
    KAFKA_OUTPUT_TOPIC: str = Field(default = "validated-templates",
                                    env="KAFKA_OUTPUT_TOPIC",
                                    description="Output Kafka topic for validated templates")
    KAFKA_LOG_TOPIC: str = Field(default = "logs-proc-template-parser",
                                 env="KAFKA_LOG_TOPIC",
                                 description="Kafka topic for logging messages")    
    KAFKA_SSL_CAFILE: str = Field(default = "./certs/ca_cert.pem",
                                   env="KAFKA_SSL_CAFILE",
                                   description="Path to the CA certificate file")
    KAFKA_SSL_CERTFILE: str = Field(default = "./certs/proc_template_parser_cert_signed.pem",
                                     env="KAFKA_SSL_CERTFILE",
                                     description="Path to the SSL certificate file")
    KAFKA_SSL_KEYFILE: str = Field(default = "./certs/proc_template_parser_key.pem",
                                   env="KAFKA_SSL_KEYFILE",
                                   description="Path to the SSL key file")
    KAFKA_SSL_PASSWORD_PATH: str = Field(default = "./certs/proc_template_parser.password",
                                         env="KAFKA_SSL_PASSWORD_PATH",
                                         description="Path to the SSL password file")
    KAFKA_VALUE_DESERIALIZER_STR: str = Field(default = 'json',
                                           env="KAFKA_VALUE_DESERIALIZER",
                                           description="Deserializer for Kafka message values")
    KAFKA_VALUE_SERIALIZER_STR: str = Field(default = 'json',
                                           env="KAFKA_VALUE_SERIALIZER",
                                           description="Serializer for Kafka message values")
    LOG_SEP: str = Field(default = "paas-orchestrator orchestrator/",
                                     env="LOG_SEP",
                                     description="Log separator used in the log parser")
    
    value_serializer: Callable = None
    value_deserializer: Callable = None
    
    @validator("value_deserializer", pre=True, always=True)
    def parse_kafka_value_deserializer(cls, v, values) -> Callable:
        function_name = values.get('KAFKA_VALUE_DESERIALIZER_STR', 'json')
        if function_name == 'json':
            return lambda x: json.loads(x.decode('utf-8'))
        elif function_name == 'string':
            return lambda x: x.decode('utf-8')
        else:
            raise ValueError(f"Unsupported deserializer: {function_name}")
    
    @validator("value_serializer", pre=True, always=True)
    def parse_kafka_value_serializer(cls, v, values) -> Callable:
        function_name = values.get('KAFKA_VALUE_SERIALIZER_STR', 'json')
        if function_name == 'json':
            return lambda x: json.dumps(x, sort_keys=True).encode('utf-8')
        elif function_name == 'string':
            return lambda x: x.encode('utf-8')
        else:
            raise ValueError(f"Unsupported serializer: {function_name}")

    def get_values(self) -> Dict[str, Any]:
        settings_dict = {}
        for key, value in self.model_dump().items():
            key = key.lower()
            if key.endswith('_str'):
                continue
            if key.startswith('kafka_'):
                key = key.replace('kafka_', '')
            settings_dict[key] = value
        
        settings_dict['input_topics'] = [settings_dict['input_topic']]
        return settings_dict
    
    