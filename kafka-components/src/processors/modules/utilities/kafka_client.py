
import copy 
import json
from logging import Formatter as logging_Formatter
# import logging.handlers
# import os
import random
import string
from kafka import KafkaConsumer, KafkaProducer # type: ignore
from modules.utilities.kafka_logging_handler import KafkaLoggingHandler

class KafkaClient():
    PROD_DEFAULT_CONFIG = {
        'acks': 'all',
        'allow_auto_create_topics': False,
        'bootstrap_servers': 'paas-kafka01-pre:9095,paas-kafka02-pre:9095,paas-kafka03-pre:9095',
        'client_id': None,
        'enable_idempotence': True,
        'max_request_size': 104857600,
        'security_protocol': "SSL",
        'ssl_check_hostname': False,
        'ssl_password': 'my-password',
        'ssl_cafile': 'ssl_ca_cert.pem',
        'ssl_certfile': 'ssl_cert_signed.pem',
        'ssl_keyfile': 'ssl_key.pem',
        'value_serializer': lambda x: json.dumps(x, sort_keys=True).encode('utf-8')
    }
    
    CONS_DEFAULT_CONFIG = {
        'auto_offset_reset': 'earliest', 
        'bootstrap_servers': 'paas-kafka01-pre:9095,paas-kafka02-pre:9095,paas-kafka03-pre:9095',
        'client_id': None, 
        'enable_auto_commit': True,
        'fetch_max_bytes': 104857600, 
        'group_id': None,
        # 'group_instance_id': None,
        'max_poll_records': 1,
        'security_protocol': "SSL",
        'ssl_check_hostname': False,
        'ssl_password': 'password',
        'ssl_cafile': 'ssl_ca_cert.pem',
        'ssl_certfile': 'ssl_cert_signed.pem',
        'ssl_keyfile': 'ssl_key.pem',
        'value_deserializer': lambda x: json.loads(x.decode('utf-8'))
    }
    
    CLIENT_DEFAULT_CONFIG = {
        'app_name': "my-client",
        'cert_dir': None,
        'log_dir': './log'
    }
    
    def __init__(self, logger, **configs: dict[str]) -> None:
        
        self.logger = logger
        self.client_configs = copy.copy(self.CLIENT_DEFAULT_CONFIG)
        for key in self.client_configs:
            if key in configs:
                self.client_configs[key] = configs[key]
        
        ssl_password = None
        if 'ssl_password_path' in configs:
            try:
                with open(configs['ssl_password_path']) as reader:
                    ssl_password = reader.read().strip()
            except Exception as e:
                self.logger.error(f"Impossible read the password file. Error: {e}")
                raise
            else:
                self.logger.debug("Read successfully the ssl password file")
        
        configs['ssl_password'] = ssl_password
        rnd_id = ''.join(random.choices(string.ascii_uppercase +
                                        string.ascii_lowercase +
                                        string.digits, k=64))
        
        configs['group_id'] = configs['group_id_base'] + rnd_id 
        # configs['group_instance_id'] = self.client_configs['app_name'] + rnd_id
        
        self.prod_configs = copy.copy(self.PROD_DEFAULT_CONFIG)
        for key in self.prod_configs:
            if key in configs:
                self.prod_configs[key] = configs[key]
                
        self.cons_configs = copy.copy(self.CONS_DEFAULT_CONFIG)
        for key in self.cons_configs:
            if key in configs:
                self.cons_configs[key] = configs[key]
        
        if "input_topics" in configs:
            self.input_topics = configs['input_topics']
            self.logger.info("Configuration for the Consumer: {")
            for key, value in self.cons_configs.items():
                self.logger.info(f"\t{key}: {value}")
            self.logger.info("}")
            self.consumer = KafkaConsumer(**self.cons_configs)
            self.consumer.subscribe(self.input_topics)
            self.logger.info(f"Subscribed to topics: {self.input_topics}")
        else:
            self.consumer = None
        
        if "output_topic" in configs or "log_topic" in configs:
            self.producer = KafkaProducer(**self.prod_configs)
            self.logger.info("Configuration for the Producer: {")
            for key, value in self.prod_configs.items():
                self.logger.info(f"\t{key}: {value}")
            self.logger.info("}")
            
            if "output_topic" in configs:
                self.output_topic = configs['output_topic']
            else:
                self.output_topic = None
                
            if "log_topic" in configs:
                self.log_topic = configs['log_topic']
                self.logger.info("Configuration for Kafka Handler:")
                self.logger.info(f"log_topic = {self.log_topic}")
                self.logger.info("")
                kafka_handler = KafkaLoggingHandler(self.producer, self.log_topic)
                formatter_str = '%(asctime)s [%(name)-12s] %(levelname)s %(message)s'
                formatter = logging_Formatter(formatter_str)
                kafka_handler.setFormatter(formatter)
                self.logger.addHandler(kafka_handler)   
                self.logger.info("Added KafkaLoggingHandler to logger")
            else:
                self.log_topic = None
        else:
            self.producer = None
        
    # Write message in kafka topic
    def send(self, value, key=None) -> None:
        if key:
            key = key.encode('utf-8') if isinstance(key, str) else key
        elif "uuid" in value:
            key = value['uuid'].encode('utf-8')
        
        if self.output_topic is not None:
            if isinstance(value, list):
                for msg in value:
                    self.producer.send(self.output_topic, value=msg, key=key)
                    # self.logger.debug(f"Message sent to topic {self.output_topic}: {value}")
            else:
                self.producer.send(self.output_topic, value=value, key=key)
                # self.logger.debug(f"Message sent to topic {self.output_topic}: {value}")
            self.producer.flush()

    def collect_all_msgs_from_topics(self, 
                                     topics: list[str] = None) -> dict[str, list]:
        attempts = 5
        if not isinstance(topics, list) and isinstance(topics, str):
            topics = [topics] if topics else self.input_topics
        self.logger.debug(f"Collect_all_msg_from_topics. Input topics: {topics}")
        
        tot_collected_msgs = 0
        while tot_collected_msgs == 0 and attempts > 0:
            group_id = ''.join(random.choices(string.ascii_uppercase +
                                        string.ascii_lowercase +
                                        string.digits, k=64))
            group_base = '-'.join(self.input_topics)
            temp_consumer_conf = copy.copy(self.cons_configs)
            temp_consumer_conf['consumer_timeout_ms'] = 1000
            temp_consumer_conf['group_id'] = group_base + group_id
            temp_consumer_conf['client_id'] = group_base + group_id
            temp_consumer_conf['max_poll_records'] = 10000
            # temp_consumer_conf['group_instance_id'] = group_base + group_id
            
            self.logger.info("Configuration for the Temporary Consumer: {")
            for key, value in temp_consumer_conf.items():
                self.logger.info(f"\t{key}: {value}")
            self.logger.info("}")
            
            temp_consumer = KafkaConsumer(**temp_consumer_conf)
            temp_consumer.subscribe(topics)
            collected_msgs = { topic:[] for topic in topics }
            messages = [message for message in temp_consumer]
            tot_collected_msgs = len(messages)
            self.logger.debug(f"Collected messages from Kafka: {tot_collected_msgs}")
            if tot_collected_msgs == 0:
                self.logger.debug(f"Retry collection. Remaining {attempts} attempts")
                attempts -= 1
        
        for message in messages:
            topic = str(message.topic)
            collected_msgs[topic].append(message)
        return collected_msgs