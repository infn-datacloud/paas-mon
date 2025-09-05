from kafka import KafkaConsumer, KafkaProducer
import random
import string
from time import sleep
from app_conf import Configuration

class KafkaClient(Configuration):
    def __init__(self, configs):
        self.configs = configs
        self.logger = configs.logger
        
        self.logger.debug("KafkaClient imported the configuration:")
        self.logger.debug(self.configs.__dict__)
        
    def get_consumer_with_timeout(self):
        try:
            with open(self.configs.kafka_ssl_password_path) as reader:
                ssl_password = reader.read().strip()
        except Exception as e:
            self.logger.error(f"Impossible read the password file. Error: {e}")
            raise
        else:
            self.logger.debug("Read successfully the ssl password file")
            group_id = ''.join(random.choices(string.ascii_uppercase +
                                        string.ascii_lowercase +
                                        string.digits, k=64))
            client_id = ''.join(random.choices(string.ascii_uppercase +
                                        string.ascii_lowercase +
                                        string.digits, k=64))
            consumer = KafkaConsumer(
                self.configs.kafka_input_topics, 
                auto_offset_reset = 'earliest', 
                bootstrap_servers = self.configs.kafka_cluster,
                client_id = self.configs.app_name + client_id,
                enable_auto_commit = True,
                fetch_max_bytes = 104857600, 
                group_id = f'{self.configs.app_name}-group-{group_id}',
                group_instance_id = self.configs.app_name + client_id + "q", # disponibile dalla versione 2.2.14 
                security_protocol = "SSL",
                ssl_check_hostname = False,
                ssl_password = ssl_password,
                ssl_cafile = self.configs.kafka_ssl_cacert_path,
                ssl_certfile = self.configs.kafka_ssl_cert_path,
                ssl_keyfile = self.configs.kafka_ssl_key_path,
                value_deserializer = lambda x: x.decode('utf-8'),
                consumer_timeout_ms = self.configs.kafka_consumer_timeout_ms
                )
            self.logger.debug(f"Kafka Consumer with timeout \'{self.configs.app_name}\' created.")
            sub_topics = self.configs.kafka_input_topics.split(',')
            self.logger.debug(f"{sub_topics=}")
            consumer.subscribe(sub_topics)
            return consumer

    def get_consumer(self):
        try:
            with open(self.configs.kafka_ssl_password_path) as reader:
                ssl_password = reader.read()
        except Exception as e:
            self.logger.error(f"Impossible read the password file. Error: {e}")
            raise
        else:
            self.logger.debug("Read successfully the ssl password file")
            group_id = ''.join(random.choices(string.ascii_uppercase +
                                        string.ascii_lowercase +
                                        string.digits, k=64))
            consumer = KafkaConsumer(
                self.configs.kafka_input_topics, 
                auto_offset_reset = 'earliest', 
                bootstrap_servers = self.configs.kafka_cluster,
                client_id = self.configs.app_name, 
                enable_auto_commit = True,
                fetch_max_bytes = 104857600, 
                group_id = f'{self.configs.app_name}-group-{group_id}',
                security_protocol="SSL",
                max_poll_records=10,
                ssl_check_hostname=False,
                ssl_password=ssl_password,
                ssl_cafile = self.configs.kafka_ssl_cacert_path,
                ssl_certfile = self.configs.kafka_ssl_cert_path,
                ssl_keyfile = self.configs.kafka_ssl_key_path,
                value_deserializer = lambda x: x.decode('utf-8')
                )
            self.logger.debug(f"Kafka Consumer \'{self.configs.app_name}\' created.")
            return consumer

    def get_producer(self):
        try:
            with open(self.configs.kafka_ssl_password_path) as reader:
                ssl_password = reader.read()
        except Exception as e:
            self.logger.error(f"Impossible read the password file. Error: {e}")
            raise
        else:
            self.logger.debug("Read successfully the ssl password file")

            producer = KafkaProducer(
                acks = self.configs.kafka_ack,
                allow_auto_create_topics = False,
                bootstrap_servers = self.configs.kafka_cluster,
                client_id = self.configs.app_name,
                enable_idempotence = True,
                max_request_size = self.configs.kafka_max_request_size,
                security_protocol = "SSL",
                ssl_check_hostname = False,
                ssl_password = ssl_password,
                ssl_cafile = self.configs.kafka_ssl_cacert_path,
                ssl_certfile = self.configs.kafka_ssl_cert_path,
                ssl_keyfile = self.configs.kafka_ssl_key_path,
                value_serializer=lambda x: x.encode())
            self.logger.debug(f"Kafka Producer \'{self.configs.app_name}\' created.")
            return producer

    def send_messages(self, records):
        if len(records) > 0:
            self.logger.debug("Sending message to Kafka...")
            try:
                self.logger.debug("Started the Producer creation...")
                producer = self.get_producer()
            except Exception as e:
                self.logger.error(f"Impossible create producer object. Error: {e}")
            else:
                sent_count = 0
                for record in sorted(records):
                    try:
                        producer.send(self.configs.kafka_output_topic, record)
                    except Exception as e:
                        self.logger.error(f"Error when sending message to kafka. Error: {e}")
                        self.logger.debug(f"Message: {record}")
                    else:
                        sent_count += 1
                
                producer.flush()
                producer.close()
                self.logger.info(f"Sent {sent_count} messages to Kafka topic '{self.configs.kafka_output_topic}'")
        else:
            self.logger.info(f"No message to write to the Kafka topic '{self.configs.kafka_output_topic}'")
        
    def get_all_msgs_from_input_topics(self):
        self.logger.debug("Collecting all messages from Kafka...")
        attemps = 12
        while attemps > 0:
            try:
                consumer = self.get_consumer_with_timeout()
                msgs = [ message for message in consumer ]
            except Exception as e:
                attemps -=1
                self.logger.error(f"{e}. Remaining {attemps} attempt(s).")
                if attemps > 0:
                    sleep(5)
                else:
                    raise Exception("Impossible Collect data from Kafka")
            else:
                if len(msgs) > 0:
                    self.logger.debug(f"Collected {len(msgs)=} message(s).")
                    attemps = 0
                else:
                    if attemps > 0:
                        attemps -=1
                        self.logger.error(f"Retrieved no message. Retry. Remaining {attemps} attempt(s).")
                    else:
                        msgs = []
                    
        consumer.close()
        return msgs