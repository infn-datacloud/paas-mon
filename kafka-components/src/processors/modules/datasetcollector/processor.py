
from time import time
from modules.datasetcollector.datasetmessage import DatasetMessage
from modules.datasetcollector.loganalyzer import LogAnalyzer
from modules.datasetcollector.providers import Providers
from modules.datasetcollector.settings import DatasetCollectorConfig
from modules.utilities.kafka_client import KafkaClient

class DatasetCollectorProcessor:
    def __init__(self, settings: DatasetCollectorConfig, logger = None):
        
        # Import logger and settings objects
        self.logger = logger
        self.settings = settings
        
        # Overwrite Kafka settings
        self.RESTORE_TOPICS = [self.settings.KAFKA_OUTPUT_TOPIC]
        self.settings.KAFKA_INPUT_TOPICS = [self.settings.KAFKA_INPUT_ORC_LOG_TOPIC,
                                            self.settings.KAFKA_INPUT_PROVIDERS_TO_RANK_TOPIC]
        # Internal objects
        self.log_analyzer = LogAnalyzer(logger)
        self.kafka_client = KafkaClient(logger, **settings.get_values())
        self.providers = Providers(logger)
        
        # Internal variables
        self.output_uuids = None
        self.keys_sent = set()
        self.depl_data = {}
        self.depl_status_db = {}
        
        # Defaine Monitoring metrics
        self.msg_sent = 0
        self.rev_orc_logs = 0
        self.rev_airanker_infer = 0
        
    def get_mon_data(self) -> dict:
        return self.log_analyzer.get_mon_data() | {"msg_sent": self.msg_sent,
                                                   "rev_orc_logs": self.rev_orc_logs,
                                                   "rev_airanker_infer": self.rev_airanker_infer,
                                                   }
    def restore_history(self):
        self.logger.info("Start of initialization phase : Collecting message from output topic")
        start_time = time()
        self.logger.debug(f"Topics to collect messages: {self.RESTORE_TOPICS}")
        collected_msgs = self.kafka_client.collect_all_msgs_from_topics(self.RESTORE_TOPICS)
        interval_s = round(time()-start_time,2)
        tot_msg_num = sum([len(v) for v in collected_msgs.values()])
        self.logger.debug(f"Collected {tot_msg_num} tot messages in {interval_s} s")
        for topic, messages in collected_msgs.items():
            self.logger.debug(f"Collected {len(messages)} from topic {topic}")    
            
            # Collect messages from the output topic and extract template uuids
            if topic == self.settings.KAFKA_OUTPUT_TOPIC:
                self.keys_sent = {message.key.decode('utf-8') for message in messages if message.key is not None and message.key != ''}
                self.logger.debug(f"Imported {len(self.keys_sent)} messages from topic {self.settings.KAFKA_OUTPUT_TOPIC}")
                self.logger.debug(f"{self.keys_sent=}")
        self.logger.info("End of initialization phase")    
        
    def send_keys(self, keys_to_send):
        for key in keys_to_send:
            
            prov_data = self.providers.get(key)
            self.logger.debug("[processor][send_keys] Collected provider data")
            
            depl_status_data = self.log_analyzer.get(key)
            self.logger.debug("[processor][send_keys] Collected deployment status")
            
            msg = DatasetMessage(prov_data, depl_status_data).get_message()
            self.logger.debug("[processor][send_keys] Created message")
            
            self.kafka_client.send(value=msg, key=key)
            self.logger.debug("[processor][send_keys] Message sent")

            self.msg_sent += 1
            
            self.keys_sent.add(key)
            self.logger.debug("[processor][send_keys] Added key in sent_keys")
            
    def check_and_send(self):
        prov_keys = self.providers.keys()
        depl_status_keys = self.log_analyzer.keys()
        common_keys = prov_keys & depl_status_keys
        keys_to_send = common_keys - self.keys_sent
        if len(keys_to_send) > 0:
            self.logger.debug(f"[processor][check_and_send] Keys to send: {keys_to_send}")
            self.send_keys(keys_to_send)
        else:
            self.logger.debug("[processor][check_and_send] No message to send")
        
    def process_new_messages(self):
        for message in self.kafka_client.consumer:
            topic = str(message.topic)
            if topic == self.settings.KAFKA_INPUT_ORC_LOG_TOPIC:
                self.rev_orc_logs += 1
                if self.log_analyzer.import_line(message.value):
                    self.logger.debug("[processor][process_new_message] New event is emitted from log analyzer")
                    self.check_and_send()
                    
            elif topic == self.settings.KAFKA_INPUT_PROVIDERS_TO_RANK_TOPIC:
                self.rev_airanker_infer += 1
                self.providers.import_msg(message.value)
                self.check_and_send()
                