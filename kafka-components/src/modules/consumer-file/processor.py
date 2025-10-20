
from time import time
from settings import ConsumerFile as Config
from modules.utilities.kafka_client import KafkaClient

class ConsumerFileProcessor:
    def __init__(self, settings: Config, logger = None):
        
        # Import logger and settings objects
        self.logger = logger
        self.settings = settings
        
        # Overwrite Kafka settings
        self.settings.KAFKA_INPUT_TOPICS = self.settings.KAFKA_INPUT_TOPICS.split(',')
              
        # Internal objects
        self.kafka_client = KafkaClient(logger, **settings.get_values())
        
        # Internal variables
        self.msg_received = 0
        
    def store_messages(self):
        self.logger.info("Collecting all messages from input topics")
        start_time = time()
        self.logger.debug(f"Topics to collect messages: {self.settings.KAFKA_INPUT_TOPICS}")
        collected_msgs = self.kafka_client.collect_all_msgs_from_topics(self.settings.KAFKA_INPUT_TOPICS)
        interval_s = round(time()-start_time,2)
        tot_msg_num = sum([len(v) for v in collected_msgs.values()])
        self.logger.debug(f"Collected {tot_msg_num} tot messages in {interval_s} s")
        for topic, messages in collected_msgs.items():
            self.logger.debug(f"Collected {len(messages)} from topic {topic}")   
            
            filename = f"{self.settings.OUTPUT_FILENAME_BASE}_{topic.replace('-', '_')}_history.txt"
            with open(filename, 'w') as fout:
                for message in messages:
                    fout.write(f"{message.value}\n")
            self.logger.debug(f"Saved messages from topic {topic} into file {filename}")
        self.logger.info("Message collection completed")
        