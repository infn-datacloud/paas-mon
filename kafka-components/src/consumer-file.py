# /bin/env python3

# Python dependecies:
# - kafka-python

# topics:
#   inputs:
#       orchestrator-logs
# output-file:
#   consumer-file.txt

from modules.consumerfile.processor import ConsumerFileProcessor as Processor
from modules.consumerfile.settings import ConsumerFile as Config
from modules.utilities.logger import create_logger

if __name__ == "__main__": 
    settings = Config()
    logger = create_logger(settings)
    logger.info(settings.show_configs())

    # Init external objects
    processor = Processor(settings, logger)
    
    # Start Restore History
    processor.store_messages()