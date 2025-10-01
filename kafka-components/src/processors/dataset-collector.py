# /bin/env python3

# Python dependecies:
# - kafka-python

# topics:
#   inputs:
#       orchestrator-logs
#   outputs:
#       validated-templates
#   logs:
#       logs-proc-inference

from modules.datasetcollector.processor import DatasetCollectorProcessor as Processor
from modules.datasetcollector.settings import DatasetCollectorConfig as Config
from modules.utilities.logger import create_logger
from modules.utilities.monitoring import Monitoring

if __name__ == "__main__": 
    settings = Config()
    logger = create_logger(settings)
    logger.info(settings.show_configs())

    # Init external objects
    processor = Processor(settings, logger)
    mon = Monitoring(settings, logger, processor)
    
    # Start Restore History
    processor.restore_history()
    processor.process_new_messages()