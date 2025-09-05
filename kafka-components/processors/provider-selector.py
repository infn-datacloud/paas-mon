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

from modules.providerselector.processor import ProviderSelectorProcessor
from modules.providerselector.settings import ProviderSelectorConfig
from modules.utilities.logger import create_logger
       
if __name__ == "__main__": 
    settings = ProviderSelectorConfig()
    logger = create_logger(settings)
    logger.info(settings.show_configs())

    processor = ProviderSelectorProcessor(settings, logger)
    processor.restore_history()
    processor.process_new_messages()