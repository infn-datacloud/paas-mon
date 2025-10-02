# /bin/env python3

# Python dependecies:
# - kafka-python

# topics:
#   inputs:
#       orchestrator-logs
#   outputs:
#       validated-templates
#   logs:
#       logs-proc-template-parser

import threading
from time import time, sleep
from modules.templateparser.settings import TemplateParserConfig
from modules.templateparser.orc_templ_collector import LogOrchestratorCollector 
from modules.templateparser.templ_parser_message import TemplateParserMessage
from modules.utilities.kafka_client import KafkaClient
from modules.utilities.logger import create_logger

MON_ENABLED = True
MON_PERIOD = 600 # 10 minutes

## Monitoring metrics
rec_orc_log = 0
templ_parsed = 0
msg_sent = 0

settings = TemplateParserConfig()
logger = create_logger(settings)

logger.info("Collecting configuration settings for Template Parser:")
for key, value in settings.model_dump().items():
    logger.info(f"\t{key} = {value}")
logger.info("}")

kafka_client = KafkaClient(logger, **settings.get_values())

# INIT: Collect all messages from output topic
logger.info("Start of initialization phase: Collecting message from output topic")
start_time = time()
collected_msgs = kafka_client.collect_all_msgs_from_topics(settings.KAFKA_OUTPUT_TOPIC)
validated_templates_uuids = {message.key.decode('utf-8') for message in collected_msgs[settings.KAFKA_OUTPUT_TOPIC] 
                             if message.key is not None and message.key != ''}
tot_msg_num = sum([len(v) for v in collected_msgs.values()])
interval_s = round(time()-start_time,2)
logger.debug(f"Collected {tot_msg_num} messages from topic {settings.KAFKA_OUTPUT_TOPIC}, in {interval_s} s")
logger.info(f"Imported {len(validated_templates_uuids)} validated template(s)")
logger.info("End of initialization phase")     

# Consume logs from orchestrator-logs topic
logger.info("Starting to consume messages from orchestrator-logs topic")

collect_template = False
str_template = []

orc_templ_collector = LogOrchestratorCollector(settings.get_values(), logger)

def monitoring_task():
    while True:
        mon = {"rec_orc_log": rec_orc_log,
               "templ_parsed": templ_parsed,
               "msg_sent": msg_sent}
        logger.info(f"Monitoring data: {mon}")
        sleep(MON_PERIOD)
            
monitor_thread = threading.Thread(target=monitoring_task)
monitor_thread.daemon = True
monitor_thread.start()
            
for message in kafka_client.consumer:
    rec_orc_log += 1
    if orc_templ_collector.import_line(message.value):
        enriched_template = orc_templ_collector.get_template_and_user_parameters()
        if enriched_template:
            templ_parsed += 1
            validated_template = TemplateParserMessage(enriched_template, logger)
            validated_template_uuid = validated_template.get_uuid()
            logger.debug(f"Collected the uuid of the current template: {validated_template_uuid}")
            if validated_template_uuid not in validated_templates_uuids:
                kafka_client.send(validated_template.get_dict())
                validated_templates_uuids.add(validated_template_uuid)
                msg_sent += 1
            else:
                logger.debug("Message not sent, template already present in the output topic")
            logger.debug("-------------------------------------------------------------------------------")
                