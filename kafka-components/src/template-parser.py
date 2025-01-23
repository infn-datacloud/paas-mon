# /bin/env python3

# Python dependecies:
# - kafka-python

from modules import kafka_module
from modules import template_parser
from time import time
import json
from datetime import datetime
import os 

# Kafka parameteres
KAFKA_LOG_ORCHESTRATOR_TOPIC = 'TEMPLATE_PARSER_KAFKA_LOG_ORCHESTRATOR_TOPIC'
KAFKA_VAL_TEMPL_TOPIC =        'TEMPLATE_PARSER_KAFKA_VAL_TEMPL_TOPIC'
KAFKA_LOG_TOPIC =              'TEMPLATE_PARSER_KAFKA_LOG_APP_TOPIC'
KAFKA_BOOTSTRAP_SERVERS =      'TEMPLATE_PARSER_KAFKA_BOOTSTRAP_SERVERS'

KAFKA_LOG_ORCHESTRATOR_TOPIC_DEFAULT = 'test'
KAFKA_VAL_TEMPL_TOPIC_DEFAULT =        'validated-templates'
KAFKA_LOG_TOPIC_DEFAULT =              'logs-parser-templates'
KAFKA_BOOTSTRAP_SERVERS_DEFAULT =      '192.168.21.96:9092'

input_topic =       os.environ.get(KAFKA_LOG_ORCHESTRATOR_TOPIC, KAFKA_LOG_ORCHESTRATOR_TOPIC_DEFAULT)
val_templ_topic =   os.environ.get(KAFKA_VAL_TEMPL_TOPIC,        KAFKA_VAL_TEMPL_TOPIC_DEFAULT)
log_topic =         os.environ.get(KAFKA_LOG_TOPIC,              KAFKA_LOG_TOPIC_DEFAULT)
bootstrap_servers = os.environ.get(KAFKA_BOOTSTRAP_SERVERS,      KAFKA_BOOTSTRAP_SERVERS_DEFAULT).split(',')

kafka_module.set_bootstrap_servers(bootstrap_servers)
kafka_module.set_output_topic(val_templ_topic)
kafka_module.set_log_topic(log_topic)

# Import historical messages from topics
start_time = time()
collected_msgs = kafka_module.collect_all_msgs_from_topics(val_templ_topic)
validated_templates = [message.value for message in collected_msgs[val_templ_topic]]
tot_msg_num = sum([len(v) for v in collected_msgs.values()])
interval_s = round(time()-start_time,2)
kafka_module.write_log(msg=f"{tot_msg_num} messages imported in {interval_s} s", status="INIT")
kafka_module.write_log(msg=f"Imported {len(validated_templates)} validated template(s)", status="INIT")

consumer = kafka_module.get_consumer_obj(input_topic
                                         )
collect_template = False
str_template = list()
for message in consumer:
    if template_parser.string_filter not in message.value: 
        continue
    
    kafka_log = datetime.fromtimestamp(float(message.timestamp)/1000)
    log_ts, ts = template_parser.extract_timestamp(message.value)
    line = str(message.value).split(template_parser.app_string_splitter)[1]
    if ts and template_parser.start_template_string in line:
        collect_template = True
        continue
    
    if ts and collect_template:
        collect_template = False

    if not ts and collect_template:
        str_template.append(line) 
    
    if template_parser.event_template_string in line and template_parser.info_template_string in line:
        str_json = line.split(template_parser.event_template_string)[1]
        depl_data = template_parser.extract_user_parameters(str_json)
        template, is_template = template_parser.import_template(str_template)  
        template['timestamp'] = log_ts.strftime(template_parser.app_ts_format)
        if is_template:
            validated_template, err_msg = template_parser.get_validated_template(template, depl_data)
            if not err_msg:
                str_val_templ = json.dumps(validated_template, sort_keys=True)
                if str_val_templ not in validated_templates:
                    kafka_module.write_output_topic_kakfa(validated_template)
                    kafka_module.write_log(uuid=depl_data['uuid'], status="ok", msg="validated")
                else:
                    kafka_module.write_log(uuid=depl_data['uuid'], status="ok", msg="validated and template already present")
            else:
                kafka_module.write_log(uuid=depl_data['uuid'], status="nok", msg=err_msg)
        str_template = list()