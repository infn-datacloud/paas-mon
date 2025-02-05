# /bin/env python3

# Python dependecies:
# - kafka-python

from time import time
import os 
from modules import kafka_module as km
from modules import orc_log_processor as olp
from modules import orc_log_processor_conf as olc
 
input_topic       = os.environ.get(olc.KAFKA_LOG_ORCHESTRATOR_TOPIC, 
                                   olc.KAFKA_LOG_ORCHESTRATOR_TOPIC_DEFAULT)
val_templ_topic   = os.environ.get(olc.KAFKA_VAL_TEMPL_TOPIC,        
                                   olc.KAFKA_VAL_TEMPL_TOPIC_DEFAULT)
log_topic         = os.environ.get(olc.KAFKA_LOG_TOPIC,              
                                   olc.KAFKA_LOG_TOPIC_DEFAULT)
bootstrap_servers = os.environ.get(olc.KAFKA_BOOTSTRAP_SERVERS,      
                                   olc.KAFKA_BOOTSTRAP_SERVERS_DEFAULT).split(',')

km.set_bootstrap_servers(bootstrap_servers)
km.set_output_topic(val_templ_topic)
km.set_log_topic(log_topic)

km.write_log(msg=f"Input topic: {input_topic}", status="INIT")
km.write_log(msg=f"Validated topic: {val_templ_topic}", status="INIT")

# Import historical messages from topics
start_time = time()
collected_msgs = km.collect_all_msgs_from_topics(val_templ_topic)
validated_templates = [message.value for message in collected_msgs[val_templ_topic]]
tot_msg_num = sum([len(v) for v in collected_msgs.values()])
interval_s = round(time()-start_time,2)
km.write_log(msg=f"{tot_msg_num} messages imported in {interval_s} s", status="INIT")
km.write_log(msg=f"Imported {len(validated_templates)} validated template(s)", status="INIT")

consumer = km.get_consumer_obj(input_topic, decode_json=False)
collect_template = False
str_template = list()
for message in consumer:
    if olp.is_line_to_reject(message): 
       continue

    log_ts, ts = olp.extract_timestamp(message)
    line = olp.extract_info(message)

    if olp.is_start_to_collect(ts, line):
        collect_template = True
        continue
    
    if ts and collect_template:
        collect_template = False

    if not ts and collect_template:
        str_template.append(line) 

    if olp.is_template_meta_data(line):
        depl_data = olp.extract_user_parameters(line)
        uuid = olp.get_uuid(depl_data)
        template, is_template = olp.import_template(str_template)  
        template = olp.add_timestamp(template, ts)
        if is_template:
            validated_template, err_msg = olp.get_val_templ(template, depl_data)
            if not err_msg:
                if validated_template not in validated_templates:
                    km.write_output_topic_kakfa(validated_template)
                    km.write_log(uuid=uuid, status=olc.LOG_STATUS_OK, msg=olc.LOG_MSG_VALIDATED)
                else:
                    km.write_log(uuid=uuid, status=olc.LOG_STATUS_OK, msg=olc.LOG_MSG_VALIDATED_AND_SENT)
            else:
                km.write_log(uuid=uuid, status=olc.LOG_STATUS_NOK, msg=err_msg)
        str_template = list()