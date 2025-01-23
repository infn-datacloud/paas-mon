# /bin/env python3

# Python dependecies:
# - kafka-python

import os
import kafka_module
import infer_processor
from time import time

# Kafka parameteres
KAFKA_VAL_TEMPL_TOPIC =   'INFER_PROC_KAFKA_VAL_TEMPL_TOPIC'
KAFKA_RALLY_TOPIC =       'INFER_PROC_KAFKA_RALLY_TOPIC'
KAFKA_FEEDER_TOPIC =      'INFER_PROC_KAFKA_FEEDER_TOPIC'
KAFKA_INFER_TOPIC =       'INFER_PROC_KAFKA_INFER_TOPIC'
KAFKA_LOG_TOPIC =         'INFER_PROC_KAFKA_LOG_TOPIC'
KAFKA_BOOTSTRAP_SERVERS = 'INFER_PROC_KAFKA_BOOTSTRAP_SERVERS'

KAFKA_VAL_TEMPL_TOPIC_DEFAULT =   'validated-templates'
KAFKA_RALLY_TOPIC_DEFAULT =       'rally'
KAFKA_FEEDER_TOPIC_DEFAULT =      'federation-registry-feeder'
KAFKA_INFER_TOPIC_DEFAULT =       'ai-ranker-inference'
KAFKA_LOG_TOPIC_DEFAULT =         'logs-inference-processor'
KAFKA_BOOTSTRAP_SERVERS_DEFAULT = '192.168.21.96:9092'

# Kafka parameteres
val_templ_topic =    os.environ.get(KAFKA_VAL_TEMPL_TOPIC,   KAFKA_VAL_TEMPL_TOPIC_DEFAULT)
rally_topic =        os.environ.get(KAFKA_RALLY_TOPIC,       KAFKA_RALLY_TOPIC_DEFAULT)
feeder_topic =       os.environ.get(KAFKA_FEEDER_TOPIC,      KAFKA_FEEDER_TOPIC_DEFAULT)
output_infer_topic = os.environ.get(KAFKA_INFER_TOPIC,       KAFKA_INFER_TOPIC_DEFAULT)
log_topic =          os.environ.get(KAFKA_LOG_TOPIC,         KAFKA_LOG_TOPIC_DEFAULT)
bootstrap_servers =  os.environ.get(KAFKA_BOOTSTRAP_SERVERS, KAFKA_BOOTSTRAP_SERVERS_DEFAULT).split(',')

kafka_module.set_bootstrap_servers(bootstrap_servers)
kafka_module.set_output_topic(output_infer_topic)
kafka_module.set_log_topic(log_topic)

# Import historical messages from topics
start_time = time()
collected_msgs = kafka_module.collect_all_msgs_from_topics(rally_topic, feeder_topic,output_infer_topic)
rally_msgs = [message.value for message in collected_msgs[rally_topic]]
for message in collected_msgs[feeder_topic]:
    infer_processor.update_providers_data(message)
output_msgs_uuid = {message.value['uuid'] for message in collected_msgs[output_infer_topic]}
tot_msg_num = sum([len(v) for v in collected_msgs.values()])
interval_s = round(time()-start_time,2)
kafka_module.write_log(msg=f"{tot_msg_num} messages imported in {interval_s} s", status="INIT")
for k,v in collected_msgs.items():
    kafka_module.write_log(msg=f"Collected {len(v)} messages from '{k}' topic", status="INIT")
kafka_module.write_log(msg=f"Imported data for {len(infer_processor.prov_data)} provider(s)", status="INIT")
kafka_module.write_log(msg=f"Imported {len(output_msgs_uuid)} uuid(s)", status="INIT")

# Start Processing
consumer = kafka_module.get_consumer_obj(rally_topic, feeder_topic, val_templ_topic)

for message in consumer:
    topic = str(message.topic)
    if topic == rally_topic:
        rally_msgs.append(message.value)
    elif topic == feeder_topic:
        infer_processor.update_providers_data(message)
    elif topic == val_templ_topic:
        dep_data = infer_processor.extract_data_from_valid_template(message.value)
        dep_data = infer_processor.best_match_finder(dep_data, infer_processor.prov_data)
        dep_data = infer_processor.remove_null(dep_data)
        dep_data = infer_processor.compute_aggregated_resource(dep_data)
        msg = infer_processor.get_msg(dep_data)
        if msg['uuid'] not in output_msgs_uuid:
            kafka_module.write_output_topic_kakfa(msg)
            kafka_module.write_log(msg="Processed and sent", uuid=msg['uuid'], status="PROCESSED-SENT")
        else:
            kafka_module.write_log(msg="Already present in topic", uuid=msg['uuid'], status="PROCESSED-NOT-SENT")