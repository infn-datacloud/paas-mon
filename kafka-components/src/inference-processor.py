# /bin/env python3

# Python dependecies:
# - kafka-python

import os
from modules import kafka_module as km
from modules import infer_processor as ip
from modules import infer_processor_conf as ipc
from time import time
 
# Kafka parameteres
val_templ_topic =    os.environ.get(ipc.KAFKA_VAL_TEMPL_TOPIC,   
                                    ipc.KAFKA_VAL_TEMPL_TOPIC_DEFAULT)
rally_topic =        os.environ.get(ipc.KAFKA_RALLY_TOPIC,       
                                    ipc.KAFKA_RALLY_TOPIC_DEFAULT)
feeder_topic =       os.environ.get(ipc.KAFKA_FEEDER_TOPIC,      
                                    ipc.KAFKA_FEEDER_TOPIC_DEFAULT)
output_infer_topic = os.environ.get(ipc.KAFKA_INFER_TOPIC,       
                                    ipc.KAFKA_INFER_TOPIC_DEFAULT)
log_topic =          os.environ.get(ipc.KAFKA_LOG_TOPIC,         
                                    ipc.KAFKA_LOG_TOPIC_DEFAULT)
bootstrap_servers =  os.environ.get(ipc.KAFKA_BOOTSTRAP_SERVERS, 
                                    ipc.KAFKA_BOOTSTRAP_SERVERS_DEFAULT).split(',')

km.set_bootstrap_servers(bootstrap_servers)
km.set_output_topic(output_infer_topic)
km.set_log_topic(log_topic)

# Import historical messages from topics
start_time = time()
collected_msgs = km.collect_all_msgs_from_topics(rally_topic, feeder_topic,output_infer_topic)
rally_msgs = [message.value for message in collected_msgs[rally_topic]]
for message in collected_msgs[feeder_topic]:
    ip.update_providers_data(message)
output_msgs_uuid = {message.value['uuid'] for message in collected_msgs[output_infer_topic]}
tot_msg_num = sum([len(v) for v in collected_msgs.values()])
interval_s = round(time()-start_time,2)
km.write_log(msg=f"{tot_msg_num} messages imported in {interval_s} s", status="INIT")
for k,v in collected_msgs.items():
    km.write_log(msg=f"Collected {len(v)} messages from '{k}' topic", status="INIT")
km.write_log(msg=f"Imported data for {len(ip.prov_data)} provider(s)", status="INIT")
km.write_log(msg=f"Imported {len(output_msgs_uuid)} uuid(s)", status="INIT")

# Start Processing
consumer = km.get_consumer_obj(rally_topic, feeder_topic, val_templ_topic, deser_format='json')

for message in consumer:
    topic = str(message.topic)
    if topic == rally_topic:
        rally_msgs.append(message.value)
    elif topic == feeder_topic:
        ip.update_providers_data(message)
    elif topic == val_templ_topic:
        dep_data = ip.extract_data_from_valid_template(message.value)
        dep_data = ip.best_match_finder(dep_data)
        dep_data = ip.remove_null(dep_data)
        dep_data = ip.compute_aggregated_resource(dep_data)    
        msg = ip.get_msg(dep_data)
        if msg['uuid'] not in output_msgs_uuid:
            km.write_output_topic_kakfa(msg)
            km.write_log(msg="Processed and sent", uuid=msg['uuid'], status="PROCESSED-SENT")
        else:
            km.write_log(msg="Already present in topic", uuid=msg['uuid'], status="PROCESSED-NOT-SENT")