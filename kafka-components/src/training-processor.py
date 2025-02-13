# /bin/env python3

# Python dependecies:
# - kafka-python

from modules import training_processor as tp
from modules import training_processor_conf as tpc
import os
from modules import kafka_module as km
from time import time
import json
 
# Kafka parameteres
orc_log_topic =     os.environ.get(tpc.KAFKA_ORC_LOG_TOPIC,   
                                   tpc.KAFKA_ORC_LOG_TOPIC_DEFAULT)
ai_infer_topic =    os.environ.get(tpc.KAFKA_AI_INFER_TOPIC,       
                                   tpc.KAFKA_AI_INFER_TOPIC_DEFAULT)
ai_train_topic =    os.environ.get(tpc.KAFKA_AI_TRAIN_TOPIC,      
                                   tpc.KAFKA_AI_TRAIN_TOPIC_DEFAULT)
log_topic =         os.environ.get(tpc.KAFKA_LOG_TOPIC,         
                                   tpc.KAFKA_LOG_TOPIC_DEFAULT)
bootstrap_servers = os.environ.get(tpc.KAFKA_BOOTSTRAP_SERVERS, 
                                   tpc.KAFKA_BOOTSTRAP_SERVERS_DEFAULT).split(',')

km.set_bootstrap_servers(bootstrap_servers)
km.set_output_topic(ai_train_topic)
km.set_log_topic(log_topic)

# Import historical messages from topics
start_time = time()
collected_msgs = km.collect_all_msgs_from_topics(ai_infer_topic, ai_train_topic)

for msg in collected_msgs[ai_train_topic]:
    tp.import_ai_ranker_training_msg(msg.value)

for msg in collected_msgs[ai_infer_topic]:
    tp.import_ai_ranker_inference_msg(msg.value)

tot_msg_num = sum([len(v) for v in collected_msgs.values()])
interval_s = round(time()-start_time,2)
km.write_log(msg=f"{tot_msg_num} messages imported in {interval_s} s", status="INIT")
for k,v in collected_msgs.items():
    km.write_log(msg=f"Collected {len(v)} messages from '{k}' topic", status="INIT")
km.write_log(msg=f"Imported {len(tp.infer_msgs)} provider candidates for deployments", status="INIT")
km.write_log(msg=f"Imported {len(tp.training_sent)} training set entries", status="INIT")

consumer = km.get_consumer_obj(orc_log_topic, ai_infer_topic, deser_format='str')
for kafka_msg in consumer:
    topic = kafka_msg.topic
    msg = kafka_msg.value
    if topic == orc_log_topic:
        if tp.is_line_to_reject(msg): 
            continue
    
        if tp.is_submission_event(msg): # SUBMISSION EVENT
            tp.update_sub_event(msg)
    
        elif tp.is_completed_event(msg): # COMPLETED EVENT
            tp.update_completed_event(msg)
            
        elif tp.is_error_event(msg): # ERROR EVENT
            tp.update_error_event(msg)
    elif topic == ai_infer_topic:
        tp.import_ai_ranker_inference_msg(msg)