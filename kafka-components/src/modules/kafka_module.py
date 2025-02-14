import string
import random
import json
from kafka import KafkaConsumer, KafkaProducer # type: ignore
from datetime import datetime
import pytz

log_topic =  None
output_topic = None
bootstrap_servers = None

BOOTSTRAP_MSG_ERR: str = "Bootstrap_servers is not set"
SYSLOG_TS_FORMAT   = "%Y-%m-%dT%H:%M:%S%z" # YYYY-MM-DDTHH:MM:SS+ZZ:ZZ

def set_bootstrap_servers(b_servers):
    global bootstrap_servers
    bootstrap_servers = b_servers

def set_log_topic(topic):
    global log_topic
    log_topic = topic
    write_log(status="INIT_COMPONENT", msg="")
    write_log(status="INIT_COMPONENT", msg="###############################################")
    write_log(status="INIT_COMPONENT", msg="")
    write_log(status="INIT_COMPONENT", msg="                     INIT                      ")
    write_log(status="INIT_COMPONENT", msg="")
    write_log(status="INIT_COMPONENT", msg="###############################################")
    write_log(status="INIT_COMPONENT", msg="")

def set_output_topic(topic):
    global output_topic
    output_topic = topic

# Write message in kafka topic
def write_msg_to_kafka(data, topic):
    global bootstrap_servers
    if bootstrap_servers is None: 
        print(BOOTSTRAP_MSG_ERR)
        return
    
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=lambda x: json.dumps(x, sort_keys=True).encode('utf-8'))
    if isinstance(data, list):
        for msg in data:
            producer.send(topic, msg)
    else:
        producer.send(topic, data)
    producer.flush()
    producer.close()

# write log in kafka
def write_log_to_kafka(data):
    if log_topic is None:
        print("Log Topic is not set")
        return

    write_msg_to_kafka(data, log_topic)
    
# write validated template in kafka
def write_output_topic_kafka(data):
    if output_topic is not None:
        write_msg_to_kafka(data, output_topic)
    else:
        print("Output Topic is not set")

# Send preformatted message log to kafka
def write_log(status, msg, timestamp=None, uuid=None):
    if timestamp is None:
        ts_now = datetime.now(pytz.timezone('Europe/Rome'))
        timestamp = ts_now.strftime(SYSLOG_TS_FORMAT)
    log = f"timestamp={timestamp}, status={status}"
    if uuid is not None:
        log +=f", uuid={uuid}"
    log+=f", msg={msg}"
    write_log_to_kafka(log)

def collect_all_msgs_from_topics(*topics):
    global bootstrap_servers
    if bootstrap_servers is None: 
        print(BOOTSTRAP_MSG_ERR)
        return
    
    group_id = ''.join(random.choices(string.ascii_uppercase +
                                  string.ascii_lowercase +
                                  string.digits, k=64))
    group_base = '-'.join(topics)
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers = bootstrap_servers,
        group_id = f'{group_base}-{group_id}',
        auto_offset_reset = 'earliest', 
        enable_auto_commit = True,
        value_deserializer = lambda x: json.loads(x.decode('utf-8')),
        max_partition_fetch_bytes = 100_000_000,
        fetch_max_bytes = 50_000_000,
        consumer_timeout_ms = 10000
    )

    collected_msgs = {topic:list() for topic in topics}
    for message in consumer:
        topic = str(message.topic)
        collected_msgs[topic].append(message)
    return collected_msgs

def get_consumer_obj(*topics, deser_format='str'):
    global bootstrap_servers
    if bootstrap_servers is None: 
        print(BOOTSTRAP_MSG_ERR)
        return

    def derserializer(deser_format):
        def decode_str_func(x):
            return x.decode('utf-8')

        def decode_json_func(x):
            return json.loads(x.decode('utf-8'))
        
        if deser_format == 'json':
            return decode_json_func
        elif deser_format == 'str':
            return decode_str_func
        else:
            return decode_str_func
            
    
    deser_func = derserializer(deser_format)
    group_base = '-'.join(topics)
    group_id = ''.join(random.choices(string.ascii_uppercase +
                                      string.ascii_lowercase +
                                      string.digits, k=64))
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers = bootstrap_servers,
        group_id = f'{group_base}-{group_id}',
        auto_offset_reset = 'earliest', 
        enable_auto_commit = True,
        value_deserializer = deser_func
    )

    return consumer

def get_consumer_obj_str(*topics):
    global bootstrap_servers
    if bootstrap_servers is None: 
        print(BOOTSTRAP_MSG_ERR)
        return

    group_base = '-'.join(topics)
    group_id = ''.join(random.choices(string.ascii_uppercase +
                                      string.ascii_lowercase +
                                      string.digits, k=64))
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers = bootstrap_servers,
        group_id = f'{group_base}-{group_id}',
        auto_offset_reset = 'earliest', 
        enable_auto_commit = True,
        value_deserializer = lambda x:x.decode('utf-8')
    )

    return consumer