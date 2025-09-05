# /bin/env python3

# Python dependecies:
# - kafka-python

import json
import glob
from datetime import datetime, timedelta
from time import time
from app_kafka import KafkaClient
from app_conf import Configuration

configs = Configuration()
logger = configs.get_logger()
logger.info(configs)

def collect_data(configs):
    logger.debug('Collecting rally check results from directory')
    records = set()
    for filename in glob.glob(f"{configs.data_dir}/*/*.json"):
        logger.debug(f'Reading file {filename}...')
        if "test" in filename: continue
        with open(filename) as f:
            json_data = json.loads(f.read())
            str_ts = json_data['info']['generated_at']
            ts = datetime.strptime(str_ts, '%Y-%m-%dT%H:%M:%S')
            if (datetime.now() - ts) > timedelta(days=30): continue
            
            record = dict(
                timestamp = str_ts,
                msg_version = "1.0.0",
                status = json_data['tasks'][0]['status'],
                tag = json_data['tasks'][0]['tags'][0],
                provider = json_data['tasks'][0]['env_name'],
                test_result = str(json_data['tasks'][0]['pass_sla'])
            )
            records.add(json.dumps(record))
    return records

def get_historical_data(kafka_client):
    msgs = kafka_client.get_all_msgs_from_input_topics()
    rally_msgs = {msg.value for msg in msgs}
    logger.debug(f"Collected {len(msgs)} messages from Kafka and got {len(rally_msgs)} after deduplication")
    return rally_msgs

def start_task():
    # Instanciate kafka_client object (used to communicate with kafka)
    kafka_client = KafkaClient(configs)
    
    # Parse rally test files
    new_data: set[str] = collect_data(configs)
    logger.info(f"Imported {len(new_data)} message from Rally directory parsing")
    
    # Extract rally check results not in Kafka
    new_records: set[str] = new_data # - stored_data
    logger.info(f"Collected {len(new_records)} new records")
    
    # Write in Kafka the new rally check results
    kafka_client.send_messages(new_records)

if __name__ == "__main__":
    logger.info(f"Start")
    start_time = time()
    start_task()
    logger.info(f"Execution time {time()-start_time:.2f}s")
    logger.info("-"*50)