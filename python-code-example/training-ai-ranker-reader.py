# Python Module dependencies: kafka-python, pandas, json, random, string
# Command to install dependencies: pip install kafka-python pandas 

from kafka import KafkaConsumer
import json
import pandas as pd
import random
import string

# Variables
kafka_topic = 'training-ai-ranker'
kakfa_cluster = '192.168.21.96:9092'
group_id_base = 'training-ai-ranker'

# Generate random group id
group_id = ''.join(random.choices(string.ascii_uppercase +
                                  string.ascii_lowercase +
                                  string.digits, k=64))
print("group_id:", group_id)

# Create Kafka Consumer object
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=[kakfa_cluster],
    group_id=f'{group_id_base}-{group_id}',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x),
    consumer_timeout_ms=500
)

# Import data from Kafka topic (list of dictionaries)
l_data = [message.value for message in consumer]

# Create Pandas dataframe from list of dictionaries
df = pd.DataFrame(l_data)
print(df)
