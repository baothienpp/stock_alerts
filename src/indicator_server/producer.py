from kafka import KafkaProducer
from src.kafka_setting import KAFKA_TOPIC
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda m: json.dumps(m).encode('utf-8'))

message = {'table': '1d',
           'symbols': ['HBI', 'BIZD'], 'period': 100, 'mode': 'append'}

producer.send(KAFKA_TOPIC, message)
producer.flush()
