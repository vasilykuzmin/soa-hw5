import json
import logging
from confluent_kafka import Producer
from google.protobuf.json_format import MessageToDict
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class KafkaProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 500,
            'request.timeout.ms': 5000,
        }
        self.producer = Producer(conf)

    async def send_event(self, key: str, event):
        event_dict = MessageToDict(event, preserving_proto_field_name=True)
        if 'timestamp' in event_dict and isinstance(event_dict['timestamp'], datetime):
            event_dict['timestamp'] = event_dict['timestamp'].strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        
        value_json = json.dumps(event_dict, ensure_ascii=False)
        value_bytes = value_json.encode('utf-8')
        
        try:
            self.producer.produce(self.topic, key=key, value=value_bytes)
            self.producer.flush()
            logger.info(f"Event sent: key={key}, type={event.event_type}")
        except Exception as e:
            logger.error(f"Failed to send: {e}")
            raise
        return None