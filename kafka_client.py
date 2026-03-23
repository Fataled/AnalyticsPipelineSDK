from aiokafka import AIOKafkaProducer
import json
import os
from dotenv import load_dotenv

load_dotenv()
producer = None

async def start_producer():
    """
    Create and start the producter
    :return:
    """
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=[os.getenv('KAFKA_BOOTSRAP_SERVERS')], # This should be wherever kafka is running
        value_serializer=lambda v: json.dumps(v).encode('utf-8') # Converts dicts to json bytes
    )
    await producer.start()

async def stop_producer():
    global producer
    if producer:
        await producer.stop()

async def send_event(topic: str, event: dict):
    """
    Send an event and waits for confirmation that it was received
    """
    await producer.send_and_wait(topic, event)