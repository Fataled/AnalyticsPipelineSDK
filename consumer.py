from aiokafka import AIOKafkaConsumer
from clickhouse_client import setup_table, insert_event
import json
import asyncio

async def consume():

    setup_table()

    consumer = AIOKafkaConsumer(
        "events",
        bootstrap_servers=["localhost:9092"],
        group_id="analytics-consumer",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    await consumer.start()
    print("consumer started, waiting for events...")

    try:
        async for message in consumer:
            event = message.value
            print(f"received event: {event}")
            await process_events(event)
    finally:
        # Always stop the consumer even in case of errors
        await consumer.stop()

async def process_events(event: dict):
    insert_event(event)
    print(f"Stored: {event['event_name']} from user {event['user_id']}")

if __name__ == "__main__":
    asyncio.run(consume())