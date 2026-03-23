import clickhouse_connect
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# ENGINE = MergeTree() — ClickHouse's main table engine, optimized for inserts and analytics queries. It merges data in the background to keep things fast
# ORDER BY timestamp — ClickHouse physically sorts data by this column on disk, making time-based queries very fast
# CREATE TABLE IF NOT EXISTS — safe to run every time, won't fail if the table already exists
# properties as String — for now we're storing it as a plain string, later we can use ClickHouse's Map type for proper key-value storage

client = clickhouse_connect.get_client(
    host=os.getenv('CLICKHOUSE_HOST'),
    port=os.getenv('CLICKHOUSE_PORT'),
    username=os.getenv('CLICKHOUSE_USER'),
    password=os.getenv('CLICKHOUSE_PASSWORD'),
)

def setup_table():
    # Create the events table if one doesn't exist
    client.command("""
                   CREATE TABLE IF NOT EXISTS events
                   (
                       event_name
                       String,
                       user_id
                       String,
                       timestamp
                       DateTime,
                       properties
                       String
                   ) ENGINE = MergeTree 
                   (
                   )
                       ORDER BY timestamp
                   """)

def insert_event(event: dict):
    timestamp = datetime.fromisoformat(event["timestamp"])

    client.insert("events", [[
        event["event_name"],
        event["user_id"],
        timestamp,
        str(event["properties"])  # store properties as a string for now
    ]], column_names=["event_name", "user_id", "timestamp", "properties"])

