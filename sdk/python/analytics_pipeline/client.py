from typing import Any
import httpx
from datetime import datetime

class AnalyticsClient:
    def __init__(self, base_url: str):
        self.base_url = base_url

    async def ingest_event(self, event_name: str, user_id: str, timestamp: datetime = None, properties: dict[str, Any] = None):
        if timestamp is None:
            timestamp = datetime.now()
        if properties is None:
            properties = {}

        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.base_url}/events/", json={
                "event_name": event_name,
                "user_id": user_id,
                "timestamp": timestamp,
                "properties": properties
            })
            response.raise_for_status()
            return response