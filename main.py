from fastapi import FastAPI
from contextlib import asynccontextmanager
from kafka_client import start_producer, stop_producer, send_event
from models import Event

@asynccontextmanager
async def lifespan(app: FastAPI):
    # On start
    await start_producer()
    yield
    # On stop
    await stop_producer()

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/events")
async def ingest_events(event: Event):
    # Convert Event to dict
    event_dict = event.model_dump()

    # Convert datetime to string json cant parse datetime
    event_dict['timestamp'] = event.timestamp.isoformat()

    # Lastly send the event
    await send_event("events", event_dict)
    return {"message": "Event received"}
