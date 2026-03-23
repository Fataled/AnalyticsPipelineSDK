from datetime import datetime
from typing import Any
from pydantic import BaseModel

class Event(BaseModel):
    event_name: str # What happened
    user_id: str # Who did it
    timestamp: datetime # When
    properties: dict[str, Any] = {} # Extra info
