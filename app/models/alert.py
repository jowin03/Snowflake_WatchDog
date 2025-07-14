from datetime import datetime
from typing import Literal
from pydantic import BaseModel

Severity = Literal["low", "medium", "high", "critical"]

class Alert(BaseModel):
    type: str
    severity: Severity
    object: str
    user: str
    timestamp: datetime
    description: str
    metadata: dict = {}