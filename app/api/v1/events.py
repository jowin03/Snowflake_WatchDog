from fastapi import APIRouter
from typing import List
from app.models.alert import Alert
from app.services.queries import analyze_query_logs   # or any other service you need

router = APIRouter(prefix="/insider-events", tags=["events"])

@router.get("/", response_model=List[Alert])
def get_insider_events():
    # placeholder – return the same set for demo
    return analyze_query_logs()