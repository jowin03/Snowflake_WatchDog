from fastapi import APIRouter
from app.models.alert import Alert
from app.services.queries import analyze_query_logs
# from app.services.access import excessive_privileges
from app.services.notifier import send_slack_alert

router = APIRouter(prefix="/access-risks", tags=["risks"])

@router.get("/", response_model=list[Alert])
async def get_access_risks():
    alerts = analyze_query_logs() 
    # fire Slack for anything high/critical
    for a in alerts:
        if a.severity in {"high", "critical"}:
            await send_slack_alert(a)
    return alerts