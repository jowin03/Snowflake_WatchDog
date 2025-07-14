import os
import httpx
from app.models.alert import Alert

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL")

async def send_slack_alert(alert: Alert) -> None:
    """Send a single alert to Slack if webhook is configured."""
    if not SLACK_WEBHOOK:
        return
    color = {"low": "#36a64f", "medium": "#ffcc00",
             "high": "#ff9900", "critical": "#ff0000"}.get(alert.severity, "#cccccc")
    payload = {
        "attachments": [{
            "color": color,
            "title": f"{alert.type} ({alert.severity})",
            "fields": [
                {"title": "User",     "value": alert.user,      "short": True},
                {"title": "Object",   "value": alert.object,    "short": True},
                {"title": "Time",     "value": str(alert.timestamp), "short": False},
            ],
            "text": alert.description
        }]
    }
    async with httpx.AsyncClient() as client:
        await client.post(SLACK_WEBHOOK, json=payload)