from typing import List
from app.models.alert import Alert
from app.core.snowflake import get_connection

def excessive_privileges() -> List[Alert]:
    """
    Detect roles/users that have *too many* privileges on sensitive tables.
    For the demo we just return an empty list so the app starts.
    """
    return []