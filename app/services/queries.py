from datetime import datetime, time
from typing import List
from app.models.alert import Alert
from app.core.snowflake import get_connection

def analyze_query_logs() -> List[Alert]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT query_text, user_name, start_time
        FROM snowflake.account_usage.query_history
        WHERE start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP)
    """)
    alerts = []
    for query_text, user, ts in cur.fetchall():
        # naive rule: full table scan
        if "FROM" in query_text.upper() and "WHERE" not in query_text.upper():
            alerts.append(
                Alert(
                    type="FULL_TABLE_SCAN",
                    severity="medium",
                    object=query_text[:50],
                    user=user,
                    timestamp=ts,
                    description="Query without WHERE clause on PII table",
                )
            )
        # off-hours
        if ts.time() < time(6, 0) or ts.time() > time(22, 0):
            alerts.append(
                Alert(
                    type="OFF_HOURS_ACCESS",
                    severity="low",
                    object=query_text[:50],
                    user=user,
                    timestamp=ts,
                    description="Query executed outside business hours",
                )
            )
    cur.close()
    return alerts