"""
Singleton Snowflake connector that:
  • Reads config from Pydantic settings
  • Auto-reconnects on dropped connections
  • Retries transient errors (network blips)
"""
from __future__ import annotations
import logging
import time
from typing import Any

import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import DatabaseError, OperationalError

from .config import settings

logger = logging.getLogger("snowflake")

# ------------------------------------------------------------------
# Retry decorator
# ------------------------------------------------------------------
def _retry(
    func,
    retries: int = 3,
    backoff: float = 1.0,
    exceptions=(DatabaseError, OperationalError),
):
    for attempt in range(1, retries + 1):
        try:
            return func()
        except exceptions as exc:
            logger.warning("Snowflake transient error: %s (attempt %s)", exc, attempt)
            if attempt == retries:
                raise
            time.sleep(backoff * attempt)


# ------------------------------------------------------------------
# Singleton connection holder
# ------------------------------------------------------------------
class _ConnectionMgr:
    _instance: SnowflakeConnection | None = None

    @classmethod
    def _new_connection(cls) -> SnowflakeConnection:
        logger.info("Opening new Snowflake connection …")
        return snowflake.connector.connect(
            account=settings.snowflake_account,
            user=settings.snowflake_user,
            password=settings.snowflake_password,
            database=settings.snowflake_database,
            warehouse=settings.snowflake_warehouse,
            role=settings.snowflake_role,
            session_parameters={
                "QUERY_TAG": "snowwatch",
                "TIMEZONE": "UTC",
            },
        )

    @classmethod
    def get(cls) -> SnowflakeConnection:
        """
        Returns an *open* connection. If the existing one is closed,
        automatically builds a new one.
        """
        if cls._instance is None or not cls._instance.is_closed():
            cls._instance = _retry(cls._new_connection)
        return cls._instance


# Public façade
def get_connection() -> SnowflakeConnection:
    return _ConnectionMgr.get()