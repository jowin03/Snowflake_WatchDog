"""
High-level façade for all metadata operations.
Internally delegates to three sub-services:
  - sensitive_columns
  - grants
  - lineage
"""
from __future__ import annotations
from typing import List, Dict, Any
from dataclasses import dataclass

from app.core.snowflake import get_connection


@dataclass(slots=True)
class SensitiveColumn:
    catalog: str
    schema: str
    table: str
    column: str
    data_type: str


@dataclass(slots=True)
class GrantRecord:
    grantee_name: str
    granted_to: str   # USER or ROLE
    privilege: str
    table_catalog: str
    table_schema: str
    table_name: str


@dataclass(slots=True)
class LineageEdge:
    source_table: str
    source_column: str
    target_table: str
    target_column: str


# ----------------------------------------------------------
# Public API
# ----------------------------------------------------------
class MetadataScanner:
    """
    Thread-safe — every public method opens its own cursor.
    """

    # ---------- 1. Sensitive columns ----------
    @staticmethod
    def sensitive_columns(keywords: List[str] | None = None) -> List[SensitiveColumn]:
        keywords = keywords or {"email", "ssn", "dob", "phone", "passport", "credit_card"}
        like_exprs = [f"%{kw}%" for kw in keywords]

        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT table_catalog,
                   table_schema,
                   table_name,
                   column_name,
                   data_type
            FROM   information_schema.columns
            WHERE  LOWER(column_name) LIKE ANY (%s)
            """,
            (like_exprs,),
        )
        rows = cur.fetchall()
        cur.close()
        return [SensitiveColumn(*r) for r in rows]

    # ---------- 2. Grants ----------
    @staticmethod
    def grants_on_tables() -> List[GrantRecord]:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT grantee_name,
                   granted_to,
                   privilege,
                   table_catalog,
                   table_schema,
                   table_name
            FROM   information_schema.table_privileges
            """
        )
        rows = cur.fetchall()
        cur.close()
        return [GrantRecord(*r) for r in rows]

    # ---------- 3. Lineage ----------
    @staticmethod
    def column_lineage() -> List[LineageEdge]:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT  referenced_object_name  AS source_table,
                    referenced_column_name  AS source_column,
                    object_name             AS target_table,
                    column_name             AS target_column
            FROM    snowflake.account_usage.object_dependencies d
            JOIN    snowflake.account_usage.access_history  h
                   ON d.object_id = h.object_id
            WHERE   d.referenced_object_domain = 'TABLE'
              AND   d.object_domain            = 'VIEW'
            """
        )
        rows = cur.fetchall()
        cur.close()
        return [LineageEdge(*r) for r in rows]