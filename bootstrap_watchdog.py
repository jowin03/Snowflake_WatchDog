#!/usr/bin/env python3
"""
Bootstrap WATCHDOG.MAIN with realistic data + grants + queries
so that Snowwatch can detect real alerts.
"""
import os
import datetime as dt
import snowflake.connector
from faker import Faker
from dotenv import load_dotenv

load_dotenv()
fake = Faker()

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    role="ACCOUNTADMIN",
)

cur = conn.cursor()

# ------------------------------------------------------------------
# 1. Ensure database & schema exist
# ------------------------------------------------------------------
cur.execute("CREATE DATABASE IF NOT EXISTS WATCHDOG")
cur.execute("USE DATABASE WATCHDOG")
cur.execute("CREATE SCHEMA IF NOT EXISTS MAIN")
cur.execute("USE SCHEMA MAIN")

# ------------------------------------------------------------------
# 2. Sensitive tables
# ------------------------------------------------------------------
cur.execute(
    """
    CREATE OR REPLACE TABLE CUSTOMERS (
        id            INT PRIMARY KEY,
        full_name     STRING,
        email         STRING,
        ssn           STRING,
        dob           DATE,
        credit_card   STRING,
        phone         STRING,
        created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
)

cur.execute(
    """
    CREATE OR REPLACE TABLE ORDERS (
        order_id    INT PRIMARY KEY,
        customer_id INT,
        amount      NUMBER(10,2),
        order_date  TIMESTAMP
    )
    """
)

# ------------------------------------------------------------------
# 3. Populate with 100 realistic PII rows
# ------------------------------------------------------------------
customers = []
orders = []

for i in range(1, 101):
    customers.append((
        i,
        fake.name(),
        fake.email(),
        fake.ssn(),
        fake.date_of_birth(minimum_age=18, maximum_age=75),
        fake.credit_card_number(),
        fake.phone_number(),
    ))
    orders.append((
        i,
        i,
        round(fake.random_int(min=10, max=5000), 2),
        fake.date_time_this_year(),
    ))

cur.executemany(
    "INSERT INTO CUSTOMERS (id, full_name, email, ssn, dob, credit_card, phone) "
    "VALUES (%s,%s,%s,%s,%s,%s,%s)",
    customers,
)
cur.executemany(
    "INSERT INTO ORDERS (order_id, customer_id, amount, order_date) "
    "VALUES (%s,%s,%s,%s)",
    orders,
)

# ------------------------------------------------------------------
# 4. View that joins CUSTOMERS + ORDERS -> lineage edge
# ------------------------------------------------------------------
cur.execute(
    """
    CREATE OR REPLACE VIEW CUSTOMER_ORDERS_V AS
    SELECT c.id,
           c.email,
           c.ssn,
           o.amount,
           o.order_date
    FROM   CUSTOMERS c
    JOIN   ORDERS o ON c.id = o.customer_id
    """
)

# ------------------------------------------------------------------
# 5. Grants to roles -> access-risk findings
# ------------------------------------------------------------------
roles = ["ANALYST_ROLE", "VIEWER_ROLE"]
for r in roles:
    cur.execute(f"CREATE ROLE IF NOT EXISTS {r}")
    cur.execute(f"GRANT ROLE {r} TO USER {os.getenv('SNOWFLAKE_USER')}")

# ANALYST gets SELECT on everything
cur.execute("GRANT USAGE ON DATABASE WATCHDOG TO ROLE ANALYST_ROLE")
cur.execute("GRANT USAGE ON SCHEMA WATCHDOG.MAIN TO ROLE ANALYST_ROLE")
cur.execute("GRANT SELECT ON ALL TABLES IN SCHEMA WATCHDOG.MAIN TO ROLE ANALYST_ROLE")
cur.execute("GRANT SELECT ON ALL VIEWS  IN SCHEMA WATCHDOG.MAIN TO ROLE ANALYST_ROLE")

# VIEWER only on the view
cur.execute("GRANT USAGE ON DATABASE WATCHDOG TO ROLE VIEWER_ROLE")
cur.execute("GRANT USAGE ON SCHEMA WATCHDOG.MAIN TO ROLE VIEWER_ROLE")
cur.execute("GRANT SELECT ON VIEW CUSTOMER_ORDERS_V TO ROLE VIEWER_ROLE")

# ------------------------------------------------------------------
# 6. Risky queries to populate ACCOUNT_USAGE.QUERY_HISTORY
#    (executed right now so that scanner finds them)
# ------------------------------------------------------------------
risky_queries = [
    "SELECT * FROM CUSTOMERS",                          # full table scan
    "SELECT email, ssn FROM CUSTOMERS",                 # PII export
    "SELECT * FROM CUSTOMER_ORDERS_V WHERE 1=1",        # view scan
]

for q in risky_queries:
    cur.execute(q)

# ------------------------------------------------------------------
# 7. Off-hours query (simulate night-time access)
# ------------------------------------------------------------------
cur.execute(
    """
    SELECT email, credit_card
    FROM   CUSTOMERS
    WHERE  created_at < DATEADD(day, -30, CURRENT_TIMESTAMP)
    """
)

print("✅ WATCHDOG.MAIN is ready with sensitive data, lineage, and risky queries.")
cur.close()
conn.close()