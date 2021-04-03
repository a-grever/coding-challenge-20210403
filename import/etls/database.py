import os

from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, Column
from sqlalchemy import BigInteger, Text, DateTime, Boolean
from sqlalchemy.engine.url import URL


def get_pg_engine():
    url = URL(
        drivername="postgresql+psycopg2",
        username=os.environ['PGUSER'],
        password=os.environ['PGPASSWORD'],
        host=os.environ['PGHOST'],
        port=5432,
        database=os.environ['PGDATABASE'],
    )
    return create_engine(url, pool_pre_ping=True)


metadata = MetaData()

raw_t_user_events = Table(
    "user_events",
    metadata,
    Column("event_sk", BigInteger, autoincrement=True, primary_key=True),
    Column("id", Text),
    Column("event_type", Text),
    Column("username", Text),
    Column("user_email", Text),
    Column("user_type", Text),
    Column("organization_name", Text),
    Column("plan_name", Text),
    Column("received_at", DateTime(True)),
    schema="raw",
)

crm_t_users = Table(
    "users",
    metadata,
    Column("event_sk", BigInteger, autoincrement=False, primary_key=True),
    Column("id", Text),
    Column("username", Text),
    Column("user_email", Text),
    Column("user_type", Text),
    Column("organization_name", Text),
    Column("plan_name", Text),
    Column("valid_from", DateTime(True)),
    Column("valid_to", DateTime(True)),
    Column("is_valid", Boolean),
    Column("is_deleted", Boolean),
    schema="crm",
)

crm_t_organizations = Table(
    "organizations",
    metadata,
    Column("organization_key", Text, primary_key=True),
    Column("organization_name", Text),
    Column("created_at", DateTime(True)),
    schema="crm",
)
