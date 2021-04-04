import os
from typing import Any, List

from sqlalchemy import create_engine, text
from sqlalchemy import MetaData, Table, Column
from sqlalchemy import BigInteger, Integer, Text, DateTime, Boolean, Date
from sqlalchemy.engine.url import URL
from sqlalchemy.dialects import postgresql


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

crm_t_users_dim = Table(
    "users_dim",
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

crm_t_organizations_dim = Table(
    "organizations_dim",
    metadata,
    Column("organization_key", Text, primary_key=True),
    Column("organization_name", Text),
    Column("created_at", DateTime(True)),
    schema="crm",
)

reports_t_user_events_daily = Table(
    "user_events_daily",
    metadata,
    Column("event_date", Date, primary_key=True),
    Column("n_created", Integer),
    Column("n_updated", Integer),
    Column("n_deleted", Integer),
    Column("n_unique", Integer),
    Column("n_total", Integer),
    schema="reports",
)


def get_pg_engine() -> Any:
    """ Return a sqlalchemy engine using the database connection details
    from environment variables.

    Returns
    -------
    sqlalchemy.engine.base.Engine
        db engine
    """
    url = URL(
        drivername="postgresql+psycopg2",
        username=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        host=os.environ["PGHOST"],
        port=5432,
        database=os.environ["PGDATABASE"],
    )
    return create_engine(url, pool_pre_ping=True)


def update_stmt(target_table: Any, update_columns: List) -> Any:
    """If inserting a row triggers e.g. a unique constraint an error is raised.
    If in that case the exisiting row should be updated this function generates
    an insert query with an update logic, e.g.
    INSERT INTO <>
    ON CONFLICT (<primary_key>) DO UPDATE SET created_at = excluded.created_at

    Note that the this is postgres specific. The target_table must have a primary key.

    Parameters
    ----------
    target_table : sqlalchemy.Table
        table to insert into
    update_columns : List[sqlalchemy.Column]
        the columns that are to be updated if a conflict arises

    Returns
    -------
    sa.dialects.postgresql.dml.Insert
        insert statement with conflict handling

    Raises
    ------
    RuntimeError
        if the target table has no primary key
    """
    insert_stmt = postgresql.insert(target_table, inline=True)
    constraint = target_table.primary_key
    if not constraint:
        raise RuntimeError(f"table {target_table.fullname} must have a primary key")
    update_columns_str = [col.name for col in update_columns]
    update_dict = {col.name: col for col in insert_stmt.excluded if col.name in update_columns_str}
    return insert_stmt.on_conflict_do_update(constraint=constraint, set_=update_dict)


def insert_from_select(*, params: dict, target_table: Any, select_stmt: str) -> Any:
    """ Perform and insert statement based on the select stmnt in <select_stmt>.
    The table <target_table> must have a primary key, any row returned by the select
    statement, whose primary key already exist in the target table causes an upsert.

    Parameters
    ----------
    params : dict
        parameters to be inserted in the select_stmt
    target_table : sqlalchemy.Table
        table to insert into
    select_stmt : str
        select query

    Returns
    -------
    Any
        [description]
    """
    target_columns = list(target_table.c)
    upsert_columns = [c for c in target_table.c if not c.primary_key]
    insert_stmt = update_stmt(target_table=target_table, update_columns=upsert_columns)

    ins_from_select_stmt = insert_stmt.from_select(target_columns, text(select_stmt).columns())

    engine = get_pg_engine()
    result = engine.execute(ins_from_select_stmt, params)
    return result
