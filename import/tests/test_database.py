import pytest
import sqlalchemy as sa

import database


def test_get_pg_engine(pg_env, mock_engine):
    engine = database.get_pg_engine()
    assert str(engine.url) == "postgresql+psycopg2://test-user:test-pwd@test-host:5432/test-db"


def test_update_stmt(pg_env, mock_table):
    engine = database.get_pg_engine()
    stmt = database.update_stmt(target_table=mock_table, update_columns=[mock_table.c.created_at])
    stmnt_values = stmt.values({"id": 1, "created_at": "2020-01-01"})
    compiled = stmnt_values.compile(engine)
    compiled_stmt = compiled.string % {v: repr(k.value) for k, v in compiled.bind_names.items()}
    assert compiled_stmt == (
        f"INSERT INTO {mock_table.fullname} (id, created_at) VALUES "
        r"(1, '2020-01-01') ON CONFLICT (id) DO UPDATE SET created_at = excluded.created_at"
    )


def test_update_stmt_missing_pk(pg_env, mock_table):
    tbl = sa.Table("test_table", sa.MetaData(), sa.Column("id", sa.types.VARCHAR(24)))
    tbl.primary_key = False
    with pytest.raises(RuntimeError):
        database.update_stmt(target_table=tbl, update_columns=[mock_table.c.item])


def test_insert_from_select(mock_table, mocker, mock_engine):
    params = {"limit": 100}
    select_stmt = f"SELECT id FROM {mock_table.fullname} LIMIT :limit"
    database.insert_from_select(
        engine=mock_engine, params=params, target_table=mock_table, select_stmt=select_stmt
    )
    mock_engine.execute.assert_called_with(mocker.ANY, params)
