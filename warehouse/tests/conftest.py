import pytest
import sqlalchemy as sa


@pytest.fixture
def pg_env(monkeypatch):
    monkeypatch.setenv("PGHOST", "test-host")
    monkeypatch.setenv("PGUSER", "test-user")
    monkeypatch.setenv("PGDATABASE", "test-db")
    monkeypatch.setenv("PGPASSWORD", "test-pwd")


@pytest.fixture
def mock_engine(mocker, monkeypatch):
    return mocker.patch("sqlalchemy.engine.base.Engine", autospec=True)


@pytest.fixture()
def mock_table(schema="test_raw"):
    """
    A test table
    """
    t = sa.Table(
        "test_table",
        sa.MetaData(),
        sa.Column("id", sa.types.VARCHAR(24), primary_key=True),
        sa.Column("item", sa.dialects.postgresql.JSONB),
        sa.Column("created_at", sa.types.TIMESTAMP(timezone=True)),
    )
    return t
