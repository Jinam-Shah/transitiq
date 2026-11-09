"""Publishing gold to Postgres. Full tests need TEST_POSTGRES_URL (set in CI); plan checks don't."""

import os

import duckdb
import pytest
from transitiq_pipelines.gold import publish_pg as P

PG_URL = os.getenv("TEST_POSTGRES_URL")
needs_pg = pytest.mark.skipif(not PG_URL, reason="TEST_POSTGRES_URL not set")


@pytest.fixture
def duck(tmp_path):
    path = tmp_path / "w.duckdb"
    c = duckdb.connect(str(path))
    c.execute("create schema gold")
    c.execute("""
        create table gold.dim_route as
        select 'R' || i as route_id, i::integer as route_type, i % 2 = 0 as is_even,
               (date '2026-01-01' + to_days(i))::date as d,
               timestamptz '2026-01-01 00:00:00+00' as ts,
               (i * 1.5)::double as x
        from range(5) t(i)
    """)
    c.close()
    return path


SPEC = (P.PublishSpec("gold.dim_route", ("route_id",), 100),)


def test_plan_rejects_missing_table_and_row_limit(duck):
    c = duckdb.connect(str(duck), read_only=True)
    with pytest.raises(P.PublishError, match="does not exist"):
        P.plan(c, (P.PublishSpec("gold.nope", None, 10),))
    with pytest.raises(P.PublishError, match="exceeds limit"):
        P.plan(c, (P.PublishSpec("gold.dim_route", None, 3),))
    assert P.plan(c, (P.PublishSpec("ops.missing", None, 10, optional=True),)) == []


def test_unknown_types_are_rejected(tmp_path):
    path = tmp_path / "t.duckdb"
    c = duckdb.connect(str(path))
    c.execute("create schema gold; create table gold.t as select 1.5 as x")  # DECIMAL
    with pytest.raises(P.PublishError, match="no Postgres type"):
        P.plan(c, (P.PublishSpec("gold.t", None, 10),))


def test_targets(monkeypatch):
    monkeypatch.delenv("NEON_DATABASE_URL", raising=False)
    monkeypatch.setenv("TRANSITIQ_PG_URL", "postgresql://local")
    assert P.targets("auto") == [("local", "postgresql://local")]
    with pytest.raises(P.PublishError):
        P.targets("neon")
    monkeypatch.setenv("NEON_DATABASE_URL", "postgresql://neon")
    assert [t for t, _ in P.targets("auto")] == ["local", "neon"]


@needs_pg
def test_publish_is_atomic_typed_and_idempotent(duck):
    import psycopg

    for _ in range(2):
        tables, size = P.publish(duck, PG_URL, specs=SPEC)
        assert tables == [("dim_route", 5)] and size > 0
    with psycopg.connect(PG_URL) as pg, pg.cursor() as cur:
        cur.execute("select count(*), sum(route_type) from gold.dim_route")
        assert cur.fetchone() == (5, 10)
        cur.execute(
            "select data_type from information_schema.columns where table_schema='gold' "
            "and table_name='dim_route' order by ordinal_position"
        )
        assert [r[0] for r in cur.fetchall()] == [
            "text",
            "integer",
            "boolean",
            "date",
            "timestamp with time zone",
            "double precision",
        ]
        cur.execute("select count(*) from information_schema.tables where table_name like '%__new'")
        assert cur.fetchone()[0] == 0


@needs_pg
def test_budget_exceeded_is_an_error(duck):
    with pytest.raises(P.PublishError, match="budget"):
        P.publish(duck, PG_URL, budget_bytes=1, specs=SPEC)
