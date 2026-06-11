"""The adapter owns SET LOCAL statement_timeout (routes hold no SQL session)."""

from datetime import timedelta

from osa.infrastructure.data.postgres_data_read_store import statement_timeout_sql


def test_statement_timeout_sql_renders_integer_milliseconds() -> None:
    assert statement_timeout_sql(timedelta(seconds=30)) == "SET LOCAL statement_timeout = '30000ms'"
    assert (
        statement_timeout_sql(timedelta(minutes=30)) == "SET LOCAL statement_timeout = '1800000ms'"
    )


def test_statement_timeout_sql_truncates_subsecond() -> None:
    # int milliseconds — no operator-supplied string ever reaches raw SQL.
    assert (
        statement_timeout_sql(timedelta(milliseconds=1500.7))
        == "SET LOCAL statement_timeout = '1500ms'"
    )
