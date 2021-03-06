from datetime import date
from typing import Any

from database import reports_t_user_events_daily, insert_from_select, get_pg_engine
from common import get_import_date

SELECT_QUERY = """
    SELECT
        received_at::date AS "event_date",
        COUNT(*) FILTER(WHERE event_type = 'User Created') AS "n_created",
        COUNT(*) FILTER(WHERE event_type = 'User Updated') AS "n_updated",
        COUNT(*) FILTER(WHERE event_type = 'User Deleted') AS "n_deleted",
        COUNT(distinct id) AS "n_unique",
        COUNT(*) AS "n_total"
    FROM
        "raw".user_events
    WHERE
        received_at::date = :import_date
    GROUP BY
        1
    """


def run(*, engine: Any, import_date: date) -> None:
    """ Fill the table reports.user_events_daily with data from a single day.

    Parameters
    ----------
    sqlalchemy.engine.base.Engine
        db engine
    import_date : date
        date for which to import events
    """
    print(f"processing user events for {import_date}")
    insert_from_select(
        engine=engine,
        params={"import_date": import_date},
        target_table=reports_t_user_events_daily,
        select_stmt=SELECT_QUERY,
    )
    print(f"imported user events for {import_date} to {reports_t_user_events_daily.fullname}")


if __name__ == "__main__":  # pragma: no cover
    run(engine=get_pg_engine(), import_date=get_import_date())
