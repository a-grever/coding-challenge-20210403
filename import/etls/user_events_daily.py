from datetime import datetime
import os

from database import reports_t_user_events_daily, insert_from_select


select_query = """
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

if __name__ == "__main__":
    import_date = datetime.strptime(os.environ["IMPORT_DATE"], "%Y-%m-%d").date()
    print(f"processing user events for {import_date}")
    res = insert_from_select(
        params={"import_date": import_date},
        target_table=reports_t_user_events_daily,
        select_stmt=select_query,
    )
    print(f"imported user events for {import_date} to {reports_t_user_events_daily.fullname}")
