from datetime import datetime
import os

from database import crm_t_users_dim, insert_from_select


select_query = """
    SELECT DISTINCT ON (event_sk)
        event_sk,
        id,
        username,
        user_email,
        user_type,
        organization_name,
        plan_name,
        valid_from,
        lead(valid_from, 1) OVER (PARTITION BY id ORDER BY valid_from) AS "valid_to",
        lead(valid_from, 1) OVER (PARTITION BY id ORDER BY valid_from) IS NULL AS "is_valid",
        is_deleted
    FROM (
    (
        --- fetching the currently valid user rows to be invalidated
        SELECT DISTINCT ON (u.id)
            u.event_sk,
            u.id,
            u.username,
            u.user_email,
            u.user_type,
            u.organization_name,
            u.plan_name,
            u.valid_from,
            u.is_deleted
        FROM
            "raw".user_events e
        JOIN
            crm.users_dim u ON e.id=u.id
        WHERE
            u.is_valid
            AND e.received_at::date = :import_date
    )
    UNION ALL
    (
        --- chunk of user events
        SELECT
            event_sk,
            id,
            username,
            user_email,
            user_type,
            organization_name,
            plan_name,
            received_at AS "valid_from",
            event_type = 'User Deleted' AS is_deleted
        FROM
            "raw".user_events
        WHERE
            received_at::date = :import_date
    )
    ) updates
    """

if __name__ == "__main__":
    import_date = datetime.strptime(os.environ["IMPORT_DATE"], "%Y-%m-%d").date()
    print(f"processing user events for {import_date}")
    insert_from_select(
        params={"import_date": import_date}, target_table=crm_t_users_dim, select_stmt=select_query
    )
    print(f"imported user events for {import_date} to {crm_t_users_dim.fullname}")
