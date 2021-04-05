from datetime import date

import database as db
import users_dim
import user_events_daily


events = [
    {
        "id": "3e0a16993813100223734db91f7c79d9",
        "event_type": "User Created",
        "username": "Zeus",
        "user_email": "zeus@olympus.org",
        "user_type": "Admin",
        "organization_name": "God of War",
        "plan_name": "Enterprise",
        "received_at": "2020-12-06 22:03:16.759617",
    },
    {
        "id": "3e0a16993813100223734db91f7c79d9",
        "event_type": "User Updated",
        "username": "Zeus",
        "user_email": "zeus@olympus.com",
        "user_type": "Admin",
        "organization_name": "God of War",
        "plan_name": "Enterprise",
        "received_at": "2020-12-08 04:03:02.759617",
    },
    {
        "id": "3e0a16993813100223734db91f7c79d9",
        "event_type": "User Updated",
        "username": "Zeus",
        "user_email": "zeus@olympus.org",
        "user_type": "Admin",
        "organization_name": "God of War",
        "plan_name": "Enterprise",
        "received_at": "2020-12-08 20:03:16.759617",
    },
    {
        "id": "3e0a16993813100223734db91f7c79d9",
        "event_type": "User Deleted",
        "username": "Zeus",
        "user_email": "zeus@olympus.org",
        "user_type": "Admin",
        "organization_name": "God of War",
        "plan_name": "Enterprise",
        "received_at": "2020-12-10 20:03:16.759617",
    },
]

received_dates = [date(2020, 12, 6), date(2020, 12, 8), date(2020, 12, 10)]

user_events_daily_rows = [
    {
        "event_date": date(2020, 12, 6),
        "n_created": 1,
        "n_updated": 0,
        "n_deleted": 0,
        "n_unique": 1,
        "n_total": 1,
    },
    {
        "event_date": date(2020, 12, 8),
        "n_created": 0,
        "n_updated": 2,
        "n_deleted": 0,
        "n_unique": 1,
        "n_total": 2,
    },
    {
        "event_date": date(2020, 12, 6),
        "n_created": 0,
        "n_updated": 0,
        "n_deleted": 1,
        "n_unique": 1,
        "n_total": 1,
    },
]


def insert_user_events(engine):
    insert_stmt = db.raw_t_user_events.insert().values(events)
    engine.execute(insert_stmt)


def test_complete():
    engine = db.get_pg_engine()
    insert_user_events(engine)

    for dt in received_dates:
        users_dim.run(engine=engine, import_date=dt)
        user_events_daily.run(engine=engine, import_date=dt)

    res = engine.execute("SELECT * FROM crm.users_dim WHERE is_valid IS TRUE").fetchall()
    assert len(res) == 1

    rows = engine.execute("SELECT * FROM reports.user_events_daily").fetchall()

    assert len(rows) == 3
    for row in rows:
        dict(row) in user_events_daily_rows
