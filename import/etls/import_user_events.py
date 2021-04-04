from datetime import datetime, date
import json
from pathlib import Path
import os

from database import get_pg_engine, raw_t_user_events

data_folder = Path(__file__).parent.parent.parent / "data"


def get_user_events():
    """ read sample user events from json and yield single entries

    Yields
    -------
    dict
        single organisation row
    """
    events_sample = data_folder / "events_sample.json"
    if not events_sample.exists():
        print(f"couldn't find the file {events_sample.resolve()}")
    with events_sample.open("r") as f:
        user_events = json.load(f)
    yield from user_events


def import_user_events_single_day(received_date: date) -> None:
    """ import organisation from sample json to database for data <received_date>.

    Parameters
    ----------
    received_date : date
        date for which to import events
    """
    engine = get_pg_engine()
    user_events_single_day = (
        user_event
        for user_event in get_user_events()
        if datetime.fromisoformat(user_event["received_at"]).date() == received_date
    )
    insert_stmt = raw_t_user_events.insert(dml=None).values(list(user_events_single_day))
    res = engine.execute(insert_stmt)
    print(f"imported rows: {res.rowcount}")


def import_user_events() -> None:
    """ import organisation from sample json to database
    """
    engine = get_pg_engine()
    insert_stmt = raw_t_user_events.insert(dml=None).values(list(get_user_events()))
    res = engine.execute(insert_stmt)
    print(f"imported rows: {res.rowcount}")


if __name__ == "__main__":
    if os.environ.get("IMPORT_DATE"):
        import_date = datetime.strptime(os.environ["IMPORT_DATE"], "%Y-%m-%d").date()
        print(f"importing user events for {import_date}")
        import_user_events_single_day(received_date=import_date)
    else:
        print("IMPORT_DATE not specified, importing all user events")
        import_user_events()
