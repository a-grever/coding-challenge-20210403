import json
from pathlib import Path
from typing import Generator

from database import get_pg_engine, crm_t_organizations_dim

data_folder = Path(__file__).parent.parent.parent / "data"


def get_organisations() -> Generator[dict, None, None]:
    """ read sample organisations from json and yield single entries

    Yields
    -------
    dict
        single organisation row
    """
    org_sample = data_folder / "orgs_sample.json"
    if not org_sample.exists():
        print(f"couldn't find the file {org_sample.resolve()}")
    with org_sample.open("r") as org_sample_file:
        user_events = json.load(org_sample_file)
    yield from user_events


def import_organisations() -> None:
    """ import organisation from sample json to database.
    """
    engine = get_pg_engine()
    insert_stmt = crm_t_organizations_dim.insert(dml=None).values(list(get_organisations()))
    res = engine.execute(insert_stmt)
    print(f"imported organisations: {res.rowcount}")


if __name__ == "__main__":
    import_organisations()
