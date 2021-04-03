import json
from pathlib import Path

from database import get_pg_engine, crm_t_organizations

data_folder = Path(__file__).parent.parent.parent / 'data'


def get_organisations():
    org_sample = data_folder / 'orgs_sample.json'
    if not org_sample.exists():
        print(f"couldn't find the file {org_sample.resolve()}")
    with org_sample.open('r') as f:
        user_events = json.load(f)
    yield from user_events


def import_organisations():
    engine = get_pg_engine()
    insert_stmt = crm_t_organizations.insert().values(list(get_organisations()))
    res = engine.execute(insert_stmt)
    print(f'imported organisations: {res.rowcount}')


if __name__ == '__main__':
    import_organisations()
