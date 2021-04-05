from io import StringIO
import json
from pathlib import Path
from typing import Any, Generator

from database import get_pg_engine, crm_t_organizations_dim

data_folder = Path(__file__).parent.parent.parent / "data"


def get_organisations() -> Generator[dict, None, None]:
    """read sample organisations from json and yield single entries

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


def copy_organisations_from_file(engine: Any):
    """using the COPY command to load organisations into the warehouse.

    Parameters
    ----------
    sqlalchemy.engine.base.Engine
        db engine
    """
    conn = engine.raw_connection()
    cur = conn.cursor()

    columns = [
        "organization_key",
        "organization_name",
        "created_at",
    ]
    organisations_tsv = StringIO(
        "\n".join("\t".join([o[col] for col in columns]) for o in get_organisations())
    )

    cur.copy_from(
        file=organisations_tsv, table=crm_t_organizations_dim.fullname, sep="\t", columns=columns
    )
    conn.commit()
    conn.close()
    print(f"copied organizations to {crm_t_organizations_dim.fullname}")


if __name__ == "__main__":
    copy_organisations_from_file(engine=get_pg_engine())
