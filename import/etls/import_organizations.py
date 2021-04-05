from io import StringIO
import json
from pathlib import Path
from typing import Any, Generator

from database import get_pg_engine, crm_t_organizations_dim

data_folder = Path(__file__).parent.parent.parent / "data"


def get_organizations() -> Generator[dict, None, None]:
    """read sample organizations from json and yield single entries

    Yields
    -------
    dict
        single organisation row

    Raises
    ------
    RuntimeError
        if the file orgs_sample.json can't be found
    """
    org_sample = data_folder / "orgs_sample.json"
    if not org_sample.exists():
        raise RuntimeError(f"couldn't find the file {org_sample.resolve()}")

    with org_sample.open("r") as org_sample_file:
        user_events = json.load(org_sample_file)
    yield from user_events


def copy_organizations_from_file(engine: Any):
    """using the COPY command to load organizations into the warehouse.

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
    organizations_tsv = StringIO(
        "\n".join("\t".join([o[col] for col in columns]) for o in get_organizations())
    )

    cur.copy_from(
        file=organizations_tsv, table=crm_t_organizations_dim.fullname, sep="\t", columns=columns
    )
    conn.commit()
    conn.close()
    print(f"copied organizations to {crm_t_organizations_dim.fullname}")


if __name__ == "__main__":  # pragma: no cover
    copy_organizations_from_file(engine=get_pg_engine())
