from datetime import datetime, date
import os


def get_import_date() -> date:
    """ Process the IMPORT_DATE env var to datetime.date

    Returns
    -------
    date
        date object based on env var

    Raises
    ------
    RuntimeError
        if the env var can't be processed
    """
    try:
        return datetime.strptime(os.environ["IMPORT_DATE"], "%Y-%m-%d").date()
    except KeyError as error:
        raise RuntimeError("environment variable 'IMPORT_DATE' not set") from error
    except ValueError as error:
        raise RuntimeError(
            (
                f"environment variable IMPORT_DATE={os.environ['IMPORT_DATE']} "
                "doesn't have format YYYY-MM-DD "
            )
        ) from error
