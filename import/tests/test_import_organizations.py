from io import StringIO
import json

import pytest

import import_organizations


@pytest.fixture()
def organizations():
    return [
        {
            "organization_key": "ff3959a49ac10fc70181bc00e308fbeb",
            "organization_name": "Super Mario",
            "created_at": "2018-01-24 17:28:09.000000",
        },
        {
            "organization_key": "b3ee5192e608a45edb0d96d5d912b482",
            "organization_name": "Animal Crossing",
            "created_at": "2020-02-02 19:43:46.000000",
        },
    ]


def test_get_organizations(mocker, organizations):
    mock_data_folder = mocker.patch.object(import_organizations, "data_folder")
    mock_org_sample = mock_data_folder / "orgs_sample.json"
    mock_org_sample.exists.return_value = True
    mock_org_sample.open.return_value = StringIO(json.dumps(organizations))
    orgs = list(import_organizations.get_organizations())
    assert orgs == organizations


def test_get_organizations_missing_file(mocker):
    mock_data_folder = mocker.patch.object(import_organizations, "data_folder")
    mock_org_sample = mock_data_folder / "orgs_sample.json"
    mock_org_sample.exists.return_value = False
    with pytest.raises(RuntimeError):
        list(import_organizations.get_organizations())


def test_copy_organizations_from_file(mocker, mock_engine, organizations):
    mocker.patch.object(import_organizations, "get_organizations", return_value=organizations)
    import_organizations.copy_organizations_from_file(engine=mock_engine)
    mock_con = mock_engine.raw_connection.return_value
    mock_cur = mock_con.cursor.return_value

    mock_cur.copy_from.assert_called_with(
        file=mocker.ANY,
        table=import_organizations.crm_t_organizations_dim.fullname,
        sep="\t",
        columns=["organization_key", "organization_name", "created_at",],
    )
    mock_con.commit.assert_called_once()
    mock_con.close.assert_called_once()
