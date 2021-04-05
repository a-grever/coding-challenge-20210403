from datetime import date

import users_dim


def test_run(mocker, mock_engine):
    dt = date(2021, 4, 5)
    mocker.spy(users_dim, "insert_from_select")
    users_dim.run(engine=mock_engine, import_date=dt)
    users_dim.insert_from_select.assert_called_once_with(
        engine=mock_engine,
        params={"import_date": date(2021, 4, 5)},
        target_table=users_dim.crm_t_users_dim,
        select_stmt=users_dim.SELECT_QUERY,
    )
