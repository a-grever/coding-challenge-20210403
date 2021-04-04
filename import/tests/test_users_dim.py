from datetime import date

import users_dim


def test_run_users_dim(mocker):
    dt = date(2021, 4, 5)
    mocker.patch.object(users_dim, "insert_from_select", autospec=True)
    users_dim.run_users_dim(import_date=dt)
    users_dim.insert_from_select.assert_called_once_with(
        params={"import_date": date(2021, 4, 5)},
        target_table=users_dim.crm_t_users_dim,
        select_stmt=users_dim.SELECT_QUERY,
    )
