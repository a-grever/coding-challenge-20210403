from datetime import date

import user_events_daily


def test_run_user_events_daily(mocker):
    dt = date(2021, 4, 5)
    mocker.patch.object(user_events_daily, "insert_from_select", autospec=True)
    user_events_daily.run_user_events_daily(import_date=dt)
    user_events_daily.insert_from_select.assert_called_once_with(
        params={"import_date": date(2021, 4, 5)},
        target_table=user_events_daily.reports_t_user_events_daily,
        select_stmt=user_events_daily.SELECT_QUERY,
    )
