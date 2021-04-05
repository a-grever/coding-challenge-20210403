from datetime import date

import user_events_daily


def test_run(mocker, mock_engine):
    dt = date(2021, 4, 5)
    mocker.spy(user_events_daily, "insert_from_select")
    user_events_daily.run(engine=mock_engine, import_date=dt)
    user_events_daily.insert_from_select.assert_called_once_with(
        engine=mock_engine,
        params={"import_date": date(2021, 4, 5)},
        target_table=user_events_daily.reports_t_user_events_daily,
        select_stmt=user_events_daily.SELECT_QUERY,
    )
