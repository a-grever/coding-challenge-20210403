from io import StringIO
import json

import pytest

import user_event_producer


@pytest.fixture()
def user_events():
    return [
        {
            "id": "3e0a16993813100223734db91f7c79d9",
            "event_type": "User Created",
            "username": "Zeus",
            "user_email": "zeus@olympus.org",
            "user_type": "Admin",
            "organization_name": "God of War",
            "plan_name": "Enterprise",
            "received_at": "2020-12-06 22:03:16.759617",
        },
        {
            "id": "3e0a16993813100223734db91f7c79d9",
            "event_type": "User Updated",
            "username": "Zeus",
            "user_email": "zeus@olympus.com",
            "user_type": "Admin",
            "organization_name": "God of War",
            "plan_name": "Enterprise",
            "received_at": "2020-12-08 04:03:02.759617",
        },
    ]


def test_get_user_events(mocker, user_events):
    mock_data_folder = mocker.patch.object(user_event_producer, "data_folder")
    mock_org_sample = mock_data_folder / "events_sample.json"
    mock_org_sample.exists.return_value = True
    mock_org_sample.open.return_value = StringIO(json.dumps(user_events))
    events = list(user_event_producer.get_user_events())
    assert events == user_events


def test_test_get_user_events_missing_file(mocker):
    mock_data_folder = mocker.patch.object(user_event_producer, "data_folder")
    mock_org_sample = mock_data_folder / "events_sample.json"
    mock_org_sample.exists.return_value = False
    with pytest.raises(RuntimeError):
        list(user_event_producer.get_user_events())


def test_main(mocker, monkeypatch, user_events):
    monkeypatch.setenv("QUEUE_HOST", "test-queue")
    mock_con = mocker.patch.object(user_event_producer.pika, "BlockingConnection").return_value
    mock_chan = mock_con.channel.return_value
    user_event_producer.main(user_events=user_events)

    assert mock_chan.basic_publish.call_count == len(user_events)
    mock_chan.queue_declare.assert_called_once_with(
        queue="user_events", durable=True, exclusive=False
    )
