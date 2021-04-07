import json

import pytest

import user_event_consumer


@pytest.fixture()
def user_event():
    return {
        "id": "3e0a16993813100223734db91f7c79d9",
        "event_type": "User Updated",
        "username": "Zeus",
        "user_email": "zeus@olympus.com",
        "user_type": "Admin",
        "organization_name": "God of War",
        "plan_name": "Enterprise",
        "received_at": "2020-12-08 04:03:02.759617",
    }


def test_callback(mocker, mock_engine, user_event):
    mocker.patch.object(user_event_consumer, "get_pg_engine", return_value=mock_engine)
    mock_ch = mocker.MagicMock()
    mock_method = mocker.MagicMock()
    user_event_consumer.callback(
        ch=mock_ch, method=mock_method, properties=None, body=json.dumps(user_event).encode()
    )
    mock_engine.execute.assert_called_once()
    mock_ch.basic_ack.assert_called_once_with(delivery_tag=mock_method.delivery_tag)


def test_callback_reused_engine(mocker, monkeypatch, mock_engine, user_event):
    monkeypatch.setattr(user_event_consumer, "engine", mock_engine)
    mock_ch = mocker.MagicMock()
    mock_method = mocker.MagicMock()
    user_event_consumer.callback(
        ch=mock_ch, method=mock_method, properties=None, body=json.dumps(user_event).encode()
    )
    mock_engine.execute.assert_called_once()
    mock_ch.basic_ack.assert_called_once_with(delivery_tag=mock_method.delivery_tag)


def test_main(mocker, monkeypatch):
    monkeypatch.setenv("QUEUE_HOST", "test-queue")
    mock_con = mocker.patch.object(user_event_consumer.pika, "BlockingConnection").return_value
    mock_chan = mock_con.channel.return_value
    user_event_consumer.main()

    mock_chan.basic_consume.assert_called_once_with(
        queue="user_events", on_message_callback=user_event_consumer.callback
    )
    mock_chan.queue_declare.assert_called_once_with(queue="user_events", durable=True)
