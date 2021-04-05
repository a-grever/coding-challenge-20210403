import json
import os
from pathlib import Path
from typing import Generator, Iterable

import pika

data_folder = Path(__file__).resolve().parents[2] / "data"


def get_user_events() -> Generator[dict, None, None]:
    """ read sample user events from json and yield single entries

    Yields
    -------
    dict
        single organisation row
    """
    events_sample = data_folder / "events_sample.json"
    if not events_sample.exists():
        raise RuntimeError(f"couldn't find the file {events_sample.resolve()}")
    with events_sample.open("r") as events_sample_file:
        user_events = json.load(events_sample_file)
    yield from user_events


def main(user_events: Iterable[dict]):
    """Producer sending user events to the queue 'user_events'.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(os.environ["QUEUE_HOST"]))
    channel = connection.channel()

    channel.queue_declare(queue="user_events", durable=True, exclusive=False)

    channel.confirm_delivery()

    cnt = 0

    for user_event in user_events:
        channel.basic_publish(
            exchange="",
            routing_key="user_events",
            body=json.dumps(user_event),
            mandatory=True,
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        )
        print(user_event.get("event_type"))
        cnt += 1

    print(f"Sent {cnt} messages")
    connection.close()


if __name__ == "__main__":  # pragma: no cover
    main(user_events=get_user_events())
