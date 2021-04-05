import json
import os
from typing import Any

import pika

from database import get_pg_engine, raw_t_user_events

engine = None


def callback(ch: Any, method: Any, properties: Any, body: bytes) -> None:
    """Callback function to process user events from the queue 'user_events'."""
    global engine
    if engine is None:
        engine = get_pg_engine()

    event = json.loads(body.decode())
    insert_stmt = raw_t_user_events.insert().values(event)
    engine.execute(insert_stmt)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    """Consumer importing user events from the queue user_events. """
    connection = pika.BlockingConnection(pika.ConnectionParameters(os.environ["QUEUE_HOST"]))
    channel = connection.channel()

    channel.queue_declare(queue="user_events", durable=True)

    channel.basic_qos(prefetch_count=5)
    channel.basic_consume(queue="user_events", on_message_callback=callback)

    channel.start_consuming()


if __name__ == "__main__":
    main()
