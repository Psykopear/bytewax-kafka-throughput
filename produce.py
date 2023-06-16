import argparse
import json
import os
import sys

from time import time
from datetime import datetime
from confluent_kafka import Producer


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("topic", help="Topic where messages will be sent")
    parser.add_argument(
        "messages_per_second",
        help="Number of messages per second to generate",
        type=int,
    )
    parser.add_argument(
        "brokers",
        help="List of kafka brokers, separated by a comma, eg: 'localhost:9092,localhost:9093'",
    )
    args = parser.parse_args()

    config = {"bootstrap.servers": args.brokers}
    producer = Producer(config)

    # Create some content to send, we don't really care
    # about the value
    content = json.dumps(
        {
            "ts": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "id": "one",
            "param": 0.1,
        }
    ).encode()

    # Send at most `args.messages_per_second` messages in one second.
    # Not properly distributed, but it should do for our test
    start = time()
    messages_in_last_sec = 0
    flush_counter = 0
    while True:
        elapsed = time() - start
        if elapsed > 1:
            if messages_in_last_sec < args.messages_per_second:
                print(
                    "Could not produce messages fast enough.\n"
                    f"Produced {messages_in_last_sec} messages in {elapsed} seconds"
                )
            messages_in_last_sec = 0
            start = time()
        elif messages_in_last_sec >= args.messages_per_second:
            # Busy wait if we already produced enough messages
            continue
        messages_in_last_sec += 1
        flush_counter += 1
        producer.produce(args.topic, content)

        # Do not flush at every message, but also
        # avoid filling up the buffer
        if flush_counter >= 1000:
            producer.flush()
            flush_counter = 0
