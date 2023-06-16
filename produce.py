import base64
import json
import os
import sys

from time import time
from datetime import datetime
from confluent_kafka import Producer

BROKERS = os.environ.get("BROKERS", "localhost:19092,localhost:29092,localhost:39092")


if __name__ == "__main__":
    limit = int(sys.argv[1])
    topic = sys.argv[2]
    config = {"bootstrap.servers": BROKERS}

    producer = Producer(config)
    # Repeatedly send the same message,
    # we don't really care about the content
    content = {
        "dtm": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "device_id": "one",
        "pi": 0.1,
    }
    row = json.dumps(
        {"content": base64.b64encode(json.dumps(content).encode("utf-8")).decode()}
    ).encode()

    # Send at most `limit` messages in one second.
    # Not properly distributed, but it should do for our test
    start = time()
    i = 0
    j = 0
    while True:
        elapsed = time() - start
        if elapsed > 1:
            if i < limit:
                print(f"Could not produce messages fast enough.\nProduced {i} messages in {elapsed} seconds")
            i = 0
            start = time()
        elif i >= limit:
            continue
        i += 1
        j += 1
        producer.produce(topic, row)
        # Do not flush at every message, but also
        # avoid filling up the buffer
        if j >= 1000:
            producer.flush()
            j = 0
