import base64
import json
import os
import sys

from time import time
from datetime import datetime
from confluent_kafka import Producer

BROKERS = os.environ.get("BROKERS", "localhost:19092,localhost:29092,localhost:39092")

if __name__ == "__main__":
    limit = int(sys.argv[2])
    topic = sys.argv[3]
    config = {"bootstrap.servers": BROKERS}

    producer = Producer(config)
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
    while True:
        elapsed = time() - start
        if elapsed > 1:
            print(f"Produced {i} messages in {elapsed} seconds")
            i = 0
            start = time()
        elif i >= limit:
            continue
        i += 1
        producer.produce(topic, row)
        producer.flush()
