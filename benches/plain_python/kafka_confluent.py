import os

from confluent_kafka import Consumer, Producer

GROUP_ID = os.environ.get("GROUP_ID")
CONSUME_TOPIC = os.environ.get("CONSUME_TOPIC")
PRODUCE_TOPIC = os.environ.get("PRODUCE_TOPIC")
BROKERS = os.environ.get("BROKERS")


if __name__ == "__main__":
    consumer = Consumer(
        {
            "bootstrap.servers": BROKERS,
            "group.id": GROUP_ID,
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([CONSUME_TOPIC])
    config = {"bootstrap.servers": BROKERS}
    producer = Producer(config)

    while True:
        msg = consumer.poll(0.001)
        if msg is None:
            continue
        _, payload_input = msg.key(), msg.value()
        producer.produce(PRODUCE_TOPIC, payload_input)
        producer.flush()
