import os

from confluent_kafka import Consumer, Producer

ID = "plain-confluent-kafka"
CONSUME_TOPIC = f"{ID}-in"
PRODUCE_TOPIC = f"{ID}-out"
BROKERS = os.environ.get("BROKERS", "localhost:19092,localhost:29092,localhost:39092")


if __name__ == "__main__":
    consumer = Consumer(
        {
            "bootstrap.servers": BROKERS,
            "group.id": ID,
            "auto.offset.reset": "end",
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
        if msg.error():
            raise StopIteration(msg.error())

        _, payload_input = msg.key(), msg.value()
        producer.produce(PRODUCE_TOPIC, payload_input)
        producer.flush()
