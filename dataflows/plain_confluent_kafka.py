import os

from confluent_kafka import Consumer, Producer

BROKER_ADDRESS = os.environ.get("BROKER_ADDRESS", "localhost:19092")
CONSUME_TOPICS = os.environ.get("CONSUME_TOPICS", "input").split(", ")
PRODUCE_TOPIC = os.environ.get("PRODUCE_TOPIC", "output")
ERROR_VALUE = ("ERROR", None)


consumer = Consumer(
    {
        "bootstrap.servers": BROKER_ADDRESS,
        "group.id": "plain_confluent_kafka",
        "auto.offset.reset": "end",
        "enable.auto.commit": True,
    }
)
consumer.subscribe(CONSUME_TOPICS)
config = {"bootstrap.servers": BROKER_ADDRESS}
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
