import os

from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaOutput, KafkaInput
from bytewax.run import cli_main

BROKER_ADDRESS = os.environ.get("BROKER_ADDRESS", "localhost:19092")
CONSUME_TOPICS = os.environ.get("CONSUME_TOPICS", "input").split(", ")
PRODUCE_TOPIC = os.environ.get("PRODUCE_TOPIC", "output")


i = 0


class StopDataflow(Exception):
    pass


def stop(item):
    global i
    i += 1
    if i > 10000:
        print(i)
        raise StopDataflow()
    return item


flow = Dataflow()
flow.input(
    "sensor_input",
    KafkaInput(
        brokers=[BROKER_ADDRESS],
        topics=CONSUME_TOPICS,
        add_config={
            "enable.auto.commit": True,
            "group.id": "kafka_connectors",
        },
    ),
)
# flow.map(stop)
flow.output(
    "avg_device_output",
    KafkaOutput(
        brokers=[BROKER_ADDRESS],
        topic=PRODUCE_TOPIC,
        add_config={
            "queue.buffering.max.kbytes": "512",
        },
    ),
)

try:
    cli_main(flow)
except StopDataflow as e:
    print(e)
    pass
