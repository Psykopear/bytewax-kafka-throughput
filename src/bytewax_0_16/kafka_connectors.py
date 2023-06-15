import os

from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaOutput, KafkaInput
from bytewax.run import cli_main

ID = "bw016-kafka-connectors"
BROKER_ADDRESS = os.environ.get("BROKER_ADDRESS", "localhost:19092")
CONSUME_TOPICS = os.environ.get("CONSUME_TOPICS", ID).split(", ")
PRODUCE_TOPIC = os.environ.get("PRODUCE_TOPIC", "output")


flow = Dataflow()
flow.input(
    "sensor_input",
    KafkaInput(
        brokers=[BROKER_ADDRESS],
        topics=CONSUME_TOPICS,
        add_config={
            "enable.auto.commit": True,
            "group.id": ID,
        },
    ),
)
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

cli_main(flow)
