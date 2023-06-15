import os

from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaOutput, KafkaInput
from bytewax.run import cli_main

GROUP_ID = os.environ.get("GROUP_ID")
CONSUME_TOPIC = os.environ.get("CONSUME_TOPIC")
PRODUCE_TOPIC = os.environ.get("PRODUCE_TOPIC")
BROKERS = os.environ.get("BROKERS")


if __name__ == "__main__":
    flow = Dataflow()
    flow.input(
        "sensor_input",
        KafkaInput(
            brokers=BROKERS,
            topics=[CONSUME_TOPIC],
            add_config={
                "enable.auto.commit": True,
                "group.id": GROUP_ID,
            },
        ),
    )
    flow.output(
        "avg_device_output",
        KafkaOutput(
            brokers=BROKERS,
            topic=PRODUCE_TOPIC,
            add_config={
                "queue.buffering.max.kbytes": "512",
            },
        ),
    )
    cli_main(flow)
