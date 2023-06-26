import os

from bytewax.connectors.kafka import KafkaInput, KafkaOutput
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

GROUP_ID = os.environ.get("GROUP_ID")
CONSUME_TOPIC = os.environ.get("CONSUME_TOPIC")
PRODUCE_TOPIC = os.environ.get("PRODUCE_TOPIC")
BROKERS = os.environ.get("BROKERS").split(",")


if __name__ == "__main__":
    flow = Dataflow()
    flow.input(
        "sensor_input",
        KafkaInput(
            BROKERS,
            [CONSUME_TOPIC],
            add_config={
                "enable.auto.commit": True,
                "group.id": GROUP_ID,
            },
            batch_size=10000,
        ),
    )
    flow.output(
        "avg_device_output",
        KafkaOutput(
            brokers=BROKERS, topic=PRODUCE_TOPIC, add_config={"linger.ms": "500"}
        ),
    )

    cli_main(flow)
