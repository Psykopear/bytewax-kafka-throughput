import os

from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaOutput, KafkaInput
from bytewax.run import cli_main

ID = "bw016-kafka-connectors"
CONSUME_TOPIC = f"{ID}-in"
PRODUCE_TOPIC = f"{ID}-out"
BROKERS = os.environ.get("BROKERS", "localhost:19092,localhost:29092,localhost:39092")


if __name__ == "__main__":
    flow = Dataflow()
    flow.input(
        "sensor_input",
        KafkaInput(
            brokers=BROKERS,
            topics=[CONSUME_TOPIC],
            add_config={
                "enable.auto.commit": True,
                "group.id": ID,
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
