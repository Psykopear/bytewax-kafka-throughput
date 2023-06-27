import os

from bytewax.connectors.kafka import KafkaInput
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main
from bytewax.outputs import DynamicOutput, StatelessSink

GROUP_ID = os.environ.get("GROUP_ID")
CONSUME_TOPIC = os.environ.get("CONSUME_TOPIC")
PRODUCE_TOPIC = os.environ.get("PRODUCE_TOPIC")
BROKERS = os.environ.get("BROKERS").split(",")


class NullSource(StatelessSink):
    def write(self, item):
        pass

    def close():
        pass


class NullOutput(DynamicOutput):
    def build(self, worker_index, worker_count) -> StatelessSink:
        return NullSource()


if __name__ == "__main__":
    flow = Dataflow()
    flow.input(
        "kafka_in",
        KafkaInput(
            BROKERS,
            [CONSUME_TOPIC],
            add_config={
                "enable.auto.commit": True,
                "group.id": GROUP_ID,
            },
            batch_size=500000,
            timeout=0.1,
        ),
    )
    flow.output(
        "null_output",
        NullOutput(),
    )

    cli_main(flow)
