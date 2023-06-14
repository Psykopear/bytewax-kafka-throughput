import os

from bytewax.inputs import DynamicInput, StatelessSource
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaOutput
from bytewax.run import cli_main
from confluent_kafka import Consumer

BROKER_ADDRESS = os.environ.get("BROKER_ADDRESS", "localhost:19092")
CONSUME_TOPICS = os.environ.get("CONSUME_TOPICS", "input-multiple").split(", ")
PRODUCE_TOPIC = os.environ.get("PRODUCE_TOPIC", "output")


class KafkaSource(StatelessSource):
    def __init__(self, consumer, topic):
        self.consumer = consumer
        self.topic = topic

    def next(self):
        msg = self.consumer.poll(0.001)
        if msg is None:
            return None
        if msg.error():
            raise StopIteration()
        return msg.key(), msg.value()

    def close(self) -> None:
        self.consumer.close()


class CustomKafkaInput(DynamicInput):
    def build(self, worker_index, worker_count):
        consumer = Consumer(
            {
                "bootstrap.servers": BROKER_ADDRESS,
                "group.id": "custom_kafka_in",
                "auto.offset.reset": "end",
                "enable.auto.commit": True,
            }
        )
        consumer.subscribe(CONSUME_TOPICS)
        return KafkaSource(consumer, CONSUME_TOPICS)


flow = Dataflow()
flow.input("sensor_input", CustomKafkaInput())
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
