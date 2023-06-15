import os

from bytewax.inputs import DynamicInput, StatelessSource
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaOutput
from bytewax.run import cli_main
from confluent_kafka import Consumer

ID = "bw016-custom-kafka-in"
CONSUME_TOPIC = f"{ID}-in"
PRODUCE_TOPIC = f"{ID}-out"
BROKERS = os.environ.get("BROKERS", "localhost:19092,localhost:29092,localhost:39092")


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
    def __init__(self, broker, topics, group_id):
        self.broker = broker
        self.topics = topics
        self.group_id = group_id

    def build(self, worker_index, worker_count):
        consumer = Consumer(
            {
                "bootstrap.servers": self.broker,
                "group.id": self.group_id,
                "auto.offset.reset": "end",
                "enable.auto.commit": True,
            }
        )
        consumer.subscribe(self.topics)
        return KafkaSource(consumer, self.topics)


if __name__ == "__main__":
    flow = Dataflow()
    flow.input("sensor_input", CustomKafkaInput(BROKERS, [CONSUME_TOPIC], ID))
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
