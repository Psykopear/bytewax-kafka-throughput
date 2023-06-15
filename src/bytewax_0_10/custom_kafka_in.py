import os

from bytewax import Dataflow
from bytewax.inputs import ManualInputConfig, AdvanceTo, Emit
from bytewax.execution import cluster_main
from kafka import KafkaConsumer, KafkaProducer

GROUP_ID = os.environ.get("GROUP_ID")
CONSUME_TOPIC = os.environ.get("CONSUME_TOPIC")
PRODUCE_TOPIC = os.environ.get("PRODUCE_TOPIC")
BROKERS = os.environ.get("BROKERS")


def input_builder(worker_index, worker_count, resume_epoch):
    consumer = KafkaConsumer(
        bootstrap_servers=BROKERS,
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )
    consumer.subscribe(topics=[CONSUME_TOPIC])
    yield AdvanceTo(resume_epoch)
    epoch = resume_epoch
    epoch = 0
    while True:
        result = consumer.poll(timeout_ms=250, max_records=500)
        for records in result.values():
            for record in records:
                yield Emit(record.value)
        yield AdvanceTo(epoch)
        epoch += 1


def output_builder(worker_index, worker_count):
    print("worker_index: ", worker_index, " worker_count: ", worker_count)
    producer = KafkaProducer(bootstrap_servers=BROKERS)

    def send_to_kafka(item):
        producer.send(PRODUCE_TOPIC, item)

    return send_to_kafka


if __name__ == "__main__":
    flow = Dataflow()
    flow.capture()
    cluster_main(flow, ManualInputConfig(input_builder), output_builder)
