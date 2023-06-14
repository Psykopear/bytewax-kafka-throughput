import os

from bytewax import Dataflow
from bytewax.inputs import ManualInputConfig, AdvanceTo, Emit
from bytewax.execution import cluster_main
from kafka import KafkaConsumer, KafkaProducer

BROKER_ADDRESS = os.environ.get("BROKER_ADDRESS")
CONSUME_TOPICS = os.environ.get("CONSUME_TOPICS", "input-multiple").split(", ")
PRODUCE_TOPIC = os.environ.get("PRODUCE_TOPIC")

print("BROKER_ADDRESS: ", BROKER_ADDRESS)
print("CONSUME_TOPICS: ", CONSUME_TOPICS)
print("PRODUCE_TOPIC: ", PRODUCE_TOPIC)
producer = KafkaProducer(bootstrap_servers=BROKER_ADDRESS)


def input_builder(worker_index, worker_count, resume_epoch):
    consumer = KafkaConsumer(
        bootstrap_servers=BROKER_ADDRESS,
        group_id="dataflow-active-power-import-hourly-average",
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )
    consumer.subscribe(topics=CONSUME_TOPICS)
    print(
        f"Consumer is configured to address {BROKER_ADDRESS} on topics {CONSUME_TOPICS}."
    )

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

    def send_to_kafka(item):
        producer.send(PRODUCE_TOPIC, item)

    return send_to_kafka


flow = Dataflow()
flow.capture()

if __name__ == "__main__":
    cluster_main(flow, ManualInputConfig(input_builder), output_builder)
