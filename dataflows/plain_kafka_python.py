import os

from kafka import KafkaConsumer, KafkaProducer


BROKER_ADDRESS = os.environ.get("BROKER_ADDRESS", "localhost:19092")
CONSUME_TOPICS = os.environ.get("CONSUME_TOPICS", "input-multiple").split(", ")
PRODUCE_TOPIC = os.environ.get("PRODUCE_TOPIC", "output")
ERROR_VALUE = ("ERROR", None)


consumer = KafkaConsumer(
    bootstrap_servers=BROKER_ADDRESS,
    group_id="plain_kafka_python",
    enable_auto_commit=True,
    auto_offset_reset="latest",
)
consumer.subscribe(topics=CONSUME_TOPICS)
producer = KafkaProducer(bootstrap_servers=BROKER_ADDRESS)

while True:
    results = []
    result = consumer.poll(timeout_ms=250, max_records=500)
    for records in result.values():
        for record in records:
            results.extend([record.value for record in records])

    for payload_input in results:
        producer.send(PRODUCE_TOPIC, payload_input)
