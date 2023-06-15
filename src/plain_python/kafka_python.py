import os

from kafka import KafkaConsumer, KafkaProducer

ID = "plain-kafka-python"
CONSUME_TOPIC = f"{ID}-in"
PRODUCE_TOPIC = f"{ID}-out"
BROKERS = os.environ.get("BROKERS", "localhost:19092,localhost:29092,localhost:39092")


if __name__ == "__main__":
    consumer = KafkaConsumer(
        bootstrap_servers=BROKERS,
        group_id=ID,
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )
    consumer.subscribe(topics=[CONSUME_TOPIC])
    producer = KafkaProducer(bootstrap_servers=BROKERS)

    while True:
        result = consumer.poll(timeout_ms=250, max_records=500)
        for records in result.values():
            for record in records:
                producer.send(PRODUCE_TOPIC, record.value)
