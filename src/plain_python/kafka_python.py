import os

from kafka import KafkaConsumer, KafkaProducer

GROUP_ID = os.environ.get("GROUP_ID")
CONSUME_TOPIC = os.environ.get("CONSUME_TOPIC")
PRODUCE_TOPIC = os.environ.get("PRODUCE_TOPIC")
BROKERS = os.environ.get("BROKERS")


if __name__ == "__main__":
    consumer = KafkaConsumer(
        bootstrap_servers=BROKERS,
        group_id=GROUP_ID,
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
