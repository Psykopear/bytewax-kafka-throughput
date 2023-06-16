import tomllib
import logging

from time import time
from concurrent.futures import wait
from csv import DictWriter
from subprocess import Popen, check_output
from time import sleep
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(module)s:%(name)s\t%(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__file__)


def loader(msg, step, max):
    """Can you see it?"""
    print(
        f"{chr(step%256+10240)} {msg} {round(step * 100 / max)}%", end="\r", flush=True
    )


def sample_group(id: str, topic: str, brokers: str):
    """
    Sample lag using `rpk`.
    """
    with open(f"results/{id}.csv", "w") as file:
        writer = DictWriter(file, ["elapsed", "lag"])
        writer.writeheader()

        # more or less one minute of samples
        samples = 1200
        start = time()
        for i in range(samples):
            sleep(0.1)
            elapsed = f"{time() - start:.2f}"
            loader(f"Sampling lag for {bench['id']}", i, samples)
            ps = check_output(["rpk", "group", "describe", id, "--brokers", brokers])
            for line in ps.splitlines():
                line = line.decode()
                if topic in line:
                    lag = int(line.split()[4])
                    writer.writerow({"elapsed": elapsed, "lag": lag})
    print()


if __name__ == "__main__":
    # Load config
    with open("config.toml", "rb") as f:
        config = tomllib.load(f)
    benches = config["benches"]
    messages_per_second = config["messages_per_second"]
    brokers = ",".join(config["kafka_brokers"])

    # Just check we have everything
    for bench in benches:
        for key in ["folder", "file", "id", "consume_topic", "produce_topic"]:
            assert key in bench, f"Missing {key} in bench config"

    # We'll need this
    admin = AdminClient({"bootstrap.servers": brokers})

    for bench in benches:
        producer_process = None
        process = None
        group_id = bench["id"]
        consume_topic = bench["consume_topic"]
        produce_topic = bench["produce_topic"]

        try:
            logger.info("Querying existing consumer groups")
            groups = admin.list_consumer_groups().result().valid
            groups = [group.group_id for group in groups]
            if group_id in groups:
                logger.info(f"Deleting existing consumer group {group_id}!")
                wait(admin.delete_consumer_groups([group_id]).values())

            logger.info("Resetting topics")
            wait(admin.delete_topics([consume_topic, produce_topic]).values())
            topics = [
                NewTopic(consume_topic, num_partitions=1),
                NewTopic(produce_topic, num_partitions=1),
            ]
            wait(admin.create_topics(topics).values())

            logger.info("Running producer")
            producer_process = Popen(
                ["python", "produce.py", consume_topic, messages_per_second, brokers]
            )

            exe = f"{bench['folder']}/.venv/bin/python"
            file = f"{bench['folder']}/{bench['file']}"
            env = {
                "BROKERS": brokers,
                "GROUP_ID": group_id,
                "CONSUME_TOPIC": consume_topic,
                "PRODUCE_TOPIC": produce_topic,
            }

            logger.info("Running script")
            process = Popen([exe, file], env=env)
            # Sample lag
            sample_group(group_id, consume_topic, brokers)
        finally:
            if process is not None:
                process.kill()
            if producer_process is not None:
                producer_process.kill()
