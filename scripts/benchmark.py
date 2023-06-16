import tomllib
import logging

from time import time
from concurrent.futures import wait
from csv import DictWriter
from subprocess import Popen, check_output
from time import sleep
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(module)s\t%(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__file__)


def loader(msg: str, step: int, max: int):
    """Can you see it?"""
    print(
        f"{chr(step%256+10240)} {msg} {round(step * 100 / max)}%", end="\r", flush=True
    )


def sample_group(
    samples: int, messages_per_second: int, group_id: str, topic: str, brokers: str
):
    """
    Sample lag using `rpk`.
    This is a bit hacky, but it works if you have `rpk` in the path.
    TODO: To make this benchmark suite a bit more generic,
          we could have a config option for each benchmark
          to set the command to be used to extract whatever
          metric you want to check, but that's non trivial.
    """
    with open(f"results/{messages_per_second}_{group_id}.csv", "w") as file:
        writer = DictWriter(file, ["elapsed", "lag"])
        writer.writeheader()

        # more or less one minute of samples
        start = time()
        for i in range(samples):
            sleep(0.1)
            elapsed = f"{time() - start:.2f}"
            loader(f"Sampling lag for {group_id}", i, samples)
            ps = check_output(
                ["rpk", "group", "describe", group_id, "--brokers", brokers]
            )
            for line in ps.splitlines():
                line = line.decode()
                if topic in line:
                    try:
                        lag = int(line.split()[4])
                        writer.writerow({"elapsed": elapsed, "lag": lag})
                    except ValueError:
                        logger.debug("Error getting lag value")
                        break
    print()


def run_benchmark(bench, messages_per_second, samples, brokers, admin):
    producer_process = None
    process = None
    group_id = bench["group_id"]
    consume_topic = bench["consume_topic"]
    produce_topic = bench["produce_topic"]
    logger.info(
        f"Starting benchmark for {group_id} with {messages_per_second} messages per second"
    )

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
        sleep(1)
        producer_process = Popen(
            [
                "python",
                "scripts/produce.py",
                consume_topic,
                f"{messages_per_second}",
                brokers,
            ]
        )

        logger.info("Running script")
        exe = f"{bench['folder']}/.venv/bin/python"
        file = f"{bench['folder']}/{bench['file']}"
        env = {
            "BROKERS": brokers,
            "GROUP_ID": group_id,
            "CONSUME_TOPIC": consume_topic,
            "PRODUCE_TOPIC": produce_topic,
        }

        sleep(1)
        process = Popen([exe, file], env=env)
        # Sample lag
        sample_group(samples, messages_per_second, group_id, consume_topic, brokers)
    finally:
        if process is not None:
            process.kill()
            logger.info("Script process killed")
        if producer_process is not None:
            producer_process.kill()
            logger.info("Producer process killed")


def run(config):
    benches = config["benches"]
    brokers = ",".join(config["kafka_brokers"])
    samples = config["samples"]

    # We'll need this
    admin = AdminClient({"bootstrap.servers": brokers})

    for limit in config["messages_per_second"]:
        for bench in benches:
            run_benchmark(bench, limit, samples, brokers, admin)


def get_config():
    # Load config
    with open("config.toml", "rb") as f:
        config = tomllib.load(f)

    # Just check we have everything
    assert "kafka_brokers" in config, "Missing kafka_brokers config"
    assert "messages_per_second" in config, "Missing messages_per_second config"
    assert "samples" in config, "Missing samples config"

    for i in config["benches"]:
        for key in ["folder", "file", "group_id", "consume_topic", "produce_topic"]:
            assert key in i, f"Missing {key} in bench config"
    return config


if __name__ == "__main__":
    run(get_config())
