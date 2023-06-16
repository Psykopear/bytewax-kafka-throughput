import tomllib

from time import time
from concurrent.futures import wait
from csv import DictWriter
from subprocess import Popen, check_output
from time import sleep
from confluent_kafka.admin import AdminClient, NewTopic


BROKERS = "localhost:19092,localhost:29092,localhost:39092"
LOADER = ["-", "\\", "|", "/"]


def print_msg(msg, **kwargs):
    print(f"==> {msg}", **kwargs)


def loader(msg, step, max):
    perc = round(step * 100 / max)
    print_msg(f"{msg} {LOADER[step % len(LOADER)]} {perc}%", end="\r", flush=True)


def sample_group(id: str, topic: str):
    with open(f"results/{id}.csv", "w") as file:
        writer = DictWriter(file, ["epoch", "lag"])
        writer.writeheader()

        # more or less one minute of samples
        samples = 1200
        start = time()
        for epoch in range(samples):
            sleep(0.1)
            elapsed = f"{time() - start:.2f}"
            loader(f"Sampling lag for {bench['id']}", epoch, samples)
            ps = check_output(["rpk", "group", "describe", id, "--brokers", BROKERS])
            for line in ps.splitlines():
                line = line.decode()
                if topic in line:
                    lag = int(line.split()[4])
                    writer.writerow({"epoch": elapsed, "lag": lag})
    print()


if __name__ == "__main__":
    # Then load the configuration
    with open("config.toml", "rb") as f:
        benches = tomllib.load(f)["benches"]

    # Just check we have everything
    for bench in benches:
        for key in ["folder", "file", "id", "consume_topic", "produce_topic"]:
            assert key in bench, f"Missing {key} in bench config"

    for bench in benches:
        producer_process = None
        process = None
        try:
            print_msg("Recreating consumer groups and topics")
            config = {"bootstrap.servers": BROKERS}
            admin = AdminClient(config)
            groups = admin.list_consumer_groups().result().valid
            for future in admin.delete_consumer_groups([group.group_id for group in groups]).values():
                wait([future])
            for future in admin.delete_topics(
                [bench["consume_topic"], bench["produce_topic"]]
            ).values():
                wait([future])
            consume_topic = NewTopic(bench["consume_topic"], num_partitions=1)
            produce_topic = NewTopic(bench["produce_topic"], num_partitions=1)
            for future in admin.create_topics([consume_topic, produce_topic]).values():
                wait([future])

            print_msg("Running producer")
            producer_process = Popen(
                ["python", "produce.py", "10000", bench["consume_topic"]]
            )
            exe = f"{bench['folder']}/.venv/bin/python"
            file = f"{bench['folder']}/{bench['file']}"
            env = {
                "BROKERS": BROKERS,
                "GROUP_ID": bench["id"],
                "CONSUME_TOPIC": bench["consume_topic"],
                "PRODUCE_TOPIC": bench["produce_topic"],
            }

            print_msg("Running script")
            process = Popen([exe, file], env=env)
            # Sample lag
            sample_group(bench["id"], bench["consume_topic"])
        except:
            raise
        finally:
            if process is not None:
                process.kill()
            if producer_process is not None:
                producer_process.kill()
