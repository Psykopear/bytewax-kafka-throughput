"""
Sample lag values from redpanda's rpk every ~0.5 seconds
and save everything in a csv
"""
from subprocess import Popen, check_output
from csv import DictWriter
from time import sleep

BROKERS = "localhost:19092,localhost:29092,localhost:39092"
GROUPS = [
    "bw016-custom-kafka-in",
    "bw016-kafka-connectors",
    "plain-confluent-kafka",
    "plain-kafka-python",
    "bw010-custom-kafka-in",
]


def sample_group(group: str):
    with open(f"results/{group}.csv", "w") as file:
        writer = DictWriter(file, ["epoch", "lag"])
        writer.writeheader()

        print(f"Sampling lag for {group}")
        for epoch in range(20):
            print(".", end="", flush=True)
            ps = check_output(["rpk", "group", "describe", group, "--brokers", BROKERS])
            for line in ps.splitlines():
                line = line.decode()
                if group in line and "GROUP" not in line:
                    lag = int(line.split()[4])
                    writer.writerow({"epoch": epoch, "lag": lag})
            sleep(1)
    print()


if __name__ == "__main__":
    # 1) Launch the dataflow
    # 2) Launch the producer
    # 3) Sample the lag
    # 4) Stop everything
    proc = Popen(
        [".bytewax-v0.16.2/bin/python", "src/bytewax_0_16/kafka_connectors.py"]
    )
    sample_group("bw016-kafka-connectors")
    # proc.kill()
