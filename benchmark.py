import tomllib

from csv import DictWriter
from subprocess import Popen, check_output
from time import sleep


BROKERS = "localhost:19092,localhost:29092,localhost:39092"


def sample_group(id: str):
    with open(f"results/{id}.csv", "w") as file:
        writer = DictWriter(file, ["epoch", "lag"])
        writer.writeheader()

        print(f"Sampling lag for {id}")
        for epoch in range(20):
            print(".", end="", flush=True)
            ps = check_output(["rpk", "group", "describe", id, "--brokers", BROKERS])
            for line in ps.splitlines():
                line = line.decode()
                if id in line:
                    lag = int(line.split()[4])
                    writer.writerow({"epoch": epoch, "lag": lag})
            sleep(1)
    print()


if __name__ == "__main__":
    with open("config.toml", "rb") as f:
        benches = tomllib.load(f)
        # Just check we have everything
        for bench in benches:
            for key in ["folder", "file", "id", "consume_topic", "produce_topic"]:
                assert key in bench, f"Missing {key} in bench config"

    limit = 3000

    for bench in benches:
        producer_process = Popen(
            ["python", "produce.py", limit, bench["consume_topic"]]
        )
        exe = f"{bench['folder']}/.venv/bin/python"
        file = f"{bench['folder']}/{bench['file']}"
        env = {
            "GROUP_ID": bench["id"],
            "CONSUME_TOPIC": bench["consume_topic"],
            "PRODUCE_TOPIC": bench["produce_topic"],
        }
        process = Popen([exe, file], env=env)
        print(process)
        process.kill()
