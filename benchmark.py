import tomllib
import traceback

from csv import DictWriter
from subprocess import Popen, check_output
from time import sleep


BROKERS = "localhost:19092,localhost:29092,localhost:39092"


def log(msg):
    print(f"==> {msg}")


def sample_group(id: str, topic: str):
    with open(f"results/{id}.csv", "w") as file:
        writer = DictWriter(file, ["epoch", "lag"])
        writer.writeheader()

        for epoch in range(20):
            print(".", end="", flush=True)
            ps = check_output(["rpk", "group", "describe", id, "--brokers", BROKERS])
            for line in ps.splitlines():
                line = line.decode()
                if topic in line:
                    lag = int(line.split()[4])
                    writer.writerow({"epoch": epoch, "lag": lag})
            sleep(1)
    print()


if __name__ == "__main__":
    # First of all, call the script to prepare the virtualenvironments
    # print(check_output(["./prepare_envs.sh"]))

    # Then load the configuration
    with open("config.toml", "rb") as f:
        benches = tomllib.load(f)['benches']

    # Just check we have everything
    for bench in benches:
        for key in ["folder", "file", "id", "consume_topic", "produce_topic"]:
            assert key in bench, f"Missing {key} in bench config"

    for bench in benches:
        producer_process = None
        process = None
        try:
            log("Running producer")
            producer_process = Popen(
                ["python", "produce.py", "3000", bench["consume_topic"]]
            )
            exe = f"{bench['folder']}/.venv/bin/python"
            file = f"{bench['folder']}/{bench['file']}"
            env = {
                "BROKERS": BROKERS,
                "GROUP_ID": bench["id"],
                "CONSUME_TOPIC": bench["consume_topic"],
                "PRODUCE_TOPIC": bench["produce_topic"],
            }
            log("Running script")
            process = Popen([exe, file], env=env)
            # Sample here
            log(f"Sampling lag for {bench['id']}")
            sample_group(bench["id"], bench["consume_topic"])
        except Exception:
            log(f"Error executing bench {bench['id']}:")
            print(traceback.format_exc())
            continue
        finally:
            if process is not None:
                process.kill()
            if producer_process is not None:
                producer_process.kill()
