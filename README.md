# Kafka-to-Kafka throughput benchmarks [WIP]

Benchmarks for kafka to kafka throughput with various versions of bytewax, compared with a plain python script.
In the benchmark we actually use `redpanda`, a kafka compatible streaming data platform, for ease of local deployment,
but the benchmarks can be ran with a Kafka deployment if needed.

## Run the benchmarks

A `docker-compose.yml` file is given to setup local infrastructure.

To run the benchmarks, first run the redpanda cluster with:
```
docker compose up -d
```

Wait for the cluster to be up and running, you can check the redpanda console at `http://localhost:8080`.

Now prepare the virtualenvs with:

```
./prepare_envs.sh
```

And finally run the benchmarks with:

```
python benchmark.py
```

## Results
