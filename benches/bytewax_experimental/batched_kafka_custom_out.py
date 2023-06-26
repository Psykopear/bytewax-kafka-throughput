import os
import json
import base64
import datetime
import traceback

from bytewax.connectors.kafka import KafkaInput
from bytewax.outputs import DynamicOutput, StatelessSink
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

from confluent_kafka import Producer

GROUP_ID = os.environ.get("GROUP_ID")
CONSUME_TOPIC = os.environ.get("CONSUME_TOPIC")
PRODUCE_TOPIC = os.environ.get("PRODUCE_TOPIC")
BROKERS = os.environ.get("BROKERS").split(",")


class CustomKafkaSink(StatelessSink):
    def __init__(self, producer, topic):
        self._producer = producer
        self._topic = topic
        self.batch = 1000
        self.i = 0

    def write(self, key_value):
        key, value = key_value
        self._producer.produce(self._topic, value, key)
        self.i += 1
        if self.i >= self.batch:
            self._producer.flush()
            self.i = 0

    def close(self):
        self._producer.flush()


class CustomKafkaOutput(DynamicOutput):
    def __init__(
        self,
        brokers,
        topic,
        add_config,
    ):
        add_config = add_config or {}

        self._brokers = brokers
        self._topic = topic
        self._add_config = add_config

    def build(self, worker_index, worker_count):
        config = {
            "bootstrap.servers": ",".join(self._brokers),
        }
        config.update(self._add_config)
        producer = Producer(config)
        return CustomKafkaSink(producer, self._topic)


def preprocess(key_payload_input):
    _, payload_input = key_payload_input

    try:
        payload_decoded = payload_input.decode("utf-8")
        payload = json.loads(payload_decoded)
        content = payload["content"]
        base64_bytes = base64.b64decode(content)
        measurement = base64_bytes.decode("utf-8")
        measurement_object = json.loads(measurement)
        dt = datetime.strptime(measurement_object["dtm"], "%Y-%m-%dT%H:%M:%S")
        device_id = measurement_object["device_id"]
        pi = measurement_object["pi"]
        data = (device_id, (device_id, dt, pi))
        return data
    except Exception:
        traceback.print_exc()
        return None


class Device:
    def __init__(self):
        self.sum_vals = 0
        self.num_vals = 0
        self.current_hr = None
        self.device_id = None

    def update_avg(self, data):
        (device_id, dt, pi) = data
        hour = int(dt.hour)
        if self.device_id is None:
            self.device_id = device_id

        if self.current_hr is None:
            self.current_hr = hour

        if hour == self.current_hr:
            self.sum_vals += pi
            self.num_vals += 1
        else:
            self.sum_vals = pi
            self.num_vals = 1
            self.current_hr = None
        self.avg = self.sum_vals / self.num_vals
        return self, (dt, pi, self.avg, self.sum_vals, self.num_vals)


def prep_for_kafka(item):
    deviceId, (
        datetime,
        activePowerImportLatest,
        activePowerImportHourlyAverage,
        sumVals,
        numVals,
    ) = item
    x = {
        "activePowerImportHourlyAverage": activePowerImportHourlyAverage,
        "activePowerImportLatest": activePowerImportLatest,
        "deviceId": deviceId,
        "timestamp": datetime.isoformat(),
        "numVals": numVals,
        "sumVals": sumVals,
    }
    return deviceId, json.dumps(x).encode("utf-8")


if __name__ == "__main__":
    flow = Dataflow()
    flow.input(
        "sensor_input",
        KafkaInput(
            BROKERS,
            [CONSUME_TOPIC],
            add_config={
                "enable.auto.commit": True,
                "group.id": GROUP_ID,
            },
            batch_size=10000,
        ),
    )
    # flow.filter_map(preprocess)
    # flow.stateful_map("average", lambda: Device(), Device.update_avg)
    # flow.map(prep_for_kafka)
    flow.output(
        "avg_device_output",
        CustomKafkaOutput(
            brokers=BROKERS, topic=PRODUCE_TOPIC, add_config={"linger.ms": "500"}
        ),
    )

    cli_main(flow)
