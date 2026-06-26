import json
import socket
import threading
import time
from datetime import datetime, timezone

import psutil
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = "localhost:9094"
KAFKA_TOPIC = "cpu-metrics"
PRODUCE_INTERVAL = 2.0  # seconds

INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "my-super-secret-token"
INFLUXDB_ORG = "myorg"
INFLUXDB_BUCKET = "metrics"

HOST = socket.gethostname()


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------

def _wait_for_kafka(retries: int = 20, delay: int = 3) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode(),
            )
            print(f"[producer] connected to Kafka at {KAFKA_BOOTSTRAP}")
            return p
        except NoBrokersAvailable:
            print(f"[producer] Kafka not ready (attempt {attempt}/{retries}), retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka")


def producer_loop() -> None:
    producer = _wait_for_kafka()
    print(f"[producer] publishing to '{KAFKA_TOPIC}' every {PRODUCE_INTERVAL}s")

    while True:
        cpu = psutil.cpu_percent(interval=None)
        per_core = psutil.cpu_percent(percpu=True)
        mem = psutil.virtual_memory()

        msg = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "host": HOST,
            "cpu_utilization": cpu,
            "cpu_per_core": per_core,
            "memory_used_percent": mem.percent,
        }

        producer.send(KAFKA_TOPIC, value=msg).get(timeout=10)
        print(f"[producer] CPU={cpu:.1f}%  MEM={mem.percent:.1f}%")
        time.sleep(PRODUCE_INTERVAL)


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------

def consumer_loop() -> None:
    influx = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = influx.write_api(write_options=SYNCHRONOUS)
    print(f"[consumer] connected to InfluxDB at {INFLUXDB_URL}, bucket='{INFLUXDB_BUCKET}'")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="cpu-metrics-group",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode()),
    )
    print(f"[consumer] subscribed to '{KAFKA_TOPIC}'")

    for msg in consumer:
        data = msg.value
        ts = datetime.fromisoformat(data["timestamp"])

        points = [
            Point("cpu_utilization")
            .tag("host", data["host"])
            .field("total_percent", float(data["cpu_utilization"]))
            .time(ts, WritePrecision.NS),

            Point("memory_utilization")
            .tag("host", data["host"])
            .field("used_percent", float(data["memory_used_percent"]))
            .time(ts, WritePrecision.NS),
        ]

        for core_idx, core_val in enumerate(data.get("cpu_per_core", [])):
            points.append(
                Point("cpu_per_core")
                .tag("host", data["host"])
                .tag("core", str(core_idx))
                .field("percent", float(core_val))
                .time(ts, WritePrecision.NS)
            )

        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
        print(
            f"[consumer] persisted {len(points)} points — "
            f"CPU={data['cpu_utilization']
            :.1f}%  MEM={data['memory_used_percent']:.1f}%"
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    consumer_thread = threading.Thread(target=consumer_loop, daemon=True)
    consumer_thread.start()

    # Small delay so consumer is subscribed before first message arrives
    time.sleep(2)

    producer_thread = threading.Thread(target=producer_loop, daemon=True)
    producer_thread.start()

    print("[main] running — press Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[main] shutting down")
