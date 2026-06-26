import json
import threading
import time

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

KAFKA_BOOTSTRAP = "localhost:9094"
TOPIC = "demo-partitions"
NUM_PARTITIONS = 3
CONSUMER_GROUP = "demo-group"
PRODUCE_INTERVAL = 1.0


# ---------------------------------------------------------------------------
# Admin — create topic
# ---------------------------------------------------------------------------

def _wait_for_kafka(retries: int = 20, delay: int = 3) -> KafkaAdminClient:
    for attempt in range(1, retries + 1):
        try:
            client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
            print(f"[admin] connected to Kafka at {KAFKA_BOOTSTRAP}")
            return client
        except NoBrokersAvailable:
            print(f"[admin] Kafka not ready (attempt {attempt}/{retries}), retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka")


def create_topic() -> None:
    admin = _wait_for_kafka()
    try:
        admin.create_topics([
            NewTopic(name=TOPIC, num_partitions=NUM_PARTITIONS, replication_factor=1)
        ])
        print(f"[admin] created topic '{TOPIC}' with {NUM_PARTITIONS} partitions")
    except TopicAlreadyExistsError:
        print(f"[admin] topic '{TOPIC}' already exists")
    finally:
        admin.close()


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------

def producer_loop() -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    print(f"[producer] publishing to '{TOPIC}' every {PRODUCE_INTERVAL}s")

    msg_id = 0
    while True:
        # Cycling key forces even distribution: key-0→p0, key-1→p1, key-2→p2
        key = f"key-{msg_id % NUM_PARTITIONS}"
        payload = {"id": msg_id, "text": f"Message {msg_id}"}

        record = producer.send(TOPIC, key=key, value=payload).get(timeout=10)
        print(f"[producer]  id={msg_id:>4}  key={key}  → partition={record.partition}")

        msg_id += 1
        time.sleep(PRODUCE_INTERVAL)


# ---------------------------------------------------------------------------
# Consumers
# ---------------------------------------------------------------------------

def consumer_loop(consumer_id: int) -> None:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode()),
    )
    print(f"[consumer-{consumer_id}] joined group '{CONSUMER_GROUP}', waiting for assignment...")

    for msg in consumer:
        data = msg.value
        print(
            f"[consumer-{consumer_id}]  partition={msg.partition}  "
            f"offset={msg.offset:>4}  id={data['id']:>4}  text='{data['text']}'"
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    create_topic()

    # Start 3 consumers first so they form a group before messages arrive
    for i in range(1, NUM_PARTITIONS + 1):
        threading.Thread(target=consumer_loop, args=(i,), daemon=True).start()

    # Give consumers time to complete group rebalance and get partitions assigned
    time.sleep(5)

    threading.Thread(target=producer_loop, daemon=True).start()

    print("[main] running — press Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[main] shutting down")
