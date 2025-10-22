from confluent_kafka import Consumer
import json

def run_consumer(broker="localhost:9092", topic="flights.raw", group_id="test-consumer"):
    """
    Simple Kafka consumer that prints messages from a given topic.
    """
    conf = {
        "bootstrap.servers": broker,
        "group.id": group_id,
        "auto.offset.reset": "earliest"
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])

    print(f"✅ Listening on topic '{topic}' ... Press Ctrl+C to stop.")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("⚠️ Consumer error:", msg.error())
                continue

            try:
                data = json.loads(msg.value().decode("utf-8"))
                print(f"[{topic}] {json.dumps(data, indent=2)}")
            except Exception:
                print(f"[{topic}] {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        print("\n⛔ Stopping consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    # Change topic here if you want to test external.opensky.states or flights.status
    run_consumer(topic="flights.raw")
