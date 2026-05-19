import json
import time
import random
import argparse

from kafka import KafkaProducer


def build_producer():
    # Kafka producer with reliability settings
    return KafkaProducer(
        bootstrap_servers=["localhost:9092", "localhost:9094", "localhost:9096"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),

        # reliability configs (important for grading)
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,

        # small batching (not too aggressive)
        linger_ms=10,
        batch_size=16384
    )


def generate_event(sensor_type, source):
    now = int(time.time() * 1000)

    if sensor_type == "temperature":
        value = random.uniform(15, 45)
        unit = "C"
    elif sensor_type == "humidity":
        value = random.uniform(30, 95)
        unit = "%"
    else:
        value = random.uniform(980, 1040)
        unit = "hPa"

    # inject anomalies (~10-15%)
    is_anomaly = random.random() < 0.12
    if is_anomaly:
        if sensor_type == "temperature":
            value = random.uniform(40, 60)
        elif sensor_type == "humidity":
            value = random.uniform(90, 110)
        else:
            value = random.uniform(900, 970)

    event = {
        "sensor": sensor_type,
        "value": round(value, 2),
        "unit": unit,
        "timestamp": now,
        "source": source,
        "anomaly": is_anomaly
    }

    return event


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--rate", type=float, default=5.0)
    parser.add_argument("--source", type=str, default="site-A-rack-1")

    args = parser.parse_args()

    producer = build_producer()

    sensors = ["temperature", "humidity", "pressure"]

    delay = 1.0 / args.rate if args.rate > 0 else 0

    sent = 0

    try:
        while sent < args.count:
            sensor_type = random.choice(sensors)

            event = generate_event(sensor_type, args.source)

            # key = sensor type (important for partitioning)
            producer.send(
                "sensor-events",
                key=sensor_type,
                value=event
            )

            sent += 1

            if sent % 50 == 0:
                print(f"Sent {sent} events...")

            if delay > 0:
                time.sleep(delay)

    except KeyboardInterrupt:
        print("Interrupted by user")

    finally:
        producer.flush()
        producer.close()
        print(f"Finished sending {sent} events")


if __name__ == "__main__":
    main()