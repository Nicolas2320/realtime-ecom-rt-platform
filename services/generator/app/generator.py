import json
import os
import random
import signal
import time
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

RUNNING = True

def iso_now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def random_event(user_ids, product_ids, make_invalid=False) -> dict:
    event_type = random.choices(
        ["click", "add_to_cart", "checkout", "purchase"],
        weights=[0.72, 0.18, 0.07, 0.03],
        k=1,
    )[0]

    user_id = random.choice(user_ids)
    session_id = str(uuid.uuid4())
    product_id = random.choice(product_ids)
    invalid_case = "none"

    # minimal fields to keep contract simple but useful
    ev = {
        "event_id": str(uuid.uuid4()),
        "event_version": 1,
        "event_type": event_type,
        "event_time": iso_now_utc(),
        "ingest_time": iso_now_utc(),
        "user_id": user_id,
        "session_id": session_id,
        "product_id": None,
        "quantity": None,
        "price": None,
        "currency": None,
        "order_id": None,
        "source": "generator",
    }

    if event_type in ("click", "add_to_cart", "purchase"):
        ev["product_id"] = product_id

        invalid_case = random.choice([
            "invalid_event_type",  # Invalid event_type
            "missing_product_id",  # Missing product_id for click/add_to_cart
        ])

    if event_type == "add_to_cart":
        ev["quantity"] = random.randint(1, 3)
        ev["price"] = round(random.uniform(5, 200), 2)
        ev["currency"] = "EUR"
        invalid_case = random.choice([
            "invalid_event_type",  # Invalid event_type
            "invalid_quantity",  # Invalid quantity
            "invalid_price",  # Invalid price
            "missing_product_id",  # Missing product_id for click/add_to_cart
        ])

    if event_type == "purchase":
        ev["order_id"] = f"ord_{uuid.uuid4().hex[:10]}"
        ev["quantity"] = random.randint(1, 5)
        ev["price"] = round(random.uniform(10, 500), 2)
        ev["currency"] = "EUR"
        invalid_case = random.choice([
            "invalid_event_type",  # Invalid event_type
            "invalid_quantity",  # Invalid quantity
            "missing_product_id",  # Missing product_id for click/add_to_cart
        ])

    if make_invalid:
        if invalid_case == "invalid_event_type":
            ev["event_type"] = "invalid_event_type"  # Invalid event type
        elif invalid_case == "invalid_quantity":
            ev["quantity"] = random.randint(-5, -1)  # Quantity should be an integer
        elif invalid_case == "invalid_price":
            ev["price"] = round(random.uniform(-500, -10), 2)  # Price should be a float
        elif invalid_case == "missing_product_id":
            ev["product_id"] = None  # Missing product_id for click/add_to_cart

    return ev, invalid_case if make_invalid else 'no_missing'


def inject_late_event(ev: dict, max_seconds: int) -> dict:
    late_seconds = random.randint(1, max_seconds)
    t = datetime.now(timezone.utc) - timedelta(seconds=late_seconds)
    ev = dict(ev)
    ev["event_time"] = t.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    # ingest_time stays "now" (when we received it)
    ev["ingest_time"] = iso_now_utc()
    return ev


def shutdown(*_):
    global RUNNING
    RUNNING = False


def main():
    random.seed(int(os.getenv("SEED", "42")))

    brokers = os.getenv("KAFKA_BROKERS", "redpanda-0:9092")
    topic = os.getenv("TOPIC", "ecom.events.raw.v1")

    eps = float(os.getenv("EVENTS_PER_SEC", "20"))
    dup_rate = float(os.getenv("DUP_RATE", "0.02"))
    late_rate = float(os.getenv("LATE_RATE", "0.05"))
    late_max = int(os.getenv("LATE_MAX_SECONDS", "600"))
    log_every = int(os.getenv("LOG_EVERY", "50"))
    invalid_rate = float(os.getenv("INVALID_RATE", "0.05"))

    user_ids = [f"user_{i:04d}" for i in range(1, 201)]
    product_ids = [f"sku_{i:05d}" for i in range(1, 501)]

    producer = Producer({"bootstrap.servers": brokers, "linger.ms": 10})
    sent = 0

    # Keep a small buffer to resend duplicates intentionally
    recent_events = []

    def delivery_cb(err, msg):
        if err is not None:
            print(f"[DELIVERY ERROR] {err}")

    interval = 1.0 / eps if eps > 0 else 0.05

    print(f"[GEN] brokers={brokers} topic={topic} eps={eps} dup={dup_rate} late={late_rate}")

    while RUNNING:
        # Decide whether to duplicate a previous event
        do_dup = recent_events and (random.random() < dup_rate)

        if do_dup:
            ev = random.choice(recent_events)
            ev = dict(ev)
            # update ingest_time to simulate re-arrival
            ev["ingest_time"] = iso_now_utc()
            tag = "DUP"

        elif random.random() < invalid_rate:
            ev, invalid_case = random_event(user_ids, product_ids, make_invalid=True)
            tag = f"INVALID: {invalid_case}"
        else:
            ev, _ = random_event(user_ids, product_ids)
            tag = "NEW"

        # Decide whether to make it "late" (event_time in the past)
        if random.random() < late_rate:
            ev = inject_late_event(ev, late_max)
            tag += " + LATE"

        key = ev["user_id"].encode("utf-8")
        value = json.dumps(ev).encode("utf-8")

        producer.produce(topic=topic, key=key, value=value, on_delivery=delivery_cb)
        producer.poll(0)

        sent += 1
        if not do_dup:
            recent_events.append(ev)
            if len(recent_events) > 500:
                recent_events.pop(0)

        if sent % log_every == 0:
            print(f"[GEN] sent={sent} last={tag} type={ev['event_type']} user={ev['user_id']} event_time={ev['event_time']}")

        time.sleep(interval)

    print("[GEN] shutting down...")
    producer.flush(5)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    main()
