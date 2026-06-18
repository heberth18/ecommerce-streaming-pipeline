import os
import time
import random
import uuid
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

from event_schemas import OrderEvent, PaymentEvent
from anomalies import (
    make_duplicate_event,
    make_late_event,
    make_invalid_schema_event,
    make_gateway_failure_spike,
    make_volume_spike,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
EVENTS_PER_SECOND = float(os.environ.get("EVENTS_PER_SECOND", "1"))
SLEEP_BETWEEN_EVENTS = 1.0 / EVENTS_PER_SECOND

GATEWAYS = ["stripe", "paypal", "mercadopago"]
CURRENCIES = ["USD", "CLP", "EUR"]

# 70/30 split keeps anomalies frequent enough to appear in dashboards
# without dominating normal traffic
ANOMALY_WEIGHTS = {
    "normal": 70,
    "gateway_failure_spike": 10,
    "duplicate": 8,
    "late_event": 7,
    "invalid_schema": 5,
}


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        retries=5,
        acks="all",  # payment events must not be silently dropped
        value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def publish(producer: KafkaProducer, topic: str, key: str, value: str) -> None:
    future = producer.send(topic, key=key, value=value)
    try:
        future.get(timeout=10)
        logger.info(f"→ {topic} | key={key} | {value[:80]}...")
    except KafkaError as e:
        logger.error(f"Failed to publish to {topic}: {e}")


def generate_normal_pair(producer: KafkaProducer) -> None:
    order_id = str(uuid.uuid4())
    customer_id = str(uuid.uuid4())
    gateway = random.choice(GATEWAYS)
    amount = round(random.uniform(10, 500), 2)
    currency = random.choice(CURRENCIES)

    order = OrderEvent(
        order_id=order_id,
        customer_id=customer_id,
        amount=amount,
        currency=currency,
        items_count=random.randint(1, 5),
    )
    publish(producer, "orders", key=customer_id, value=order.to_json())

    status = random.choices(["success", "failure", "timeout"], weights=[75, 15, 10])[0]
    failure_reason = "insufficient_funds" if status == "failure" else None

    payment = PaymentEvent(
        payment_id=str(uuid.uuid4()),
        order_id=order_id,
        amount=amount,
        currency=currency,
        gateway=gateway,
        status=status,
        failure_reason=failure_reason,
    )
    publish(producer, "payments", key=order_id, value=payment.to_json())


def run_anomaly(producer: KafkaProducer, anomaly_type: str) -> None:
    gateway = random.choice(GATEWAYS)
    order_id = str(uuid.uuid4())

    if anomaly_type == "gateway_failure_spike":
        events = make_gateway_failure_spike(order_id=order_id, gateway=gateway, count=10)
        for event in events:
            publish(producer, "payments", key=order_id, value=event)

    elif anomaly_type == "duplicate":
        payment = PaymentEvent(
            payment_id=str(uuid.uuid4()),
            order_id=order_id,
            amount=99.99,
            currency="USD",
            gateway=gateway,
            status="success",
        )
        original = payment.to_json()
        publish(producer, "payments", key=order_id, value=original)
        time.sleep(0.5)
        publish(producer, "payments", key=order_id, value=make_duplicate_event(original))

    elif anomaly_type == "late_event":
        event = make_late_event(
            payment_id=str(uuid.uuid4()),
            order_id=order_id,
            gateway=gateway,
            minutes_late=10,
        )
        publish(producer, "payments", key=order_id, value=event)

    elif anomaly_type == "invalid_schema":
        event = make_invalid_schema_event(order_id=order_id)
        publish(producer, "payments", key=order_id, value=event)


def main() -> None:
    logger.info("Starting simulator...")
    producer = build_producer()

    anomaly_population = list(ANOMALY_WEIGHTS.keys())
    anomaly_weights = list(ANOMALY_WEIGHTS.values())

    try:
        while True:
            choice = random.choices(anomaly_population, weights=anomaly_weights)[0]

            if choice == "normal":
                generate_normal_pair(producer)
            else:
                run_anomaly(producer, choice)

            time.sleep(SLEEP_BETWEEN_EVENTS)

    except KeyboardInterrupt:
        logger.info("Simulator stopped.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()