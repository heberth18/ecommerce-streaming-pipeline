from datetime import datetime, timezone, timedelta
from typing import Optional
import uuid
import json


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_uuid() -> str:
    return str(uuid.uuid4())


def make_duplicate_event(original_event_json: str) -> str:
    """Returns the same event with the same event_id to test Spark deduplication."""
    return original_event_json


def make_late_event(payment_id: str, order_id: str, gateway: str, minutes_late: int = 10) -> str:
    """
    Event timestamped beyond the 2-minute watermark.
    Spark will route it to the DLQ with reason: late_arrival.
    """
    late_timestamp = (datetime.now(timezone.utc) - timedelta(minutes=minutes_late)).isoformat()
    event = {
        "event_id": _new_uuid(),
        "event_type": "payment_attempted",
        "event_timestamp": late_timestamp,
        "payment_id": payment_id,
        "order_id": order_id,
        "amount": 99.99,
        "currency": "USD",
        "gateway": gateway,
        "status": "success",
        "failure_reason": None,
    }
    return json.dumps(event)


def make_invalid_schema_event(order_id: str) -> str:
    """
    Event missing required fields (payment_id, amount, gateway, status).
    Spark will route it to the DLQ with reason: invalid_schema.
    """
    event = {
        "event_id": _new_uuid(),
        "event_type": "payment_attempted",
        "event_timestamp": _now_iso(),
        "order_id": order_id,
    }
    return json.dumps(event)


def make_gateway_failure_spike(order_id: str, gateway: str, count: int = 10) -> list[str]:
    """
    Burst of consecutive failures for the same gateway.
    Should push failure_rate above the alert threshold within a single window.
    """
    events = []
    for _ in range(count):
        event = {
            "event_id": _new_uuid(),
            "event_type": "payment_attempted",
            "event_timestamp": _now_iso(),
            "payment_id": str(uuid.uuid4()),
            "order_id": order_id,
            "amount": 99.99,
            "currency": "USD",
            "gateway": gateway,
            "status": "failure",
            "failure_reason": "gateway_timeout",
        }
        events.append(json.dumps(event))
    return events


def make_volume_spike(gateway: str, count: int = 50) -> list[str]:
    """Burst of valid events to verify the pipeline holds under high load."""
    events = []
    for _ in range(count):
        event = {
            "event_id": _new_uuid(),
            "event_type": "payment_attempted",
            "event_timestamp": _now_iso(),
            "payment_id": str(uuid.uuid4()),
            "order_id": str(uuid.uuid4()),
            "amount": round(50 + 200 * __import__("random").random(), 2),
            "currency": "USD",
            "gateway": gateway,
            "status": "success",
            "failure_reason": None,
        }
        events.append(json.dumps(event))
    return events