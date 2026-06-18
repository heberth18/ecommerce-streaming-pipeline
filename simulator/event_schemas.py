
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional
import uuid
import json


def _now_iso() -> str:
    """Timestamp actual en formato ISO8601 con timezone UTC."""
    return datetime.now(timezone.utc).isoformat()


def _new_uuid() -> str:
    return str(uuid.uuid4())


@dataclass
class OrderEvent:
    order_id: str
    customer_id: str
    amount: float
    currency: str
    items_count: int
    event_id: str = field(default_factory=_new_uuid)
    event_type: str = "order_created"
    event_timestamp: str = field(default_factory=_now_iso)

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class PaymentEvent:
    payment_id: str
    order_id: str
    amount: float
    currency: str
    gateway: str
    status: str                        # "success" | "failure" | "timeout"
    event_id: str = field(default_factory=_new_uuid)
    event_type: str = "payment_attempted"
    event_timestamp: str = field(default_factory=_now_iso)
    failure_reason: Optional[str] = None

    def to_json(self) -> str:
        return json.dumps(asdict(self))