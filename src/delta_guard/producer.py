"""
producer.py — Authentic AI Agent Tool Execution Generator.

Emits realistic agent tool calls (LangGraph / MCP server events) with
4 poison-record signatures for gatekeeper testing:
  1. type_mismatch     -> cost_usd sent as string ("FREE")
  2. stale_timestamp   -> event timestamp >24h old or in the future
  3. missing_required  -> agent_id dropped entirely
  4. cost_anomaly      -> cost_usd exceeds plausible boundary ($850.0)
"""
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

# Running on your HOST MACHINE (outside Docker) → use localhost:9092
# Running INSIDE a Docker container             → use kafka:29092
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "agent-events"
KNOWN_AGENTS = [f"agent_{i:03d}" for i in range(1, 21)]
TOOL_NAMES = ["sql_query_executor", "vector_search", "web_scraper", "db_writer"]
POISON_RATE = 0.15


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_valid_event() -> dict:
    tool = random.choice(TOOL_NAMES)
    return {
        "agent_id": random.choice(KNOWN_AGENTS),
        "session_id": str(uuid.uuid4()),
        "action_id": str(uuid.uuid4()),
        "timestamp": _now_iso(),
        "tool_name": tool,
        "execution_time_ms": random.randint(45, 1200),
        "cost_usd": round(random.uniform(0.0001, 0.25), 6),
        "status": "SUCCESS",
        "tool_args": {"query": fake.sentence(), "limit": random.randint(1, 50)},
    }


def make_poison_event() -> dict:
    kind = random.choice(["type_mismatch", "stale_timestamp", "missing_required", "cost_anomaly"])
    event = make_valid_event()

    if kind == "type_mismatch":
        event["cost_usd"] = "UNMETERED"
    elif kind == "stale_timestamp":
        drift = timedelta(hours=random.choice([-48, 24]))
        event["timestamp"] = (datetime.now(timezone.utc) + drift).isoformat()
    elif kind == "missing_required":
        del event["agent_id"]
    elif kind == "cost_anomaly":
        event["cost_usd"] = 850.0

    event["_poison_type"] = kind
    return event


def run(rate_per_sec: float = 5.0, duration_sec: int | None = None):
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    sent = 0
    start = time.time()
    try:
        while duration_sec is None or (time.time() - start) < duration_sec:
            event = make_poison_event() if random.random() < POISON_RATE else make_valid_event()
            producer.send(TOPIC, key=event.get("agent_id", "unknown"), value=event)
            sent += 1
            if sent % 25 == 0:
                print(f"[producer] sent {sent} agent tool events (last: {event.get('_poison_type', 'valid')})")
            time.sleep(1.0 / rate_per_sec)
    except KeyboardInterrupt:
        print(f"\n[producer] stopped after sending {sent} events")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run(rate_per_sec=5.0)
