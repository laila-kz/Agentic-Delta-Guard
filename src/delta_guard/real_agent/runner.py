"""
src/delta_guard/real_agent/runner.py
Execution & Replay Engine for Autonomous Real Agent Evaluation.

Modes:
- Live Mode: Runs real LLM (OpenAI/Anthropic) or local model, emitting events to Kafka and saving traces.
- Offline Mode: Uses deterministic ReAct simulator without network or API keys.
- Replay Mode: Reads recorded traces from runs/<id>/agent_trace.jsonl and streams to Kafka at variable rate.
- Fault Injection Mode: Intercepts events via --scenario to evaluate contract enforcement and routing.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Resolve project root
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Load .env if present
env_path = PROJECT_ROOT / ".env"
if env_path.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=env_path)
    except ImportError:
        pass

from src.delta_guard.langchain_adapter import (
    LangChainEventCallback,
    LangChainKafkaProducer,
    create_langchain_producer,
)
from src.delta_guard.real_agent.agent import AutonomousReActAgent, build_llm_model

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("real_agent_runner")


class TraceRecordingKafkaProducer(LangChainKafkaProducer):
    """Kafka producer wrapper that records every emitted contract event to disk."""

    def __init__(
        self,
        trace_file: Path,
        publish_to_kafka: bool = True,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "agent-events",
    ):
        super().__init__(bootstrap_servers=bootstrap_servers, topic=topic)
        self.trace_file = trace_file
        self.publish_to_kafka = publish_to_kafka
        self.recorded_events: List[Any] = []
        self.trace_file.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.trace_file, "a", encoding="utf-8")

    def send(self, event: Any) -> None:
        self.recorded_events.append(event)
        if isinstance(event, str):
            self._fh.write(event + "\n")
        else:
            self._fh.write(json.dumps(event) + "\n")
        self._fh.flush()
        if self.publish_to_kafka:
            super().send(event)

    def close(self) -> None:
        super().close()
        if self._fh:
            try:
                self._fh.close()
            except Exception:
                pass


def load_tasks(task_path: Path, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Loads tasks from JSONL task suite."""
    tasks = []
    with open(task_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                tasks.append(json.loads(line))
    return tasks[:limit] if limit else tasks


def run_evaluation(
    tasks: List[Dict[str, Any]],
    run_id: str,
    publish_to_kafka: bool = True,
    force_offline: bool = False,
    model_name: Optional[str] = None,
    scenario: Optional[str] = None,
) -> Dict[str, Any]:
    """Runs autonomous agent across the task suite and records traces."""
    run_dir = PROJECT_ROOT / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    trace_path = run_dir / "agent_trace.jsonl"
    manifest_path = run_dir / "manifest.jsonl"

    logger.info("Initializing run %s", run_id)
    logger.info("Publishing to Kafka: %s (localhost:9092)", publish_to_kafka)
    logger.info("Traces writing to: %s", trace_path)
    if scenario:
        logger.info("Fault injection scenario active: %s -> Manifest: %s", scenario, manifest_path)

    base_producer = TraceRecordingKafkaProducer(
        trace_file=trace_path,
        publish_to_kafka=publish_to_kafka,
    )

    if scenario:
        from tests.real_agent.fault_injector import FaultInjectingProducerProxy
        producer: Any = FaultInjectingProducerProxy(
            base_producer=base_producer,
            manifest_path=manifest_path,
            scenario_id=scenario,
        )
    else:
        producer = base_producer

    callback = LangChainEventCallback(
        agent_id="real_autonomous_agent",
        kafka_producer=producer,
        strict=False,
    )

    llm = build_llm_model(model_name=model_name, force_offline=force_offline)
    agent = AutonomousReActAgent(model=llm)

    start_time = time.time()
    results = []

    print("\n" + "=" * 65)
    print(f"  AGENTIC DELTA GUARD -- REAL AGENT EVALUATION")
    print(f"  Run ID       : {run_id}")
    print(f"  Scenario     : {scenario or 'A (Baseline / No faults)'}")
    print(f"  Task Count   : {len(tasks)}")
    print(f"  Model Engine : {getattr(llm, 'model_name', str(type(llm).__name__))}")
    print(f"  Kafka Socket : localhost:9092 (topic: agent-events)")
    print("=" * 65 + "\n")

    for i, t in enumerate(tasks, start=1):
        task_id = t.get("task_id", i)
        prompt = t.get("prompt", "")
        session_id = f"eval_sess_{run_id}_{task_id:03d}"

        logger.info("[Task %02d/%02d] (ID %s): %s", i, len(tasks), task_id, prompt)
        res = agent.run(prompt=prompt, callback=callback, session_id=session_id)
        results.append({
            "task_id": task_id,
            "category": t.get("category", "general"),
            "prompt": prompt,
            "session_id": session_id,
            "steps_count": len(res.get("steps", [])),
            "final_answer": res.get("final_answer", "")[:120],
        })

    # Flush kafka producer
    producer.flush()
    producer.close()
    elapsed = time.time() - start_time

    summary = {
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "scenario": scenario or "A",
        "total_tasks": len(tasks),
        "total_events_emitted": len(producer.recorded_events),
        "events_dropped": producer.dropped_count,
        "elapsed_seconds": round(elapsed, 2),
        "throughput_eps": round(len(producer.recorded_events) / max(elapsed, 0.001), 2),
        "trace_file": str(trace_path),
        "manifest_file": str(manifest_path) if scenario else None,
    }

    # Save run summary
    with open(run_dir / "summary.json", "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)

    print("\n" + "=" * 65)
    print("  EVALUATION EXECUTION COMPLETED")
    print(f"  Run Identifier          : {summary['run_id']}")
    print(f"  Scenario Tested         : {summary['scenario']}")
    print(f"  Total Tasks Processed   : {summary['total_tasks']}")
    print(f"  Total Events Emitted    : {summary['total_events_emitted']}")
    print(f"  Telemetry Drops         : {summary['events_dropped']}")
    print(f"  Elapsed Duration        : {summary['elapsed_seconds']}s")
    print(f"  Throughput              : {summary['throughput_eps']} ev/s")
    print(f"  Trace File Stored       : {summary['trace_file']}")
    if summary["manifest_file"]:
        print(f"  Manifest File Stored    : {summary['manifest_file']}")
    print("=" * 65 + "\n")

    return summary


def run_replay(trace_file: Path, rate_eps: float = 50.0) -> None:
    """Replays pre-recorded events to Kafka at a controlled throughput rate."""
    if not trace_file.exists():
        logger.error("Trace file not found: %s", trace_file)
        sys.exit(1)

    producer = LangChainKafkaProducer(bootstrap_servers="localhost:9092", topic="agent-events")
    delay = 1.0 / max(rate_eps, 1.0)
    replayed = 0

    print(f"\n[Replay] Streaming events from {trace_file} to Kafka at ~{rate_eps} ev/s...")
    with open(trace_file, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    event = json.loads(line)
                except Exception:
                    event = line
                producer.send(event)
                replayed += 1
                if delay > 0:
                    time.sleep(delay)

    producer.flush()
    producer.close()
    print(f"[Replay] Completed. Successfully published {replayed} events to Kafka topic 'agent-events'.\n")


def main():
    parser = argparse.ArgumentParser(description="Real Agent Evaluation Runner for Agentic Delta Guard")
    parser.add_argument("--tasks", type=str, default="tests/real_agent/tasks.jsonl", help="Path to tasks.jsonl")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of tasks to run")
    parser.add_argument("--run-id", type=str, default=None, help="Custom run identifier")
    parser.add_argument("--no-kafka", action="store_true", help="Record traces without sending to Kafka")
    parser.add_argument("--offline", action="store_true", help="Force offline simulated LLM mode (no API calls)")
    parser.add_argument("--model", type=str, default=None, help="Override model name (e.g. gpt-4o-mini)")
    parser.add_argument("--replay", type=str, default=None, help="Replay mode: path to agent_trace.jsonl to stream")
    parser.add_argument("--rate", type=float, default=50.0, help="Replay throughput (ev/s)")
    parser.add_argument(
        "--scenario",
        type=str,
        default=None,
        choices=["A", "B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B9", "B10"],
        help="Fault injection scenario ID to inject (Phase 3)",
    )
    args = parser.parse_args()

    if args.replay:
        run_replay(Path(args.replay), rate_eps=args.rate)
        return

    run_id = args.run_id or f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    tasks_path = PROJECT_ROOT / args.tasks
    tasks = load_tasks(tasks_path, limit=args.limit)

    run_evaluation(
        tasks=tasks,
        run_id=run_id,
        publish_to_kafka=not args.no_kafka,
        force_offline=args.offline,
        model_name=args.model,
        scenario=args.scenario,
    )


if __name__ == "__main__":
    main()
