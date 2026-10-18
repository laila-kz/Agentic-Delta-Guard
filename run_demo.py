"""
run_demo.py - Single-Command Launcher for Agentic Delta Guard Demo
=====================================================================

Launches the full streaming pipeline in Docker Compose mode:
  - Kafka KRaft broker (no ZooKeeper)
  - Synthetic event producer (~15% poison payloads)
  - PySpark Streaming Gatekeeper (Bronze / Quarantine routing)
  - FastAPI Web Console at http://localhost:8888

Alternatively, use the --eval flag to run the offline Real Agent
Evaluation Matrix (all 11 adversarial scenarios, no Kafka required):

    python run_demo.py --eval

Usage:
    python run_demo.py             # Full streaming demo (Docker required)
    python run_demo.py --eval      # Real agent evaluation matrix (offline)
    python run_demo.py --test      # Run full test suite then benchmark
"""

import sys
import time
import subprocess
import webbrowser
import os
import argparse
from pathlib import Path

# ── Project paths ──────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT / "src"))
os.environ["PYTHONPATH"] = str(PROJECT_ROOT / "src")

VENV_PYTHON = str(PROJECT_ROOT / ".venv" / "Scripts" / "python.exe")
if not Path(VENV_PYTHON).exists():
    VENV_PYTHON = sys.executable

BANNER = """
╔══════════════════════════════════════════════════════════════╗
║       🛡️  AGENTIC DELTA GUARD — DEMO LAUNCHER  🛡️           ║
║  Data-Contract Firewall for Autonomous AI Agent Streams      ║
╚══════════════════════════════════════════════════════════════╝
"""


# ── Helpers ────────────────────────────────────────────────────────────────────

def _run(cmd: list, **kwargs) -> subprocess.CompletedProcess:
    """Run a blocking subprocess with consistent project-root CWD."""
    return subprocess.run(cmd, cwd=PROJECT_ROOT, **kwargs)


def _popen(cmd: list, log_file=None) -> subprocess.Popen:
    """Launch a background subprocess, optionally redirecting output to a file."""
    kwargs: dict = {"cwd": PROJECT_ROOT, "env": dict(os.environ, PYTHONPATH=str(PROJECT_ROOT / "src"))}
    if log_file:
        kwargs.update(stdout=log_file, stderr=log_file)
    return subprocess.Popen(cmd, **kwargs)


# ── Kafka check ────────────────────────────────────────────────────────────────

def check_kafka() -> None:
    print("[1/4] Checking Kafka broker status...")
    try:
        res = _run(
            ["docker", "compose", "ps", "--format", "json"],
            capture_output=True, text=True,
        )
        if "kafka" not in res.stdout:
            print("      Starting Kafka + Kafka-UI via Docker Compose...")
            _run(["docker", "compose", "up", "-d", "kafka", "kafka-ui"], check=True)
            print("      Waiting 12s for Kafka broker readiness...")
            time.sleep(12)
        else:
            print("      [OK] Kafka container is already active.")
    except Exception as exc:
        print(f"      [WARN] Docker unavailable ({exc}). Assuming external Kafka on localhost:9092.")


# ── Streaming demo mode ────────────────────────────────────────────────────────

def run_streaming_demo() -> None:
    """Start Producer + Gatekeeper + Web Console; open browser; watch until Ctrl+C."""
    check_kafka()

    logs_dir = PROJECT_ROOT / "logs"
    logs_dir.mkdir(exist_ok=True)

    processes: list[tuple[str, subprocess.Popen]] = []

    print("[2/4] Launching synthetic event Producer...")
    prod_log = open(logs_dir / "producer.log", "w", encoding="utf-8")
    processes.append(("Producer", _popen([VENV_PYTHON, "src/delta_guard/producer.py"], log_file=prod_log)))

    print("[3/4] Launching PySpark Streaming Gatekeeper...")
    gate_log = open(logs_dir / "gatekeeper.log", "w", encoding="utf-8")
    processes.append(("Gatekeeper", _popen([VENV_PYTHON, "src/delta_guard/gatekeeper.py"], log_file=gate_log)))

    print("[4/4] Starting Web Console at http://localhost:8888 ...")
    serv_log = open(logs_dir / "status_server.log", "w", encoding="utf-8")
    processes.append(("StatusServer", _popen(
        [VENV_PYTHON, "-m", "uvicorn", "delta_guard.status_server:app",
         "--host", "0.0.0.0", "--port", "8888"],
        log_file=serv_log,
    )))

    print("\n[OK] All services started.")
    print("     Logs are streamed to: logs/producer.log | logs/gatekeeper.log | logs/status_server.log")
    print("     Opening dashboard: http://localhost:8888\n")
    time.sleep(3)
    try:
        webbrowser.open("http://localhost:8888")
    except Exception:
        pass

    print("[INFO] Press Ctrl+C to stop all services gracefully.\n")
    try:
        while True:
            time.sleep(1)
            for name, proc in processes:
                if proc.poll() is not None:
                    print(f"[WARN] '{name}' exited with code {proc.returncode}. Check logs/.")
    except KeyboardInterrupt:
        print("\n\nShutting down demo services...")
        for name, proc in processes:
            print(f"      Terminating {name}...")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        for fh in [prod_log, gate_log, serv_log]:
            fh.close()
        print("[OK] Demo cleanup complete.\n")


# ── Real agent evaluation mode (offline, no Kafka) ─────────────────────────────

def run_evaluation_matrix() -> None:
    """Run the full 11-scenario real agent evaluation matrix (no Docker needed)."""
    print("\n[EVAL] Running Real Agent Evaluation Matrix — 11 adversarial scenarios...")
    print("       This writes results to runs/ and docs/reports/real_agent_validation.md\n")
    _run(
        [VENV_PYTHON, "scripts/run_real_agent_evaluation.py", "--scenarios", "all"],
        check=False,
    )


# ── Test + benchmark mode ──────────────────────────────────────────────────────

def run_tests_and_benchmark() -> None:
    """Run the full test suite followed by the throughput benchmark."""
    print("\n[TEST] Running full pytest suite (excluding Docker chaos tests)...\n")
    _run(
        [VENV_PYTHON, "-m", "pytest", "tests/", "-v",
         "-m", "not (chaos_docker or requires_docker)"],
        check=False,
    )
    print("\n[BENCH] Running validation throughput benchmark...\n")
    _run([VENV_PYTHON, "src/delta_guard/benchmark.py"], check=False)


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Agentic Delta Guard — Demo Launcher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python run_demo.py           # Full streaming demo (requires Docker)\n"
            "  python run_demo.py --eval    # Offline real agent evaluation matrix\n"
            "  python run_demo.py --test    # Test suite + throughput benchmark\n"
        ),
    )
    parser.add_argument("--eval", action="store_true", help="Run the real agent evaluation matrix (offline, no Docker)")
    parser.add_argument("--test", action="store_true", help="Run the full test suite and benchmark")
    args = parser.parse_args()

    print(BANNER)

    if args.eval:
        run_evaluation_matrix()
    elif args.test:
        run_tests_and_benchmark()
    else:
        run_streaming_demo()


if __name__ == "__main__":
    main()
