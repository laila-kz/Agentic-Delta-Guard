"""
run_pipeline.py - Centralized Terminal Pipeline Launcher (Path A)
==================================================================

Orchestrates Kafka health check, synthetic event Producer, PySpark Streaming
Gatekeeper, and the live Textual Terminal HUD — all from a single command.

Modes:
    python run_pipeline.py              # Full pipeline + live Textual HUD
    python run_pipeline.py --no-hud     # Background workers only (no HUD)
    python run_pipeline.py --triage     # Run triage on quarantine after pipeline exits

Flow:
    [1/3] Kafka health check (Docker Compose auto-start if needed)
    [2/3] Producer + Gatekeeper launched in background (logs → logs/)
    [3/3] Textual HUD launched in foreground (press q to quit)

On exit (Ctrl+C or q):
    - Background Producer + Gatekeeper are terminated gracefully
    - Log files are flushed and closed
    - Optionally runs triage.py on quarantined records (--triage flag)
"""

import sys
import time
import subprocess
import os
import signal
import argparse
from pathlib import Path

# ── Project paths ──────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT / "src"))
os.environ["PYTHONPATH"] = str(PROJECT_ROOT / "src")

LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_DIR.mkdir(exist_ok=True)

VENV_PYTHON = str(PROJECT_ROOT / ".venv" / "Scripts" / "python.exe")
if not Path(VENV_PYTHON).exists():
    VENV_PYTHON = sys.executable

BANNER = """
╔═══════════════════════════════════════════════════════════════╗
║     🛡️  AGENTIC DELTA GUARD — TERMINAL PIPELINE (PATH A)     ║
║   Kafka → PySpark Gatekeeper → Bronze/Quarantine Delta Lake   ║
╚═══════════════════════════════════════════════════════════════╝
"""


# ── Helpers ────────────────────────────────────────────────────────────────────

def _run(cmd: list, **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=PROJECT_ROOT, **kwargs)


def _popen(cmd: list, log_path: Path) -> tuple[subprocess.Popen, object]:
    log_fh = open(log_path, "w", encoding="utf-8")
    proc = subprocess.Popen(
        cmd,
        cwd=PROJECT_ROOT,
        stdout=log_fh,
        stderr=log_fh,
        env=dict(os.environ, PYTHONPATH=str(PROJECT_ROOT / "src")),
    )
    return proc, log_fh


# ── Kafka check ────────────────────────────────────────────────────────────────

def check_kafka() -> None:
    print("[1/3] Checking Kafka broker status...")
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


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Agentic Delta Guard — Terminal Pipeline Launcher")
    parser.add_argument("--no-hud",  action="store_true", help="Launch workers only, skip the Textual HUD")
    parser.add_argument("--triage",  action="store_true", help="Run triage.py on quarantine records after pipeline exits")
    args = parser.parse_args()

    print(BANNER)
    check_kafka()

    # ── Start background workers ──────────────────────────────────────────────
    print("[2/3] Starting background stream workers (Producer & Gatekeeper)...")

    p_prod, prod_log = _popen(
        [VENV_PYTHON, "src/delta_guard/producer.py"],
        LOGS_DIR / "producer.log",
    )
    print(f"      [OK] Producer started  → logs/producer.log")

    p_gate, gate_log = _popen(
        [VENV_PYTHON, "src/delta_guard/gatekeeper.py"],
        LOGS_DIR / "gatekeeper.log",
    )
    print(f"      [OK] Gatekeeper started → logs/gatekeeper.log")

    # ── Cleanup handler ───────────────────────────────────────────────────────
    def cleanup(signum=None, frame=None) -> None:
        print("\n\nShutting down pipeline processes...")
        for name, proc, fh in [("Producer", p_prod, prod_log), ("Gatekeeper", p_gate, gate_log)]:
            print(f"      Terminating {name}...")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            fh.close()
        print("[OK] All background processes cleaned up.\n")

        if args.triage:
            print("[TRIAGE] Running automated incident triage on quarantined records...")
            _run([VENV_PYTHON, "src/delta_guard/triage.py"], check=False)

    signal.signal(signal.SIGINT,  cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    if args.no_hud:
        # ── Background-only mode ──────────────────────────────────────────────
        print("\n[INFO] --no-hud mode: workers running in background.")
        print("       Tail logs:  Get-Content logs\\gatekeeper.log -Wait")
        print("       Stop:       Ctrl+C\n")
        try:
            while True:
                time.sleep(1)
                for name, proc in [("Producer", p_prod), ("Gatekeeper", p_gate)]:
                    if proc.poll() is not None:
                        print(f"[WARN] '{name}' exited with code {proc.returncode}. Check logs/.")
        except KeyboardInterrupt:
            cleanup()
    else:
        # ── HUD foreground mode ───────────────────────────────────────────────
        print("\n[3/3] Launching Real-time Textual Terminal HUD (press 'q' to quit)...")
        time.sleep(2)
        try:
            _run(
                [VENV_PYTHON, "src/delta_guard/hud.py"],
                env=dict(os.environ, PYTHONPATH=str(PROJECT_ROOT / "src")),
            )
        except Exception as exc:
            print(f"[WARN] HUD exited with error: {exc}")
        finally:
            cleanup()


if __name__ == "__main__":
    main()
