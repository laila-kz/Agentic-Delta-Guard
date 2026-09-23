"""
run_pipeline.py - Centralized Terminal Pipeline Launcher

Orchestrates Kafka streaming, Producer, PySpark Gatekeeper, and the live Textual Terminal HUD.

Usage:
    python run_pipeline.py
"""

import sys
import time
import subprocess
import os
import signal
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT / "src"))
os.environ["PYTHONPATH"] = str(PROJECT_ROOT / "src")

# Ensure logs directory exists
LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_DIR.mkdir(exist_ok=True)

def check_kafka():
    print("[1/3] Checking Kafka broker status...")
    try:
        res = subprocess.run(
            ["docker", "compose", "ps", "--format", "json"],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT
        )
        if "kafka" not in res.stdout:
            print("   Starting Kafka via Docker Compose...")
            subprocess.run(["docker", "compose", "up", "-d", "kafka", "kafka-ui"], check=True, cwd=PROJECT_ROOT)
            print("   Waiting 10s for Kafka broker readiness...")
            time.sleep(10)
        else:
            print("   [OK] Kafka broker container is active.")
    except Exception as e:
        print(f"   [WARN] Docker check warning: {e}. Assuming external Kafka on localhost:9092.")

def main():
    print("\n=======================================================")
    print("  AGENTIC DELTA GUARD - CENTRAL TERMINAL LAUNCHER  ")
    print("=======================================================\n")
    
    check_kafka()

    venv_python = str(PROJECT_ROOT / ".venv" / "Scripts" / "python.exe")
    if not Path(venv_python).exists():
        venv_python = sys.executable

    print("[2/3] Starting background stream workers (Producer & Gatekeeper)...")
    
    # Redirect background worker output to log files so terminal remains clean for HUD
    producer_log = open(LOGS_DIR / "producer_bg.log", "w", encoding="utf-8")
    gatekeeper_log = open(LOGS_DIR / "gatekeeper_bg.log", "w", encoding="utf-8")

    p_prod = subprocess.Popen(
        [venv_python, "src/delta_guard/producer.py"],
        cwd=PROJECT_ROOT,
        stdout=producer_log,
        stderr=producer_log,
        env=dict(os.environ, PYTHONPATH=str(PROJECT_ROOT / "src"))
    )
    print("   [OK] Producer background process started.")

    p_gate = subprocess.Popen(
        [venv_python, "src/delta_guard/gatekeeper.py"],
        cwd=PROJECT_ROOT,
        stdout=gatekeeper_log,
        stderr=gatekeeper_log,
        env=dict(os.environ, PYTHONPATH=str(PROJECT_ROOT / "src"))
    )
    print("   [OK] PySpark Gatekeeper background process started.")

    print("\n[3/3] Launching Real-time Textual Terminal HUD...")
    time.sleep(2)

    def cleanup(signum=None, frame=None):
        print("\nShutting down pipeline processes...")
        for name, proc, log in [("Producer", p_prod, producer_log), ("Gatekeeper", p_gate, gatekeeper_log)]:
            print(f"   Terminating {name}...")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            log.close()
        print("[OK] All processes cleaned up successfully.\n")

    # Register cleanup hooks
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    try:
        # Run HUD directly in foreground terminal
        subprocess.run(
            [venv_python, "src/delta_guard/hud.py"],
            cwd=PROJECT_ROOT,
            env=dict(os.environ, PYTHONPATH=str(PROJECT_ROOT / "src"))
        )
    except Exception as e:
        print(f"[WARN] HUD exited: {e}")
    finally:
        cleanup()

if __name__ == "__main__":
    main()
