"""
run_demo.py - Single-Command Launcher for Agentic Delta Guard Demo

Runs Kafka in Docker (if not running), starts Producer, Gatekeeper, and Status Server
in parallel within a single Python process, and opens http://localhost:8888 in your browser.

Usage:
    python run_demo.py
"""

import sys
import time
import subprocess
import webbrowser
import os
from pathlib import Path

# Ensure root is in PYTHONPATH
PROJECT_ROOT = Path(__file__).parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT / "src"))
os.environ["PYTHONPATH"] = str(PROJECT_ROOT / "src")

def check_kafka():
    print("[1/4] Checking Kafka broker status...")
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
            print("   [OK] Kafka container is active.")
    except Exception as e:
        print(f"   [WARN] Docker check warning: {e}. Proceeding assuming external Kafka on localhost:9092.")

def start_services():
    processes = []
    venv_python = str(PROJECT_ROOT / ".venv" / "Scripts" / "python.exe")
    if not Path(venv_python).exists():
        venv_python = sys.executable



    print("[2/4] Launching Producer background stream...")
    p_prod = subprocess.Popen(
        [venv_python, "src/delta_guard/producer.py"],
        cwd=PROJECT_ROOT,
        env=dict(os.environ, PYTHONPATH=str(PROJECT_ROOT / "src"))
    )
    processes.append(("Producer", p_prod))

    print("[3/4] Launching Streaming PySpark Gatekeeper...")
    p_gate = subprocess.Popen(
        [venv_python, "src/delta_guard/gatekeeper.py"],
        cwd=PROJECT_ROOT,
        env=dict(os.environ, PYTHONPATH=str(PROJECT_ROOT / "src"))
    )
    processes.append(("Gatekeeper", p_gate))

    print("[4/4] Starting Console Status Server on http://localhost:8888...")
    p_serv = subprocess.Popen(
        [venv_python, "-m", "uvicorn", "delta_guard.status_server:app", "--host", "0.0.0.0", "--port", "8888"],
        cwd=PROJECT_ROOT,
        env=dict(os.environ, PYTHONPATH=str(PROJECT_ROOT / "src"))
    )
    processes.append(("StatusServer", p_serv))

    return processes

def main():
    print("\n=======================================================")
    print("  AGENTIC DELTA GUARD - ONE-COMMAND DEMO LAUNCHER  ")
    print("=======================================================\n")
    
    check_kafka()
    processes = start_services()

    print("\n[OK] All processes started successfully!")
    print("   Opening console dashboard: http://localhost:8888")
    time.sleep(3)
    try:
        webbrowser.open("http://localhost:8888")
    except Exception:
        pass
    
    print("\n[INFO] Press Ctrl+C at any time to stop all services gracefully.\n")
    
    try:
        while True:
            time.sleep(1)
            # Check if any process terminated unexpectedly
            for name, proc in processes:
                if proc.poll() is not None:
                    print(f"[WARN] Process {name} exited with code {proc.returncode}")
    except KeyboardInterrupt:
        print("\nShutting down demo services...")
        for name, proc in processes:
            print(f"   Terminating {name}...")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        print("[OK] Demo cleanup complete.\n")


if __name__ == "__main__":
    main()
