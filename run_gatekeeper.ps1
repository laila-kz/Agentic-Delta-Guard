# run_gatekeeper.ps1
# Runs the Gatekeeper inside the PySpark container where Java 11 + PySpark + Delta Lake are fully preconfigured.
# It mounts the current repository, connects to the Kafka network, and executes gatekeeper.py.

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  Starting AgenticDeltaGuard Gatekeeper (PySpark + Delta)" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# Ensure local checkpoint, cache, and data directories exist
New-Item -ItemType Directory -Path "checkpoints\gatekeeper" -Force | Out-Null
New-Item -ItemType Directory -Path "data\bronze\agent_events" -Force | Out-Null
New-Item -ItemType Directory -Path "data\quarantine\agent_events" -Force | Out-Null
New-Item -ItemType Directory -Path ".ivy2" -Force | Out-Null

docker run -it --rm `
  --name gatekeeper-runner `
  --network kafka_streaming_project_default `
  -v "${PWD}:/workspace" `
  -v "${PWD}\.ivy2:/root/.ivy2" `
  -w /workspace `
  easewithdata/pyspark-jupyter-lab:latest `
  python3 src/delta_guard/gatekeeper.py
