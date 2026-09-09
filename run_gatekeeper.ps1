# run_gatekeeper.ps1
# Two modes:
#   .\run_gatekeeper.ps1           → Docker mode (fully pre-configured, recommended)
#   .\run_gatekeeper.ps1 -Native   → Native mode (host Python + Java 17 + winutils)
param([switch]$Native)

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  Starting AgenticDeltaGuard Gatekeeper (PySpark + Delta)" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# Ensure local checkpoint, cache, and data directories exist
New-Item -ItemType Directory -Path "checkpoints\gatekeeper" -Force | Out-Null
New-Item -ItemType Directory -Path "data\bronze\agent_events" -Force | Out-Null
New-Item -ItemType Directory -Path "data\quarantine\agent_events" -Force | Out-Null
New-Item -ItemType Directory -Path ".ivy2" -Force | Out-Null

if ($Native) {
    Write-Host "  Mode: NATIVE (host Python + Java 17)" -ForegroundColor Yellow
    Write-Host "==========================================================" -ForegroundColor Cyan

    # Java 17 — required by PySpark 3.5 (Java 21 removed Subject.getSubject())
    $env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
    $env:PATH = "$env:JAVA_HOME\bin;$env:PATH"

    # winutils.exe — required by Spark on Windows (bundled in .hadoop/bin)
    $env:HADOOP_HOME = (Resolve-Path ".\.hadoop").Path
    $env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"

    # Connect to Redpanda/Kafka exposed on localhost (Docker publishes port 9092)
    $env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

    Write-Host "  JAVA_HOME  : $env:JAVA_HOME" -ForegroundColor Gray
    Write-Host "  HADOOP_HOME: $env:HADOOP_HOME" -ForegroundColor Gray
    Write-Host "  Kafka      : $env:KAFKA_BOOTSTRAP_SERVERS" -ForegroundColor Gray
    Write-Host ""

    python src/delta_guard/gatekeeper.py
} else {
    Write-Host "  Mode: DOCKER (recommended)" -ForegroundColor Green
    Write-Host "==========================================================" -ForegroundColor Cyan

    docker run -it --rm `
      --name gatekeeper-runner `
      --network kafka_streaming_project_default `
      -v "${PWD}:/workspace" `
      -v "${PWD}\.ivy2:/root/.ivy2" `
      -w /workspace `
      easewithdata/pyspark-jupyter-lab:latest `
      python3 src/delta_guard/gatekeeper.py
}
