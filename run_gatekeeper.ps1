# run_gatekeeper.ps1
# ============================================================
# Agentic Delta Guard - PySpark Gatekeeper launcher for Windows
# ============================================================
#
# Execution modes:
#   .\run_gatekeeper.ps1              -> Native mode (host Python + Java 17 + winutils)
#   .\run_gatekeeper.ps1 -Docker      -> Docker mode (PySpark inside container)
#   .\run_gatekeeper.ps1 -Once        -> Native one-shot mode (availableNow=True, then exits)
#   .\run_gatekeeper.ps1 -Reset       -> Clear checkpoint before starting (force full replay)
#
# Examples:
#   .\run_gatekeeper.ps1                        # continuous streaming, native Python
#   .\run_gatekeeper.ps1 -Once                  # process current Kafka batch then exit
#   .\run_gatekeeper.ps1 -Once -Reset           # clear checkpoint + process batch + exit
#   .\run_gatekeeper.ps1 -Docker                # run inside Docker container

param(
    [switch]$Docker,
    [switch]$Once,
    [switch]$Reset
)

Write-Host ""
Write-Host "===============================================================" -ForegroundColor Cyan
Write-Host "  Agentic Delta Guard - PySpark Streaming Gatekeeper" -ForegroundColor Cyan
Write-Host "  Engine: PySpark 3.5 | Delta Lake 3.2 | Kafka Connector" -ForegroundColor Cyan
Write-Host "===============================================================" -ForegroundColor Cyan
Write-Host ""

# Build argument list for gatekeeper.py
$gatekeeperArgs = @("src/delta_guard/gatekeeper.py")
if ($Once)  { $gatekeeperArgs += "--once" }
if ($Reset) { $gatekeeperArgs += "--reset-checkpoint" }

if ($Docker) {
    # -- Docker mode (recommended for reproducible environments) -------------
    Write-Host "  Mode: DOCKER" -ForegroundColor Green
    Write-Host "  Container: easewithdata/pyspark-jupyter-lab:latest" -ForegroundColor Gray
    Write-Host ""

    # Ensure local directories exist so Docker bind mounts do not fail
    New-Item -ItemType Directory -Path "checkpoints\gatekeeper" -Force | Out-Null
    New-Item -ItemType Directory -Path "data\bronze\agent_events"    -Force | Out-Null
    New-Item -ItemType Directory -Path "data\quarantine\agent_events" -Force | Out-Null
    New-Item -ItemType Directory -Path ".ivy2"                        -Force | Out-Null

    docker run -it --rm `
      --name gatekeeper-runner `
      --network kafka_streaming_project_default `
      -v "${PWD}:/workspace" `
      -v "${PWD}\.ivy2:/root/.ivy2" `
      -w /workspace `
      -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 `
      easewithdata/pyspark-jupyter-lab:latest `
      python3 @gatekeeperArgs

} else {
    # -- Native mode (host Python + Java 17) ---------------------------------
    Write-Host "  Mode: NATIVE (host Python + Java 17 + winutils)" -ForegroundColor Yellow
    Write-Host ""

    # Java 17 - required by PySpark 3.5 (Java 21 removed Subject.getSubject())
    $env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
    $env:PATH      = "$env:JAVA_HOME\bin;$env:PATH"

    # winutils.exe - required by Spark on Windows (bundled under .hadoop/bin)
    $env:HADOOP_HOME = (Resolve-Path ".\.hadoop").Path
    $env:PATH        = "$env:HADOOP_HOME\bin;$env:PATH"

    # Ensure SPARK_LOCAL_DIRS is NOT set in env because Windows paths (C:\) break Spark local dir splitting
    Remove-Item Env:\SPARK_LOCAL_DIRS -ErrorAction SilentlyContinue

    # Kafka: connect to Docker-exposed port
    $env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

    # Ensure required directories exist
    New-Item -ItemType Directory -Path "checkpoints\gatekeeper" -Force | Out-Null
    New-Item -ItemType Directory -Path "data\bronze\agent_events"    -Force | Out-Null
    New-Item -ItemType Directory -Path "data\quarantine\agent_events" -Force | Out-Null
    New-Item -ItemType Directory -Path "scratch\spark_temp"          -Force | Out-Null

    Write-Host "  JAVA_HOME   : $env:JAVA_HOME"  -ForegroundColor Gray
    Write-Host "  HADOOP_HOME : $env:HADOOP_HOME" -ForegroundColor Gray
    Write-Host "  Kafka       : $env:KAFKA_BOOTSTRAP_SERVERS" -ForegroundColor Gray
    if ($Once)  { Write-Host "  --once flag : enabled (availableNow=True)" -ForegroundColor Gray }
    if ($Reset) { Write-Host "  --reset     : checkpoint will be cleared"   -ForegroundColor Gray }
    Write-Host ""

    # Resolve venv Python
    $venvPython = ".\.venv\Scripts\python.exe"
    if (-not (Test-Path $venvPython)) {
        $venvPython = "python"
        Write-Host "  [WARN] .venv not found - falling back to system Python." -ForegroundColor Red
    }

    & $venvPython @gatekeeperArgs
}
