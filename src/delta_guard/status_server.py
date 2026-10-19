"""
status_server.py — Minimal FastAPI server for the Agentic Delta Guard console.

Serves:
  GET /             → console.html  (the live animated pipeline view)
  GET /status.json  → live pipeline state written by gatekeeper.py / triage.py
  GET /favicon.ico  → 204 No Content

Run locally:
  uvicorn delta_guard.status_server:app --host 0.0.0.0 --port 8888 --reload

Inside Docker (no reload):
  uvicorn delta_guard.status_server:app --host 0.0.0.0 --port 8888
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

# Project root: two levels up from src/delta_guard/status_server.py
_HERE = Path(__file__).resolve()
PROJECT_ROOT = _HERE.parents[2]

CONSOLE_HTML = PROJECT_ROOT / "console.html"
STATUS_JSON  = PROJECT_ROOT / "status.json"

# How many seconds old a status.json may be before we report the pipeline as stale.
# If the Gatekeeper dies or stalls, status.json stops updating and the console
# will reflect STALE_OR_STOPPED rather than silently displaying stale healthy metrics.
STATUS_STALE_SECONDS: int = 60

# ── Empty status served before gatekeeper writes its first batch ─────────────
_EMPTY_STATUS = {
    "bronze_count": 0,
    "quarantine_count": 0,
    "throughput_eps": 0.0,
    "per_signature_counts": {},
    "patch_count": 0,
    "last_patch": None,
    "kafka_online": False,
    "updated_at": "",
}

app = FastAPI(title="Agentic Delta Guard Console", docs_url=None, redoc_url=None)

# Allow browser fetch() from any origin (needed for local file:// development)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/favicon.ico", include_in_schema=False)
async def favicon() -> Response:
    return Response(status_code=204)


@app.get("/status.json")
async def status() -> JSONResponse:
    """Return live pipeline state.

    Adds `pipeline_status` to the response:
      - 'healthy'         — status.json exists and was updated within STATUS_STALE_SECONDS
      - 'stale_or_stopped'— status.json has not been updated recently (Gatekeeper may have stopped)
      - 'starting'        — status.json does not exist yet (Gatekeeper not yet launched)
    """
    if STATUS_JSON.exists():
        try:
            data = json.loads(STATUS_JSON.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            data = _EMPTY_STATUS.copy()
            data["pipeline_status"] = "starting"
        else:
            # Determine staleness from the updated_at timestamp written by gatekeeper.py
            updated_at_str = data.get("updated_at", "")
            pipeline_status = "healthy"
            if updated_at_str:
                try:
                    updated_at = datetime.fromisoformat(updated_at_str.replace("Z", "+00:00"))
                    now_utc = datetime.now(timezone.utc)
                    age_seconds = (now_utc - updated_at).total_seconds()
                    if age_seconds > STATUS_STALE_SECONDS:
                        pipeline_status = "stale_or_stopped"
                except ValueError:
                    pipeline_status = "unknown"
            else:
                pipeline_status = "starting"
            data["pipeline_status"] = pipeline_status
    else:
        data = _EMPTY_STATUS.copy()
        data["pipeline_status"] = "starting"

    return JSONResponse(
        content=data,
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
        },
    )


@app.get("/")
async def console() -> FileResponse:
    """Serve the animated pipeline console."""
    return FileResponse(CONSOLE_HTML, media_type="text/html")
