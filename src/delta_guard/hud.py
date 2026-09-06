"""Real-time terminal HUD for the Agentic Delta Guard pipeline."""

from __future__ import annotations

import socket
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import psutil
from textual.app import App, ComposeResult
from textual.containers import Container, Grid, Horizontal, Vertical
from textual.widgets import Footer, Header, Label, Static


REPO_ROOT = Path(__file__).resolve().parents[2]
BRONZE_PATH = REPO_ROOT / "data" / "bronze" / "agent_events"
QUARANTINE_PATH = REPO_ROOT / "data" / "quarantine" / "agent_events"
KAFKA_HOST = "localhost"
KAFKA_PORT = 9092
REFRESH_SECONDS = 2.0


@dataclass
class PipelineSnapshot:
    bronze_rows: int = 0
    quarantine_rows: int = 0
    bronze_bytes: int = 0
    quarantine_bytes: int = 0
    kafka_online: bool = False
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    checked_at: str = "--:--:--"
    error: str = ""

    @property
    def total_rows(self) -> int:
        return self.bronze_rows + self.quarantine_rows

    @property
    def quarantine_rate(self) -> float:
        if not self.total_rows:
            return 0.0
        return self.quarantine_rows / self.total_rows * 100

    @property
    def total_bytes(self) -> int:
        return self.bronze_bytes + self.quarantine_bytes


def _directory_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _delta_rows(path: Path) -> int:
    if not path.exists() or not (path / "_delta_log").exists():
        return 0
    try:
        from deltalake import DeltaTable

        return DeltaTable(str(path)).to_pyarrow_table().num_rows
    except Exception:
        return 0


def _kafka_is_online() -> bool:
    try:
        with socket.create_connection((KAFKA_HOST, KAFKA_PORT), timeout=0.35):
            return True
    except OSError:
        return False


def collect_snapshot() -> PipelineSnapshot:
    checked_at = datetime.now(timezone.utc).astimezone().strftime("%H:%M:%S")
    return PipelineSnapshot(
        bronze_rows=_delta_rows(BRONZE_PATH),
        quarantine_rows=_delta_rows(QUARANTINE_PATH),
        bronze_bytes=_directory_bytes(BRONZE_PATH),
        quarantine_bytes=_directory_bytes(QUARANTINE_PATH),
        kafka_online=_kafka_is_online(),
        cpu_percent=psutil.cpu_percent(interval=None),
        memory_percent=psutil.virtual_memory().percent,
        checked_at=checked_at,
    )


def _format_bytes(value: int) -> str:
    units = ("B", "KB", "MB", "GB")
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{value} B"


class MetricCard(Static):
    """Compact labelled metric panel."""

    def __init__(self, title: str, value: str = "--", subtitle: str = "") -> None:
        super().__init__(id=title.lower().replace(" ", "-"))
        self.title = title
        self.value = value
        self.subtitle = subtitle

    def render(self) -> str:
        return f"[bold #d8e5ef]{self.title}[/]\n[bold #56e0c4]{self.value}[/]\n[dim]{self.subtitle}[/]"

    def set_metric(self, value: str, subtitle: str = "") -> None:
        self.value = value
        self.subtitle = subtitle
        self.refresh()


class PipelineHud(App[None]):
    """Live operational view for the local streaming pipeline."""

    TITLE = "AGENTIC DELTA GUARD // LIVE PIPELINE HUD"
    CSS = """
    Screen {
        background: #071016;
        color: #d8e5ef;
    }
    Header {
        background: #0d1d27;
        color: #56e0c4;
        height: 3;
    }
    Footer {
        background: #0d1d27;
        color: #86a3b3;
    }
    #main {
        height: 1fr;
        padding: 1 2;
    }
    #status {
        height: 3;
        content-align: left middle;
        color: #91aebb;
        border-bottom: solid #193541;
    }
    #metrics {
        height: 8;
        grid-size: 4;
        grid-gutter: 1 2;
        margin: 1 0;
    }
    MetricCard {
        background: #0d1d27;
        border: solid #193541;
        padding: 1 2;
        height: 7;
    }
    #lower {
        height: 1fr;
        layout: horizontal;
    }
    .panel {
        background: #0d1d27;
        border: solid #193541;
        padding: 1 2;
        width: 1fr;
        height: 1fr;
        margin-right: 1;
    }
    .panel:last-child {
        margin-right: 0;
    }
    .panel-title {
        color: #56e0c4;
        text-style: bold;
        margin-bottom: 1;
    }
    #health, #footprint, #incidents {
        height: 1fr;
    }
    """
    BINDINGS = [("q", "quit", "Quit"), ("r", "refresh", "Refresh now")]

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Container(id="main"):
            yield Label("INITIALIZING TELEMETRY...", id="status")
            with Grid(id="metrics"):
                yield MetricCard("Bronze rows")
                yield MetricCard("Quarantine")
                yield MetricCard("Quarantine rate")
                yield MetricCard("Storage footprint")
            with Horizontal(id="lower"):
                with Vertical(classes="panel"):
                    yield Label("SYSTEM HEALTH", classes="panel-title")
                    yield Static("Waiting for telemetry", id="health")
                with Vertical(classes="panel"):
                    yield Label("DATA PLANE", classes="panel-title")
                    yield Static("Waiting for telemetry", id="footprint")
                with Vertical(classes="panel"):
                    yield Label("INCIDENT CHANNEL", classes="panel-title")
                    yield Static("Waiting for telemetry", id="incidents")
        yield Footer()

    def on_mount(self) -> None:
        psutil.cpu_percent(interval=None)
        self.refresh_metrics()
        self.set_interval(REFRESH_SECONDS, self.refresh_metrics)

    def action_refresh(self) -> None:
        self.refresh_metrics()

    def refresh_metrics(self) -> None:
        started = time.perf_counter()
        snapshot = collect_snapshot()
        elapsed_ms = (time.perf_counter() - started) * 1000
        kafka_state = "ONLINE" if snapshot.kafka_online else "OFFLINE"
        kafka_color = "#56e0c4" if snapshot.kafka_online else "#ff6b6b"

        self.query_one("#status", Label).update(
            f"● KAFKA [{kafka_color}]{kafka_state}[/]   "
            f"● TELEMETRY LIVE   LAST SYNC {snapshot.checked_at}   "
            f"SCAN {elapsed_ms:.0f}ms"
        )
        self.query_one("#bronze-rows", MetricCard).set_metric(
            f"{snapshot.bronze_rows:,}", "validated records"
        )
        self.query_one("#quarantine", MetricCard).set_metric(
            f"{snapshot.quarantine_rows:,}", "quarantined records"
        )
        self.query_one("#quarantine-rate", MetricCard).set_metric(
            f"{snapshot.quarantine_rate:.1f}%", "of observed records"
        )
        self.query_one("#storage-footprint", MetricCard).set_metric(
            _format_bytes(snapshot.total_bytes), "Bronze + quarantine"
        )
        self.query_one("#health", Static).update(
            f"CPU load       {snapshot.cpu_percent:5.1f}%\n"
            f"Memory         {snapshot.memory_percent:5.1f}%\n"
            f"Kafka broker   [{kafka_color}]{kafka_state}[/]\n"
            f"Refresh cycle  {REFRESH_SECONDS:.1f}s"
        )
        self.query_one("#footprint", Static).update(
            f"Bronze         {_format_bytes(snapshot.bronze_bytes)}\n"
            f"Quarantine     {_format_bytes(snapshot.quarantine_bytes)}\n"
            f"Total records  {snapshot.total_rows:,}\n"
            f"Delta paths    {int(BRONZE_PATH.exists()) + int(QUARANTINE_PATH.exists())}/2"
        )
        incident_color = "#ffca6b" if snapshot.quarantine_rows else "#56e0c4"
        incident_text = "ACTIVE REVIEW" if snapshot.quarantine_rows else "CLEAR"
        self.query_one("#incidents", Static).update(
            f"State          [{incident_color}]{incident_text}[/]\n"
            f"Quarantine     {snapshot.quarantine_rows:,} records\n"
            f"Rate           {snapshot.quarantine_rate:.1f}%\n"
            "Action         inspect triage log"
        )


def main() -> None:
    PipelineHud().run()


if __name__ == "__main__":
    main()
