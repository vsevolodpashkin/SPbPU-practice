"""FastAPI microservice exposing system metrics for the monitoring teaching demo.

Owns everything under Security/Monitoring/app: the /health, /metrics and /load
endpoints. All Prometheus metrics are registered on a dedicated custom
CollectorRegistry so they stay cleanly separated from any other instrumentation.
"""

import asyncio
import logging
import math
import os
import threading
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

import psutil
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    generate_latest,
)

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

VERSION = "1.0.0"
_START_TIME = time.time()
_EXECUTOR = ThreadPoolExecutor(max_workers=8, thread_name_prefix="load-worker")

REGISTRY = CollectorRegistry()

CPU_GAUGE = Gauge("app_cpu_usage_percent", "Overall CPU usage in percent across all cores.", registry=REGISTRY)
MEM_USAGE_GAUGE = Gauge("app_memory_usage_bytes", "Resident memory (RSS) of the service process in bytes.", registry=REGISTRY)
MEM_TOTAL_GAUGE = Gauge("app_memory_total_bytes", "Total physical memory of the host in bytes.", registry=REGISTRY)
DISK_USAGE_GAUGE = Gauge("app_disk_usage_bytes", "Used disk space in bytes for the given mount path.", ["path"], registry=REGISTRY)
DISK_TOTAL_GAUGE = Gauge("app_disk_total_bytes", "Total disk space in bytes for the given mount path.", ["path"], registry=REGISTRY)
NET_RX_COUNTER = Counter(
    "app_network_rx_bytes_total", "Bytes received on the given network interface since service startup.",
    ["interface"], registry=REGISTRY,
)
NET_TX_COUNTER = Counter(
    "app_network_tx_bytes_total", "Bytes transmitted on the given network interface since service startup.",
    ["interface"], registry=REGISTRY,
)
LOAD_ACTIVE_GAUGE = Gauge("app_load_active", "Number of background load jobs currently running.", registry=REGISTRY)
LOAD_REQUESTS_COUNTER = Counter("app_load_requests_total", "Total number of /load requests received.", registry=REGISTRY)
REQUEST_COUNTER = Counter(
    "app_request_count_total", "Total HTTP requests by method, endpoint and status code.",
    ["method", "endpoint", "status"], registry=REGISTRY,
)

# Make sure every label combination required by the contract is exposed from the
# very first scrape, even before the first real sample arrives.
DISK_USAGE_GAUGE.labels("/").set(0.0)
DISK_TOTAL_GAUGE.labels("/").set(0.0)
NET_RX_COUNTER.labels("eth0")
NET_TX_COUNTER.labels("eth0")

active_load_jobs: dict[int, list[bytearray]] = {}
_job_counter = 0


def _next_job_id() -> int:
    """Return the next sequential load job id (called from the event loop only)."""
    global _job_counter
    _job_counter += 1
    return _job_counter


def _network_snapshot() -> tuple[int, int]:
    """Return (rx_bytes, tx_bytes) for eth0, falling back to all-NIC totals."""
    per_nic = psutil.net_io_counters(pernic=True)
    if "eth0" in per_nic:
        return int(per_nic["eth0"].bytes_recv), int(per_nic["eth0"].bytes_sent)
    totals = psutil.net_io_counters()
    return int(totals.bytes_recv), int(totals.bytes_sent)


async def _update_system_metrics_loop() -> None:
    """Tick every second: refresh CPU/memory/disk gauges and network counters."""
    psutil.cpu_percent(interval=None)  # first call only initializes the sampler
    prev_rx: int | None = None
    prev_tx: int | None = None
    while True:
        try:
            CPU_GAUGE.set(psutil.cpu_percent(interval=None))
            MEM_USAGE_GAUGE.set(psutil.Process(os.getpid()).memory_info().rss)
            virtual = psutil.virtual_memory()
            MEM_TOTAL_GAUGE.set(virtual.total)
            disk = psutil.disk_usage("/")
            DISK_USAGE_GAUGE.labels("/").set(disk.used)
            DISK_TOTAL_GAUGE.labels("/").set(disk.total)
            rx, tx = _network_snapshot()
            if prev_rx is not None:
                if rx > prev_rx:
                    NET_RX_COUNTER.labels("eth0").inc(rx - prev_rx)
                if tx > prev_tx:
                    NET_TX_COUNTER.labels("eth0").inc(tx - prev_tx)
            prev_rx, prev_tx = rx, tx
        except Exception:
            logger.exception("system metrics update failed")
        await asyncio.sleep(1.0)


def _busy_burn(seconds: float, stop_event: threading.Event) -> None:
    """Burn one CPU core with arithmetic until `seconds` elapse or stop is set."""
    deadline = time.monotonic() + seconds
    value = 0.0
    while not stop_event.is_set() and time.monotonic() < deadline:
        value += 0.5
        value %= 1_000_000.0


def _run_load_job(job_id: int, cpu_seconds: float, mem_mb: int, duration: int) -> None:
    """Run one load job in a worker thread; burns CPU, pins memory, self-terminates."""
    stop_event = threading.Event()
    cpu_threads = min(4, max(1, int(cpu_seconds)))
    burn_seconds = min(cpu_seconds, 8.0)
    workers = [
        threading.Thread(target=_busy_burn, args=(burn_seconds, stop_event), daemon=True)
        for _ in range(cpu_threads)
    ]
    for worker in workers:
        worker.start()
    buffers: list[bytearray] = []
    if mem_mb > 0:
        try:
            buffers.append(bytearray(mem_mb * 1024 * 1024))
        except MemoryError:
            logger.exception("job %d: cannot allocate %d MB", job_id, mem_mb)
    active_load_jobs[job_id] = buffers
    time.sleep(duration)
    stop_event.set()
    for worker in workers:
        worker.join(timeout=1.0)
    active_load_jobs.pop(job_id, None)
    LOAD_ACTIVE_GAUGE.set(len(active_load_jobs))
    logger.info("load job %d finished", job_id)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Start the metrics updater task on startup and stop it on shutdown."""
    updater = asyncio.create_task(_update_system_metrics_loop())
    try:
        yield
    finally:
        updater.cancel()
        _EXECUTOR.shutdown(wait=False, cancel_futures=True)


app = FastAPI(title="Monitoring Demo Service", version=VERSION, lifespan=lifespan)


@app.middleware("http")
async def _count_requests(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Record method, URL path and status code for every HTTP request."""
    response = await call_next(request)
    REQUEST_COUNTER.labels(
        request.method, request.url.path, str(response.status_code)
    ).inc()
    return response


@app.get("/health")
async def health() -> JSONResponse:
    """Liveness probe: 200 while the process is alive, 500 otherwise."""
    if not psutil.Process(os.getpid()).is_running():
        return JSONResponse(status_code=500, content={"status": "dead", "version": VERSION})
    return JSONResponse(
        content={
            "status": "ok",
            "liveness": "alive",
            "uptime_seconds": round(time.time() - _START_TIME, 3),
            "process_uptime_seconds": round(
                time.time() - psutil.Process(os.getpid()).create_time(), 3
            ),
            "version": VERSION,
            "active_load_jobs": len(active_load_jobs),
        }
    )


@app.get("/metrics")
async def metrics() -> Response:
    """Expose all app_* metrics in Prometheus text exposition format."""
    return Response(content=generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)


@app.post("/load")
async def start_load(
    cpu_seconds: float = Query(..., ge=0.0, le=10.0, description="CPU burn seconds per thread"),
    mem_mb: int = Query(..., ge=0, le=1024, description="Memory to pin in MB"),
    duration: int = Query(..., ge=1, le=60, description="Total job lifetime in seconds"),
) -> JSONResponse:
    """Schedule a background CPU/memory load job; returns 202 immediately.

    The heavy work runs in the thread pool executor so the event loop is never
    blocked, and the job always self-terminates after `duration` seconds.
    """
    if math.isnan(cpu_seconds):
        raise HTTPException(status_code=422, detail="cpu_seconds must be a finite number")
    job_id = _next_job_id()
    active_load_jobs[job_id] = []
    LOAD_ACTIVE_GAUGE.set(len(active_load_jobs))
    LOAD_REQUESTS_COUNTER.inc()
    asyncio.get_running_loop().run_in_executor(
        _EXECUTOR, _run_load_job, job_id, cpu_seconds, mem_mb, duration
    )
    logger.info(
        "load job %d scheduled: cpu_seconds=%.2f mem_mb=%d duration=%d",
        job_id, cpu_seconds, mem_mb, duration,
    )
    return JSONResponse(
        status_code=202,
        content={"started": True, "job_id": job_id, "ends_in_seconds": duration},
    )
