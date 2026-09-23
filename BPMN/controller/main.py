"""Minimal Camunda 7 external-task worker (controller service) for a BPMN teaching demo.

Polls Camunda's REST API for external tasks on one topic, simulates work,
completes the tasks back via REST, and exposes /health and /stats endpoints.
"""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse

CAMUNDA_URL: str = os.getenv("CAMUNDA_URL", "http://camunda:8080/engine-rest")
CAMUNDA_USER: str = os.getenv("CAMUNDA_USER", "demo")
CAMUNDA_PASSWORD: str = os.getenv("CAMUNDA_PASSWORD", "demo")
WORKER_TOPIC: str = os.getenv("WORKER_TOPIC", "do-work")
WORKER_ID: str = os.getenv("WORKER_ID", "controller-worker")
POLL_INTERVAL_SECONDS: float = float(os.getenv("POLL_INTERVAL_SECONDS", "2.0"))
WORK_DURATION_SECONDS: float = float(os.getenv("WORK_DURATION_SECONDS", "1.5"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger: logging.Logger = logging.getLogger("controller")

STARTED_AT: datetime = datetime.now(timezone.utc)
_started_monotonic: float = time.monotonic()
camunda_reachable: bool = False
tasks_processed: int = 0
tasks_failed: int = 0
last_completed_at: str | None = None
worker_task: asyncio.Task[None] | None = None

client: httpx.AsyncClient | None = None


def _now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


async def _camunda_healthcheck() -> bool:
    """Ping Camunda's REST API with retries/backoff (~60s total)."""
    assert client is not None
    delay: float = 2.0
    deadline: float = time.monotonic() + 60.0
    while time.monotonic() < deadline:
        try:
            response: httpx.Response = await client.get(
                f"{CAMUNDA_URL}/process-definition/count"
            )
            if response.status_code == 200:
                logger.info("camunda_reachable process_definition_count=%s", response.text)
                return True
            logger.warning("camunda_healthcheck_status=%d retrying_in=%.1fs", response.status_code, delay)
        except httpx.RequestError as exc:
            logger.warning("camunda_healthcheck_error=%s retrying_in=%.1fs", exc, delay)
        await asyncio.sleep(delay)
        delay = min(delay * 2, 15.0)
    return False


async def _poll_once() -> None:
    """Fetch-and-lock one external task, process it, and complete it via REST."""
    global camunda_reachable, tasks_processed, tasks_failed, last_completed_at
    assert client is not None
    lock_body: dict[str, Any] = {
        "workerId": WORKER_ID,
        "maxTasks": 1,
        "usePriority": False,
        "topics": [{"topicName": WORKER_TOPIC, "lockDuration": 10000}],
    }
    try:
        response: httpx.Response = await client.post(
            f"{CAMUNDA_URL}/external-task/fetchAndLock", json=lock_body
        )
    except httpx.RequestError as exc:
        logger.warning("fetch_and_lock_failed error=%s", exc)
        camunda_reachable = False
        return
    if response.status_code != 200:
        logger.warning("fetch_and_lock_status=%d", response.status_code)
        camunda_reachable = False
        return
    camunda_reachable = True
    tasks: list[dict[str, Any]] = response.json()
    for task in tasks:
        task_id: str = task.get("id", "")
        await _process_task(task_id, task)


async def _process_task(task_id: str, task: dict[str, Any]) -> None:
    """Simulate work for a locked task, then complete (or fail) it in Camunda."""
    global tasks_processed, tasks_failed, last_completed_at
    assert client is not None
    process_instance_id: str = task.get("processInstanceId", "")
    activity_id: str = task.get("activityId", "")
    variables: dict[str, Any] = task.get("variables", {})
    logger.info(
        "external_task_picked_up task_id=%s activity=%s process_instance=%s variables=%s",
        task_id,
        activity_id,
        process_instance_id,
        variables,
    )
    try:
        await asyncio.sleep(WORK_DURATION_SECONDS)
        processed_at: str = _now_iso()
        complete_body: dict[str, Any] = {
            "workerId": WORKER_ID,
            "variables": {
                "result": {"value": "ok", "type": "String"},
                "processed_by": {"value": WORKER_ID, "type": "String"},
                "processed_at": {"value": processed_at, "type": "String"},
            },
        }
        complete_response: httpx.Response = await client.post(
            f"{CAMUNDA_URL}/external-task/{task_id}/complete", json=complete_body
        )
        complete_response.raise_for_status()
        tasks_processed += 1
        last_completed_at = processed_at
        logger.info("external_task_completed task_id=%s", task_id)
    except Exception as exc:  # noqa: BLE001 - any failure must be reported to Camunda
        tasks_failed += 1
        logger.error("external_task_failed task_id=%s error=%s", task_id, exc)
        failure_body: dict[str, Any] = {
            "workerId": WORKER_ID,
            "errorMessage": str(exc)[:2000],
            "retries": 3,
            "retryTimeout": 5000,
        }
        try:
            await client.post(
                f"{CAMUNDA_URL}/external-task/{task_id}/handleFailure", json=failure_body
            )
        except httpx.RequestError as fail_exc:
            logger.error("handle_failure_call_failed task_id=%s error=%s", task_id, fail_exc)


async def _worker_loop() -> None:
    """Poll Camunda forever for external tasks on the worker topic."""
    logger.info("worker_loop_started topic=%s poll_interval=%.1fs", WORKER_TOPIC, POLL_INTERVAL_SECONDS)
    while True:
        await _poll_once()
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Startup: open HTTP client, healthcheck Camunda, launch the worker loop."""
    global client, camunda_reachable, worker_task
    client = httpx.AsyncClient(
        base_url=CAMUNDA_URL,
        auth=httpx.BasicAuth(CAMUNDA_USER, CAMUNDA_PASSWORD),
        timeout=httpx.Timeout(15.0),
    )
    camunda_reachable = await _camunda_healthcheck()
    worker_task = asyncio.create_task(_worker_loop())
    logger.info("controller_started camunda_reachable=%s", camunda_reachable)
    yield
    if worker_task is not None:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
    if client is not None:
        await client.aclose()
    logger.info("controller_stopped")


app: FastAPI = FastAPI(title="BPMN Controller", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health() -> JSONResponse:
    """Return service health and Camunda reachability."""
    uptime_seconds: float = time.monotonic() - _started_monotonic
    return JSONResponse(
        {
            "status": "ok",
            "version": "1.0.0",
            "camunda_url": CAMUNDA_URL,
            "camunda_reachable": camunda_reachable,
            "worker_topic": WORKER_TOPIC,
            "uptime_seconds": round(uptime_seconds, 2),
        }
    )


@app.get("/stats")
async def stats() -> JSONResponse:
    """Return worker statistics."""
    return JSONResponse(
        {
            "tasks_processed": tasks_processed,
            "tasks_failed": tasks_failed,
            "last_completed_at": last_completed_at,
            "worker_topic": WORKER_TOPIC,
            "started_at": STARTED_AT.isoformat(),
        }
    )
