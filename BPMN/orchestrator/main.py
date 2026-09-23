"""Minimal FastAPI orchestrator wrapping the Camunda 7 REST API.

Deploys a demo BPMN process (start -> external service task -> end), starts
process instances and exposes lifecycle queries. Intended for a local
docker-compose teaching demo on an internal network only.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Optional

import httpx
from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger: logging.Logger = logging.getLogger("orchestrator")

CAMUNDA_URL: str = os.getenv("CAMUNDA_URL", "http://camunda:8080/engine-rest").rstrip("/")
CAMUNDA_USER: str = os.getenv("CAMUNDA_USER", "demo")
CAMUNDA_PASSWORD: str = os.getenv("CAMUNDA_PASSWORD", "demo")
BPMN_FILE: Path = Path(__file__).resolve().parent / "bpmn" / "demo.bpmn"

VERSION: str = "1.0.0"
_started_at: float = time.monotonic()

# Global mutable state: httpx client cache, reachability flag, deployment id.
_client: Optional[httpx.AsyncClient] = None
camunda_reachable: bool = False
latest_deployment_id: Optional[str] = None

_ACTIVITY_FIELDS: tuple[str, ...] = (
    "id",
    "parentActivityInstanceId",
    "activityId",
    "activityName",
    "activityType",
    "activityInstanceState",
    "executionIds",
    "incidentIds",
)


def normalize_variables(vars_dict: dict[str, Any]) -> dict[str, Any]:
    """Translate flat or already-typed variables into Camunda's typed format.

    Accepts either Camunda's typed form ``{"data": {"value": ..., "type": ...}}``
    (passed through unchanged) or flat values ``{"data": "hello"}``, which are
    auto-wrapped with a best-guess type (String/Integer/Boolean/Double).
    """
    out: dict[str, Any] = {}
    for key, value in vars_dict.items():
        if isinstance(value, dict) and "value" in value:
            out[key] = value
        elif isinstance(value, bool):
            out[key] = {"value": value, "type": "Boolean"}
        elif isinstance(value, int):
            out[key] = {"value": value, "type": "Integer"}
        elif isinstance(value, float):
            out[key] = {"value": value, "type": "Double"}
        else:
            out[key] = {"value": str(value), "type": "String"}
    return out


def _get_client() -> httpx.AsyncClient:
    """Return the module-level httpx client, raising if not yet initialised."""
    if _client is None:
        raise RuntimeError("httpx client is not initialised")
    return _client


async def _check_camunda() -> bool:
    """Probe Camunda's REST API; return True if it answers successfully."""
    try:
        resp = await _get_client().get(f"{CAMUNDA_URL}/process-definition/count")
        resp.raise_for_status()
        return True
    except httpx.HTTPError as exc:
        logger.debug("Camunda health probe failed: %s", exc)
        return False


async def _wait_for_camunda(max_wait: float = 60.0) -> None:
    """Retry the Camunda health probe with exponential backoff up to max_wait."""
    global camunda_reachable
    deadline: float = time.monotonic() + max_wait
    delay: float = 1.0
    while True:
        camunda_reachable = await _check_camunda()
        if camunda_reachable:
            logger.info("Camunda is reachable at %s", CAMUNDA_URL)
            return
        if time.monotonic() >= deadline:
            logger.warning(
                "Camunda not reachable at %s after %.0fs", CAMUNDA_URL, max_wait
            )
            return
        logger.info("Camunda not reachable yet; retrying in %.1fs", delay)
        await asyncio.sleep(delay)
        delay = min(delay * 2, 10.0)


async def _camunda(method: str, path: str, **kwargs: Any) -> Any:
    """Call the Camunda REST API, returning parsed JSON or raising HTTPException.

    Non-2xx responses are re-raised as HTTPException with the Camunda status
    code and the original response body; transport errors map to 502.
    """
    url: str = f"{CAMUNDA_URL}{path}"
    try:
        resp = await _get_client().request(method, url, **kwargs)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code, detail=exc.response.text
        ) from exc
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Camunda request failed: {exc}",
        ) from exc


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Open the shared httpx client on startup and close it on shutdown."""
    global _client
    _client = httpx.AsyncClient(
        auth=httpx.BasicAuth(CAMUNDA_USER, CAMUNDA_PASSWORD),
        timeout=10.0,
    )
    try:
        await _wait_for_camunda()
        yield
    finally:
        await _client.aclose()
        _client = None


app: FastAPI = FastAPI(
    title="BPMN Demo Orchestrator",
    version=VERSION,
    lifespan=lifespan,
)


class StartBody(BaseModel):
    """Optional JSON body for POST /start (variables and/or business key)."""

    variables: Optional[dict[str, Any]] = None
    businessKey: Optional[str] = None


@app.get("/health")
async def health() -> dict[str, Any]:
    """Report service health, Camunda reachability and uptime."""
    global camunda_reachable
    camunda_reachable = await _check_camunda()
    return {
        "status": "ok",
        "version": VERSION,
        "camunda_url": CAMUNDA_URL,
        "camunda_reachable": camunda_reachable,
        "uptime_seconds": round(time.monotonic() - _started_at, 3),
    }


@app.get("/bpmn")
async def bpmn() -> Response:
    """Return the demo BPMN XML process definition."""
    return Response(content=BPMN_FILE.read_bytes(), media_type="application/xml")


@app.post("/deploy")
async def deploy() -> dict[str, Any]:
    """Deploy demo.bpmn to Camunda and remember the latest deployment id."""
    global latest_deployment_id
    with BPMN_FILE.open("rb") as fh:
        payload: dict[str, Any] = await _camunda(
            "POST",
            "/deployment/create",
            files={"data": ("demo.bpmn", fh, "application/xml")},
            data={"deployment-name": "demo-deployment"},
        )
    latest_deployment_id = payload.get("id")
    logger.info("Deployed demo BPMN (deployment id: %s)", latest_deployment_id)
    return payload


@app.post("/start")
async def start_process(
    key: str = Query(..., description="BPMN process definition key"),
    body: Optional[StartBody] = None,
) -> dict[str, Any]:
    """Start a process instance by definition key.

    The optional JSON body may carry ``variables`` (flat or Camunda-typed,
    see normalize_variables) and/or a ``businessKey``.
    """
    payload: dict[str, Any] = {}
    if body is not None:
        if body.variables:
            payload["variables"] = normalize_variables(body.variables)
        if body.businessKey is not None:
            payload["businessKey"] = body.businessKey
    started: dict[str, Any] = await _camunda(
        "POST",
        f"/process-definition/key/{key}/start",
        json=payload,
    )
    return {
        "process_instance_id": str(started["id"]),
        "definition_key": started.get("definitionKey", key),
        "business_key": started.get("businessKey"),
    }


def _flatten_activities(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten nested activity-instance trees into one list of trimmed dicts."""
    flattened: list[dict[str, Any]] = []

    def walk(instances: list[dict[str, Any]]) -> None:
        for item in instances:
            flattened.append(
                {field: item[field] for field in _ACTIVITY_FIELDS if field in item}
            )
            walk(item.get("childActivityInstances") or [])

    walk(payload.get("activityInstances") or [])
    return flattened


@app.get("/process/{instance_id}")
async def process_details(instance_id: str) -> dict[str, Any]:
    """Return process instance state augmented with a flattened activity list."""
    instance: dict[str, Any] = await _camunda(
        "GET", f"/process-instance/{instance_id}"
    )
    activities: dict[str, Any] = await _camunda(
        "GET", f"/process-instance/{instance_id}/activity-instances"
    )
    instance["activity_instances"] = _flatten_activities(activities)
    return instance


@app.get("/process/{instance_id}/tasks")
async def process_tasks(instance_id: str) -> Any:
    """Return active tasks for a process instance (as-is from Camunda)."""
    return await _camunda("GET", f"/task?processInstanceId={instance_id}")


@app.get("/process/{instance_id}/history")
async def process_history(instance_id: str) -> Any:
    """Return historic activity instances for a process instance (as-is)."""
    return await _camunda(
        "GET", f"/history/activity-instance?processInstanceId={instance_id}"
    )


@app.get("/process/{instance_id}/variables")
async def process_variables(instance_id: str) -> Any:
    """Return process instance variables with deserialized values (as-is)."""
    return await _camunda(
        "GET",
        f"/process-instance/{instance_id}/variables?deserializeValues=true",
    )
