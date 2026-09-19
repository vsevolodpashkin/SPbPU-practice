"""Minimal FastAPI service emitting structured JSON logs to Logstash over TCP.

Part of the ELK teaching demo: three endpoints (``/health``, ``/login``,
``/users/{user_id}``) produce structured, field-rich JSON records. Each record
flows ``python-logstash-async`` -> Logstash -> Elasticsearch -> Kibana.

Every log line carries per-request context propagated via ``ContextVar`` so
that handlers and middleware can stay focused on their own concerns and the
filter does the heavy lifting for fields like ``request_id`` / ``method`` /
``path`` / ``client_ip``.
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
import os
import random
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from logstash_async.formatter import LogstashFormatter
from logstash_async.handler import AsynchronousLogstashHandler

# ---------------------------------------------------------------------------
# Static "database"
# ---------------------------------------------------------------------------

USERS: dict[int, dict[str, Any]] = {
    1: {"id": 1, "username": "alice", "email": "alice@example.com", "role": "admin"},
    2: {"id": 2, "username": "bob", "email": "bob@example.com", "role": "user"},
    3: {"id": 3, "username": "demo", "email": "demo@example.com", "role": "viewer"},
}

CREDENTIALS: dict[str, tuple[str, int]] = {
    "alice": ("secret123", 1),
    "bob": ("horsebattery", 2),
    "demo": ("demo", 3),
}

APP_VERSION = "1.0.0"
_START_MONOTONIC = time.monotonic()
_STARTED_AT = datetime.now(timezone.utc).isoformat()

# ---------------------------------------------------------------------------
# Per-request context propagated via contextvars (no global mutable state)
# ---------------------------------------------------------------------------

_request_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "request_id", default=None
)
_method_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "method", default=None
)
_path_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "path", default=None
)
_client_ip_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "client_ip", default=None
)

# ---------------------------------------------------------------------------
# App + loggers
# ---------------------------------------------------------------------------

app = FastAPI(title="ELK demo service", version=APP_VERSION)

_access_logger = logging.getLogger("app.access")
_auth_logger = logging.getLogger("app.auth")
_users_logger = logging.getLogger("app.users")
_error_logger = logging.getLogger("app.error")

_logstash_handler: AsynchronousLogstashHandler | None = None


class _RequestContextFilter(logging.Filter):
    """Inject per-request context (set by middleware) into every log record.

    The filter mutates the record before the handler enqueues it; since
    python-logstash-async's worker formats the same record object later, the
    injected fields show up in the final JSON document.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = _request_id_var.get() or "none"
        record.method = _method_var.get() or "unknown"
        record.path = _path_var.get() or "unknown"
        record.client_ip = _client_ip_var.get() or "none"
        return True


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _logstash_queue_size() -> int:
    """Best-effort read of python-logstash-async internal queue depth."""
    worker = getattr(_logstash_handler, "_worker_thread", None) if _logstash_handler else None
    queue = getattr(worker, "queue", None) or getattr(worker, "_queue", None) if worker else None
    if queue is None:
        return 0
    try:
        return int(queue.qsize())
    except Exception:
        return 0


def _error_type_for(status: int) -> str | None:
    if status == 404:
        return "not_found"
    if status == 401:
        return "invalid_credentials"
    if status == 422:
        return "validation_error"
    return None


def _configure_logging() -> None:
    """Wire the ``app`` logger to Logstash (async TCP) and a WARNING stdout line."""
    global _logstash_handler
    host = os.environ.get("LOGSTASH_HOST", "logstash")
    port = int(os.environ.get("LOGSTASH_PORT", "5000"))
    _logstash_handler = AsynchronousLogstashHandler(
        host=host, port=port, database_path=None
    )
    _logstash_handler.setFormatter(LogstashFormatter())
    _logstash_handler.setLevel(logging.INFO)
    _logstash_handler.addFilter(_RequestContextFilter())

    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    console.addFilter(_RequestContextFilter())

    root = logging.getLogger("app")
    root.setLevel(logging.INFO)
    if root.handlers:
        root.handlers.clear()
    root.addHandler(_logstash_handler)
    root.addHandler(console)

    root.info(
        "service_started",
        extra={
            "started_at": _STARTED_AT,
            "logstash_host": host,
            "logstash_port": port,
            "version": APP_VERSION,
        },
    )


_configure_logging()


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

@app.middleware("http")
async def access_log_middleware(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Emit one INFO line per HTTP request and a WARNING if latency > 200ms."""
    rid_token = _request_id_var.set(uuid.uuid4().hex)
    method_token = _method_var.set(request.method)
    path_token = _path_var.set(request.url.path)
    ip_token = _client_ip_var.set(_client_ip(request))
    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        latency_ms = (time.perf_counter() - start) * 1000.0
        _error_logger.exception(
            "request_failed",
            extra={
                "method": request.method,
                "path": request.url.path,
                "status_code": 500,
                "latency_ms": round(latency_ms, 3),
                "client_ip": _client_ip(request),
                "error_type": "internal_error",
            },
        )
        raise
    else:
        latency_ms = (time.perf_counter() - start) * 1000.0
        access_extra: dict[str, Any] = {
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "latency_ms": round(latency_ms, 3),
            "client_ip": _client_ip(request),
        }
        err_type = _error_type_for(response.status_code)
        if err_type is not None:
            access_extra["error_type"] = err_type
        _access_logger.info("http_request", extra=access_extra)
        if latency_ms > 200.0:
            _access_logger.warning(
                "slow_request",
                extra={**access_extra, "threshold_ms": 200, "error_type": "slow_request"},
            )
        return response
    finally:
        _request_id_var.reset(rid_token)
        _method_var.reset(method_token)
        _path_var.reset(path_token)
        _client_ip_var.reset(ip_token)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health() -> dict[str, Any]:
    """Liveness probe: version, uptime and pending log queue depth."""
    return {
        "status": "ok",
        "version": APP_VERSION,
        "uptime_seconds": round(time.monotonic() - _START_MONOTONIC, 3),
        "log_queue_size": _logstash_queue_size(),
    }


async def _simulate_db_latency(user_id: int) -> None:
    """Spread realistic-ish latency; ids divisible by 7 occasionally spike."""
    base_ms = random.uniform(5.0, 50.0)
    spike_ms = random.uniform(50.0, 250.0) if user_id % 7 == 0 else 0.0
    await asyncio.sleep((base_ms + spike_ms) / 1000.0)


@app.post("/login")
async def login(username: str, password: str) -> JSONResponse:
    """Authenticate against the hardcoded credential store (demo only)."""
    credentials = CREDENTIALS.get(username)
    if credentials is None:
        _auth_logger.warning(
            "login_failed",
            extra={
                "username": username,
                "reason": "unknown_user",
                "error_type": "invalid_credentials",
            },
        )
        return JSONResponse(status_code=401, content={"detail": "invalid_credentials"})
    expected_password, user_id = credentials
    if password != expected_password:
        _auth_logger.warning(
            "login_failed",
            extra={
                "username": username,
                "user_id": user_id,
                "reason": "bad_password",
                "error_type": "invalid_credentials",
            },
        )
        return JSONResponse(status_code=401, content={"detail": "invalid_credentials"})
    _auth_logger.info(
        "login_succeeded",
        extra={"username": username, "user_id": user_id},
    )
    return JSONResponse(
        status_code=200,
        content={
            "access_token": "fake-jwt-" + uuid.uuid4().hex,
            "user_id": user_id,
            "username": username,
            "token_type": "bearer",
        },
    )


@app.get("/users/{user_id}")
async def get_user(user_id: int) -> JSONResponse:
    """Look up a user by integer id with a simulated DB latency."""
    await _simulate_db_latency(user_id)
    user = USERS.get(user_id)
    if user is None:
        _users_logger.error(
            "user_not_found",
            extra={"user_id": user_id, "error_type": "not_found"},
        )
        return JSONResponse(
            status_code=404,
            content={"detail": "user not found", "user_id": user_id},
        )
    _users_logger.info(
        "user_lookup",
        extra={"user_id": user_id, "username": user["username"]},
    )
    return JSONResponse(status_code=200, content=user)
