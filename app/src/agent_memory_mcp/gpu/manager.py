"""On-demand GPU container lifecycle manager with cross-project Redis coordination.

Adapted from tg_kb's GpuServiceManager. The key addition over the original is
shared coordination via Redis: both AMM and tg_kb register the same physical
TEI containers, but each project tracks its own usage. The Redis key
`gpu:touch:<service>` stores the most recent touch timestamp from any project,
so the idle checker won't stop a container that the *other* project just used.
"""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass

import httpx
import structlog

log = structlog.get_logger(__name__)


# Redis is optional — if unavailable, the manager falls back to local-only
# coordination (which is what tg_kb originally did).
try:
    import redis
    _redis_available = True
except ImportError:
    _redis_available = False


@dataclass
class GpuService:
    """Registration for a GPU-backed container."""
    name: str
    container_name: str
    health_url: str
    health_timeout: int = 120
    last_used_at: float = 0.0
    last_started_at: float = 0.0
    last_stopped_at: float = 0.0
    last_exhausted_at: float = 0.0
    last_attempt_failed_at: float = 0.0
    last_attempt_reason: str = ""
    _running: bool = False

    def touch(self) -> None:
        self.last_used_at = time.monotonic()


class GpuServiceManager:
    """Start/stop GPU containers on demand via Docker API + Redis coordination."""

    def __init__(
        self,
        docker_host: str,
        redis_url: str | None = None,
        project_id: str = "amm",
    ) -> None:
        import docker
        self._docker = docker.DockerClient(base_url=docker_host, timeout=30)
        self._services: dict[str, GpuService] = {}
        self._running = False
        self._idle_thread: threading.Thread | None = None
        self._project_id = project_id

        # Redis is for cross-project coordination only; not required.
        self._redis = None
        if redis_url and _redis_available:
            try:
                self._redis = redis.Redis.from_url(
                    redis_url, socket_timeout=2, socket_connect_timeout=2,
                    decode_responses=True,
                )
                self._redis.ping()
                log.info("gpu_redis_coordination_enabled", url=redis_url)
            except Exception as exc:
                log.warning(
                    "gpu_redis_coordination_unavailable",
                    error=str(exc)[:200],
                )
                self._redis = None

    def register(self, svc: GpuService) -> None:
        self._services[svc.name] = svc
        try:
            container = self._docker.containers.get(svc.container_name)
            if container.status == "running":
                svc._running = True
                svc.touch()
                svc.last_started_at = time.monotonic()
                log.info("gpu_service_adopted_running", name=svc.name)
        except Exception:
            log.warning("gpu_service_register_probe_failed", name=svc.name)

    # ----- Redis coordination helpers -----

    def _redis_key(self, service_name: str) -> str:
        return f"gpu:touch:{service_name}"

    def _redis_touch(self, service_name: str, idle_timeout: int) -> None:
        """Write our touch into Redis with TTL = 2 * idle_timeout."""
        if not self._redis:
            return
        try:
            value = f"{time.time()}:{self._project_id}"
            self._redis.set(
                self._redis_key(service_name),
                value,
                ex=max(idle_timeout * 2, 60),
            )
        except Exception:
            pass  # Coordination is best-effort

    def _redis_external_age_sec(self, service_name: str) -> float | None:
        """Return age in seconds of the most recent touch from ANY project, or None.

        Returns None if Redis is unavailable or the key is missing.
        Returns 0 if the key is set but malformed.
        """
        if not self._redis:
            return None
        try:
            raw = self._redis.get(self._redis_key(service_name))
            if not raw:
                return None
            ts_str = raw.split(":", 1)[0]
            ts = float(ts_str)
            return max(0.0, time.time() - ts)
        except Exception:
            return None

    def is_idle(self, svc: GpuService, idle_timeout: int) -> bool:
        """Check if a service is idle, considering both local and cross-project usage."""
        if not svc._running:
            return False
        local_idle = (time.monotonic() - svc.last_used_at) > idle_timeout
        if not local_idle:
            return False
        # Local says idle; check Redis to see if another project just used it.
        external_age = self._redis_external_age_sec(svc.name)
        if external_age is None:
            return True  # No coordination data — trust local
        return external_age > idle_timeout

    # ----- Core lifecycle -----

    _START_RETRY_BACKOFFS_SEC = (30, 60, 120, 240, 480)

    async def ensure_running(self, name: str, idle_timeout: int = 300) -> None:
        """Ensure the named GPU service container is running and healthy.

        Always touches both local timer and the shared Redis coordination key.
        """
        svc = self._services.get(name)
        if not svc:
            raise ValueError(f"Unknown GPU service: {name}")

        svc.touch()
        self._redis_touch(name, idle_timeout)

        # Cheap path: if we think it's running, also verify Docker state quickly.
        if svc._running:
            try:
                container = await asyncio.to_thread(
                    self._docker.containers.get, svc.container_name
                )
                if container.status == "running":
                    return
                # Container was stopped externally (maybe by tg_kb's idle checker).
                svc._running = False
            except Exception:
                pass

        last_error: Exception | None = None
        attempts = (0,) + self._START_RETRY_BACKOFFS_SEC
        for attempt_idx, backoff in enumerate(attempts, start=1):
            if backoff:
                log.info("gpu_service_start_backoff",
                         name=name, attempt=attempt_idx, wait_sec=backoff)
                await asyncio.sleep(backoff)
                svc.touch()
                self._redis_touch(name, idle_timeout)
            try:
                await self._start_container(svc)
                await self._wait_healthy(svc)
                svc._running = True
                svc.last_started_at = time.monotonic()
                svc.last_attempt_failed_at = 0.0
                svc.last_attempt_reason = ""
                log.info("gpu_service_started", name=name, attempts=attempt_idx)
                return
            except Exception as exc:
                last_error = exc
                exit_info = await self._inspect_failed_start(svc)
                svc.last_attempt_failed_at = time.monotonic()
                svc.last_attempt_reason = exit_info.get("reason") or "start_failed"
                log.warning(
                    "gpu_service_start_attempt_failed",
                    name=name, attempt=attempt_idx,
                    error=str(exc)[:200], **exit_info,
                )

        svc.last_exhausted_at = time.monotonic()
        log.error("gpu_service_start_exhausted",
                  name=name, attempts=len(attempts))
        if last_error:
            raise last_error
        raise RuntimeError(f"GPU service {name} could not be started")

    async def _inspect_failed_start(self, svc: GpuService) -> dict:
        info: dict = {}
        try:
            container = await asyncio.to_thread(
                self._docker.containers.get, svc.container_name
            )
            info["status"] = container.status
            state = getattr(container, "attrs", {}).get("State", {})
            info["exit_code"] = state.get("ExitCode")
            try:
                tail = await asyncio.to_thread(container.logs, tail=5)
                if isinstance(tail, bytes):
                    tail = tail.decode("utf-8", errors="replace")
                last_line = tail.strip().splitlines()[-1] if tail.strip() else ""
                if "OUT_OF_MEMORY" in last_line.upper():
                    info["reason"] = "cuda_oom"
                info["last_log"] = last_line[:200]
            except Exception:
                pass
        except Exception:
            pass
        return info

    async def _start_container(self, svc: GpuService) -> None:
        try:
            container = await asyncio.to_thread(
                self._docker.containers.get, svc.container_name
            )
            if container.status == "running":
                log.info("gpu_container_already_running", name=svc.name)
                return
            log.info("gpu_container_starting", name=svc.name, was=container.status)
            await asyncio.to_thread(container.start)
        except Exception:
            log.exception("gpu_container_start_failed", name=svc.name)
            raise

    async def _wait_healthy(self, svc: GpuService) -> None:
        deadline = time.monotonic() + svc.health_timeout
        async with httpx.AsyncClient(timeout=5, trust_env=False) as client:
            while time.monotonic() < deadline:
                try:
                    container = await asyncio.to_thread(
                        self._docker.containers.get, svc.container_name
                    )
                    if container.status == "exited":
                        raise RuntimeError(
                            f"{svc.name} container exited during warmup"
                        )
                except RuntimeError:
                    raise
                except Exception:
                    pass

                try:
                    resp = await client.get(svc.health_url)
                    if resp.status_code == 200:
                        return
                except Exception:
                    pass
                await asyncio.sleep(3)
        raise TimeoutError(f"{svc.name} not healthy after {svc.health_timeout}s")

    def _stop_service_sync(self, name: str) -> None:
        svc = self._services.get(name)
        if not svc or not svc._running:
            return
        try:
            container = self._docker.containers.get(svc.container_name)
            container.stop(timeout=30)
            svc._running = False
            svc.last_stopped_at = time.monotonic()
            log.info("gpu_service_stopped", name=name)
        except Exception:
            log.exception("gpu_service_stop_failed", name=name)

    def _sync_state_sync(self, svc: GpuService) -> None:
        """Reconcile local _running flag with actual Docker state."""
        try:
            container = self._docker.containers.get(svc.container_name)
            actually_running = container.status == "running"
        except Exception:
            return

        if actually_running and not svc._running:
            svc._running = True
            if not svc.last_used_at:
                svc.touch()
            if not svc.last_started_at:
                svc.last_started_at = time.monotonic()
            log.info("gpu_service_adopted_running", name=svc.name)
        elif not actually_running and svc._running:
            svc._running = False
            svc.last_stopped_at = time.monotonic()
            log.info("gpu_service_stopped_externally", name=svc.name)

    def _idle_checker_loop(self, idle_timeout: int) -> None:
        """Background loop: stops containers that have been idle longer than timeout."""
        log.info("gpu_idle_checker_loop_starting", idle_timeout=idle_timeout)
        tick = 0
        while self._running:
            try:
                time.sleep(30)
                tick += 1
                for name, svc in self._services.items():
                    self._sync_state_sync(svc)
                    if self.is_idle(svc, idle_timeout):
                        log.info(
                            "gpu_idle_stopping",
                            name=name,
                            local_idle_sec=int(time.monotonic() - svc.last_used_at),
                            redis_idle_sec=self._redis_external_age_sec(name),
                        )
                        self._stop_service_sync(name)
                    elif tick % 10 == 0:
                        log.info(
                            "gpu_idle_tick", name=name,
                            running=svc._running,
                            local_idle_sec=int(time.monotonic() - svc.last_used_at) if svc.last_used_at else -1,
                            timeout=idle_timeout,
                        )
            except BaseException:
                log.exception("gpu_idle_iteration_error")
        log.info("gpu_idle_checker_loop_exited")

    def start_idle_checker_thread(self, idle_timeout: int = 300) -> None:
        if self._idle_thread and self._idle_thread.is_alive():
            return
        self._running = True
        self._idle_timeout = idle_timeout
        self._idle_thread = threading.Thread(
            target=self._idle_checker_loop,
            args=(idle_timeout,),
            name="gpu-idle-checker",
            daemon=True,
        )
        self._idle_thread.start()
        log.info("gpu_idle_thread_spawned", idle_timeout=idle_timeout)

    async def stop_all(self) -> None:
        self._running = False
        for name in list(self._services):
            svc = self._services.get(name)
            if svc and svc._running:
                try:
                    await asyncio.to_thread(self._stop_service_sync, name)
                except Exception:
                    log.exception("gpu_service_stop_failed", name=name)
        if self._docker:
            self._docker.close()


_manager: GpuServiceManager | None = None


def get_gpu_manager() -> GpuServiceManager | None:
    return _manager


def set_gpu_manager(mgr: GpuServiceManager) -> None:
    global _manager
    _manager = mgr
