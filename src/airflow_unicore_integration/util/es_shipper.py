"""Ship finished UNICORE job logs to Elasticsearch, when the deployment reads logs from it.

Tasks running on an HPC site cannot reach an Elasticsearch service inside the Kubernetes cluster,
and Airflow never asks an executor for the logs of a task that already finished. The scheduler is
the one process that can reach both, so it hands the finished job's streams to Elasticsearch when
the job ends, and the deployment's normal Elasticsearch log reader serves them from then on.
"""

from __future__ import annotations

import logging
import os
import queue
import shutil
import tempfile
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from airflow.providers.common.compat.sdk import conf as global_conf

from .job import redact_credentials

logger = logging.getLogger(__name__)

# These docs are indexed for good, and a batch job can print hundreds of megabytes. Keep only
# what is worth reading and drop the middle of an oversized stream.
DEFAULT_MAX_STREAM_BYTES = 1024 * 1024


@dataclass(frozen=True)
class LogTarget:
    """The identity bits Elasticsearch keys a task's logs by."""

    dag_id: str
    task_id: str
    run_id: str
    try_number: int
    map_index: int = -1
    ti_id: Any = None

    @property
    def id(self) -> Any:
        """The provider's reader prefers docs tagged with the task instance uuid."""
        return self.ti_id


def shipping_enabled(conf: Any, section: str = "unicore.executor") -> bool:
    """
    Report whether finished job logs should be indexed.

    ``auto`` (the default) follows the deployment's own log setup: shipping is only worth doing
    when Airflow reads task logs from Elasticsearch, which is what ``[elasticsearch] host``
    switches on. ``True``/``False`` override that for testing or to opt out of a shared cluster.
    """
    setting = str(conf.get(section, "SHIP_LOGS_TO_ES", fallback="auto")).strip().lower()
    if setting in ("true", "false"):
        return setting == "true"
    return bool(global_conf.get("elasticsearch", "host", fallback=""))


def cap_stream(text: str, max_bytes: int) -> str:
    """Keep the head and the tail of an oversized stream and say what was dropped."""
    encoded = text.encode("utf-8", errors="replace")
    if len(encoded) <= max_bytes:
        return text
    head = max_bytes // 4
    dropped = len(encoded) - max_bytes
    return (
        encoded[:head].decode("utf-8", errors="replace")
        + f"\n*** {dropped} bytes omitted; the complete log is in the job's working directory ***\n"
        + encoded[-(max_bytes - head) :].decode("utf-8", errors="replace")
    )


def _default_io() -> Any:
    from airflow.providers.elasticsearch.log.es_task_handler import ElasticsearchRemoteLogIO

    return _quiet_io(ElasticsearchRemoteLogIO.from_config())


def _quiet_io(io: Any) -> Any:
    """
    Switch off ``write_stdout`` on an otherwise config-built log IO.

    ``upload`` echoes every parsed line to stdout when that flag is set. For a task process that
    is the point; in a scheduler it would dump every finished job's log into the scheduler's own
    log stream.
    """
    try:
        io.write_stdout = False
        return io
    except Exception:  # noqa: BLE001 - frozen attrs classes need a rebuild instead
        import attrs

        return attrs.evolve(io, write_stdout=False)


class ElasticsearchLogShipper:
    """
    Index finished job logs through the Elasticsearch provider's own upload path.

    Uploads run on a single worker thread: a scheduler heartbeat must never wait for
    Elasticsearch, and losing a log beats stalling every task in the fleet.
    """

    def __init__(
        self,
        io_factory: Callable[[], Any] | None = None,
        max_stream_bytes: int = DEFAULT_MAX_STREAM_BYTES,
        queue_size: int = 64,
    ) -> None:
        self.max_stream_bytes = max_stream_bytes
        self._io_factory = io_factory or _default_io
        self._queue: queue.Queue = queue.Queue(maxsize=queue_size)
        self._worker: threading.Thread | None = None
        self._io: Any = None
        self._lock = threading.Lock()
        self._shipped: set[str] = set()

    def submit(self, text: str, target: LogTarget) -> bool:
        """Queue one finished task's logs; returns False when they had to be dropped."""
        with self._lock:
            if self._worker is None:
                self._worker = threading.Thread(
                    target=self._run, name="unicore-es-log-shipper", daemon=True
                )
                self._worker.start()
        try:
            self._queue.put_nowait((text, target))
        except queue.Full:
            logger.warning(
                "Log upload queue is full, dropping the logs of %s/%s (run %s, try %s)",
                target.dag_id,
                target.task_id,
                target.run_id,
                target.try_number,
            )
            return False
        return True

    def flush(self, timeout: float = 30.0) -> None:
        """Give queued uploads a chance to finish before the process exits."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and self._queue.unfinished_tasks:
            time.sleep(0.1)

    def _run(self) -> None:
        while True:
            try:
                text, target = self._queue.get()
            except Exception as exc:  # noqa: BLE001 - say so instead of dying with a raw traceback
                logger.warning("The Elasticsearch log upload worker stopped: %s", exc)
                return
            try:
                self.ship_now(text, target)
            except Exception as exc:  # noqa: BLE001 - log shipping may never kill the worker
                logger.warning(
                    "Could not ship the logs of %s/%s to Elasticsearch: %s",
                    target.dag_id,
                    target.task_id,
                    redact_credentials(str(exc)),
                )
            finally:
                self._queue.task_done()

    def ship_now(self, text: str, target: LogTarget) -> None:
        """Index one finished task's logs, skipping streams that are already there."""
        io = self._get_io()
        log_id = io.log_id_template.format(
            dag_id=target.dag_id,
            task_id=target.task_id,
            run_id=target.run_id,
            try_number=target.try_number,
            map_index=target.map_index,
        )
        if log_id in self._shipped or _already_shipped(io, log_id):
            self._shipped.add(log_id)
            return

        # upload() deletes the *directory* holding the file when delete_local_logs is on, so the
        # file has to live in a directory of our own - never in the shared temp dir.
        directory = tempfile.mkdtemp(prefix="unicore-job-log-")
        try:
            path = os.path.join(directory, "job.log")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(text)
            io.upload(path, target)
        finally:
            shutil.rmtree(directory, ignore_errors=True)

        self._shipped.add(log_id)
        if _count_docs(io, log_id, refresh=True) == 0:
            # upload() reports nothing when the bulk request is refused (its writer swallows the
            # failure and returns False), so a quiet upload still has to be checked.
            logger.warning(
                "Uploaded %s bytes of task log as %s, but Elasticsearch holds no documents for it",
                len(text),
                log_id,
            )
        else:
            logger.info("Shipped %s bytes of task log to Elasticsearch as %s", len(text), log_id)

    def _get_io(self) -> Any:
        if self._io is None:
            self._io = self._io_factory()
        return self._io


def _count_docs(io: Any, log_id: str, refresh: bool = False) -> int:
    """
    Count the documents Elasticsearch holds for a log id, or -1 when it cannot be asked.

    ``refresh`` matters right after a write: a bulk request only becomes searchable at the next
    index refresh, so counting without it would report a successful upload as an empty one.
    """
    query = {"match_phrase": {"log_id": log_id}}
    attempts: list[dict[str, Any]] = [{"query": query}]
    if refresh:
        attempts.insert(0, {"query": query, "refresh": True})
    attempts.append({"body": {"query": query}})
    for kwargs in attempts:
        try:
            response = io.client.count(index=io.target_index, **kwargs)
        except TypeError:  # client versions disagree on how the query is passed
            continue
        except Exception as exc:  # noqa: BLE001 - a probe must not decide whether logs exist
            logger.debug("Could not count the documents of %s: %s", log_id, exc)
            return -1
        count = (
            response.get("count") if hasattr(response, "get") else getattr(response, "count", None)
        )
        return int(count or 0)
    return -1


def _already_shipped(io: Any, log_id: str) -> bool:
    """Best effort check against a previous scheduler process; a failing probe never blocks."""
    return _count_docs(io, log_id) > 0
