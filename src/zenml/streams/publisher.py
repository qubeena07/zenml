#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Background batched publisher for stream events."""

import atexit
import threading
import time
from collections import defaultdict, deque
from typing import Deque, Dict, List, Optional
from uuid import UUID

from zenml.logger import get_logger
from zenml.models import EventBatchRequest, StreamEvent

logger = get_logger(__name__)

_QUEUE_MAXSIZE = 4096
_FLUSH_BATCH_SIZE = 64
# After the server returns 501 we mute the publisher for this long
# before probing again. Long-lived processes (notebooks, REPLs) recover
# once an operator turns streaming on — at the cost of one failed batch
# per window.
_DISABLED_RECHECK_SECONDS = 5 * 60.0
# Worker wait granularity when the buffer is empty. Short enough that
# `shutdown()` and `flush()` aren't perceived as laggy.
_WORKER_IDLE_WAIT_SECONDS = 0.5

_publisher_lock = threading.Lock()
_publisher: Optional["_StreamPublisher"] = None


class _StreamPublisher:
    """Thread-safe batched publisher; one daemon thread drains the buffer."""

    def __init__(self) -> None:
        self._buf: Deque[StreamEvent] = deque()
        self._cond = threading.Condition()
        self._inflight = 0
        self._stop = threading.Event()
        # Monotonic deadline at which a server-disabled producer may
        # retry. `None` means "not disabled". Read/written under
        # `_cond`.
        self._disabled_until: Optional[float] = None
        self._thread: Optional[threading.Thread] = None
        self._start_lock = threading.Lock()
        # Counters surfaced at shutdown for operator visibility.
        self._dropped_queue_full = 0
        self._dropped_no_store = 0

    def _ensure_thread(self) -> None:
        with self._start_lock:
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._run,
                    name="zenml-stream-publisher",
                    daemon=True,
                )
                self._thread.start()
                atexit.register(self._atexit)

    def _is_disabled(self) -> bool:
        """Check (and lazily clear) the server-disabled deadline."""
        with self._cond:
            return self._check_disabled_locked()

    def _check_disabled_locked(self) -> bool:
        deadline = self._disabled_until
        if deadline is None:
            return False
        if time.monotonic() >= deadline:
            self._disabled_until = None
            return False
        return True

    def publish(self, event: StreamEvent) -> None:
        """Enqueue an event for delivery. Never blocks user code."""
        if self._thread is None:
            self._ensure_thread()
        with self._cond:
            if self._check_disabled_locked():
                return
            if len(self._buf) >= _QUEUE_MAXSIZE:
                # Why: dropping oldest keeps the publisher responsive;
                # merging arbitrary payloads is unsafe.
                self._buf.popleft()
                self._dropped_queue_full += 1
            self._buf.append(event)
            self._cond.notify()

    def flush(self, timeout: Optional[float] = None) -> bool:
        """Wait for the buffer + in-flight batches to drain.

        Returns:
            True if drained before the deadline, False on timeout.
        """
        if self._thread is None:
            return True
        deadline = (time.time() + timeout) if timeout is not None else None
        with self._cond:
            while self._buf or self._inflight > 0:
                if deadline is None:
                    self._cond.wait(timeout=_WORKER_IDLE_WAIT_SECONDS)
                    continue
                remaining = deadline - time.time()
                if remaining <= 0:
                    return False
                self._cond.wait(timeout=remaining)
            return True

    def shutdown(self, timeout: float = 2.0) -> None:
        """Stop the daemon thread; final flush attempt."""
        if self._thread is None:
            return
        self.flush(timeout=timeout)
        self._stop.set()
        # Wake the worker if it's waiting on an empty buffer.
        with self._cond:
            self._cond.notify_all()
        self._thread.join(timeout=timeout)
        self._thread = None
        if self._dropped_queue_full or self._dropped_no_store:
            logger.warning(
                "Stream publisher dropped events on shutdown: "
                "%d due to queue overflow, %d due to no ZenML client.",
                self._dropped_queue_full,
                self._dropped_no_store,
            )

    def _atexit(self) -> None:
        try:
            self.shutdown(timeout=2.0)
        except Exception:
            pass

    def _run(self) -> None:
        while not self._stop.is_set():
            if self._is_disabled():
                self._drain_discard()
                if self._stop.wait(timeout=_WORKER_IDLE_WAIT_SECONDS):
                    return
                continue
            try:
                batch = self._collect_batch()
            except Exception:
                logger.exception("Stream publisher batch collection failed")
                time.sleep(0.5)
                continue
            if not batch:
                continue
            self._send_batch(batch)

    def _drain_discard(self) -> None:
        with self._cond:
            self._buf.clear()
            self._cond.notify_all()

    def _collect_batch(self) -> List[StreamEvent]:
        """Atomically drain a batch and reserve an in-flight slot.

        Holding `_cond` across drain + `_inflight += 1` is what
        prevents `flush()` from observing an empty buffer with zero
        inflight while a batch is mid-flight.
        """
        with self._cond:
            while not self._buf and not self._stop.is_set():
                self._cond.wait(timeout=_WORKER_IDLE_WAIT_SECONDS)
            if not self._buf:
                return []
            batch: List[StreamEvent] = []
            while self._buf and len(batch) < _FLUSH_BATCH_SIZE:
                batch.append(self._buf.popleft())
            self._inflight += 1
            return batch

    def _release_inflight(self) -> None:
        with self._cond:
            self._inflight -= 1
            if self._inflight == 0 and not self._buf:
                self._cond.notify_all()

    def _mark_disabled(self) -> None:
        """Mute publishes for a TTL window after a 501 from the server."""
        with self._cond:
            if self._disabled_until is None:
                logger.warning(
                    "Streaming disabled on server; publish() will be a "
                    "no-op for ~%.0fs.",
                    _DISABLED_RECHECK_SECONDS,
                )
            self._disabled_until = time.monotonic() + _DISABLED_RECHECK_SECONDS

    def _send_batch(self, events: List[StreamEvent]) -> None:
        try:
            self._send_batch_inner(events)
        finally:
            self._release_inflight()

    def _send_batch_inner(self, events: List[StreamEvent]) -> None:
        from zenml.client import Client

        try:
            zen_store = Client().zen_store
        except Exception:
            self._dropped_no_store += len(events)
            logger.exception(
                "Dropping %d stream events: ZenML client unavailable.",
                len(events),
            )
            return

        # Group by run id so a single URL/run mismatch can't fail the
        # whole batch on the server.
        grouped: Dict[UUID, List[StreamEvent]] = defaultdict(list)
        for event in events:
            grouped[event.pipeline_run_id].append(event)

        for run_id, run_events in grouped.items():
            try:
                zen_store.publish_run_events(
                    pipeline_run_id=run_id,
                    batch=EventBatchRequest(events=run_events),
                )
            except NotImplementedError:
                self._mark_disabled()
                return
            except Exception as exc:
                logger.warning(
                    "Failed to publish %d events for run %s: %s",
                    len(run_events),
                    run_id,
                    exc,
                )


def get_publisher() -> _StreamPublisher:
    """Return the per-process publisher singleton, lazily initialized."""
    global _publisher
    if _publisher is not None:
        return _publisher
    with _publisher_lock:
        if _publisher is None:
            _publisher = _StreamPublisher()
        return _publisher


def flush_and_drain(timeout: float = 2.0) -> bool:
    """Drain pending events. Called by step finalizers.

    Returns:
        True if the queue was drained before the deadline (or the
        publisher was never started); False on timeout.
    """
    publisher = _publisher
    if publisher is None:
        return True
    return publisher.flush(timeout=timeout)
