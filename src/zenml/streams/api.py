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
"""Public producer API for streaming events from inside a step."""

from datetime import datetime
from typing import Any, Dict, NamedTuple, Optional
from uuid import UUID

from zenml.logger import get_logger
from zenml.models import StreamEvent
from zenml.streams.publisher import get_publisher

logger = get_logger(__name__)


class _RunContext(NamedTuple):
    """The run/step to attribute a stream event to."""

    pipeline_run_id: UUID
    step_run_id: Optional[UUID]
    step_name: Optional[str]


def _resolve_run_context() -> Optional[_RunContext]:
    """Identify the run / step to attribute events to, or None."""
    from zenml.execution.pipeline.dynamic.run_context import (
        DynamicPipelineRunContext,
    )
    from zenml.steps.step_context import get_step_context

    try:
        ctx = get_step_context()
    except RuntimeError:
        ctx = None
    if ctx is not None:
        return _RunContext(
            pipeline_run_id=ctx.pipeline_run.id,
            step_run_id=ctx.step_run.id,
            step_name=ctx.step_name,
        )

    dyn = DynamicPipelineRunContext.get()
    if dyn is None:
        return None
    return _RunContext(
        pipeline_run_id=dyn.run.id,
        step_run_id=None,
        step_name=None,
    )


def publish(
    payload: Dict[str, Any],
    *,
    kind: str = "event",
    stream_id: Optional[str] = None,
    index: Optional[int] = None,
    ts: Optional[datetime] = None,
) -> None:
    """Publish a single event to the current run's live stream.

    Args:
        payload: The event payload. Free-form JSON-encodable dict.
        kind: Event kind (also routes the SSE `event:` field).
        stream_id: Optional sub-stream identifier; opaque to ZenML.
        index: Optional in-order index within a sub-stream.
        ts: Event timestamp; defaults to wall-clock now. Pass an
            explicit value when backfilling from another source.
    """
    info = _resolve_run_context()
    if info is None:
        logger.debug(
            "streams.publish() called outside a step context; dropping"
        )
        return

    fields: Dict[str, Any] = dict(
        pipeline_run_id=info.pipeline_run_id,
        step_run_id=info.step_run_id,
        step_name=info.step_name,
        kind=kind,
        stream_id=stream_id,
        index=index,
        payload=payload,
    )
    if ts is not None:
        fields["ts"] = ts
    event = StreamEvent(**fields)
    get_publisher().publish(event)


def flush(timeout: float = 2.0) -> bool:
    """Block until all queued events have been sent (or timeout).

    Returns:
        True if the queue was drained, False if `timeout` elapsed first.
    """
    return get_publisher().flush(timeout=timeout)
