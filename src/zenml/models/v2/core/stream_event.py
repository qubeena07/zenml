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
"""Wire models for live event streaming on pipeline runs."""

import re
from datetime import datetime
from typing import Any, ClassVar, Dict, FrozenSet, List, Optional, Pattern
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from zenml.utils.time_utils import utc_now

# SSE event names this server emits as control frames. A producer that
# used one of these as `kind` would emit `event: end\ndata: ...` on the
# wire, which a browser's `addEventListener("end", ...)` handler would
# fire on by mistake. The wire envelope already prevents *forging*
# control frames; this is purely about client-side disambiguation.
RESERVED_STREAM_EVENT_KINDS: FrozenSet[str] = frozenset(
    {"end", "gap", "error", "ping"}
)

# Allowed shape of `kind`. Bounded length and no newlines — the latter
# would let the field smuggle SSE control frames into other consumers'
# streams via the server's frame formatter.
STREAM_EVENT_KIND_PATTERN: Pattern[str] = re.compile(
    r"\A[A-Za-z0-9._-]{1,64}\Z"
)

# Producer-side cap on a single batch's event count. Independent of the
# total request body size cap (the server also enforces an envelope
# `max_request_body_size_in_bytes`); this just bounds per-call work.
MAX_EVENTS_PER_BATCH: int = 1000


class StreamEvent(BaseModel):
    """A single producer-published event on a pipeline run's stream."""

    _KIND_PATTERN: ClassVar[Pattern[str]] = STREAM_EVENT_KIND_PATTERN
    _RESERVED_KINDS: ClassVar[FrozenSet[str]] = RESERVED_STREAM_EVENT_KINDS

    pipeline_run_id: UUID
    step_run_id: Optional[UUID] = None
    step_name: Optional[str] = None
    kind: str
    stream_id: Optional[str] = None
    index: Optional[int] = None
    ts: datetime = Field(default_factory=utc_now)
    payload: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("kind")
    @classmethod
    def _validate_kind(cls, value: str) -> str:
        if not cls._KIND_PATTERN.match(value):
            raise ValueError(
                f"kind {value!r} must match {cls._KIND_PATTERN.pattern}"
            )
        if value in cls._RESERVED_KINDS:
            raise ValueError(
                f"kind {value!r} collides with an SSE control event name "
                f"({sorted(cls._RESERVED_KINDS)}); pick a different `kind`."
            )
        return value


class EventBatchRequest(BaseModel):
    """Producer-side batched ingest body for run events."""

    events: List[StreamEvent]

    @field_validator("events")
    @classmethod
    def _bound_events(cls, value: List[StreamEvent]) -> List[StreamEvent]:
        if len(value) > MAX_EVENTS_PER_BATCH:
            raise ValueError(
                f"Batch has {len(value)} events; max is {MAX_EVENTS_PER_BATCH}"
            )
        return value


class EventBatchResponse(BaseModel):
    """Server-side response from the batched ingest endpoint."""

    count: int
    last_id: Optional[str] = None
