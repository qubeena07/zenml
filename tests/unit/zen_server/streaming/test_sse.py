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
"""Tests for SSE wire framing and event encoding."""

import uuid

import pytest
from fastapi import HTTPException

from zenml.models import StreamEvent
from zenml.zen_server.streaming.broker import BrokerEvent
from zenml.zen_server.streaming.hub import EndMarker, GapMarker
from zenml.zen_server.streaming.sse import (
    EVENT_PAYLOAD_BYTES_MAX,
    _frame_for,
    encode_event_for_publish,
    format_sse_frame,
)
from zenml.zen_server.streaming.wire import (
    EndFrame,
    EventFrame,
    encode_frame,
)


def _ev(run_id: uuid.UUID, kind: str = "token") -> StreamEvent:
    return StreamEvent(pipeline_run_id=run_id, kind=kind, payload={"v": 1})


def test_format_sse_frame_basic():
    """Format sse frame basic."""
    frame = format_sse_frame("event", '{"a":1}')
    assert frame == b'event: event\ndata: {"a":1}\n\n'


def test_format_sse_frame_with_id():
    """Format sse frame with id."""
    frame = format_sse_frame("event", '{"a":1}', event_id="42")
    assert frame.startswith(b"id: 42\n")


def test_format_sse_frame_rejects_newlines_in_kind():
    """Format sse frame rejects newlines in kind."""
    with pytest.raises(ValueError):
        format_sse_frame("bad\nkind", "{}")


def test_format_sse_frame_rejects_newlines_in_data():
    """Format sse frame rejects newlines in data."""
    with pytest.raises(ValueError):
        format_sse_frame("event", "{\n}")


def test_encode_event_for_publish_rejects_run_id_mismatch():
    """Encode event for publish rejects run id mismatch."""
    event = _ev(uuid.uuid4())
    with pytest.raises(HTTPException) as exc:
        encode_event_for_publish(event, uuid.uuid4())
    assert exc.value.status_code == 400


def test_encode_event_for_publish_wraps_in_event_frame():
    """Producer payloads land on the broker tagged as `EventFrame`."""
    run_id = uuid.uuid4()
    event = _ev(run_id, kind="custom")
    payload = encode_event_for_publish(event, run_id)
    from zenml.zen_server.streaming.wire import decode_frame

    frame = decode_frame(payload)
    assert isinstance(frame, EventFrame)
    assert frame.event.kind == "custom"


def test_encode_event_for_publish_rejects_oversize():
    """Encode event for publish rejects oversize."""
    run_id = uuid.uuid4()
    huge = "x" * (EVENT_PAYLOAD_BYTES_MAX + 100)
    event = StreamEvent(
        pipeline_run_id=run_id, kind="big", payload={"v": huge}
    )
    with pytest.raises(HTTPException) as exc:
        encode_event_for_publish(event, run_id)
    assert exc.value.status_code == 413


def test_encode_event_for_publish_returns_bytes():
    """Producer payload is bytes the wire decoder can parse back."""
    from zenml.zen_server.streaming.wire import decode_frame

    run_id = uuid.uuid4()
    event = _ev(run_id)
    payload = encode_event_for_publish(event, run_id)
    frame = decode_frame(payload)
    assert isinstance(frame, EventFrame)
    assert frame.event.pipeline_run_id == run_id
    assert frame.event.kind == "token"


def test_frame_for_end_marker_is_terminal():
    """Frame for end marker is terminal."""
    frame, terminal = _frame_for(EndMarker(), None, uuid.uuid4())
    assert terminal is True
    assert b"event: end" in frame


def test_frame_for_gap_marker_is_not_terminal():
    """Frame for gap marker is not terminal."""
    frame, terminal = _frame_for(
        GapMarker(reason="overflow"), None, uuid.uuid4()
    )
    assert terminal is False
    assert b"event: gap" in frame
    assert b"overflow" in frame


def test_frame_for_event_kind_filter_skipped_but_id_advances():
    """Filtered-out events emit a `ping` frame that advances Last-Event-ID.

    SSE *comments* don't advance the client's `lastEventId` per the
    WHATWG spec — only a dispatched event does. So filtered events
    must surface as an event with an `id:` line.
    """
    run_id = uuid.uuid4()
    payload = encode_frame(EventFrame(event=_ev(run_id, kind="other")))
    frame, terminal = _frame_for(
        BrokerEvent(id="5", payload=payload), {"keep"}, run_id
    )
    assert terminal is False
    assert b"id: 5" in frame
    assert b"event: ping" in frame


def test_frame_for_event_kind_filter_allowed():
    """Events whose kind matches the filter are emitted in full."""
    run_id = uuid.uuid4()
    payload = encode_frame(EventFrame(event=_ev(run_id, kind="keep")))
    frame, terminal = _frame_for(
        BrokerEvent(id="5", payload=payload), {"keep"}, run_id
    )
    assert terminal is False
    assert b"event: keep" in frame
    assert b"id: 5" in frame


def test_frame_for_end_frame_terminates_stream():
    """An `EndFrame` payload on the broker terminates the SSE stream."""
    run_id = uuid.uuid4()
    payload = encode_frame(EndFrame(pipeline_run_id=run_id))
    frame, terminal = _frame_for(
        BrokerEvent(id="9", payload=payload), None, run_id
    )
    assert terminal is True
    assert b"event: end" in frame


def test_frame_for_unknown_wire_frame_emits_ping():
    """Forward-compat: unknown frame `type` advances id without dispatching data."""
    run_id = uuid.uuid4()
    payload = b'{"type": "future", "whatever": 1}'
    frame, terminal = _frame_for(
        BrokerEvent(id="9", payload=payload), None, run_id
    )
    assert terminal is False
    assert b"id: 9" in frame
    assert b"event: ping" in frame


def test_frame_for_undecodable_event_returns_none():
    """Frame for undecodable event returns none."""
    assert (
        _frame_for(
            BrokerEvent(id="1", payload=b"not json"), None, uuid.uuid4()
        )
        is None
    )
