"""Events, messages, and the recorded log of a run.

An :class:`Event` is one observable step: a local action, a message send,
or a message delivery. Every event carries the acting node's vector clock
*after* the event, the payload, and the acting node's application state
after the event -- which is what makes replay verifiable: a correct replay
must reproduce every recorded ``state_after`` exactly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Event:
    seq: int
    node: str
    kind: str  # "local" | "send" | "receive"
    clock: dict
    message_id: str | None = None
    payload: Any = None
    state_after: dict | None = None


@dataclass(frozen=True)
class Message:
    id: str
    sender: str
    receiver: str
    payload: Any
    clock: dict  # sender's vector clock at send time


class Log:
    """The ordered record of one run."""

    def __init__(self, nodes):
        self.nodes = list(nodes)
        self.events: list[Event] = []

    def record(self, event: Event) -> None:
        self.events.append(event)

    def __len__(self) -> int:
        return len(self.events)

    def send_event(self, message_id: str) -> Event:
        for event in self.events:
            if event.kind == "send" and event.message_id == message_id:
                return event
        raise KeyError(f"no send event for message {message_id!r}")

    def receive_event(self, message_id: str) -> Event:
        for event in self.events:
            if event.kind == "receive" and event.message_id == message_id:
                return event
        raise KeyError(f"no receive event for message {message_id!r}")

    def receive_order(self) -> list[str]:
        """Message ids in recorded delivery order."""
        return [e.message_id for e in self.events
                if e.kind == "receive" and e.message_id is not None]

    def final_states(self, initial_states: dict) -> dict:
        """Each node's state after its last recorded event."""
        states = {n: dict(s) for n, s in initial_states.items()}
        for event in self.events:
            if event.state_after is not None:
                states[event.node] = dict(event.state_after)
        return states
