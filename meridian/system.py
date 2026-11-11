"""Simulated distributed nodes, and the script that drives them.

A :class:`Script` is the ground truth of "what happened": a list of local
actions, message sends, and message deliveries. A :class:`System` executes
a script deterministically, maintaining one vector clock per node and
recording every step in a :class:`~meridian.events.Log`.

Handlers must be deterministic pure functions ``handler(state, payload)
-> new_state``: the same state and payload must always produce the same
new state. That is the property that makes replay possible at all.
"""

from __future__ import annotations

from typing import Any, Callable

from .clock import VectorClock
from .events import Event, Log, Message

Handler = Callable[[dict, Any], dict]


class Script:
    """An ordered list of steps to execute on a System."""

    def __init__(self):
        self.steps: list[tuple] = []
        self._counter = 0

    def local(self, node: str, payload: Any) -> "Script":
        self.steps.append(("local", node, payload))
        return self

    def send(self, sender: str, receiver: str, payload: Any) -> str:
        """Queue a send step; returns the message reference id."""
        ref = f"m{self._counter}"
        self._counter += 1
        self.steps.append(("send", sender, receiver, payload, ref))
        return ref

    def deliver(self, ref: str) -> "Script":
        self.steps.append(("deliver", ref))
        return self

    def deliver_steps(self) -> list[str]:
        return [s[1] for s in self.steps if s[0] == "deliver"]


class _Node:
    def __init__(self, name, all_nodes, initial_state, handler):
        self.name = name
        self.clock = VectorClock(all_nodes)
        self.state = dict(initial_state)
        self.handler = handler


class System:
    """A set of simulated nodes with vector clocks and a recorded log."""

    def __init__(self, node_defs: dict[str, tuple[dict, Handler]]):
        # node_defs: {name: (initial_state, handler)}
        self.node_defs = {
            name: (dict(state), handler)
            for name, (state, handler) in node_defs.items()
        }
        self.reset()

    def reset(self) -> None:
        nodes = list(self.node_defs)
        self.nodes = {
            name: _Node(name, nodes, state, handler)
            for name, (state, handler) in self.node_defs.items()
        }
        self.log = Log(nodes)
        self.inflight: dict[str, Message] = {}

    def initial_states(self) -> dict:
        return {name: dict(state) for name, (state, _) in self.node_defs.items()}

    def handlers(self) -> dict[str, Handler]:
        return {name: handler for name, (_, handler) in self.node_defs.items()}

    def state(self) -> dict:
        return {name: dict(node.state) for name, node in self.nodes.items()}

    # -- script steps -------------------------------------------------

    def _record(self, node, kind, clock, message_id=None, payload=None):
        event = Event(
            seq=len(self.log),
            node=node,
            kind=kind,
            clock=dict(clock),
            message_id=message_id,
            payload=payload,
            state_after=dict(self.nodes[node].state),
        )
        self.log.record(event)
        return event

    def _local(self, node: str, payload: Any) -> None:
        n = self.nodes[node]
        n.clock.tick(node)
        n.state = n.handler(dict(n.state), payload)
        self._record(node, "local", n.clock.values, payload=payload)

    def _send(self, sender: str, receiver: str, payload: Any, ref: str) -> Message:
        n = self.nodes[sender]
        n.clock.tick(sender)
        msg = Message(ref, sender, receiver, payload, dict(n.clock.values))
        self.inflight[ref] = msg
        self._record(sender, "send", n.clock.values, message_id=ref, payload=payload)
        return msg

    def _deliver(self, ref: str) -> None:
        if ref not in self.inflight:
            raise ValueError(f"message {ref!r} is not in flight")
        msg = self.inflight.pop(ref)
        n = self.nodes[msg.receiver]
        n.clock.merge(VectorClock(list(self.nodes), msg.clock))
        n.clock.tick(msg.receiver)
        n.state = n.handler(dict(n.state), msg.payload)
        self._record(msg.receiver, "receive", n.clock.values,
                     message_id=ref, payload=msg.payload)

    def execute(self, script: Script) -> Log:
        """Reset to initial state and run the script. Returns the Log."""
        self.reset()
        for step in script.steps:
            if step[0] == "local":
                self._local(step[1], step[2])
            elif step[0] == "send":
                self._send(step[1], step[2], step[3], step[4])
            elif step[0] == "deliver":
                self._deliver(step[1])
            else:  # pragma: no cover - defensive
                raise ValueError(f"unknown script step {step[0]!r}")
        return self.log
