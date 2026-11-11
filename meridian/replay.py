"""Deterministic replay, the step debugger, and counterfactual forks.

* :func:`replay` re-executes a recorded log in causal order (a topological
  order over the vector-clock happens-before relation) and *verifies* every
  step: the replayed state after each event must equal the recorded
  ``state_after`` exactly, or :class:`ReplayDivergence` is raised.
* :class:`Debugger` steps a replayed timeline forwards and backwards.
* :func:`counterfactual` answers "what if this message arrived first?" by
  re-executing the recorded script with two concurrent deliveries swapped.
  Swapping is only allowed for deliveries whose sends were concurrent --
  swapping causally ordered deliveries would rewrite history, so it raises.
"""

from __future__ import annotations

from dataclasses import dataclass

from .clock import VectorClock
from .events import Event, Log
from .system import Script, System


class ReplayDivergence(Exception):
    """Replay produced a state different from the recorded log."""


def _happens_before(a: dict, b: dict) -> bool:
    nodes = set(a) | set(b)
    le = all(a.get(n, 0) <= b.get(n, 0) for n in nodes)
    lt = any(a.get(n, 0) < b.get(n, 0) for n in nodes)
    return le and lt


def causal_order(events: list[Event]) -> list[Event]:
    """A deterministic topological order over happens-before.

    Kahn's algorithm, always picking the lowest recorded sequence number
    among the events whose predecessors have all been emitted. Because a
    cause is always recorded before its effect, this reproduces the
    recorded order exactly -- while still being derived purely from the
    clocks, which is what makes it a *causal* order rather than a replay
    of a tape.
    """
    by_seq = {e.seq: e for e in events}
    preds = {e.seq: set() for e in events}
    for e1 in events:
        for e2 in events:
            if e1.seq != e2.seq and _happens_before(e1.clock, e2.clock):
                preds[e2.seq].add(e1.seq)
    emitted: list[int] = []
    remaining = set(by_seq)
    while remaining:
        ready = [s for s in remaining if preds[s] <= set(emitted)]
        if not ready:
            raise ValueError("causal cycle in event log")
        nxt = min(ready)
        emitted.append(nxt)
        remaining.discard(nxt)
    return [by_seq[s] for s in emitted]


def replay(log: Log, initial_states: dict, handlers: dict) -> list:
    """Re-execute ``log`` from ``initial_states``.

    Returns the timeline: a list of ``(event, global_state_snapshot)``,
    one entry per event, in causal order. Raises :class:`ReplayDivergence`
    if any replayed state differs from the recorded ``state_after``.
    """
    order = causal_order(log.events)
    states = {n: dict(s) for n, s in initial_states.items()}
    timeline = []
    for event in order:
        if event.kind in ("receive", "local"):
            states[event.node] = handlers[event.node](dict(states[event.node]),
                                                      event.payload)
        if event.state_after is not None and states[event.node] != event.state_after:
            raise ReplayDivergence(
                f"event {event.seq} ({event.node} {event.kind}): "
                f"replayed {states[event.node]} != recorded {event.state_after}"
            )
        timeline.append((event, {n: dict(s) for n, s in states.items()}))
    return timeline


def timelines_equal(a: list, b: list) -> bool:
    """True if two timelines have identical events and snapshots."""
    if len(a) != len(b):
        return False
    for (e1, s1), (e2, s2) in zip(a, b):
        if (e1.seq, e1.node, e1.kind, e1.clock, e1.payload) != (
            e2.seq, e2.node, e2.kind, e2.clock, e2.payload,
        ):
            return False
        if s1 != s2:
            return False
    return True


class Debugger:
    """Step forwards and backwards over a replayed timeline.

    Index -1 means "before the first event" (initial states).
    """

    def __init__(self, timeline: list, initial_states: dict):
        self.timeline = list(timeline)
        self.initial_states = {n: dict(s) for n, s in initial_states.items()}
        self.index = -1

    def __len__(self) -> int:
        return len(self.timeline)

    def step_forward(self):
        """Advance one event; returns the event, or None at the end."""
        if self.index + 1 >= len(self.timeline):
            return None
        self.index += 1
        return self.timeline[self.index][0]

    def step_back(self):
        """Go back one event; returns the event stepped back from."""
        if self.index < 0:
            return None
        event = self.timeline[self.index][0]
        self.index -= 1
        return event

    def goto(self, index: int) -> None:
        if not -1 <= index < len(self.timeline):
            raise IndexError(f"timeline index {index} out of range")
        self.index = index

    def state(self) -> dict:
        if self.index < 0:
            return {n: dict(s) for n, s in self.initial_states.items()}
        return {n: dict(s) for n, s in self.timeline[self.index][1].items()}

    def event(self):
        if self.index < 0:
            return None
        return self.timeline[self.index][0]


@dataclass
class CounterfactualResult:
    new_log: Log
    old_final: dict
    new_final: dict
    changed_nodes: set

    @property
    def outcome_changed(self) -> bool:
        return bool(self.changed_nodes)


def counterfactual(system: System, script: Script, log: Log,
                   ref_a: str, ref_b: str) -> CounterfactualResult:
    """Re-execute ``script`` with the deliveries of ``ref_a``/``ref_b`` swapped.

    Only valid when the two sends were concurrent (neither message knew
    about the other) and the two deliveries are adjacent in the script.
    Returns the new log, both final states, and which nodes differ.
    """
    send_a = log.send_event(ref_a)
    send_b = log.send_event(ref_b)
    clock_a = VectorClock(list(system.nodes), send_a.clock)
    clock_b = VectorClock(list(system.nodes), send_b.clock)
    if not clock_a.is_concurrent_with(clock_b):
        raise ValueError(
            f"messages {ref_a} and {ref_b} were not sent concurrently; "
            "their delivery order is fixed by causality and cannot be swapped"
        )
    delivers = [i for i, s in enumerate(script.steps) if s[0] == "deliver"]
    pos = {script.steps[i][1]: i for i in delivers}
    if ref_a not in pos or ref_b not in pos:
        raise ValueError("both messages must be delivered in the script")
    ia, ib = pos[ref_a], pos[ref_b]
    if abs(ia - ib) != 1:
        raise ValueError(
            "the two deliveries are not adjacent in the script; "
            "swapping non-adjacent deliveries is not supported"
        )
    swapped = Script()
    swapped.steps = [tuple(s) for s in script.steps]
    swapped._counter = script._counter
    swapped.steps[ia], swapped.steps[ib] = swapped.steps[ib], swapped.steps[ia]

    old_final = log.final_states(system.initial_states())
    new_log = system.execute(swapped)
    new_final = new_log.final_states(system.initial_states())
    changed = {n for n in old_final if old_final[n] != new_final.get(n)}
    return CounterfactualResult(new_log, old_final, new_final, changed)
