"""The demo scenario: two clients race on a shared counter.

Clients A and B both read the counter (value 0) and then send a ``set``
based on that stale read: A sends ``set(10)``, B sends ``set(20)``.
Whichever message the store S applies *last* wins, so the final value
depends entirely on delivery order -- a classic lost-update race.

Recorded delivery order is A then B, so the recorded outcome is 20.
The counterfactual fork (deliver B first) yields 10.
"""

from __future__ import annotations

from .system import Script, System


def store_handler(state, payload):
    state = dict(state)
    if payload.get("op") == "set":
        state["value"] = payload["value"]
    return state


def client_handler(state, payload):
    state = dict(state)
    if payload.get("op") == "read":
        state["last_read"] = payload["value"]
    return state


def node_defs():
    return {
        "A": ({}, client_handler),
        "B": ({}, client_handler),
        "S": ({"value": 0}, store_handler),
    }


def build_race_script():
    """The race, with A delivered before B. Returns (script, ref_a, ref_b)."""
    script = Script()
    script.local("A", {"op": "read", "value": 0})
    ref_a = script.send("A", "S", {"op": "set", "value": 10})
    script.local("B", {"op": "read", "value": 0})
    ref_b = script.send("B", "S", {"op": "set", "value": 20})
    script.deliver(ref_a)
    script.deliver(ref_b)
    return script, ref_a, ref_b


def run_race():
    """Execute the race once. Returns (system, script, log, ref_a, ref_b)."""
    system = System(node_defs())
    script, ref_a, ref_b = build_race_script()
    log = system.execute(script)
    return system, script, log, ref_a, ref_b
