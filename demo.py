"""Demo: a real ordering bug, reproduced byte-identically on replay.

Two clients race on a shared counter. Both read 0; A sends set(10),
B sends set(20). The store applies A then B, so the recorded outcome
is 20 -- a lost update (the correct total would be 2 increments, but
that is a story for the store's next version).

Meridian then:
1. replays the log and verifies every recorded state is reproduced,
2. steps the debugger forwards and backwards through the timeline,
3. forks a counterfactual: what if B's message had arrived first?

Run: python demo.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from meridian import Debugger, counterfactual, replay, timelines_equal
from meridian.race import node_defs, run_race


def show_timeline(log):
    print("Recorded timeline (seq, node, kind, clock, payload):")
    for e in log.events:
        print(f"  {e.seq}: {e.node:2} {e.kind:8} {e.clock}  {e.payload}")
    print()


def main():
    system, script, log, ref_a, ref_b = run_race()
    print("=" * 70)
    print("THE BUG: two clients race on a shared counter")
    print("=" * 70)
    print("A read 0, sent set(10). B read 0, sent set(20).")
    print("Store applied A then B, so the final value is 20.")
    print()
    show_timeline(log)

    print("1. DETERMINISTIC REPLAY")
    print("-" * 70)
    timeline1 = replay(log, system.initial_states(), system.handlers())
    timeline2 = replay(log, system.initial_states(), system.handlers())
    print(f"Replayed {len(timeline1)} events; every recorded state reproduced.")
    print(f"Two independent replays are identical: {timelines_equal(timeline1, timeline2)}")
    print(f"Final store value on replay: {timeline1[-1][1]['S']['value']}")
    print()

    print("2. TIME-TRAVEL DEBUGGER")
    print("-" * 70)
    dbg = Debugger(timeline1, system.initial_states())
    print(f"Before any event: store = {dbg.state()['S']}")
    while dbg.step_forward() is not None:
        ev = dbg.event()
        print(f"  after #{ev.seq} ({ev.node} {ev.kind}): store = {dbg.state()['S']}")
    print("Stepping back twice:")
    dbg.step_back()
    dbg.step_back()
    print(f"  now at event #{dbg.event().seq}: store = {dbg.state()['S']}")
    print()

    print("3. COUNTERFACTUAL: what if B's message arrived first?")
    print("-" * 70)
    result = counterfactual(system, script, log, ref_a, ref_b)
    print(f"Recorded final value: {result.old_final['S']['value']}")
    print(f"Counterfactual final value: {result.new_final['S']['value']}")
    print(f"Nodes whose outcome changed: {sorted(result.changed_nodes)}")
    print()
    print("The same two messages, in the other order, produce a different")
    print("outcome -- and replay proves the recorded order is what happened.")


if __name__ == "__main__":
    main()
