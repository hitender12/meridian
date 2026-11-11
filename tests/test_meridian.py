"""Tests for Meridian. Run: python -m unittest discover -s tests -v"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from meridian import (
    Debugger,
    ReplayDivergence,
    Script,
    System,
    VectorClock,
    counterfactual,
    replay,
    timelines_equal,
)
from meridian.race import client_handler, node_defs, run_race, store_handler


class TestVectorClock(unittest.TestCase):
    def test_tick_and_happens_before(self):
        a = VectorClock(["A", "B"])
        b = VectorClock(["A", "B"])
        a.tick("A")          # a = {A:1, B:0}
        b.merge(a)           # b learns about a: {A:1, B:0}
        b.tick("B")          # b = {A:1, B:1}: strictly after a
        self.assertTrue(a.happens_before(b))
        self.assertEqual(a.compare(b), -1)
        self.assertEqual(b.compare(a), 1)

    def test_merge_is_elementwise_max(self):
        a = VectorClock(["A", "B"], {"A": 2, "B": 0})
        b = VectorClock(["A", "B"], {"A": 1, "B": 3})
        self.assertEqual(a.merged(b).to_dict(), {"A": 2, "B": 3})

    def test_concurrent_clocks(self):
        a = VectorClock(["A", "B"])
        b = VectorClock(["A", "B"])
        a.tick("A")
        b.tick("B")
        self.assertTrue(a.is_concurrent_with(b))
        self.assertIsNone(a.compare(b))
        self.assertFalse(a.happens_before(b))
        self.assertFalse(b.happens_before(a))

    def test_equal_clocks_are_not_concurrent(self):
        a = VectorClock(["A", "B"], {"A": 1, "B": 0})
        b = VectorClock(["A", "B"], {"A": 1, "B": 0})
        self.assertFalse(a.is_concurrent_with(b))
        self.assertEqual(a.compare(b), 0)

    def test_unknown_node_rejected(self):
        with self.assertRaises(ValueError):
            VectorClock(["A"], {"Z": 1})


class TestRaceScenario(unittest.TestCase):
    def test_recorded_outcome_is_20(self):
        _, _, log, _, _ = run_race()
        final = log.final_states({"A": {}, "B": {}, "S": {"value": 0}})
        self.assertEqual(final["S"]["value"], 20)

    def test_sends_are_concurrent(self):
        _, _, log, ref_a, ref_b = run_race()
        clock_a = VectorClock(["A", "B", "S"], log.send_event(ref_a).clock)
        clock_b = VectorClock(["A", "B", "S"], log.send_event(ref_b).clock)
        self.assertTrue(clock_a.is_concurrent_with(clock_b))


class TestReplay(unittest.TestCase):
    def test_replay_reproduces_recorded_states(self):
        system, _, log, _, _ = run_race()
        timeline = replay(log, system.initial_states(), system.handlers())
        self.assertEqual(len(timeline), len(log))
        # Final replayed state equals the recorded final state.
        self.assertEqual(timeline[-1][1]["S"]["value"], 20)

    def test_replay_is_deterministic(self):
        system, _, log, _, _ = run_race()
        t1 = replay(log, system.initial_states(), system.handlers())
        t2 = replay(log, system.initial_states(), system.handlers())
        self.assertTrue(timelines_equal(t1, t2))

    def test_two_runs_of_the_same_script_are_identical(self):
        system, script, log1, _, _ = run_race()
        log2 = system.execute(script)
        self.assertEqual(
            [(e.seq, e.node, e.kind, e.clock, e.payload, e.state_after)
             for e in log1.events],
            [(e.seq, e.node, e.kind, e.clock, e.payload, e.state_after)
             for e in log2.events],
        )

    def test_replay_divergence_is_detected(self):
        system, _, log, _, _ = run_race()
        tampered = list(log.events)
        last = tampered[-1]
        tampered[-1] = type(last)(
            seq=last.seq, node=last.node, kind=last.kind, clock=last.clock,
            message_id=last.message_id, payload=last.payload,
            state_after={"value": 999},
        )
        log.events = tampered
        with self.assertRaises(ReplayDivergence):
            replay(log, system.initial_states(), system.handlers())

    def test_causal_order_reproduces_recorded_order(self):
        from meridian import causal_order

        _, _, log, _, _ = run_race()
        ordered = causal_order(log.events)
        self.assertEqual([e.seq for e in ordered],
                         [e.seq for e in log.events])


class TestDebugger(unittest.TestCase):
    def test_step_forward_and_back(self):
        system, _, log, _, _ = run_race()
        timeline = replay(log, system.initial_states(), system.handlers())
        dbg = Debugger(timeline, system.initial_states())
        self.assertEqual(dbg.state()["S"], {"value": 0})
        events = []
        while dbg.step_forward() is not None:
            events.append(dbg.event().seq)
        self.assertEqual(events, [e.seq for e in log.events])
        self.assertEqual(dbg.state()["S"], {"value": 20})
        self.assertIsNone(dbg.step_forward())  # at the end
        dbg.step_back()  # back to event #4: store is 10
        self.assertEqual(dbg.state()["S"], {"value": 10})
        dbg.step_back()  # back to event #3: store is still 0
        self.assertEqual(dbg.state()["S"], {"value": 0})
        dbg.goto(-1)
        self.assertEqual(dbg.state()["S"], {"value": 0})

    def test_goto_out_of_range(self):
        system, _, log, _, _ = run_race()
        timeline = replay(log, system.initial_states(), system.handlers())
        dbg = Debugger(timeline, system.initial_states())
        with self.assertRaises(IndexError):
            dbg.goto(len(timeline))


class TestCounterfactual(unittest.TestCase):
    def test_swapped_delivery_changes_outcome(self):
        system, script, log, ref_a, ref_b = run_race()
        result = counterfactual(system, script, log, ref_a, ref_b)
        self.assertEqual(result.old_final["S"]["value"], 20)
        self.assertEqual(result.new_final["S"]["value"], 10)
        self.assertEqual(result.changed_nodes, {"S"})
        self.assertTrue(result.outcome_changed)

    def test_swapped_run_replays_cleanly(self):
        system, script, log, ref_a, ref_b = run_race()
        result = counterfactual(system, script, log, ref_a, ref_b)
        # The counterfactual log itself replays without divergence.
        timeline = replay(result.new_log, system.initial_states(),
                          system.handlers())
        self.assertEqual(timeline[-1][1]["S"]["value"], 10)

    def test_causally_ordered_sends_cannot_be_swapped(self):
        # A sends to B; B receives it, then B sends to S. B's send
        # causally follows A's send, so the delivery order is fixed.
        defs = {
            "A": ({}, client_handler),
            "B": ({}, client_handler),
            "S": ({"value": 0}, store_handler),
        }
        system = System(defs)
        script = Script()
        m0 = script.send("A", "B", {"op": "read", "value": 0})
        script.deliver(m0)
        m1 = script.send("B", "S", {"op": "set", "value": 5})
        script.deliver(m1)
        log = system.execute(script)
        with self.assertRaises(ValueError):
            counterfactual(system, script, log, m0, m1)


if __name__ == "__main__":
    unittest.main()
