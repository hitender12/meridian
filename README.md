# Meridian — time-travel debugging for distributed systems

Step **backwards** through a distributed bug.

## The problem

Heisenbugs in distributed systems vanish the moment you add logging or retry.
By the time you look, the interleaving that caused the failure is gone, and
the system will not reproduce it on demand.

## The novel idea

Deterministic replay from **vector-clock-ordered event logs** plus a
**deterministic scheduler**. Every local action, send, and delivery is
recorded with its vector clock and the acting node's state afterwards. The
replay engine re-executes the log in causal order (a topological order over
the happens-before relation) and *verifies* every step: the replayed state
after each event must equal the recorded state exactly, or replay raises a
divergence. On top of the verified replay sits a step debugger (forwards and
backwards across nodes) and a **counterfactual fork**: "what if this message
had arrived first?" — answered by re-executing the recorded script with two
concurrent deliveries swapped.

## Quickstart

```bash
# Run the demo: a real ordering bug, replayed byte-identically
python demo.py

# Run the tests
python -m unittest discover -s tests -v
```

Python 3.10+ standard library only. No dependencies to install.

## What it does

- **Vector clocks** (`meridian/clock.py`): tick, merge, happens-before,
  concurrency detection.
- **Event log recorder** (`meridian/system.py`): simulated nodes execute a
  script of local actions, sends, and deliveries; every step is logged with
  its clock and resulting state.
- **Deterministic replay** (`meridian/replay.py`): causal-order scheduler
  (Kahn's algorithm over happens-before) that re-executes the log and
  verifies every recorded state.
- **Time-travel debugger**: step forward/back, jump to any event, inspect
  every node's state at any point.
- **Counterfactual fork**: swap two concurrent deliveries and re-execute;
  reports which nodes' outcomes change. Refuses to swap causally ordered
  deliveries — that would rewrite history.

## The demo bug

Two clients race on a shared counter. Both read 0; A sends `set(10)`,
B sends `set(20)`. The store applies A then B, so the final value is 20 —
a lost update. Meridian replays it exactly, lets you step through it, and
shows the counterfactual: deliver B first and the outcome is 10.

```
Recorded final value: 20
Counterfactual final value: 10
Nodes whose outcome changed: ['S']
```

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md). Core modules:

- `meridian/clock.py` — vector clocks.
- `meridian/events.py` — events, messages, the log.
- `meridian/system.py` — simulated nodes and script execution.
- `meridian/replay.py` — causal ordering, verified replay, debugger,
  counterfactuals.
- `meridian/race.py` — the demo race scenario.

## Honest status (v0.1.0)

What genuinely works: vector clocks with full ordering semantics, scripted
simulation with exact logging, verified deterministic replay (divergence is
detected and raised, and tested), the step debugger, and counterfactual
forks with causality guards. 17 tests pass, and the demo runs end to end.

What is simplified (see ARCHITECTURE.md for detail):

- Nodes are **simulated in-process**, not real networked processes. There is
  no real network, no packet loss, no crash injection — the log is produced
  by a script, not captured from production.
- Handlers must be deterministic pure functions; nondeterminism (random,
  wall-clock time, real I/O) breaks replay by design.
- Counterfactuals only swap **adjacent** deliveries in the script.
- Clocks are logical only; there is no wall-clock or timeout modelling.
- The replay verifies application state, not message contents beyond payload.

## Evaluation plan

- **Fidelity:** replay a recorded run twice; timelines must be identical,
  and every recorded state must be reproduced (this is tested).
- **Bug reproduction:** take a known flaky ordering failure, record it once,
  and show it replays identically N times with no flakiness.
- **Counterfactual soundness:** swapped deliveries must only ever be
  concurrent pairs; attempting a causal swap must raise.
- **Scale:** log size and replay time vs. number of events/nodes.

## License

MIT. See [LICENSE](LICENSE).
