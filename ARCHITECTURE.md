# Meridian architecture

## Data model

```
VectorClock            # {node: counter}; tick / merge / happens-before
Event                  # seq, node, kind (local|send|receive), clock,
                       # message_id, payload, state_after
Message                # id, sender, receiver, payload, sender's clock
Log                    # ordered events; final_states(initial)
Script                 # steps: local / send / deliver
System                 # nodes + clocks + inflight messages + log
```

## Pipeline

1. **Script** (`system.Script`): the ground truth of "what happened". A
   script lists local actions, sends (which return a message reference),
   and deliveries of those references, in order.
2. **Execute** (`System.execute`): resets all nodes to initial state, then
   runs the script. Each step ticks or merges the acting node's vector
   clock, applies the node's handler (a deterministic pure function
   `state, payload -> new_state`), and records an `Event` with the clock
   and the node's state afterwards.
3. **Replay** (`replay.replay`): derives a causal order with
   `causal_order` — Kahn's algorithm over the happens-before relation
   between event clocks, tie-broken by recorded sequence number — then
   re-executes the handlers from the initial states. After every event the
   replayed state is compared with the recorded `state_after`; any
   mismatch raises `ReplayDivergence`. Because a cause is always recorded
   before its effect, the Kahn order reproduces the recorded order
   exactly, while still being derived purely from the clocks.
4. **Debug** (`replay.Debugger`): holds the replayed timeline — a list of
   `(event, global_state_snapshot)` — and an index. `step_forward`,
   `step_back`, `goto`, `state`, `event`. Index -1 is the initial state.
5. **Counterfactual** (`replay.counterfactual`): given two message
   references, it checks (a) their sends were concurrent (neither clock
   happened before the other) and (b) their deliveries are adjacent in
   the script. It then builds a copy of the script with the two delivery
   steps swapped, re-executes it on a fresh system, and reports the new
   final states and which nodes changed. Non-concurrent pairs raise
   `ValueError` — swapping them would rewrite causality, not explore it.

## Why this design

- **Script as ground truth, log as observation.** Re-executing a script is
  always well-defined; re-ordering a raw log is not, because a receive's
  clock already encodes the order it was delivered in. The counterfactual
  therefore re-runs the script, never edits the log.
- **Verification, not trust.** Replay does not assume it is correct; it
  checks every recorded state and fails loudly on divergence. A replay
  that silently differs is worse than no replay.
- **Handlers are pure functions.** Determinism is a contract, enforced by
  the divergence check. Anything nondeterministic (random, wall clock,
  real I/O) is the caller's problem to eliminate or model explicitly.

## Known limitations (honest)

- **Simulated, not captured.** Nodes run in-process from a script. There is
  no production capture agent (no eBPF, no network tap), so Meridian today
  replays scenarios you write, not incidents you suffered. A capture agent
  is the big next step.
- **Determinism is assumed.** Handlers using `random`, `time`, or I/O will
  diverge on replay — by design, and the divergence error will say so.
- **Counterfactual scope.** Only adjacent deliveries can be swapped, and
  only when their sends were concurrent. Longer-range reorderings need a
  proper partial-order exploration (future work).
- **No failure injection.** No packet loss, duplication, partitions, or
  crashes yet; the log models delivery order only.
- **Logical time only.** No timeouts, leases, or wall-clock deadlines.

## Future work

- Production capture: an eBPF/network-tap recorder that builds logs from a
  real system, plus a state-snapshot mechanism for replay start points.
- Partial-order exploration: enumerate *all* valid delivery orders for a
  set of concurrent messages (bounded model checking over the log).
- Failure injection: loss, duplication, partitions, node crashes.
- Clock extensions: hybrid logical clocks for wall-clock correlation.
