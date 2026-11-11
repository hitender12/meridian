"""Vector clocks: Lamport-style causal timestamps for distributed events.

A vector clock is one counter per node. Comparing two clocks tells you
whether one event happened before another, or whether the two events are
concurrent (neither knew about the other) -- which is exactly the
information a deterministic replay needs to order a log, and a
counterfactual fork needs to know which message orders are even possible.
"""

from __future__ import annotations


class VectorClock:
    """A vector clock over a fixed set of node names."""

    def __init__(self, nodes, values=None):
        self.nodes = tuple(nodes)
        self.values = {n: 0 for n in self.nodes}
        if values:
            for name, val in dict(values).items():
                if name not in self.values:
                    raise ValueError(f"unknown node {name!r}")
                self.values[name] = int(val)

    def copy(self) -> "VectorClock":
        return VectorClock(self.nodes, dict(self.values))

    def tick(self, node: str) -> "VectorClock":
        """Record a local event on ``node`` (mutates and returns self)."""
        self.values[node] += 1
        return self

    def merge(self, other: "VectorClock") -> "VectorClock":
        """Element-wise max with ``other`` (mutates and returns self)."""
        for name in self.nodes:
            self.values[name] = max(self.values[name], other.values.get(name, 0))
        return self

    def merged(self, other: "VectorClock") -> "VectorClock":
        merged = self.copy()
        merged.merge(other)
        return merged

    def happens_before(self, other: "VectorClock") -> bool:
        """True if every event this clock has seen, ``other`` has also seen,
        and ``other`` has seen at least one more."""
        le = all(self.values[n] <= other.values[n] for n in self.nodes)
        lt = any(self.values[n] < other.values[n] for n in self.nodes)
        return le and lt

    def is_concurrent_with(self, other: "VectorClock") -> bool:
        """True if neither clock happened before the other."""
        return (
            not self.happens_before(other)
            and not other.happens_before(self)
            and self.values != other.values
        )

    def compare(self, other: "VectorClock") -> int | None:
        """-1 if self happened before other, 1 if after, 0 if equal,
        None if concurrent."""
        if self.values == other.values:
            return 0
        if self.happens_before(other):
            return -1
        if other.happens_before(self):
            return 1
        return None

    def to_dict(self) -> dict:
        return dict(self.values)

    def __eq__(self, other) -> bool:
        return isinstance(other, VectorClock) and self.values == other.values

    def __repr__(self) -> str:
        return f"VectorClock({self.values})"
