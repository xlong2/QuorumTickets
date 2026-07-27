# Design decisions

## Deterministic protocol core

Raft protocol state is isolated in `RaftNode`. Tests drive elections and message delivery
directly instead of sleeping and hoping a particular schedule occurs. Timers and HTTP
are adapters around the core. This makes safety behavior fast, repeatable, and easier to
review.

## Full-log replication

The leader currently sends its complete log with a sentinel previous coordinate of
index `0`, term `0`. Followers still perform Raft's previous-entry validation and
conflicting-suffix replacement. This reduces implementation surface for a portfolio
demonstration, at the cost of O(log size) traffic on every heartbeat.

The next scalability step would maintain `nextIndex` and `matchIndex` for every follower,
retry rejected appends from the reported match coordinate, and advance commit based on
the median replicated index.

## In-memory state

Current term, vote, log, and ticket inventory are held in memory. This project therefore
demonstrates process failure and network unavailability, but not crash recovery. A
production extension would persist term and vote before replying, use a write-ahead log,
and restore the state machine from a snapshot plus its remaining log suffix.

## Client semantics

Inventory is reserved as soon as a leader appends a purchase, preventing multiple
pending commands from collectively overselling. Inventory changes only after majority
commit.

A `NO_QUORUM` result is an unknown outcome rather than a definitive rejection. Stable
client request IDs and a deduplication table would provide safe retry semantics.

## Fixed membership

The peer list is supplied through `PEERS` and remains fixed for the process lifetime.
Joint-consensus membership changes are deliberately out of scope.

## Deployment security

The container runs as a non-root user and the Kubernetes manifest drops Linux
capabilities and uses a read-only root filesystem. The HTTP endpoints themselves do not
authenticate or encrypt traffic. A production deployment would use mutual TLS for Raft
RPCs, authentication for ticket requests, network policies, and secret rotation.
