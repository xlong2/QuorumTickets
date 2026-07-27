# Project evolution

The original coursework progressed through four related exercises:

1. Lamport logical clocks and distributed resource ordering.
2. Raft leader election and log replication modeled with Akka finite-state-machine actors.
3. A replicated movie-ticket state machine with an invariant that inventory never becomes
   negative.
4. Containerization and a Kubernetes deployment of frontend and backend actors.

Those exercises established the conceptual arc, but the repository also contained IDE
metadata, compiled artifacts, duplicate submissions, large archives, paper copies, and
known timing-dependent defects. The portfolio refactor keeps the useful engineering
story while changing its evidence:

| Coursework evidence | Portfolio evidence |
|---|---|
| Console traces | Deterministic JUnit assertions |
| One 1,200-line actor | Protocol, transport, HTTP, and state-machine boundaries |
| Random sleeps | Scheduled election and heartbeat tasks outside the core |
| Ten nearly identical pod files | One three-replica StatefulSet |
| IDE-only run instructions | Maven wrapper, runnable JAR, Docker Compose, and CI |
| PDFs describing intermittent bugs | Executable failover and invariant tests |

The standalone `LogicalClock` and its tests preserve the first exercise. The active Raft
ticketing service combines the strongest ideas from the later exercises into one project
that can be built and evaluated without the original course environment.
