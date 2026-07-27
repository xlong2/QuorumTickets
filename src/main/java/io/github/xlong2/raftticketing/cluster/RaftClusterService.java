package io.github.xlong2.raftticketing.cluster;

import io.github.xlong2.raftticketing.core.AppendEntriesRequest;
import io.github.xlong2.raftticketing.core.AppendEntriesResponse;
import io.github.xlong2.raftticketing.core.LogEntry;
import io.github.xlong2.raftticketing.core.NodeStatus;
import io.github.xlong2.raftticketing.core.PurchaseResult;
import io.github.xlong2.raftticketing.core.PurchaseStatus;
import io.github.xlong2.raftticketing.core.RaftNode;
import io.github.xlong2.raftticketing.core.Role;
import io.github.xlong2.raftticketing.core.VoteRequest;
import io.github.xlong2.raftticketing.core.VoteResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Coordinates timers and transport around the deterministic {@link RaftNode} core. */
public final class RaftClusterService implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(RaftClusterService.class.getName());

    private final RaftNode node;
    private final Set<String> peers;
    private final ClusterTransport transport;
    private final ScheduledExecutorService scheduler;
    private final long electionMinimumMillis;
    private final long electionMaximumMillis;
    private final long heartbeatMillis;
    private final long replicationTimeoutMillis;
    private final Object purchaseLock = new Object();

    private ScheduledFuture<?> electionFuture;
    private ScheduledFuture<?> heartbeatFuture;
    private boolean started;

    public RaftClusterService(RaftNode node, ClusterTransport transport) {
        this(
                node,
                transport,
                Executors.newScheduledThreadPool(3),
                Duration.ofMillis(1_200),
                Duration.ofMillis(2_400),
                Duration.ofMillis(350),
                Duration.ofMillis(1_500));
    }

    RaftClusterService(
            RaftNode node,
            ClusterTransport transport,
            ScheduledExecutorService scheduler,
            Duration electionMinimum,
            Duration electionMaximum,
            Duration heartbeat,
            Duration replicationTimeout) {
        this.node = Objects.requireNonNull(node, "node");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.peers =
                node.members().stream()
                        .filter(member -> !member.equals(node.id()))
                        .collect(java.util.stream.Collectors.toUnmodifiableSet());
        this.electionMinimumMillis = electionMinimum.toMillis();
        this.electionMaximumMillis = electionMaximum.toMillis();
        this.heartbeatMillis = heartbeat.toMillis();
        this.replicationTimeoutMillis = replicationTimeout.toMillis();
        if (electionMinimumMillis < 1 || electionMaximumMillis <= electionMinimumMillis) {
            throw new IllegalArgumentException("invalid election timeout range");
        }
    }

    public synchronized void start() {
        if (started) {
            return;
        }
        started = true;
        scheduleElection();
    }

    public synchronized VoteResponse onVoteRequest(VoteRequest request) {
        VoteResponse response = node.handleVoteRequest(request);
        if (response.voteGranted()) {
            stopHeartbeat();
            scheduleElection();
        }
        return response;
    }

    public synchronized AppendEntriesResponse onAppendEntries(AppendEntriesRequest request) {
        int termBeforeRequest = node.currentTerm();
        AppendEntriesResponse response = node.handleAppendEntries(request);
        boolean recognizedLeader =
                request.term() >= termBeforeRequest && node.members().contains(request.leaderId());
        if (recognizedLeader) {
            stopHeartbeat();
            scheduleElection();
        }
        return response;
    }

    public PurchaseResult purchase(int tickets) {
        synchronized (purchaseLock) {
            if (tickets < 1) {
                return result(PurchaseStatus.INVALID, tickets, "tickets must be positive");
            }
            if (node.role() != Role.LEADER) {
                return result(
                        PurchaseStatus.NOT_LEADER,
                        tickets,
                        "request must be sent to the current leader");
            }

            Optional<LogEntry> candidate = node.appendPurchase(tickets);
            if (candidate.isEmpty()) {
                return result(
                        PurchaseStatus.REJECTED,
                        tickets,
                        "not enough unreserved ticket inventory");
            }

            LogEntry entry = candidate.orElseThrow();
            AppendEntriesRequest replication = node.fullReplicationRequest();
            List<CompletableFuture<AppendEntriesResponse>> responses = new ArrayList<>();
            for (String peer : peers) {
                responses.add(transport.appendEntries(peer, replication));
            }

            int acknowledgements = 1;
            for (CompletableFuture<AppendEntriesResponse> responseFuture : responses) {
                try {
                    AppendEntriesResponse response =
                            responseFuture.get(replicationTimeoutMillis, TimeUnit.MILLISECONDS);
                    if (response.term() > node.currentTerm()) {
                        stepDown(response.term());
                    } else if (response.success()
                            && response.term() == entry.term()
                            && response.matchIndex() >= entry.index()) {
                        acknowledgements += 1;
                    }
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    return result(
                            PurchaseStatus.NO_QUORUM,
                            tickets,
                            "interrupted while waiting for replication");
                } catch (ExecutionException | TimeoutException exception) {
                    LOGGER.log(Level.FINE, "replication attempt did not complete", exception);
                }
            }

            if (node.role() == Role.LEADER && acknowledgements >= node.majority()) {
                node.commitThrough(entry.index());
                broadcastHeartbeat();
                return result(PurchaseStatus.COMMITTED, tickets, "purchase committed by a quorum");
            }

            return result(
                    PurchaseStatus.NO_QUORUM,
                    tickets,
                    "quorum was not reached; the final outcome is unknown");
        }
    }

    public NodeStatus status() {
        return node.status();
    }

    @Override
    public synchronized void close() {
        started = false;
        cancel(electionFuture);
        cancel(heartbeatFuture);
        scheduler.shutdownNow();
    }

    private void runElection() {
        final VoteRequest request;
        synchronized (this) {
            if (!started || node.role() == Role.LEADER) {
                return;
            }
            request = node.beginElection();
            LOGGER.info(
                    () ->
                            "node="
                                    + node.id()
                                    + " event=election-started term="
                                    + request.term());
            if (node.role() == Role.LEADER) {
                activateLeader();
                return;
            }
            scheduleElection();
        }

        for (String peer : peers) {
            transport.requestVote(peer, request)
                    .whenComplete(
                            (response, failure) -> {
                                if (failure != null) {
                                    LOGGER.log(Level.FINE, "vote request failed for " + peer, failure);
                                    return;
                                }
                                acceptVoteResponse(response);
                            });
        }
    }

    private synchronized void acceptVoteResponse(VoteResponse response) {
        boolean becameLeader = node.recordVote(response);
        if (becameLeader) {
            activateLeader();
        } else if (node.role() == Role.FOLLOWER) {
            stopHeartbeat();
            scheduleElection();
        }
    }

    private synchronized void activateLeader() {
        cancel(electionFuture);
        stopHeartbeat();
        LOGGER.info(
                () ->
                        "node="
                                + node.id()
                                + " event=leader-elected term="
                                + node.currentTerm());
        broadcastHeartbeat();
        heartbeatFuture =
                scheduler.scheduleWithFixedDelay(
                        this::broadcastHeartbeat,
                        heartbeatMillis,
                        heartbeatMillis,
                        TimeUnit.MILLISECONDS);
    }

    private void broadcastHeartbeat() {
        final AppendEntriesRequest request;
        synchronized (this) {
            if (!started || node.role() != Role.LEADER) {
                return;
            }
            request = node.fullReplicationRequest();
        }

        for (String peer : peers) {
            transport.appendEntries(peer, request)
                    .whenComplete(
                            (response, failure) -> {
                                if (failure != null) {
                                    LOGGER.log(Level.FINEST, "heartbeat failed for " + peer, failure);
                                } else if (response.term() > node.currentTerm()) {
                                    stepDown(response.term());
                                }
                            });
        }
    }

    private synchronized void stepDown(int newerTerm) {
        if (newerTerm > node.currentTerm()) {
            node.becomeFollower(newerTerm, null);
            stopHeartbeat();
            scheduleElection();
        }
    }

    private synchronized void scheduleElection() {
        cancel(electionFuture);
        if (!started || node.role() == Role.LEADER) {
            return;
        }
        long delay =
                ThreadLocalRandom.current()
                        .nextLong(electionMinimumMillis, electionMaximumMillis + 1);
        electionFuture = scheduler.schedule(this::runElection, delay, TimeUnit.MILLISECONDS);
    }

    private synchronized void stopHeartbeat() {
        cancel(heartbeatFuture);
        heartbeatFuture = null;
    }

    private PurchaseResult result(PurchaseStatus status, int requested, String message) {
        return new PurchaseResult(
                status,
                requested,
                node.remainingTickets(),
                node.leaderId(),
                message);
    }

    private static void cancel(ScheduledFuture<?> future) {
        if (future != null) {
            future.cancel(false);
        }
    }
}
