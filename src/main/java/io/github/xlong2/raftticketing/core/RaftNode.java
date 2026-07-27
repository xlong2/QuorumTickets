package io.github.xlong2.raftticketing.core;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Thread-safe, deterministic Raft state machine.
 *
 * <p>Networking and timers live outside this class so consensus rules can be tested without sleeps
 * or sockets.
 */
public final class RaftNode {
    private final String id;
    private final Set<String> members;
    private final int majority;
    private final List<LogEntry> log = new ArrayList<>();
    private final TicketStateMachine stateMachine;
    private final Set<String> votesReceived = new HashSet<>();

    private Role role = Role.FOLLOWER;
    private int currentTerm;
    private String votedFor;
    private String leaderId;
    private int commitIndex;
    private int lastApplied;

    public RaftNode(String id, Set<String> members, int initialTickets) {
        this.id = requireText(id, "id");
        this.members = Set.copyOf(Objects.requireNonNull(members, "members"));
        if (this.members.isEmpty() || !this.members.contains(id)) {
            throw new IllegalArgumentException("members must include this node");
        }
        this.majority = (this.members.size() / 2) + 1;
        this.stateMachine = new TicketStateMachine(initialTickets);
    }

    public synchronized VoteRequest beginElection() {
        currentTerm += 1;
        role = Role.CANDIDATE;
        leaderId = null;
        votedFor = id;
        votesReceived.clear();
        votesReceived.add(id);

        if (votesReceived.size() >= majority) {
            role = Role.LEADER;
            leaderId = id;
        }

        return new VoteRequest(currentTerm, id, lastLogIndex(), lastLogTerm());
    }

    /** Returns true exactly when this response makes the candidate become leader. */
    public synchronized boolean recordVote(VoteResponse response) {
        Objects.requireNonNull(response, "response");

        if (response.term() > currentTerm) {
            becomeFollower(response.term(), null);
            return false;
        }
        if (role != Role.CANDIDATE
                || response.term() != currentTerm
                || !response.voteGranted()
                || !members.contains(response.voterId())) {
            return false;
        }

        votesReceived.add(response.voterId());
        if (votesReceived.size() >= majority) {
            role = Role.LEADER;
            leaderId = id;
            return true;
        }
        return false;
    }

    public synchronized VoteResponse handleVoteRequest(VoteRequest request) {
        Objects.requireNonNull(request, "request");

        if (request.term() < currentTerm || !members.contains(request.candidateId())) {
            return new VoteResponse(id, currentTerm, false);
        }
        if (request.term() > currentTerm) {
            becomeFollower(request.term(), null);
        }

        boolean candidateLogIsCurrent =
                request.lastLogTerm() > lastLogTerm()
                        || (request.lastLogTerm() == lastLogTerm()
                                && request.lastLogIndex() >= lastLogIndex());
        boolean canVote = votedFor == null || votedFor.equals(request.candidateId());
        boolean granted = canVote && candidateLogIsCurrent;

        if (granted) {
            votedFor = request.candidateId();
            leaderId = null;
        }
        return new VoteResponse(id, currentTerm, granted);
    }

    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        Objects.requireNonNull(request, "request");

        if (request.term() < currentTerm || !members.contains(request.leaderId())) {
            return new AppendEntriesResponse(id, currentTerm, false, lastLogIndex());
        }

        if (request.term() > currentTerm || role != Role.FOLLOWER) {
            becomeFollower(request.term(), request.leaderId());
        } else {
            leaderId = request.leaderId();
        }

        if (request.previousLogIndex() > lastLogIndex()) {
            return new AppendEntriesResponse(id, currentTerm, false, lastLogIndex());
        }
        if (request.previousLogIndex() > 0
                && termAt(request.previousLogIndex()) != request.previousLogTerm()) {
            return new AppendEntriesResponse(
                    id, currentTerm, false, request.previousLogIndex() - 1);
        }

        int expectedIndex = request.previousLogIndex() + 1;
        for (LogEntry incoming : request.entries()) {
            if (incoming.index() != expectedIndex) {
                return new AppendEntriesResponse(id, currentTerm, false, lastLogIndex());
            }

            if (incoming.index() <= lastLogIndex()) {
                LogEntry existing = log.get(incoming.index() - 1);
                if (!existing.equals(incoming)) {
                    if (incoming.index() <= commitIndex) {
                        return new AppendEntriesResponse(id, currentTerm, false, commitIndex);
                    }
                    log.subList(incoming.index() - 1, log.size()).clear();
                    log.add(incoming);
                }
            } else if (incoming.index() == lastLogIndex() + 1) {
                log.add(incoming);
            } else {
                return new AppendEntriesResponse(id, currentTerm, false, lastLogIndex());
            }
            expectedIndex += 1;
        }

        if (request.leaderCommit() > commitIndex) {
            commitIndex = Math.min(request.leaderCommit(), lastLogIndex());
            applyCommittedEntries();
        }

        return new AppendEntriesResponse(id, currentTerm, true, lastLogIndex());
    }

    public synchronized Optional<LogEntry> appendPurchase(int tickets) {
        if (role != Role.LEADER) {
            throw new IllegalStateException("only a leader can append client commands");
        }
        if (tickets < 1) {
            throw new IllegalArgumentException("tickets must be positive");
        }

        long pendingTickets = 0;
        for (int i = commitIndex; i < log.size(); i += 1) {
            pendingTickets += log.get(i).tickets();
        }
        long availableAfterPending = stateMachine.remainingTickets() - pendingTickets;
        if (tickets > availableAfterPending) {
            return Optional.empty();
        }

        LogEntry entry = new LogEntry(lastLogIndex() + 1, currentTerm, tickets);
        log.add(entry);
        return Optional.of(entry);
    }

    /**
     * Builds a valid, intentionally simple full-log replication request.
     *
     * <p>A production implementation would track nextIndex per follower and send only the missing
     * suffix. Sending from the sentinel coordinate keeps this portfolio implementation compact and
     * deterministic while preserving the Raft conflict rules.
     */
    public synchronized AppendEntriesRequest fullReplicationRequest() {
        if (role != Role.LEADER) {
            throw new IllegalStateException("only a leader can replicate its log");
        }
        return new AppendEntriesRequest(currentTerm, id, 0, 0, log, commitIndex);
    }

    public synchronized void commitThrough(int index) {
        if (role != Role.LEADER) {
            throw new IllegalStateException("only a leader can advance its commit index");
        }
        if (index < commitIndex || index > lastLogIndex()) {
            throw new IllegalArgumentException("invalid commit index: " + index);
        }
        commitIndex = index;
        applyCommittedEntries();
    }

    public synchronized void becomeFollower(int term, String knownLeaderId) {
        if (term < currentTerm) {
            return;
        }
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
        }
        role = Role.FOLLOWER;
        leaderId = knownLeaderId;
        votesReceived.clear();
    }

    public synchronized NodeStatus status() {
        return new NodeStatus(
                id,
                role,
                currentTerm,
                leaderId,
                commitIndex,
                lastApplied,
                lastLogIndex(),
                stateMachine.remainingTickets());
    }

    public synchronized List<LogEntry> logEntries() {
        return List.copyOf(log);
    }

    public synchronized String id() {
        return id;
    }

    public synchronized Role role() {
        return role;
    }

    public synchronized int currentTerm() {
        return currentTerm;
    }

    public synchronized String leaderId() {
        return leaderId;
    }

    public synchronized int commitIndex() {
        return commitIndex;
    }

    public synchronized int remainingTickets() {
        return stateMachine.remainingTickets();
    }

    public int majority() {
        return majority;
    }

    public Set<String> members() {
        return members;
    }

    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            LogEntry next = log.get(lastApplied);
            stateMachine.apply(next);
            lastApplied = next.index();
        }
    }

    private int lastLogIndex() {
        return log.size();
    }

    private int lastLogTerm() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).term();
    }

    private int termAt(int index) {
        return index == 0 ? 0 : log.get(index - 1).term();
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return value;
    }
}
