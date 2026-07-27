package io.github.xlong2.raftticketing.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RaftNodeTest {
    private static final Set<String> MEMBERS = Set.of("node-0", "node-1", "node-2");

    @Test
    void votesOnlyOncePerTermAndResetsForANewerTerm() {
        RaftNode follower = node("node-0");

        VoteResponse first = follower.handleVoteRequest(new VoteRequest(1, "node-1", 0, 0));
        VoteResponse second = follower.handleVoteRequest(new VoteRequest(1, "node-2", 0, 0));
        VoteResponse newer = follower.handleVoteRequest(new VoteRequest(2, "node-2", 0, 0));

        assertTrue(first.voteGranted());
        assertFalse(second.voteGranted());
        assertTrue(newer.voteGranted());
        assertEquals(2, follower.currentTerm());
    }

    @Test
    void rejectsCandidateWhoseLogIsStale() {
        RaftNode follower = node("node-0");
        AppendEntriesRequest existingLog =
                new AppendEntriesRequest(
                        1,
                        "node-1",
                        0,
                        0,
                        List.of(new LogEntry(1, 1, 10)),
                        0);
        assertTrue(follower.handleAppendEntries(existingLog).success());

        VoteResponse response =
                follower.handleVoteRequest(new VoteRequest(2, "node-2", 0, 0));

        assertFalse(response.voteGranted());
        assertEquals(2, response.term());
    }

    @Test
    void replicatesThenAppliesAnEntryExactlyOnce() {
        RaftNode leader = electedLeader("node-0");
        RaftNode follower = node("node-1");

        LogEntry purchase = leader.appendPurchase(10).orElseThrow();
        assertTrue(follower.handleAppendEntries(leader.fullReplicationRequest()).success());
        assertEquals(100, follower.remainingTickets(), "uncommitted entries must not be applied");

        leader.commitThrough(purchase.index());
        AppendEntriesRequest committed = leader.fullReplicationRequest();
        assertTrue(follower.handleAppendEntries(committed).success());
        assertTrue(follower.handleAppendEntries(committed).success());

        assertEquals(90, leader.remainingTickets());
        assertEquals(90, follower.remainingTickets());
        assertEquals(1, follower.status().lastApplied());
    }

    @Test
    void replacesAnUncommittedConflictingSuffix() {
        RaftNode follower = node("node-2");
        AppendEntriesRequest oldLeader =
                new AppendEntriesRequest(
                        1,
                        "node-0",
                        0,
                        0,
                        List.of(new LogEntry(1, 1, 10), new LogEntry(2, 1, 5)),
                        1);
        assertTrue(follower.handleAppendEntries(oldLeader).success());

        AppendEntriesRequest newLeader =
                new AppendEntriesRequest(
                        2,
                        "node-1",
                        0,
                        0,
                        List.of(new LogEntry(1, 1, 10), new LogEntry(2, 2, 7)),
                        2);
        assertTrue(follower.handleAppendEntries(newLeader).success());

        assertEquals(List.of(new LogEntry(1, 1, 10), new LogEntry(2, 2, 7)), follower.logEntries());
        assertEquals(83, follower.remainingTickets());
    }

    @Test
    void reservesInventoryForPendingLogEntries() {
        RaftNode leader = electedLeader("node-0");

        LogEntry first = leader.appendPurchase(60).orElseThrow();
        assertTrue(leader.appendPurchase(50).isEmpty());
        leader.commitThrough(first.index());
        assertTrue(leader.appendPurchase(41).isEmpty());
        assertTrue(leader.appendPurchase(40).isPresent());
    }

    @Test
    void stepsDownWhenAValidLeaderHasTheSameOrNewerTerm() {
        RaftNode candidate = node("node-0");
        VoteRequest election = candidate.beginElection();
        assertEquals(Role.CANDIDATE, candidate.role());

        AppendEntriesRequest heartbeat =
                new AppendEntriesRequest(election.term(), "node-1", 0, 0, List.of(), 0);
        assertTrue(candidate.handleAppendEntries(heartbeat).success());

        assertEquals(Role.FOLLOWER, candidate.role());
        assertEquals("node-1", candidate.leaderId());
    }

    @Test
    void onlyLeaderMayAppendOrCommit() {
        RaftNode follower = node("node-0");
        assertThrows(IllegalStateException.class, () -> follower.appendPurchase(1));
        assertThrows(IllegalStateException.class, () -> follower.commitThrough(0));
    }

    private static RaftNode electedLeader(String id) {
        RaftNode node = node(id);
        VoteRequest request = node.beginElection();
        String supportingNode = id.equals("node-1") ? "node-2" : "node-1";
        assertTrue(node.recordVote(new VoteResponse(supportingNode, request.term(), true)));
        assertEquals(Role.LEADER, node.role());
        return node;
    }

    private static RaftNode node(String id) {
        return new RaftNode(id, MEMBERS, 100);
    }
}
