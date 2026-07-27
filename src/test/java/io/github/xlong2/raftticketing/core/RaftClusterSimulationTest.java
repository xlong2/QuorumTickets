package io.github.xlong2.raftticketing.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RaftClusterSimulationTest {
    @Test
    void electsReplicatesFailsOverAndReconcilesTheOldLeader() {
        Set<String> members = Set.of("node-0", "node-1", "node-2");
        RaftNode node0 = new RaftNode("node-0", members, 100);
        RaftNode node1 = new RaftNode("node-1", members, 100);
        RaftNode node2 = new RaftNode("node-2", members, 100);

        VoteRequest firstElection = node0.beginElection();
        assertTrue(node0.recordVote(node1.handleVoteRequest(firstElection)));

        LogEntry firstPurchase = node0.appendPurchase(25).orElseThrow();
        AppendEntriesRequest firstReplication = node0.fullReplicationRequest();
        assertTrue(node1.handleAppendEntries(firstReplication).success());
        assertTrue(node2.handleAppendEntries(firstReplication).success());
        node0.commitThrough(firstPurchase.index());
        node1.handleAppendEntries(node0.fullReplicationRequest());
        node2.handleAppendEntries(node0.fullReplicationRequest());

        assertEquals(List.of(75, 75, 75), remaining(node0, node1, node2));

        VoteRequest failoverElection = node1.beginElection();
        assertTrue(node1.recordVote(node2.handleVoteRequest(failoverElection)));
        assertEquals(Role.LEADER, node1.role());
        assertEquals(2, node1.currentTerm());

        LogEntry secondPurchase = node1.appendPurchase(10).orElseThrow();
        assertTrue(node2.handleAppendEntries(node1.fullReplicationRequest()).success());
        node1.commitThrough(secondPurchase.index());
        node2.handleAppendEntries(node1.fullReplicationRequest());

        assertEquals(65, node1.remainingTickets());
        assertEquals(65, node2.remainingTickets());
        assertEquals(75, node0.remainingTickets(), "the isolated former leader is stale");

        assertTrue(node0.handleAppendEntries(node1.fullReplicationRequest()).success());
        assertEquals(Role.FOLLOWER, node0.role());
        assertEquals(65, node0.remainingTickets());
        assertEquals(node1.logEntries(), node0.logEntries());
    }

    private static List<Integer> remaining(RaftNode... nodes) {
        return java.util.Arrays.stream(nodes).map(RaftNode::remainingTickets).toList();
    }
}
