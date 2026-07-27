package io.github.xlong2.raftticketing.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.xlong2.raftticketing.core.AppendEntriesRequest;
import io.github.xlong2.raftticketing.core.AppendEntriesResponse;
import io.github.xlong2.raftticketing.core.PurchaseResult;
import io.github.xlong2.raftticketing.core.PurchaseStatus;
import io.github.xlong2.raftticketing.core.RaftNode;
import io.github.xlong2.raftticketing.core.VoteRequest;
import io.github.xlong2.raftticketing.core.VoteResponse;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class RaftClusterServiceTest {
    private static final Set<String> MEMBERS = Set.of("node-0", "node-1", "node-2");

    @Test
    void commitsWhenTransportObtainsAQuorum() {
        RaftNode leader = electedNode();
        ClusterTransport transport = new SuccessfulReplicationTransport();

        try (RaftClusterService service = new RaftClusterService(leader, transport)) {
            PurchaseResult result = service.purchase(12);

            assertEquals(PurchaseStatus.COMMITTED, result.status());
            assertTrue(result.committed());
            assertEquals(88, result.remainingTickets());
            assertEquals(1, leader.commitIndex());
        }
    }

    @Test
    void reportsUnknownOutcomeWhenAQuorumCannotBeReached() {
        RaftNode leader = electedNode();
        ClusterTransport transport = new FailedTransport();

        try (RaftClusterService service = new RaftClusterService(leader, transport)) {
            PurchaseResult result = service.purchase(12);

            assertEquals(PurchaseStatus.NO_QUORUM, result.status());
            assertEquals(100, result.remainingTickets());
            assertEquals(0, leader.commitIndex());
        }
    }

    private static RaftNode electedNode() {
        RaftNode node = new RaftNode("node-0", MEMBERS, 100);
        VoteRequest request = node.beginElection();
        node.recordVote(new VoteResponse("node-1", request.term(), true));
        return node;
    }

    private static final class SuccessfulReplicationTransport implements ClusterTransport {
        @Override
        public CompletableFuture<VoteResponse> requestVote(
                String peerId, VoteRequest request) {
            return CompletableFuture.completedFuture(
                    new VoteResponse(peerId, request.term(), true));
        }

        @Override
        public CompletableFuture<AppendEntriesResponse> appendEntries(
                String peerId, AppendEntriesRequest request) {
            int matchIndex = request.entries().size();
            return CompletableFuture.completedFuture(
                    new AppendEntriesResponse(peerId, request.term(), true, matchIndex));
        }
    }

    private static final class FailedTransport implements ClusterTransport {
        @Override
        public CompletableFuture<VoteResponse> requestVote(
                String peerId, VoteRequest request) {
            return CompletableFuture.failedFuture(new IllegalStateException("network unavailable"));
        }

        @Override
        public CompletableFuture<AppendEntriesResponse> appendEntries(
                String peerId, AppendEntriesRequest request) {
            return CompletableFuture.failedFuture(new IllegalStateException("network unavailable"));
        }
    }
}
