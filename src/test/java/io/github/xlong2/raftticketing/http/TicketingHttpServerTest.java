package io.github.xlong2.raftticketing.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.xlong2.raftticketing.cluster.ClusterTransport;
import io.github.xlong2.raftticketing.cluster.RaftClusterService;
import io.github.xlong2.raftticketing.core.AppendEntriesRequest;
import io.github.xlong2.raftticketing.core.AppendEntriesResponse;
import io.github.xlong2.raftticketing.core.PurchaseResult;
import io.github.xlong2.raftticketing.core.PurchaseStatus;
import io.github.xlong2.raftticketing.core.RaftNode;
import io.github.xlong2.raftticketing.core.VoteRequest;
import io.github.xlong2.raftticketing.core.VoteResponse;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class TicketingHttpServerTest {
    @Test
    void exposesStatusAndCommitsASingleNodePurchase() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        RaftNode node = new RaftNode("node-0", Set.of("node-0"), 100);
        node.beginElection();

        try (RaftClusterService service = new RaftClusterService(node, new UnusedTransport());
                TicketingHttpServer server = new TicketingHttpServer(0, objectMapper, service)) {
            server.start();
            HttpClient client = HttpClient.newHttpClient();

            HttpResponse<String> status =
                    client.send(
                            HttpRequest.newBuilder(endpoint(server, "/status")).GET().build(),
                            HttpResponse.BodyHandlers.ofString());
            assertEquals(200, status.statusCode());
            assertTrue(status.body().contains("\"role\":\"LEADER\""));

            HttpResponse<String> purchase =
                    client.send(
                            HttpRequest.newBuilder(endpoint(server, "/tickets"))
                                    .header("Content-Type", "application/json")
                                    .POST(HttpRequest.BodyPublishers.ofString("{\"tickets\":5}"))
                                    .build(),
                            HttpResponse.BodyHandlers.ofString());
            PurchaseResult result = objectMapper.readValue(purchase.body(), PurchaseResult.class);

            assertEquals(201, purchase.statusCode());
            assertEquals(PurchaseStatus.COMMITTED, result.status());
            assertEquals(95, result.remainingTickets());
        }
    }

    @Test
    void returnsAJsonClientErrorForMalformedRequests() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        RaftNode node = new RaftNode("node-0", Set.of("node-0"), 100);
        node.beginElection();

        try (RaftClusterService service = new RaftClusterService(node, new UnusedTransport());
                TicketingHttpServer server = new TicketingHttpServer(0, objectMapper, service)) {
            server.start();
            HttpResponse<String> response =
                    HttpClient.newHttpClient()
                            .send(
                                    HttpRequest.newBuilder(endpoint(server, "/tickets"))
                                            .header("Content-Type", "application/json")
                                            .POST(HttpRequest.BodyPublishers.ofString("not-json"))
                                            .build(),
                                    HttpResponse.BodyHandlers.ofString());

            assertEquals(400, response.statusCode());
            assertTrue(response.body().contains("error"));
        }
    }

    private static URI endpoint(TicketingHttpServer server, String path) {
        return URI.create("http://localhost:" + server.port() + path);
    }

    private static final class UnusedTransport implements ClusterTransport {
        @Override
        public CompletableFuture<VoteResponse> requestVote(
                String peerId, VoteRequest request) {
            return CompletableFuture.failedFuture(new AssertionError("no peers expected"));
        }

        @Override
        public CompletableFuture<AppendEntriesResponse> appendEntries(
                String peerId, AppendEntriesRequest request) {
            return CompletableFuture.failedFuture(new AssertionError("no peers expected"));
        }
    }
}
