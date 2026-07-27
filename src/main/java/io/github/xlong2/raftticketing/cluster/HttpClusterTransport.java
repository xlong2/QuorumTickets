package io.github.xlong2.raftticketing.cluster;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.xlong2.raftticketing.core.AppendEntriesRequest;
import io.github.xlong2.raftticketing.core.AppendEntriesResponse;
import io.github.xlong2.raftticketing.core.VoteRequest;
import io.github.xlong2.raftticketing.core.VoteResponse;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public final class HttpClusterTransport implements ClusterTransport {
    private final HttpClient client;
    private final ObjectMapper objectMapper;
    private final Map<String, URI> peers;
    private final Duration requestTimeout;

    public HttpClusterTransport(HttpClient client, ObjectMapper objectMapper, Map<String, URI> peers) {
        this(client, objectMapper, peers, Duration.ofSeconds(2));
    }

    public HttpClusterTransport(
            HttpClient client,
            ObjectMapper objectMapper,
            Map<String, URI> peers,
            Duration requestTimeout) {
        this.client = Objects.requireNonNull(client, "client");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.peers = Map.copyOf(Objects.requireNonNull(peers, "peers"));
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
    }

    @Override
    public CompletableFuture<VoteResponse> requestVote(String peerId, VoteRequest request) {
        return post(peerId, "/raft/vote", request, VoteResponse.class);
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(
            String peerId, AppendEntriesRequest request) {
        return post(peerId, "/raft/append", request, AppendEntriesResponse.class);
    }

    private <T> CompletableFuture<T> post(
            String peerId, String path, Object requestBody, Class<T> responseType) {
        URI baseUri = peers.get(peerId);
        if (baseUri == null) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("unknown peer: " + peerId));
        }

        final String json;
        try {
            json = objectMapper.writeValueAsString(requestBody);
        } catch (JsonProcessingException exception) {
            return CompletableFuture.failedFuture(exception);
        }

        HttpRequest request =
                HttpRequest.newBuilder(endpoint(baseUri, path))
                        .timeout(requestTimeout)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build();

        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(
                        response -> {
                            if (response.statusCode() != 200) {
                                throw new CompletionException(
                                        new IllegalStateException(
                                                "peer "
                                                        + peerId
                                                        + " returned HTTP "
                                                        + response.statusCode()));
                            }
                            try {
                                return objectMapper.readValue(response.body(), responseType);
                            } catch (JsonProcessingException exception) {
                                throw new CompletionException(exception);
                            }
                        });
    }

    private static URI endpoint(URI baseUri, String path) {
        String base = baseUri.toString();
        return URI.create((base.endsWith("/") ? base.substring(0, base.length() - 1) : base) + path);
    }
}
