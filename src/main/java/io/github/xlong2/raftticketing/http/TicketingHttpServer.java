package io.github.xlong2.raftticketing.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.xlong2.raftticketing.cluster.RaftClusterService;
import io.github.xlong2.raftticketing.core.AppendEntriesRequest;
import io.github.xlong2.raftticketing.core.PurchaseResult;
import io.github.xlong2.raftticketing.core.PurchaseStatus;
import io.github.xlong2.raftticketing.core.VoteRequest;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class TicketingHttpServer implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(TicketingHttpServer.class.getName());

    private final HttpServer server;
    private final ExecutorService executor;
    private final ObjectMapper objectMapper;
    private final RaftClusterService clusterService;

    public TicketingHttpServer(
            int port, ObjectMapper objectMapper, RaftClusterService clusterService) throws IOException {
        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException("port must be between 0 and 65535");
        }
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.clusterService = Objects.requireNonNull(clusterService, "clusterService");
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.executor = Executors.newFixedThreadPool(12);
        server.setExecutor(executor);

        server.createContext("/health", exchange -> route(exchange, this::health));
        server.createContext("/status", exchange -> route(exchange, this::status));
        server.createContext("/tickets", exchange -> route(exchange, this::purchase));
        server.createContext("/raft/vote", exchange -> route(exchange, this::requestVote));
        server.createContext("/raft/append", exchange -> route(exchange, this::appendEntries));
    }

    public void start() {
        server.start();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(1);
        executor.shutdownNow();
    }

    private void health(HttpExchange exchange) throws IOException {
        requireMethod(exchange, "GET");
        sendJson(exchange, 200, Map.of("status", "UP", "node", clusterService.status().nodeId()));
    }

    private void status(HttpExchange exchange) throws IOException {
        requireMethod(exchange, "GET");
        sendJson(exchange, 200, clusterService.status());
    }

    private void purchase(HttpExchange exchange) throws IOException {
        requireMethod(exchange, "POST");
        PurchaseRequest request = readJson(exchange, PurchaseRequest.class);
        PurchaseResult result = clusterService.purchase(request.tickets());
        sendJson(exchange, statusCode(result.status()), result);
    }

    private void requestVote(HttpExchange exchange) throws IOException {
        requireMethod(exchange, "POST");
        VoteRequest request = readJson(exchange, VoteRequest.class);
        sendJson(exchange, 200, clusterService.onVoteRequest(request));
    }

    private void appendEntries(HttpExchange exchange) throws IOException {
        requireMethod(exchange, "POST");
        AppendEntriesRequest request = readJson(exchange, AppendEntriesRequest.class);
        sendJson(exchange, 200, clusterService.onAppendEntries(request));
    }

    private void route(HttpExchange exchange, ExchangeAction action) {
        try {
            action.handle(exchange);
        } catch (MethodNotAllowedException exception) {
            safeError(exchange, 405, exception.getMessage());
        } catch (JsonProcessingException | IllegalArgumentException exception) {
            safeError(exchange, 400, exception.getMessage());
        } catch (Exception exception) {
            LOGGER.log(Level.WARNING, "HTTP request failed", exception);
            safeError(exchange, 500, "internal server error");
        } finally {
            exchange.close();
        }
    }

    private <T> T readJson(HttpExchange exchange, Class<T> type) throws IOException {
        try (InputStream body = exchange.getRequestBody()) {
            return objectMapper.readValue(body, type);
        }
    }

    private void sendJson(HttpExchange exchange, int statusCode, Object value) throws IOException {
        byte[] body = objectMapper.writeValueAsBytes(value);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(statusCode, body.length);
        exchange.getResponseBody().write(body);
    }

    private void safeError(HttpExchange exchange, int statusCode, String message) {
        try {
            sendJson(exchange, statusCode, Map.of("error", message == null ? "request failed" : message));
        } catch (IOException ignored) {
            LOGGER.log(Level.FINE, "could not send HTTP error response", ignored);
        }
    }

    private static void requireMethod(HttpExchange exchange, String expected) {
        if (!expected.equalsIgnoreCase(exchange.getRequestMethod())) {
            throw new MethodNotAllowedException("expected " + expected);
        }
    }

    private static int statusCode(PurchaseStatus status) {
        return switch (status) {
            case COMMITTED -> 201;
            case INVALID -> 400;
            case NOT_LEADER, REJECTED -> 409;
            case NO_QUORUM -> 503;
        };
    }

    @FunctionalInterface
    private interface ExchangeAction {
        void handle(HttpExchange exchange) throws IOException;
    }

    private static final class MethodNotAllowedException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private MethodNotAllowedException(String message) {
            super(message);
        }
    }
}
