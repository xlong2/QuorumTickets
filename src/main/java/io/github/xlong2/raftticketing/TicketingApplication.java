package io.github.xlong2.raftticketing;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.xlong2.raftticketing.cluster.HttpClusterTransport;
import io.github.xlong2.raftticketing.cluster.RaftClusterService;
import io.github.xlong2.raftticketing.core.RaftNode;
import io.github.xlong2.raftticketing.http.TicketingHttpServer;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public final class TicketingApplication {
    private static final Logger LOGGER = Logger.getLogger(TicketingApplication.class.getName());

    private TicketingApplication() {}

    public static void main(String[] args) throws Exception {
        Configuration configuration = Configuration.fromEnvironment(System.getenv());
        ObjectMapper objectMapper =
                new ObjectMapper()
                        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        HttpClient httpClient =
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

        RaftNode node =
                new RaftNode(
                        configuration.nodeId(),
                        configuration.peers().keySet(),
                        configuration.initialTickets());
        HttpClusterTransport transport =
                new HttpClusterTransport(httpClient, objectMapper, configuration.peers());
        RaftClusterService clusterService = new RaftClusterService(node, transport);
        TicketingHttpServer server =
                new TicketingHttpServer(configuration.port(), objectMapper, clusterService);

        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    server.close();
                                    clusterService.close();
                                },
                                "ticketing-shutdown"));

        server.start();
        clusterService.start();
        LOGGER.info(
                () ->
                        "node="
                                + configuration.nodeId()
                                + " event=started port="
                                + configuration.port()
                                + " members="
                                + configuration.peers().keySet());
        new CountDownLatch(1).await();
    }

    record Configuration(
            String nodeId, int port, int initialTickets, Map<String, URI> peers) {

        private static Configuration fromEnvironment(Map<String, String> environment) {
            String nodeId = environment.getOrDefault("NODE_ID", "node-0").trim();
            int port = parsePositiveInt(environment.getOrDefault("PORT", "8080"), "PORT");
            int initialTickets =
                    parseNonNegativeInt(
                            environment.getOrDefault("INITIAL_TICKETS", "100"),
                            "INITIAL_TICKETS");
            String defaultPeers = nodeId + "=http://localhost:" + port;
            Map<String, URI> peers =
                    parsePeers(environment.getOrDefault("PEERS", defaultPeers));
            if (!peers.containsKey(nodeId)) {
                throw new IllegalArgumentException("PEERS must contain NODE_ID " + nodeId);
            }
            return new Configuration(nodeId, port, initialTickets, peers);
        }

        private static Map<String, URI> parsePeers(String value) {
            Map<String, URI> peers = new LinkedHashMap<>();
            Arrays.stream(value.split(","))
                    .map(String::trim)
                    .filter(entry -> !entry.isEmpty())
                    .forEach(
                            entry -> {
                                String[] parts = entry.split("=", 2);
                                if (parts.length != 2
                                        || parts[0].isBlank()
                                        || parts[1].isBlank()) {
                                    throw new IllegalArgumentException(
                                            "PEERS entries must use node-id=http://host:port");
                                }
                                String id = parts[0].trim();
                                URI uri = URI.create(parts[1].trim());
                                if (peers.put(id, uri) != null) {
                                    throw new IllegalArgumentException("duplicate peer id: " + id);
                                }
                            });
            if (peers.isEmpty()) {
                throw new IllegalArgumentException("PEERS cannot be empty");
            }
            return Map.copyOf(peers);
        }

        private static int parsePositiveInt(String value, String name) {
            int parsed = parseNonNegativeInt(value, name);
            if (parsed < 1) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return parsed;
        }

        private static int parseNonNegativeInt(String value, String name) {
            try {
                int parsed = Integer.parseInt(value);
                if (parsed < 0) {
                    throw new IllegalArgumentException(name + " cannot be negative");
                }
                return parsed;
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException(name + " must be an integer", exception);
            }
        }
    }
}
