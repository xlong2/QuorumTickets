package io.github.xlong2.raftticketing.cluster;

import io.github.xlong2.raftticketing.core.AppendEntriesRequest;
import io.github.xlong2.raftticketing.core.AppendEntriesResponse;
import io.github.xlong2.raftticketing.core.VoteRequest;
import io.github.xlong2.raftticketing.core.VoteResponse;
import java.util.concurrent.CompletableFuture;

public interface ClusterTransport {
    CompletableFuture<VoteResponse> requestVote(String peerId, VoteRequest request);

    CompletableFuture<AppendEntriesResponse> appendEntries(
            String peerId, AppendEntriesRequest request);
}
