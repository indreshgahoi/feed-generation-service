package com.feed.fanout.storage.grpc;

import com.feed.fanout.domain.ColdTierClient;
import com.feed.genproto.coldtier.AppendRequest;
import com.feed.genproto.coldtier.ColdTierServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Calls feed-aggregation-service's ColdTierService.Append over gRPC --
 * BadgerDB (the cold tier's storage) is a Go-only embedded library, so
 * this cross-language write has to go over the network rather than a
 * shared library call. See doc/DESIGN.md and doc/DESIGN.md.
 *
 * Replaces the old ColdTierHttpClient: same fire-and-forget semantics
 * (a failed append is logged and dropped, never retried -- an accepted
 * loss for a DORMANT follower, per doc/DESIGN.md), but IDs travel as
 * native proto uint64 instead of hand-formatted decimal strings.
 */
public class ColdTierGrpcClient implements ColdTierClient, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ColdTierGrpcClient.class);

    private final ManagedChannel channel;
    private final ColdTierServiceGrpc.ColdTierServiceBlockingStub stub;

    public ColdTierGrpcClient(String grpcAddr) {
        // forAddress(host, port) rather than forTarget(String) since the
        // target here is always a fixed, known host:port -- no URI-scheme
        // dispatch needed. (The pom.xml's maven-shade-plugin switch is
        // what actually fixed gRPC's name resolution at runtime; see the
        // comment there.)
        int lastColon = grpcAddr.lastIndexOf(':');
        String host = grpcAddr.substring(0, lastColon);
        int port = Integer.parseInt(grpcAddr.substring(lastColon + 1));
        this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.stub = ColdTierServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(5, TimeUnit.SECONDS);
    }

    @Override
    public void appendToColdTier(long userId, long postId, long authorId) {
        try {
            stub.append(AppendRequest.newBuilder()
                    .setUserId(userId)
                    .setPostId(postId)
                    .setAuthorId(authorId)
                    .build());
        } catch (Exception e) {
            // See ColdTierClient's javadoc: a failed cold-tier write for a
            // DORMANT follower is a lost fan-out entry for a user who
            // wasn't looking anyway -- logged loudly, not retried, and
            // not allowed to fail the rest of this post's fan-out.
            log.warn("cold-tier append failed for user {} post {}: {}", userId, postId, e.getMessage());
        }
    }

    @Override
    public void close() {
        channel.shutdown();
    }
}
