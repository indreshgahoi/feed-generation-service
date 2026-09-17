package com.feed.fanout.storage.grpc;

import com.feed.genproto.coldtier.AppendRequest;
import com.feed.genproto.coldtier.AppendResponse;
import com.feed.genproto.coldtier.ColdTierServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Round-trip test of ColdTierGrpcClient against a real gRPC server
 * bound to an ephemeral loopback port -- proving the wire contract
 * actually works end to end, not just that the domain interface is
 * satisfied.
 * Mirrors this repo's "verify it, don't assert it" standard (see
 * scripts/verify_shard_parity.sh). Neither ColdTierHttpClient nor its
 * HTTP handler ever had a test like this; this closes that gap for the
 * gRPC replacement.
 */
class ColdTierGrpcClientTest {
    private Server server;
    private List<AppendRequest> received;
    private ColdTierGrpcClient client;

    @BeforeEach
    void setUp() throws Exception {
        received = new ArrayList<>();
        server = ServerBuilder.forPort(0)
                .addService(new ColdTierServiceGrpc.ColdTierServiceImplBase() {
                    @Override
                    public void append(AppendRequest request, StreamObserver<AppendResponse> responseObserver) {
                        received.add(request);
                        responseObserver.onNext(AppendResponse.newBuilder().setAppended(true).build());
                        responseObserver.onCompleted();
                    }
                })
                .build()
                .start();
        client = new ColdTierGrpcClient("localhost:" + server.getPort());
    }

    @AfterEach
    void tearDown() throws Exception {
        client.close();
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void appendToColdTier_sendsNativeUint64FieldsOverGrpc() {
        client.appendToColdTier(357748213593194496L, 111L, 222L);

        assertEquals(1, received.size());
        AppendRequest req = received.get(0);
        assertEquals(357748213593194496L, req.getUserId());
        assertEquals(111L, req.getPostId());
        assertEquals(222L, req.getAuthorId());
    }

    @Test
    void appendToColdTier_swallowsErrorsInsteadOfThrowing() throws Exception {
        // Fire-and-forget semantics: a dead server must not propagate an
        // exception to the caller (FanoutService must keep fanning out
        // to the rest of a post's followers). See ColdTierClient's javadoc.
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);

        assertDoesNotThrow(() -> client.appendToColdTier(1L, 2L, 3L));
    }
}
