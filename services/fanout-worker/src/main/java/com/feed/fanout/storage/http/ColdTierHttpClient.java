package com.feed.fanout.storage.http;

import com.feed.fanout.domain.ColdTierClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Calls feed-aggregation-service's POST /internal/cold-tier/append over
 * plain HTTP -- BadgerDB (the cold tier's storage) is a Go-only embedded
 * library, so this cross-language write has to go over the network
 * rather than a shared library call. See doc/caching.md. Uses the JDK's
 * built-in java.net.http client -- no HTTP library dependency needed for
 * one simple POST.
 */
public class ColdTierHttpClient implements ColdTierClient {
    private static final Logger log = LoggerFactory.getLogger(ColdTierHttpClient.class);

    private final String baseUrl;
    private final HttpClient client;

    public ColdTierHttpClient(String baseUrl) {
        this.baseUrl = baseUrl;
        this.client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    @Override
    public void appendToColdTier(long userId, long postId, long authorId) {
        String body = String.format(
                "{\"userId\":\"%d\",\"postId\":\"%d\",\"authorId\":\"%d\"}", userId, postId, authorId);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/internal/cold-tier/append"))
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 300) {
                log.warn("cold-tier append for user {} post {} returned status {}", userId, postId, response.statusCode());
            }
        } catch (Exception e) {
            // A failed cold-tier write for a DORMANT follower is a lost
            // fan-out entry for a user who wasn't looking anyway --
            // logged loudly, not retried, and not allowed to fail the
            // rest of this post's fan-out. See doc/caching.md for the
            // durability trade-off this tier already accepts.
            log.warn("cold-tier append failed for user {} post {}: {}", userId, postId, e.getMessage());
        }
    }
}
