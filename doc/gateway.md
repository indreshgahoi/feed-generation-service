# The Envoy Gateway

Every service used to be reachable by raw `localhost:<port>`, with the
web UI's own small Python server doing ad-hoc reverse-proxying for two of
them (`/api/ingest` -> post-ingestion-service, `/api/feed` ->
feed-aggregation-service) just so the browser had one origin to talk to.
That's replaced now by a single real ingress -- [Envoy](https://www.envoyproxy.io/)
-- in front of every client-facing service, configured entirely in
[`envoy/envoy.yaml`](../envoy/envoy.yaml). This doc covers the routing
design, what's deliberately *not* behind the gateway, and the HTTP/3
listener specifically -- what it is, why it exists on exactly one route,
and how it was verified.

## Routing table

One TCP listener (`:8080`, HTTP/1.1 and h2) handles all client traffic,
matching the exact path contract `web-ui/app.js` already used against the
old Python proxy -- **no frontend code changes were needed**:

| Path prefix | Routed to | Notes |
|---|---|---|
| `/api/ingest/*` | `post-ingestion-service:4001` | prefix stripped |
| `/api/feed/*` | `feed-aggregation-service:4002` | prefix stripped; also advertises HTTP/3 (see below) |
| `/*` (default) | `web-ui:80` (nginx) | static assets |

A second, UDP listener (`:8443`) speaks HTTP/3 (QUIC) and routes **only**
`/api/feed/*` -- see "Why HTTP/3, and why only here" below.

Envoy also runs active HTTP health checks (`GET /healthz`) against the
`post_ingestion_service` and `feed_aggregation_service` clusters. This
matters specifically because their runtime images are
`gcr.io/distroless/static-debian12` -- **no shell, so no Docker-level
`HEALTHCHECK` is even possible** for those two containers. Envoy's active
check is the real readiness gate for traffic, and it's arguably a more
honest one anyway: it exercises the exact HTTP path a real client request
takes, not a container-internal script that could pass while the HTTP
server itself is wedged.

## What's deliberately NOT behind the gateway

Envoy here is an **edge ingress for client traffic**, not a service mesh
for internal traffic. Two service-to-service calls in this system
bypass it entirely, over Compose's own network via service-name DNS --
both gRPC, not HTTP (see [doc/wire-protocols.md](wire-protocols.md) for
why, including the one call that also carries a FlatBuffers payload):

- fanout-worker's cold-tier append: `ColdTierService.Append` at `feed-aggregation-service:4102`
- feed-aggregation-service's ranking call: `RankingService.Rank` at `ranking-service:4103`

This is a deliberate, standard boundary (the same one a real
ingress-controller-plus-mesh split draws), not an oversight: routing
internal calls through the edge gateway would add a hop and a single
point of failure for traffic that never needs to leave the private
network in the first place. The cost of this boundary is real and worth
naming: there's no mTLS, no consistent retry/circuit-breaking policy, and
no unified request tracing across service-to-service calls the way there
would be if a full mesh (Istio/Linkerd-style sidecars) covered this
traffic too. For 3 internal call sites, that's a reasonable trade; it
would not be at real service-count scale.

## Why HTTP/3, and why only on the feed read path

The design doc's own numbers make feed reads the outlier in this system:
~80k QPS target, p99 < 200ms, and it's the path most exposed to real
client network conditions (mobile, lossy Wi-Fi, cross-continent
round-trips) -- exactly where QUIC's advantages (no head-of-line
blocking across streams, 0-RTT reconnection, faster handshake under
packet loss) actually pay for themselves. `post-ingestion-service`'s
write path is comparatively low-QPS and far less latency-sensitive by
comparison -- there's no payoff there for a second listener, a second
cert, and a second connection-management story.

**QUIC is terminated at the Envoy edge only.** Envoy still speaks plain
HTTP/1.1 to `feed-aggregation-service` upstream -- this is the exact
pattern real CDNs and edge proxies use (QUIC/HTTP-3 to the browser,
HTTP/2 to origin), for a concrete reason: the Envoy-to-Go-service hop is
a same-Docker-network, sub-millisecond hop that was never the latency
problem QUIC solves. Giving `feed-aggregation-service` itself an HTTP/3
stack would mean pulling `quic-go` into a service that currently has zero
CGO and zero non-stdlib HTTP dependencies, for a hop where the network
conditions QUIC is designed for (loss, high RTT, connection migration)
don't exist. Terminate where the problem actually is.

The first request from any client still goes over the TCP listener --
QUIC discovery works by a server advertising itself first. Feed
responses on `:8080` carry:

```
alt-svc: h3=":8443"; ma=86400
```

so a capable client upgrades to `:8443` on its *next* request to this
host. This repo's own verification client (below) connects directly to
`:8443` rather than relying on Alt-Svc discovery, since it's a one-shot
script, not a browser session that would naturally see and act on the
header.

## Verification: a real HTTP/3 request, not an assumed one

HTTP/3 client support in `curl` depends on how it was built (QUIC support
isn't universal even in recent versions), so "curl it and see" isn't a
given for this specific listener. Rather than assert HTTP/3 works because
the config looks right, this was verified with an actual QUIC client:
[`aioquic`](https://github.com/aiortc/aioquic)
(`pip3 install --user aioquic`), a pure-Python QUIC/HTTP-3 implementation
with no native system library dependency, driving a real TLS 1.3 + QUIC
handshake and an HTTP/3 GET against `https://localhost:8443/api/feed/v1/feed?userId=...`.

Result: a genuine `200` with a valid feed JSON body, cross-checked two
independent ways so this isn't just "the client claimed success":
- Envoy's own admin stats (`http://localhost:9901/stats`) show
  `http.ingress_h3.downstream_cx_http3_total: 1` -- Envoy's own counter
  of HTTP/3 connections served, incremented by exactly this request.
- The TCP listener's `/api/feed` responses were independently confirmed
  to carry the `alt-svc` header advertising this same listener.

This is the same standard the rest of `doc/` holds itself to (see
`doc/sharding.md`'s vnode-hash-distribution measurement, or the shard
parity script): a claim about behavior is only as good as the mechanism
that checked it, and "I wrote a QUIC listener block in YAML" and "a real
QUIC client completed a request against it" are different claims.

## What this doesn't solve

- **No TLS on the plaintext `:8080` listener.** A real deployment
  terminates TLS there too (and likely never exposes plaintext HTTP at
  all); this repo's HTTP/1.1 listener is plaintext for local-demo
  simplicity, with TLS scoped only to the QUIC listener because HTTP/3
  mandates it.
- **Self-signed, unrotated dev certificate.** `scripts/gen_dev_certs.sh`
  generates a one-year self-signed cert for `localhost` on first run.
  Fine for this repo; a real deployment needs a real CA and a rotation
  story, neither of which is attempted here.
- **Single Envoy instance, no HA.** Same category of gap as Neo4j and
  the Kafka broker elsewhere in this system -- one process, one point of
  failure for all client traffic. A real deployment runs a fleet of Envoy
  instances behind a network load balancer.
- **No rate limiting, no auth at the gateway.** This repo's engagement
  endpoints have their own rate limiting (see
  `doc/engagement-at-scale.md`), but there's nothing at the edge stopping
  abusive traffic before it reaches a service at all -- a real deployment
  puts at least coarse rate limiting and authentication in the gateway
  layer itself, not only deeper in the stack.
