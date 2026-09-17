//! Ranking Service -- the design doc calls for a two-pass ranker
//! (LightGBM pruning pass + a multi-task deep model computing
//! P(Like)/P(Comment)/P(Share)/P(Dwell)/P(Hide)) feeding the composite
//! score:
//!
//!   Score = w1*P(Like) + w2*P(Comment) + w3*P(Share) + w4*P(Dwell>5s) - w5*P(Hide)
//!
//! Training real ranking models is out of scope for a local demo, so this
//! service implements the same *shape* of pipeline -- engagement-probability
//! estimation followed by the weighted composite score -- using transparent
//! heuristics (recency decay + normalized engagement counts) in place of the
//! learned LightGBM/DNN passes. Swapping in real models later only touches
//! `estimate_probabilities`; the scoring formula stays the same.
//!
//! Wire format: the `Rank` call is gRPC (see schemas/proto/ranking.proto),
//! but the request/response bodies are raw FlatBuffers buffers (see
//! schemas/fbs/ranking.fbs), not nested protobuf messages -- deliberately,
//! for this one high-volume, list-heavy call, to avoid Protobuf's own
//! parse+allocate step. See doc/wire-protocols.md. `/healthz` stays a
//! plain HTTP endpoint on the original port, since that's what
//! docker-compose's container healthcheck probes with `wget --spider`.

use axum::{routing::get, Json, Router};
use chrono::{DateTime, Utc};
use std::net::SocketAddr;

#[path = "genfbs/ranking_generated.rs"]
#[allow(dead_code, unused_imports)]
mod ranking_generated;
use ranking_generated::feed::ranking::{
    CandidateList, RankedItem as FbRankedItem, RankedItemArgs as FbRankedItemArgs, RankedList,
    RankedListArgs,
};

mod rankingpb {
    tonic::include_proto!("feed.ranking.v1");
}
use rankingpb::ranking_service_server::{RankingService, RankingServiceServer};
use rankingpb::{RankRequest, RankResponse};

#[derive(Debug, Clone)]
struct Candidate {
    post_id: String,
    author_id: String,
    source: String,
    like_count: i64,
    comment_count: i64,
    created_at: DateTime<Utc>,
}

#[derive(Debug)]
struct RankedItem {
    post_id: String,
    author_id: String,
    source: String,
    score: f64,
    p_like: f64,
    p_comment: f64,
    p_share: f64,
    p_dwell: f64,
    p_hide: f64,
}

/// Business weights from the doc's composite scoring objective. These would
/// normally be learned/tuned by a growth team; here they're fixed constants,
/// overridable via env for experimentation.
struct Weights {
    like: f64,
    comment: f64,
    share: f64,
    dwell: f64,
    hide: f64,
}

impl Weights {
    fn from_env() -> Self {
        Weights {
            like: env_f64("RANK_W_LIKE", 1.0),
            comment: env_f64("RANK_W_COMMENT", 1.5),
            share: env_f64("RANK_W_SHARE", 2.0),
            dwell: env_f64("RANK_W_DWELL", 0.8),
            hide: env_f64("RANK_W_HIDE", 3.0),
        }
    }
}

fn env_f64(key: &str, fallback: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(fallback)
}

/// Recency decay in [0,1]: 1.0 for brand-new posts, halving roughly every
/// 24h. Stands in for the "P(Dwell > 5s)" freshness signal a real dwell-time
/// model would learn.
fn recency_decay(age_hours: f64) -> f64 {
    1.0 / (1.0 + age_hours / 24.0)
}

fn estimate_probabilities(c: &Candidate) -> (f64, f64, f64, f64, f64) {
    let age_hours = (Utc::now() - c.created_at).num_seconds().max(0) as f64 / 3600.0;
    let decay = recency_decay(age_hours);

    // Normalized engagement counts (diminishing returns via count/(count+k)),
    // scaled by recency -- a crude stand-in for a learned P(Like)/P(Comment).
    let p_like = (c.like_count as f64 / (c.like_count as f64 + 10.0)) * decay;
    let p_comment = (c.comment_count as f64 / (c.comment_count as f64 + 5.0)) * decay;
    let p_share = p_like * 0.3;
    let p_dwell = decay * 0.7;
    let p_hide = 0.05 + (1.0 - decay) * 0.1;

    (p_like, p_comment, p_share, p_dwell, p_hide)
}

fn score_candidate(weights: &Weights, c: &Candidate) -> RankedItem {
    let (p_like, p_comment, p_share, p_dwell, p_hide) = estimate_probabilities(c);
    let score = weights.like * p_like + weights.comment * p_comment + weights.share * p_share
        - weights.hide * p_hide
        + weights.dwell * p_dwell;

    RankedItem {
        post_id: c.post_id.clone(),
        author_id: c.author_id.clone(),
        source: c.source.clone(),
        score,
        p_like,
        p_comment,
        p_share,
        p_dwell,
        p_hide,
    }
}

/// Decodes a `feed.ranking.CandidateList` FlatBuffer (schemas/fbs/ranking.fbs)
/// into the local `Candidate` structs the existing scoring logic already
/// works with.
fn decode_candidates(buf: &[u8]) -> Result<Vec<Candidate>, String> {
    let list = flatbuffers::root::<CandidateList>(buf).map_err(|e| format!("invalid candidates FlatBuffer: {e}"))?;
    let Some(items) = list.items() else {
        return Ok(Vec::new());
    };
    let mut candidates = Vec::with_capacity(items.len());
    for i in 0..items.len() {
        let fb_c = items.get(i);
        let created_at = DateTime::<Utc>::from_timestamp_millis(fb_c.created_at() as i64).unwrap_or_else(Utc::now);
        candidates.push(Candidate {
            post_id: fb_c.post_id().to_string(),
            author_id: fb_c.author_id().to_string(),
            source: fb_c.source().unwrap_or("").to_string(),
            like_count: fb_c.like_count(),
            comment_count: fb_c.comment_count(),
            created_at,
        });
    }
    Ok(candidates)
}

/// Encodes scored items into a `feed.ranking.RankedList` FlatBuffer.
fn encode_ranked(ranked: &[RankedItem]) -> Vec<u8> {
    let mut fbb = flatbuffers::FlatBufferBuilder::new();

    let item_offsets: Vec<_> = ranked
        .iter()
        .map(|r| {
            let source = fbb.create_string(&r.source);
            FbRankedItem::create(
                &mut fbb,
                &FbRankedItemArgs {
                    post_id: r.post_id.parse().unwrap_or(0),
                    author_id: r.author_id.parse().unwrap_or(0),
                    source: Some(source),
                    score: r.score,
                    p_like: r.p_like,
                    p_comment: r.p_comment,
                    p_share: r.p_share,
                    p_dwell: r.p_dwell,
                    p_hide: r.p_hide,
                },
            )
        })
        .collect();

    let items_vec = fbb.create_vector(&item_offsets);
    let list = RankedList::create(&mut fbb, &RankedListArgs { items: Some(items_vec) });
    fbb.finish(list, None);
    fbb.finished_data().to_vec()
}

#[derive(Default)]
struct RankingGrpcService;

#[tonic::async_trait]
impl RankingService for RankingGrpcService {
    async fn rank(
        &self,
        request: tonic::Request<RankRequest>,
    ) -> Result<tonic::Response<RankResponse>, tonic::Status> {
        let candidates = decode_candidates(&request.into_inner().candidates_fb)
            .map_err(tonic::Status::invalid_argument)?;

        let weights = Weights::from_env();
        let mut ranked: Vec<RankedItem> = candidates.iter().map(|c| score_candidate(&weights, c)).collect();
        ranked.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

        Ok(tonic::Response::new(RankResponse { ranked_fb: encode_ranked(&ranked) }))
    }
}

async fn healthz() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let http_port: u16 = std::env::var("RANKING_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4003);
    let grpc_port: u16 = std::env::var("RANKING_GRPC_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4103);

    // /healthz only -- /rank moved to gRPC below. docker-compose's
    // container healthcheck (`wget --spider`) needs a plain HTTP endpoint,
    // so this small server stays on the original port instead of being
    // replaced outright.
    let http_app = Router::new().route("/healthz", get(healthz));
    let http_addr = SocketAddr::from(([0, 0, 0, 0], http_port));
    let http_listener = tokio::net::TcpListener::bind(http_addr).await?;
    tracing::info!("ranking-service HTTP (/healthz) listening on {}", http_addr);
    let http_server = axum::serve(http_listener, http_app);

    let grpc_addr = SocketAddr::from(([0, 0, 0, 0], grpc_port));
    tracing::info!("ranking-service gRPC (Rank) listening on {}", grpc_addr);
    let grpc_server = tonic::transport::Server::builder()
        .add_service(RankingServiceServer::new(RankingGrpcService))
        .serve(grpc_addr);

    tokio::try_join!(
        async { http_server.await.map_err(|e| Box::<dyn std::error::Error>::from(e)) },
        async { grpc_server.await.map_err(|e| Box::<dyn std::error::Error>::from(e)) },
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ranking_generated::feed::ranking::{Candidate as FbCandidate, CandidateArgs as FbCandidateArgs, CandidateListArgs};

    fn build_candidates_fb(candidates: &[(u64, u64, &str, i64, i64, u64)]) -> Vec<u8> {
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let offsets: Vec<_> = candidates
            .iter()
            .map(|&(post_id, author_id, source, like_count, comment_count, created_at)| {
                let source = fbb.create_string(source);
                FbCandidate::create(
                    &mut fbb,
                    &FbCandidateArgs { post_id, author_id, source: Some(source), like_count, comment_count, created_at },
                )
            })
            .collect();
        let items = fbb.create_vector(&offsets);
        let list = CandidateList::create(&mut fbb, &CandidateListArgs { items: Some(items) });
        fbb.finish(list, None);
        fbb.finished_data().to_vec()
    }

    // Round-trip proof, mirroring this repo's "verify it, don't assert
    // it" standard (see scripts/verify_shard_parity.sh): calls the gRPC
    // service struct directly (no network needed -- it's just a struct
    // implementing the generated trait) with a hand-built FlatBuffers
    // request, and decodes the FlatBuffers response.
    #[tokio::test]
    async fn rank_decodes_request_and_encodes_response_as_flatbuffers() {
        let now_millis = Utc::now().timestamp_millis() as u64;
        let candidates_fb = build_candidates_fb(&[
            (111, 222, "in-network", 40, 5, now_millis),
            (333, 444, "vector", 0, 0, now_millis),
        ]);

        let svc = RankingGrpcService;
        let resp = svc
            .rank(tonic::Request::new(RankRequest { candidates_fb }))
            .await
            .expect("rank should succeed")
            .into_inner();

        let ranked = flatbuffers::root::<RankedList>(&resp.ranked_fb).expect("valid RankedList FlatBuffer");
        let items = ranked.items().expect("non-empty items");
        assert_eq!(items.len(), 2);

        // The candidate with real engagement should outrank the cold one.
        let top = items.get(0);
        assert_eq!(top.post_id(), 111);
        assert_eq!(top.author_id(), 222);
        assert_eq!(top.source(), Some("in-network"));
        assert!(top.score() > items.get(1).score());
    }

    #[tokio::test]
    async fn rank_rejects_a_malformed_flatbuffer() {
        let svc = RankingGrpcService;
        let err = svc
            .rank(tonic::Request::new(RankRequest { candidates_fb: vec![1, 2, 3] }))
            .await
            .expect_err("garbage bytes should not decode as a CandidateList");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }
}
