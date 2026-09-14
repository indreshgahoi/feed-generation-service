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
//! `estimate_probabilities`; the HTTP contract and scoring formula stay the
//! same.

use axum::{routing::get, routing::post, Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Debug, Clone, Deserialize)]
struct Candidate {
    #[serde(rename = "postId")]
    post_id: String,
    #[serde(rename = "authorId")]
    author_id: String,
    source: String,
    #[serde(rename = "likeCount", default)]
    like_count: i64,
    #[serde(rename = "commentCount", default)]
    comment_count: i64,
    #[serde(rename = "createdAt")]
    created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct RankedItem {
    #[serde(rename = "postId")]
    post_id: String,
    #[serde(rename = "authorId")]
    author_id: String,
    source: String,
    score: f64,
    #[serde(rename = "pLike")]
    p_like: f64,
    #[serde(rename = "pComment")]
    p_comment: f64,
    #[serde(rename = "pShare")]
    p_share: f64,
    #[serde(rename = "pDwell")]
    p_dwell: f64,
    #[serde(rename = "pHide")]
    p_hide: f64,
}

#[derive(Debug, Deserialize)]
struct RankRequest {
    candidates: Vec<Candidate>,
}

#[derive(Debug, Serialize)]
struct RankResponse {
    ranked: Vec<RankedItem>,
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

async fn healthz() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn rank(Json(req): Json<RankRequest>) -> Json<RankResponse> {
    let weights = Weights::from_env();
    let mut ranked: Vec<RankedItem> = req.candidates.iter().map(|c| score_candidate(&weights, c)).collect();
    ranked.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
    Json(RankResponse { ranked })
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let port: u16 = std::env::var("RANKING_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4003);

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/rank", post(rank));

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("ranking-service listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
