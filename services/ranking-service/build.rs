// Compiles schemas/proto/ranking.proto into a RankingService gRPC server
// trait, generated fresh on every `cargo build` (not committed) -- see
// scripts/gen_schemas.sh for why Rust is the one language handled this
// way instead of via that shared script. This service is the RPC server
// only; feed-aggregation-service (Go) is the sole client, so no client
// stub is generated here.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["../../schemas/proto/ranking.proto"], &["../../schemas/proto"])?;
    Ok(())
}
