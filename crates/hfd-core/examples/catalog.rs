//! Models-page flow: search, pick a repo, list quants with a fit verdict.
//! `cargo run -p hfd-core --example catalog -- <search text> [free RAM GiB]`

use hfd_core::{
    Config, HubClient, MemoryBudget, RepoRef, RepoType, SearchQuery, SearchSort,
    auth::resolve_token,
};

#[tokio::main]
async fn main() -> hfd_core::Result<()> {
    let mut args = std::env::args().skip(1);
    let text = args.next().unwrap_or_else(|| "SmolVLM".into());
    let ram_gib: f64 = args.next().and_then(|v| v.parse().ok()).unwrap_or(8.0);
    let config = Config::from_env();
    let token = resolve_token(None, &config);
    let hub = HubClient::new(config, token)?;

    let query = SearchQuery {
        text,
        gguf_only: true,
        sort: SearchSort::Downloads,
        limit: 5,
    };
    let results = hub.search_models(&query).await?;
    for repo in &results {
        println!(
            "{:<55} {:>9} downloads {:>5} likes",
            repo.id, repo.downloads, repo.likes
        );
    }
    let Some(top) = results.first() else {
        return Ok(());
    };

    let budget = MemoryBudget {
        free_ram_bytes: (ram_gib * 1_073_741_824.0) as u64,
        free_vram_bytes: 0,
    };
    let quants = hub
        .repo_quants(&RepoRef::new(RepoType::Model, &top.id), "main")
        .await?;
    println!(
        "\n{} ({} quants, {} mmproj):",
        top.id,
        quants.options.len(),
        quants.mmproj.len()
    );
    for option in quants.options.iter().chain(&quants.mmproj) {
        println!(
            "  {:<10} {:>8.2} GiB  {:>2} file(s)  {:?}  {}",
            option.quant.unwrap_or("?"),
            option.size as f64 / 1_073_741_824.0,
            option.files.len(),
            budget.fit(option.size),
            option.name
        );
    }
    Ok(())
}
