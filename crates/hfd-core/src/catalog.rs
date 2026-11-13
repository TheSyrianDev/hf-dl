//! Model catalog helpers for GUIs: search the Hub, group a repo's GGUF files
//! by quant, and check which quants fit in memory.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::hub::{HubClient, RepoRef, TreeEntry};
use crate::quant::{parse_quant, parse_shard};
use crate::{Error, Result};

/// How to order search results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SearchSort {
    #[default]
    Downloads,
    Likes,
    Trending,
    Updated,
}

impl SearchSort {
    fn api_key(self) -> &'static str {
        match self {
            SearchSort::Downloads => "downloads",
            SearchSort::Likes => "likes",
            SearchSort::Trending => "trendingScore",
            SearchSort::Updated => "lastModified",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SearchQuery {
    /// Free text, matched against repo names.
    pub text: String,
    /// Only repos tagged `gguf`.
    pub gguf_only: bool,
    pub sort: SearchSort,
    /// Max results (the Hub caps this at 1000). 0 means 20.
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RepoSummary {
    pub id: String,
    #[serde(default)]
    pub downloads: u64,
    #[serde(default)]
    pub likes: u64,
    #[serde(default, rename = "lastModified")]
    pub last_modified: Option<String>,
    #[serde(default)]
    pub gated: GatedFlag,
    #[serde(default, rename = "pipeline_tag")]
    pub pipeline_tag: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
}

/// The Hub sends `false`, `"auto"` or `"manual"`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(untagged)]
pub enum GatedFlag {
    #[default]
    #[serde(skip)]
    Unknown,
    Bool(bool),
    Mode(String),
}

impl GatedFlag {
    pub fn is_gated(&self) -> bool {
        matches!(self, GatedFlag::Mode(_) | GatedFlag::Bool(true))
    }
}

/// One downloadable choice in a repo: a single GGUF file or a merged shard set.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct QuantOption {
    /// Quant name such as `Q4_K_M`, or `None` if the filename has none.
    pub quant: Option<&'static str>,
    pub bits_per_weight: Option<f32>,
    /// Display name: the file, or the first shard's name for shard sets.
    pub name: String,
    /// Every file to download, in shard order.
    pub files: Vec<String>,
    /// Total size of `files` in bytes.
    pub size: u64,
}

/// A repo's GGUF files, grouped for a model picker.
#[derive(Debug, Clone, Serialize, PartialEq, Default)]
pub struct RepoQuants {
    /// Model weights, smallest first.
    pub options: Vec<QuantOption>,
    /// Vision projector files (`mmproj*.gguf`) a multimodal model needs too.
    pub mmproj: Vec<QuantOption>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Fit {
    /// Size fits in free RAM.
    Fits,
    /// Size is over free RAM but at most 10% over it.
    Tight,
    /// More than 10% over free RAM.
    TooBig,
}

/// Memory available for a model. Matches the registry rule: fits if free RAM
/// is at least the GGUF size, tight if up to 10% short, too big beyond that.
/// VRAM is reported by the app but does not count toward the rule.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct MemoryBudget {
    pub free_ram_bytes: u64,
    pub free_vram_bytes: u64,
}

impl MemoryBudget {
    pub fn fit(&self, size: u64) -> Fit {
        fit_for(size, self.free_ram_bytes)
    }
}

/// `Fits` if `size <= budget`, `Tight` if at most 10% over, otherwise `TooBig`.
pub fn fit_for(size: u64, budget: u64) -> Fit {
    if size <= budget {
        Fit::Fits
    } else if (size as u128) * 10 <= (budget as u128) * 11 {
        Fit::Tight
    } else {
        Fit::TooBig
    }
}

impl HubClient {
    /// Search model repos on the Hub.
    pub async fn search_models(&self, query: &SearchQuery) -> Result<Vec<RepoSummary>> {
        let limit = if query.limit == 0 {
            20
        } else {
            query.limit.min(1000)
        };
        let mut url = url::Url::parse(&format!("{}/api/models", self.config().endpoint))
            .map_err(|error| Error::Invalid(format!("bad endpoint: {error}")))?;
        {
            let mut pairs = url.query_pairs_mut();
            if !query.text.trim().is_empty() {
                pairs.append_pair("search", query.text.trim());
            }
            if query.gguf_only {
                pairs.append_pair("filter", "gguf");
            }
            pairs
                .append_pair("sort", query.sort.api_key())
                .append_pair("direction", "-1")
                .append_pair("limit", &limit.to_string())
                .append_pair("full", "false")
                .append_pair("config", "false");
        }
        let mut request = self
            .http_client()
            .get(url.as_str())
            .timeout(Duration::from_secs(60));
        if let Some(token) = self.token() {
            request = request.bearer_auth(token);
        }
        let response = request.send().await?;
        let status = response.status();
        if !status.is_success() {
            return Err(Error::Invalid(format!(
                "model search failed with HTTP {status}"
            )));
        }
        Ok(response.json().await?)
    }

    /// List a repo's GGUF files grouped by quant, with shard sets merged.
    pub async fn repo_quants(&self, repo: &RepoRef, revision: &str) -> Result<RepoQuants> {
        Ok(group_quants(&self.list_tree(repo, revision).await?))
    }
}

/// Group tree entries into quant options. Pure, so it is easy to test.
pub fn group_quants(entries: &[TreeEntry]) -> RepoQuants {
    let mut sets: BTreeMap<String, Vec<(u32, &TreeEntry)>> = BTreeMap::new();
    for entry in entries {
        if !entry.path.to_ascii_lowercase().ends_with(".gguf") {
            continue;
        }
        let (key, part) = match parse_shard(&entry.path) {
            Some((part, _)) => (shard_key(&entry.path), part),
            None => (entry.path.clone(), 1),
        };
        sets.entry(key).or_default().push((part, entry));
    }

    let mut out = RepoQuants::default();
    for (_, mut parts) in sets {
        parts.sort_by_key(|(part, _)| *part);
        let first = &parts[0].1.path;
        let file_name = first.rsplit('/').next().unwrap_or(first);
        let quant = parse_quant(file_name);
        let option = QuantOption {
            quant: quant.map(|q| q.name),
            bits_per_weight: quant.map(|q| q.bits_per_weight),
            name: file_name.to_string(),
            files: parts.iter().map(|(_, e)| e.path.clone()).collect(),
            size: parts.iter().map(|(_, e)| e.size.unwrap_or(0)).sum(),
        };
        if file_name.to_ascii_lowercase().contains("mmproj") {
            out.mmproj.push(option);
        } else {
            out.options.push(option);
        }
    }
    out.options.sort_by_key(|o| o.size);
    out.mmproj.sort_by_key(|o| o.size);
    out
}

/// `model-Q4_K_M-00002-of-00003.gguf` -> `model-Q4_K_M-of-00003.gguf`.
fn shard_key(path: &str) -> String {
    let lower = path.to_ascii_lowercase();
    if let Some(of) = lower.rfind("-of-")
        && let Some(dash) = lower[..of].rfind('-')
    {
        return format!("{}{}", &path[..dash], &path[of..]);
    }
    path.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(path: &str, size: u64) -> TreeEntry {
        serde_json::from_value(serde_json::json!({"type":"file","path":path,"size":size})).unwrap()
    }

    #[test]
    fn groups_shards_and_mmproj() {
        let entries = vec![
            entry("README.md", 10),
            entry("m-Q8_0-00002-of-00002.gguf", 50),
            entry("m-Q8_0-00001-of-00002.gguf", 60),
            entry("m-Q4_K_M.gguf", 70),
            entry("mmproj-m-f16.gguf", 5),
        ];
        let q = group_quants(&entries);
        assert_eq!(q.options.len(), 2);
        assert_eq!(q.options[0].quant, Some("Q4_K_M"));
        let q8 = &q.options[1];
        assert_eq!(q8.quant, Some("Q8_0"));
        assert_eq!(q8.size, 110);
        assert_eq!(
            q8.files,
            ["m-Q8_0-00001-of-00002.gguf", "m-Q8_0-00002-of-00002.gguf"]
        );
        assert_eq!(q.mmproj.len(), 1);
        assert_eq!(q.mmproj[0].quant, Some("F16"));
    }

    #[test]
    fn fit_thresholds() {
        assert_eq!(fit_for(100, 100), Fit::Fits);
        assert_eq!(fit_for(101, 100), Fit::Tight);
        assert_eq!(fit_for(110, 100), Fit::Tight);
        assert_eq!(fit_for(111, 100), Fit::TooBig);
        let b = MemoryBudget {
            free_ram_bytes: 60,
            free_vram_bytes: 40,
        };
        // VRAM does not count toward the rule.
        assert_eq!(b.fit(60), Fit::Fits);
        assert_eq!(b.fit(66), Fit::Tight);
        assert_eq!(b.fit(70), Fit::TooBig);
    }

    #[test]
    fn gated_flag_parses() {
        let r: RepoSummary = serde_json::from_str(r#"{"id":"a/b","gated":"manual"}"#).unwrap();
        assert!(r.gated.is_gated());
        let r: RepoSummary = serde_json::from_str(r#"{"id":"a/b","gated":false}"#).unwrap();
        assert!(!r.gated.is_gated());
    }
}
