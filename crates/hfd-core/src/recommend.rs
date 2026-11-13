//! Onboarding helper: pick the recommended models for a machine.
//!
//! Inputs are the JSON from `hw-probe` and the rig's `models.json` registry.
//! The tier comes from the probe's `tier` object when it names a registry tier,
//! otherwise from RAM and VRAM. A pick gets `fits` only when the registry entry
//! has a `size_mib`; the rule matches `catalog::fit_for` (VRAM is not counted).

use serde::Serialize;
use serde_json::Value;

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Pick {
    pub model: String,
    pub quant: Option<String>,
    pub status: Option<String>,
    pub note: Option<String>,
    /// `None` when the registry gives no size for this model.
    pub fits: Option<Fit>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Fit {
    Yes,
    Tight,
    No,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Recommendation {
    pub tier: String,
    pub chat: Option<Pick>,
    pub image: Option<Pick>,
    pub video: Option<Pick>,
    pub free_ram_mib: Option<u64>,
}

/// Same rule as `catalog::fit_for`: fits if size <= free RAM; tight if up to 10% over.
pub fn fit_for(size_mib: u64, free_ram_mib: u64) -> Fit {
    if size_mib <= free_ram_mib {
        Fit::Yes
    } else if size_mib * 10 <= free_ram_mib * 11 {
        Fit::Tight
    } else {
        Fit::No
    }
}

fn pick(entry: &Value, free_ram: Option<u64>) -> Option<Pick> {
    if entry.is_null() {
        return None;
    }
    let model = entry.get("model")?.as_str()?.to_string();
    let text = |k: &str| entry.get(k).and_then(Value::as_str).map(str::to_string);
    let fits = match (entry.get("size_mib").and_then(Value::as_u64), free_ram) {
        (Some(size), Some(free)) => Some(fit_for(size, free)),
        _ => None,
    };
    Some(Pick {
        model,
        quant: text("quant"),
        status: text("status"),
        note: text("note"),
        fits,
    })
}

fn tier_from_hardware(probe: &Value) -> &'static str {
    let vram = probe["gpu"]["vram_mib"].as_u64().unwrap_or(0);
    let ram = probe["ram"]["ram_total_mib"].as_u64().unwrap_or(0);
    if vram >= 4096 {
        "GPU 4 GB"
    } else if ram <= 8192 {
        "CPU-only 8 GB RAM"
    } else if ram <= 16384 {
        "CPU-only 16 GB RAM"
    } else {
        "CPU-only 32 GB RAM"
    }
}

/// Recommend models for the machine described by `probe_json` (hw-probe output),
/// using the tiers in `registry_json` (models.json).
pub fn recommend(probe_json: &str, registry_json: &str) -> Result<Recommendation> {
    let probe: Value = serde_json::from_str(probe_json)
        .map_err(|e| Error::Invalid(format!("hw-probe output is not JSON: {e}")))?;
    let registry: Value = serde_json::from_str(registry_json)
        .map_err(|e| Error::Invalid(format!("models.json is not JSON: {e}")))?;
    let tiers = registry["tiers"]
        .as_array()
        .ok_or_else(|| Error::Invalid("models.json has no tiers array".into()))?;

    let named = probe["tier"]["tier"].as_str();
    let wanted = named
        .map(str::to_string)
        .unwrap_or_else(|| tier_from_hardware(&probe).to_string());
    let tier = tiers
        .iter()
        .find(|t| t["tier"].as_str() == Some(wanted.as_str()))
        .or_else(|| {
            // Probe tier names can carry a suffix, e.g. "GPU 4 GB (ours, ...)".
            tiers.iter().find(|t| {
                t["tier"]
                    .as_str()
                    .is_some_and(|n| n.starts_with(tier_from_hardware(&probe)))
            })
        })
        .ok_or_else(|| Error::Invalid(format!("no registry tier matches '{wanted}'")))?;

    let free_ram = probe["ram"]["ram_free_mib"].as_u64();
    Ok(Recommendation {
        tier: tier["tier"].as_str().unwrap_or_default().to_string(),
        chat: pick(&tier["chat"], free_ram),
        image: pick(&tier["image"], free_ram),
        video: pick(&tier["video"], free_ram),
        free_ram_mib: free_ram,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const REGISTRY: &str = r#"{"tiers":[
      {"tier":"CPU-only 8 GB RAM","chat":{"model":"LFM3-1.7B","quant":"Q6_K","status":"measured","size_mib":1400}},
      {"tier":"GPU 4 GB (ours, RTX 3050 Laptop)","chat":{"model":"MiniCPM5-2B","quant":"Q4_K_M"},
       "image":{"model":"Z-Image-Turbo","quant":"Q4_K"},"video":null}
    ]}"#;

    #[test]
    fn named_probe_tier_wins() {
        let probe = r#"{"ram":{"ram_total_mib":23750,"ram_free_mib":11692},
            "gpu":{"vram_mib":4096},"tier":{"tier":"GPU 4 GB (ours, RTX 3050 Laptop)"}}"#;
        let r = recommend(probe, REGISTRY).unwrap();
        assert_eq!(r.tier, "GPU 4 GB (ours, RTX 3050 Laptop)");
        assert_eq!(r.chat.unwrap().model, "MiniCPM5-2B");
        assert_eq!(r.image.unwrap().model, "Z-Image-Turbo");
        assert!(r.video.is_none());
    }

    #[test]
    fn falls_back_to_hardware_tier_and_checks_fit() {
        let probe = r#"{"ram":{"ram_total_mib":7900,"ram_free_mib":1000},"gpu":{"vram_mib":0}}"#;
        let r = recommend(probe, REGISTRY).unwrap();
        assert_eq!(r.tier, "CPU-only 8 GB RAM");
        let chat = r.chat.unwrap();
        assert_eq!(chat.fits, Some(Fit::No)); // 1400 MiB > 1000 MiB free
    }

    #[test]
    fn no_size_means_no_fit_claim() {
        let probe =
            r#"{"ram":{"ram_total_mib":23750,"ram_free_mib":11692},"gpu":{"vram_mib":4096}}"#;
        let r = recommend(probe, REGISTRY).unwrap();
        assert_eq!(r.chat.unwrap().fits, None);
    }

    #[test]
    fn fit_boundaries() {
        assert_eq!(fit_for(100, 100), Fit::Yes);
        assert_eq!(fit_for(110, 100), Fit::Tight);
        assert_eq!(fit_for(111, 100), Fit::No);
    }

    #[test]
    fn bad_json_is_an_error() {
        assert!(recommend("nope", REGISTRY).is_err());
    }
}
