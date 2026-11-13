//! Reads GGUF quantization names (Q4_K_M, IQ3_XS, BF16 ...) and split-file
//! markers (`-00001-of-00003`) from file names.

/// A quantization type found in a file name.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Quant {
    /// Canonical name, e.g. `Q4_K_M`.
    pub name: &'static str,
    /// Approximate bits per weight, for size and quality estimates.
    pub bits_per_weight: f32,
}

// Longest names first, so `Q4_K_M` wins over `Q4_K`.
const QUANTS: &[(&str, f32)] = &[
    ("IQ1_S", 1.56),
    ("IQ1_M", 1.75),
    ("IQ2_XXS", 2.06),
    ("IQ2_XS", 2.31),
    ("IQ2_S", 2.5),
    ("IQ2_M", 2.7),
    ("IQ3_XXS", 3.06),
    ("IQ3_XS", 3.3),
    ("IQ3_S", 3.44),
    ("IQ3_M", 3.66),
    ("IQ4_XS", 4.25),
    ("IQ4_NL", 4.5),
    ("TQ1_0", 1.69),
    ("TQ2_0", 2.06),
    ("Q2_K_S", 2.6),
    ("Q2_K_L", 3.0),
    ("Q2_K_XL", 3.1),
    ("IQ1_M_XL", 1.9),
    ("IQ2_XXS_XL", 2.3),
    ("IQ3_XXS_XL", 3.2),
    ("Q2_K", 2.96),
    ("Q3_K_XL", 4.0),
    ("Q3_K_S", 3.5),
    ("Q3_K_M", 3.91),
    ("Q3_K_L", 4.27),
    ("Q4_K_XL", 5.0),
    ("Q4_K_S", 4.58),
    ("Q4_K_M", 4.89),
    ("Q4_0", 4.55),
    ("Q4_1", 5.0),
    ("Q5_K_XL", 6.0),
    ("Q5_K_S", 5.54),
    ("Q5_K_M", 5.7),
    ("Q5_0", 5.54),
    ("Q5_1", 6.0),
    ("Q6_K_XL", 7.0),
    ("Q6_K", 6.56),
    ("Q8_K_XL", 9.0),
    ("Q8_0", 8.5),
    ("MXFP4", 4.25),
    ("BF16", 16.0),
    ("F16", 16.0),
    ("FP16", 16.0),
    ("F32", 32.0),
];

/// Finds the quantization in a file name, ignoring case. Returns `None`
/// for names without one.
pub fn parse_quant(file_name: &str) -> Option<Quant> {
    let name = file_name.rsplit('/').next().unwrap_or(file_name);
    let upper = name.to_ascii_uppercase();
    let bytes = upper.as_bytes();
    let mut best: Option<(usize, Quant)> = None;
    for &(quant, bits) in QUANTS {
        let mut from = 0;
        while let Some(found) = upper[from..].find(quant) {
            let start = from + found;
            let end = start + quant.len();
            let before_ok = start == 0 || !bytes[start - 1].is_ascii_alphanumeric();
            let after_ok = end == bytes.len() || !bytes[end].is_ascii_alphanumeric();
            if before_ok && after_ok && best.is_none_or(|(len, _)| quant.len() > len) {
                best = Some((
                    quant.len(),
                    Quant {
                        name: quant,
                        bits_per_weight: bits,
                    },
                ));
            }
            from = start + 1;
        }
    }
    best.map(|(_, quant)| quant)
}

/// For split files like `model-00002-of-00005.gguf`, returns `(2, 5)`.
pub fn parse_shard(file_name: &str) -> Option<(u32, u32)> {
    let lower = file_name.to_ascii_lowercase();
    let at = lower.rfind("-of-")?;
    let part: String = lower[..at]
        .chars()
        .rev()
        .take_while(char::is_ascii_digit)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    let total: String = lower[at + 4..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    let (part, total) = (part.parse().ok()?, total.parse().ok()?);
    (part >= 1 && part <= total).then_some((part, total))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(file: &str) -> Option<&'static str> {
        parse_quant(file).map(|quant| quant.name)
    }

    #[test]
    fn parses_unsloth_dynamic_q2() {
        assert_eq!(
            parse_quant("M-UD-Q2_K_XL-00001-of-00002.gguf")
                .unwrap()
                .name,
            "Q2_K_XL"
        );
    }

    #[test]
    fn finds_common_quants() {
        assert_eq!(name("Clover-35B.Q4_K_M.gguf"), Some("Q4_K_M"));
        assert_eq!(name("TwIL-8B.i1-IQ3_S.gguf"), Some("IQ3_S"));
        assert_eq!(name("model-q8_0.gguf"), Some("Q8_0"));
        assert_eq!(name("org/repo/Model-BF16.gguf"), Some("BF16"));
        assert_eq!(name("Qwen3-UD-Q4_K_XL.gguf"), Some("Q4_K_XL"));
        assert_eq!(name("d1-3B-Artemis-Q4_K_M.gguf"), Some("Q4_K_M"));
    }

    #[test]
    fn ignores_names_without_quant() {
        assert_eq!(name("model.safetensors"), None);
        assert_eq!(name("Q4KM.gguf"), None);
        assert_eq!(name("XQ4_0.gguf"), None);
    }

    #[test]
    fn reads_shards() {
        assert_eq!(parse_shard("m-Q4_K_M-00002-of-00005.gguf"), Some((2, 5)));
        assert_eq!(parse_shard("m.gguf"), None);
        assert_eq!(parse_shard("m-00006-of-00005.gguf"), None);
    }
}
