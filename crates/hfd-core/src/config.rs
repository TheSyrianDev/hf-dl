use std::env;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct Config {
    pub endpoint: String,
    pub cache_dir: PathBuf,
    pub xet_cache_dir: PathBuf,
    pub token_path: PathBuf,
    pub stored_tokens_path: PathBuf,
    pub user_agent: String,
}

impl Config {
    pub fn from_env() -> Self {
        let home = env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));

        let hf_home = env::var_os("HF_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".cache").join("huggingface"));

        let cache_dir = env::var_os("HF_HUB_CACHE")
            .map(PathBuf::from)
            .unwrap_or_else(|| hf_home.join("hub"));

        let xet_cache_dir = env::var_os("HF_XET_CACHE")
            .map(PathBuf::from)
            .unwrap_or_else(|| hf_home.join("xet"));

        let token_path = env::var_os("HF_TOKEN_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| hf_home.join("token"));

        let stored_tokens_path = env::var_os("HF_STORED_TOKENS_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| hf_home.join("stored_tokens"));

        let endpoint = env::var("HF_ENDPOINT")
            .unwrap_or_else(|_| "https://huggingface.co".to_string())
            .trim_end_matches('/')
            .to_string();

        Self {
            endpoint,
            cache_dir,
            xet_cache_dir,
            token_path,
            stored_tokens_path,
            user_agent: format!("hfd/{}", env!("CARGO_PKG_VERSION")),
        }
    }

    pub fn with_cache_dir(mut self, cache_dir: Option<PathBuf>) -> Self {
        if let Some(dir) = cache_dir {
            if env::var_os("HF_XET_CACHE").is_none() {
                self.xet_cache_dir = dir
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .map(|parent| parent.join("xet"))
                    .unwrap_or_else(|| dir.join("xet"));
            }
            self.cache_dir = dir;
        }
        self
    }
}
