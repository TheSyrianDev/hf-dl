use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::config::Config;
use crate::error::{Error, Result};

pub const OAUTH_CLIENT_ID: &str = "26be6b09-91c5-47da-9861-d2d2bb7a7e36";
const DEVICE_CODE_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:device_code";

#[derive(Debug, Clone, Deserialize)]
pub struct DeviceCodeInfo {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    #[serde(default)]
    pub verification_uri_complete: Option<String>,
    #[serde(default = "default_interval")]
    pub interval: u64,
    #[serde(default = "default_expires_in")]
    pub expires_in: u64,
}

impl DeviceCodeInfo {
    pub fn complete_uri(&self) -> &str {
        self.verification_uri_complete
            .as_deref()
            .unwrap_or(&self.verification_uri)
    }
}

fn default_interval() -> u64 {
    5
}

fn default_expires_in() -> u64 {
    900
}

pub fn resolve_token(explicit: Option<&str>, config: &Config) -> Option<String> {
    if let Some(token) = explicit {
        let token = token.trim();
        if !token.is_empty() {
            return Some(token.to_string());
        }
    }

    for key in ["HF_TOKEN", "HUGGING_FACE_HUB_TOKEN"] {
        if let Ok(token) = env::var(key) {
            let token = token.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }
    }

    if let Some(token) = read_secret(&config.token_path) {
        return Some(token);
    }

    read_stored_tokens(&config.stored_tokens_path)
        .into_iter()
        .next()
        .map(|(_, token)| token)
}

pub fn save_token(config: &Config, token: &str) -> Result<()> {
    write_secret(&config.token_path, token.trim())
}

pub fn remove_tokens(config: &Config) -> Result<()> {
    for path in [&config.token_path, &config.stored_tokens_path] {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn read_secret(path: &Path) -> Option<String> {
    let value = fs::read_to_string(path).ok()?;
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

pub fn read_stored_tokens(path: &Path) -> Vec<(String, String)> {
    let Ok(content) = fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut tokens = Vec::new();
    let mut section = None;

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            section = Some(line[1..line.len() - 1].trim().to_string());
            continue;
        }
        if let Some(name) = &section
            && let Some((key, value)) = line.split_once('=')
            && key.trim() == "hf_token"
        {
            let value = value.trim();
            if !value.is_empty() {
                tokens.push((name.clone(), value.to_string()));
            }
        }
    }

    tokens
}

pub async fn request_device_code(config: &Config) -> Result<DeviceCodeInfo> {
    let url = format!("{}/oauth/device", config.endpoint);
    let client = oauth_client(config)?;
    let response = client
        .post(&url)
        .form(&[("client_id", OAUTH_CLIENT_ID)])
        .timeout(Duration::from_secs(30))
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(Error::Status {
            status: status.as_u16(),
            url,
            body: truncate(body),
        });
    }

    let mut info: DeviceCodeInfo = response.json().await?;
    if info.interval == 0 {
        info.interval = default_interval();
    }
    if info.expires_in == 0 {
        info.expires_in = default_expires_in();
    }
    Ok(info)
}

pub async fn poll_device_token(config: &Config, info: &DeviceCodeInfo) -> Result<String> {
    let url = format!("{}/oauth/token", config.endpoint);
    let client = oauth_client(config)?;
    let deadline = Instant::now() + Duration::from_secs(info.expires_in);
    let mut interval = info.interval.max(1);

    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_secs(interval)).await;
        let response = client
            .post(&url)
            .form(&[
                ("grant_type", DEVICE_CODE_GRANT_TYPE),
                ("device_code", info.device_code.as_str()),
                ("client_id", OAUTH_CLIENT_ID),
            ])
            .timeout(Duration::from_secs(30))
            .send()
            .await;

        let Ok(response) = response else {
            continue;
        };
        let status = response.status();
        let Ok(data) = response.json::<serde_json::Value>().await else {
            continue;
        };

        if let Some(token) = data.get("access_token").and_then(|value| value.as_str()) {
            return Ok(token.to_string());
        }

        match data.get("error").and_then(|value| value.as_str()) {
            Some("authorization_pending") | None => {}
            Some("slow_down") => interval = interval.saturating_add(5),
            Some("expired_token") => {
                return Err(Error::Auth {
                    repo: "oauth/device".to_string(),
                    detail: "the device code expired; run `hfd auth login` again".to_string(),
                });
            }
            Some("access_denied") => {
                return Err(Error::Auth {
                    repo: "oauth/device".to_string(),
                    detail: "authorization was denied".to_string(),
                });
            }
            Some(error) if status.is_success() => {
                let description = data
                    .get("error_description")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default();
                return Err(Error::Auth {
                    repo: "oauth/device".to_string(),
                    detail: format!("{error}: {description}"),
                });
            }
            Some(_) => {
                let body = data.to_string();
                return Err(Error::Status {
                    status: status.as_u16(),
                    url,
                    body: truncate(body),
                });
            }
        }
    }

    Err(Error::Auth {
        repo: "oauth/device".to_string(),
        detail: "the device code expired; run `hfd auth login` again".to_string(),
    })
}

pub fn mask_token(token: &str) -> String {
    let chars: Vec<char> = token.chars().collect();
    if chars.len() <= 8 {
        return "*".repeat(chars.len());
    }
    let prefix: String = chars[..3].iter().collect();
    let suffix: String = chars[chars.len() - 4..].iter().collect();
    format!("{prefix}****{suffix}")
}

fn oauth_client(config: &Config) -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .user_agent(config.user_agent.clone())
        .connect_timeout(Duration::from_secs(30))
        .build()?)
}

fn write_secret(path: &Path, value: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut options = OpenOptions::new();
    options.create(true).write(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(value.as_bytes())?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = file.metadata()?.permissions();
        permissions.set_mode(0o600);
        file.set_permissions(permissions)?;
    }
    Ok(())
}

fn truncate(mut body: String) -> String {
    body = body.trim().to_string();
    if body.len() > 2000 {
        body.truncate(2000);
        body.push('…');
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn reads_stored_token() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stored_tokens");
        let mut file = fs::File::create(&path).unwrap();
        writeln!(file, "[Read]").unwrap();
        writeln!(file, "hf_token = hf_example").unwrap();
        assert_eq!(
            read_stored_tokens(&path),
            vec![("Read".to_string(), "hf_example".to_string())]
        );
    }

    #[test]
    fn masks_tokens() {
        assert_eq!(mask_token("hf_1234567890"), "hf_****7890");
        assert_eq!(mask_token("short"), "*****");
    }
}
