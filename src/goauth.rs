use crate::AppResult;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::tracer::ContextExt;
use lazy_static::lazy_static;

pub use super::apis::*;
pub use super::limiters::*;
pub use super::surrealstart::*;
// use anyhow::bail;
use base64::{
  Engine, engine::general_purpose::STANDARD,
  engine::general_purpose::URL_SAFE_NO_PAD,
};
use chrono::Utc;
use ring::rand::SystemRandom;
use ring::signature::{RSA_PKCS1_SHA256, RsaKeyPair};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
// use std::{env, path::PathBuf};
use tokio::fs;

use crate::{bail, debug, error, info, warn};

// NOTE  you cannot use subject in a token without domain wide delegation
#[allow(non_snake_case, non_upper_case_globals)]
pub mod scps {

  // NOTE you cannot use     "https://www.googleapis.com/auth/chat.admin.spaces" unless you put it in the domain wide delegation part in each admin console, it will be a ton of manual work if you put it.
  pub const A: [&str; 30] = [
    "https://mail.google.com",
    "https://www.googleapis.com/auth/activity",
    "https://www.googleapis.com/auth/admin.directory.device.chromeos",
    "https://www.googleapis.com/auth/admin.directory.group",
    "https://www.googleapis.com/auth/admin.directory.group.member",
    "https://www.googleapis.com/auth/admin.directory.notifications",
    "https://www.googleapis.com/auth/admin.directory.orgunit",
    "https://www.googleapis.com/auth/admin.directory.resource.calendar",
    "https://www.googleapis.com/auth/admin.directory.rolemanagement",
    "https://www.googleapis.com/auth/admin.directory.user",
    "https://www.googleapis.com/auth/admin.directory.user.security",
    "https://www.googleapis.com/auth/admin.reports.audit.readonly",
    "https://www.googleapis.com/auth/admin.reports.usage.readonly",
    "https://www.googleapis.com/auth/apps.groups.settings",
    "https://www.googleapis.com/auth/apps.licensing",
    "https://www.googleapis.com/auth/calendar",
    "https://www.googleapis.com/auth/chat.admin.spaces",
    "https://www.googleapis.com/auth/classroom.announcements",
    "https://www.googleapis.com/auth/classroom.courses",
    "https://www.googleapis.com/auth/classroom.coursework.me",
    "https://www.googleapis.com/auth/classroom.coursework.students",
    "https://www.googleapis.com/auth/classroom.guardianlinks.students",
    "https://www.googleapis.com/auth/classroom.profile.emails",
    "https://www.googleapis.com/auth/classroom.profile.photos",
    "https://www.googleapis.com/auth/classroom.push-notifications",
    "https://www.googleapis.com/auth/classroom.rosters",
    "https://www.googleapis.com/auth/classroom.topics",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
  ];

  // pub const B: [&str; 4] = [
  //   "https://www.googleapis.com/auth/chat.bot",
  //   "https://www.googleapis.com/auth/drive",
  //   "https://www.googleapis.com/auth/cloud-platform",
  //   "https://www.googleapis.com/auth/spreadsheets",
  // ];

  pub const B: [&str; 1] = [
    "https://www.googleapis.com/auth/chat.bot",
    // "https://www.googleapis.com/auth/chat.admin.spaces",
  ];

  // for one domain chats we can put the chat api in this one as well
  pub const C: [&str; 4] = [
    "https://www.googleapis.com/auth/chat.bot",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/spreadsheets",
  ];
}

pub async fn get_token_for_secrets() -> AppResult<String> {
  debug!("Checking if running in cloud environment");

  let is_cloud = CL
    .get("http://metadata.google.internal")
    .header("Metadata-Flavor", "Google")
    .send()
    .await
    .is_ok();

  if is_cloud {
    info!("Running in cloud environment, getting cloud token");
    get_cloud_token().await.cwl("Failed to get cloud token")
  } else {
    info!("RUNNING LOCALLY, GETTING LOCAL TOKEN");
    let value = json_for_secretsapi_local()
      .await
      .cwl("Failed to get local secrets JSON")?;

    let tok = token(value, "".to_string(), "secret")
      .await
      .cwl("Failed to generate token");

    debug!(
      "This is tok to access cloud secrets for cloud project admin {:#?}",
      tok
    );

    tok
  }
}

async fn get_cloud_token() -> AppResult<String> {
  let url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";
  let scopes = scps::C.join(",");
  debug!(scopes = %scopes, url = %url, "Requesting cloud token");

  let res = CL
    .get(url)
    .query(&[("scopes", scopes)])
    .header("Metadata-Flavor", "Google")
    .send()
    .await
    .cwl("Failed to send request")?;

  let status = res.status();
  if status.is_success() {
    let rfin: Value = res.json().await.cwl("Failed to parse JSON response")?;

    let token_str = rfin["access_token"]
      .as_str()
      .ok_or_else(|| {
        anyhow::anyhow!("Access token not found in cloud token response")
      })
      .cwl("Access token not found in cloud token response")?;
    info!("Successfully obtained cloud token");
    Ok(format!("Bearer {token_str}"))
  } else {
    let error_text =
      res.text().await.cwl("Failed to read error response body")?;
    error!(status = %status, error = %error_text, "Failed to get cloud token");
    bail!("Token request failed")
  }
}

#[derive(Debug, Clone)]
struct CachedToken {
  token: String,
  expires_at: u64,
}

lazy_static! {
  static ref SECRETS_CACHE: Mutex<Option<Value>> = Mutex::new(None);
  static ref TOKEN_CACHE: Mutex<HashMap<String, CachedToken>> =
    Mutex::new(HashMap::new());
  static ref DOMAIN_SECRET_CACHE: Mutex<HashMap<String, Value>> =
    Mutex::new(HashMap::new());
}

async fn load_secrets_from_disk() -> AppResult<Value> {
  let folder = "/Users/jasonjurotich/Documents/RUSTDEV/JSONSECRETSAPI";
  let target_filename = &*FIL;

  debug!(folder = %folder, target_file = %target_filename, "Loading secrets from disk");

  let mut dir = fs::read_dir(folder)
    .await
    .cwl(&format!("Failed to read directory: {folder}"))?;

  while let Some(entry) = dir
    .next_entry()
    .await
    .cwl(&format!("Failed to read directory entry in: {folder}"))?
  {
    let path = entry.path();
    if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
      debug!(file_path = %path.display(), "Skipping non-JSON file");
      continue;
    }

    let filename = match path.file_stem().and_then(|name| name.to_str()) {
      Some(name) => name.to_string(),
      None => {
        warn!(file_path = %path.display(), "Skipping file with invalid filename (no stem)");
        continue;
      }
    };

    if filename == target_filename {
      debug!(file_path = %path.display(), "Found target secrets file");
      let buffer = fs::read(&path)
        .await
        .cwl(&format!("Failed to read file: {}", path.display()))?;

      let json_data: Value = serde_json::from_slice(&buffer).cwl(&format!(
        "Failed to parse JSON from file: {}",
        path.display()
      ))?;

      debug!(json_data_size = buffer.len(), "Parsed secrets JSON data");

      if !json_data.is_object()
        || json_data.as_object().is_none_or(|obj| obj.is_empty())
      {
        warn!(file_path = %path.display(), "Secrets JSON file is empty or not an object");
        error!(
          "The secrets JSON file '{}' is empty or not a valid JSON object",
          path.display()
        );
        bail!(
          "The secrets JSON file '{}' is empty or not a valid JSON object",
          path.display()
        );
      }
      debug!(file_path = %path.display(), "Successfully read and parsed secrets JSON file");
      return Ok(json_data);
    }
  }

  error!(target_file = %target_filename, search_folder = %folder, "Target secrets JSON file not found");
  bail!(
    "Required secrets JSON file '{}' not found in directory '{}'",
    target_filename,
    folder
  );
}

pub async fn json_for_secretsapi_local() -> AppResult<Value> {
  debug!("=== json_for_secretsapi_local CALLED ===");

  // Check cache first
  {
    let cache = SECRETS_CACHE.lock().unwrap();
    if let Some(ref cached_data) = *cache {
      debug!("Returning cached secrets JSON data");
      return Ok(cached_data.clone());
    }
  }

  // Load from disk and cache
  debug!("Loading secrets JSON from disk (not cached)");
  let data = load_secrets_from_disk().await?;

  // Cache the result
  {
    let mut cache = SECRETS_CACHE.lock().unwrap();
    *cache = Some(data.clone());
  }

  debug!("Cached secrets JSON data for future use");
  Ok(data)
}

#[allow(dead_code)]
pub fn clear_secrets_cache() {
  let mut cache = SECRETS_CACHE.lock().unwrap();
  *cache = None;
  debug!("Cleared secrets JSON cache");
}

#[allow(dead_code)]
pub fn clear_token_cache() {
  let mut cache = TOKEN_CACHE.lock().unwrap();
  cache.clear();
  debug!("Cleared OAuth token cache");
}

pub fn check_token_needs_refresh(
  usr: &str,
  sub: &str,
  threshold_seconds: u64,
) -> bool {
  let cache_key = format!("{}:{}", usr, sub);
  let cache = TOKEN_CACHE.lock().unwrap();

  if let Some(cached_token) = cache.get(&cache_key) {
    let now = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_secs();

    let time_until_expiry = cached_token.expires_at.saturating_sub(now);

    if time_until_expiry < threshold_seconds {
      debug!(
        "Token for {} will expire in {} seconds, refresh recommended",
        usr, time_until_expiry
      );
      return true;
    }
  }

  false
}

#[allow(dead_code)]
pub fn clear_token_for_user(usr: &str, sub: &str) {
  let cache_key = format!("{}:{}", usr, sub);
  let mut cache = TOKEN_CACHE.lock().unwrap();
  if cache.remove(&cache_key).is_some() {
    debug!("Cleared expired OAuth token for user: {}", usr);
  } else {
    debug!("No cached token found to clear for user: {}", usr);
  }
}

#[allow(dead_code)]
pub fn clear_domain_secret_cache() {
  let mut cache = DOMAIN_SECRET_CACHE.lock().unwrap();
  cache.clear();
  debug!("Cleared domain secret cache");
}

pub fn value_to_credentials(value: Value) -> AppResult<Gcredentials> {
  serde_json::from_value(value).cwl("Failed to create Gcredentials struct")
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Tokens {
  pub access_token: String,
  pub expires_in: u32,
  pub token_type: String,
}

impl Tokens {
  pub async fn access_token(&self) -> &str {
    &self.access_token
  }
}

#[derive(Serialize, Default, Deserialize, Debug, Clone)]
pub struct Gcredentials {
  #[serde(rename = "type")]
  t: String,
  pub project_id: String,
  pub private_key_id: String,
  pub private_key: String,
  pub client_email: String,
  pub client_id: String,
  pub auth_uri: String,
  pub token_uri: String,
  pub auth_provider_x509_cert_url: String,
  pub client_x509_cert_url: String,
}

impl Gcredentials {
  pub async fn rsa_key(&self) -> AppResult<RsaKeyPair> {
    let key_str = self
      .private_key
      .replace("-----BEGIN PRIVATE KEY-----", "")
      .replace("-----END PRIVATE KEY-----", "")
      .replace('\n', "");

    let key_bytes = base64::engine::general_purpose::STANDARD
      .decode(key_str)
      .cwl("Failed to decode base64 private key string")?;

    RsaKeyPair::from_pkcs8(&key_bytes)
      .cwl("Failed to parse PKCS8 private key data")
  }

  pub async fn iss(&self) -> AppResult<String> {
    if !self.client_email.contains('@') || !self.client_email.contains('.') {
      error!(email = %self.client_email, "Invalid service account email format detected (missing '@' or '.')");
      bail!(
        "Invalid email format (missing '@' or '.'): {}",
        self.client_email
      );
    }

    if !self.client_email.ends_with(".gserviceaccount.com") {
      error!(email = %self.client_email, "Email does not appear to be a service account.");
      bail!(
        "Email does not appear to be a Google service account (expected *.gserviceaccount.com): {}",
        self.client_email
      );
    }

    Ok(self.client_email.clone())
  }

  pub async fn token_uri(&self) -> AppResult<String> {
    reqwest::Url::parse(&self.token_uri)
      .cwl(&format!("Invalid token URI format: {}", self.token_uri))?;

    if !self.token_uri.starts_with("https://oauth2.googleapis.com/") {
      error!(uri = %self.token_uri, "Unexpected token URI format.");
      bail!(
        "Unexpected token URI format (expected 'https://oauth2.googleapis.com/...'): {}",
        self.token_uri
      );
    }
    Ok(self.token_uri.clone())
  }
}

pub async fn formb(body: &str) -> Vec<(&str, &str)> {
  vec![
    ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
    ("assertion", body),
  ]
}

pub async fn jwts(
  creds: &Gcredentials,
  usr: &str,
  sub: &str,
) -> AppResult<String> {
  let dat1 = Utc::now();
  let d1 = dat1.timestamp();
  let d2 = d1 + (50 * 60);

  let header = json!({
      "alg": "RS256",
      "typ": "JWT"
  });

  let claim = if sub == "yes" {
    let issuer = creds
      .iss()
      .await
      .cwl("Failed to get issuer for admin scope")?;
    let scopes = scps::A.join(" ");
    let audience = creds
      .token_uri()
      .await
      .cwl("Failed to get token URI for admin scope")?;
    // debug!(
    //   "JWT claim details - issuer: {}, subject: {}, scopes: {}, audience: {}",
    //   issuer, usr, scopes, audience
    // );
    json!({
      "iss": issuer,
      "sub": Some(usr.to_owned()),
      "scope": scopes,
      "aud": audience,
      "iat": d1,
      "exp": d2,
    })
  } else if sub == "no" {
    json!( {
      "iss": creds.iss().await.cwl("Failed to get issuer for chat scope")?,
      "scope": scps::B.join(" "),
      "aud": creds.token_uri().await.cwl("Failed to get token URI for chat scope")?,
      "iat": d1,
      "exp": d2,
    })
  } else {
    json!( {
      "iss": creds.iss().await.cwl("Failed to get issuer for default scope")?,
      "scope": scps::C.join(" "),
      "aud": creds.token_uri().await.cwl("Failed to get token URI for default scope")?,
      "iat": d1,
      "exp": d2,
    })
  };

  debug!("JWT claim validation successful");

  let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
  let claims_b64 = URL_SAFE_NO_PAD.encode(claim.to_string().as_bytes());
  let signing_input = format!("{header_b64}.{claims_b64}");

  let key_pair = creds
    .rsa_key()
    .await
    .cwl("Failed to get RSA key from credentials")?;
  let mut signature = vec![0; key_pair.public().modulus_len()];
  let rng = SystemRandom::new();
  key_pair
    .sign(
      &RSA_PKCS1_SHA256,
      &rng,
      signing_input.as_bytes(),
      &mut signature,
    )
    .cwl("Failed to sign JWT")?;

  let signature_b64 = URL_SAFE_NO_PAD.encode(&signature);

  Ok(format!("{header_b64}.{claims_b64}.{signature_b64}"))
}

pub async fn token(value: Value, usr: String, sub: &str) -> AppResult<String> {
  debug!(user = %usr, subject = sub, "Attempting to generate OAuth2 token.");

  // Create cache key
  let cache_key = format!("{}:{}", usr, sub);

  // Check token cache first
  {
    let cache = TOKEN_CACHE.lock().unwrap();
    if let Some(cached_token) = cache.get(&cache_key) {
      let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
      if now < cached_token.expires_at {
        debug!("Returning cached OAuth2 token for user: {}", usr);
        return Ok(cached_token.token.clone());
      } else {
        debug!("Cached token expired for user: {}", usr);
      }
    }
  }

  debug!("Generating new OAuth2 token for user: {}", usr);

  // Apply OAuth rate limiting before making HTTP request
  get_global_oauth_limiter().until_ready().await;

  // value here is the local json file this is local debugging
  let credentials = value_to_credentials(value)
    .cwl("Failed to convert input value to credentials")?;

  let jwt = jwts(&credentials, &usr, sub)
    .await
    .cwl("Failed to generate JWT")?;

  // debug!("Generated JWT: {}", jwt);

  // Decode and show JWT parts for debugging
  let parts: Vec<&str> = jwt.split('.').collect();
  if parts.len() == 3 {
    if let Ok(header_bytes) = URL_SAFE_NO_PAD.decode(parts[0])
      && let Ok(_header_json) = String::from_utf8(header_bytes)
    {
      // debug!("JWT Header: {}", _header_json);
    }
    if let Ok(payload_bytes) = URL_SAFE_NO_PAD.decode(parts[1])
      && let Ok(_payload_json) = String::from_utf8(payload_bytes)
    {
      // debug!("JWT Payload: {}", _payload_json);
    }
  }

  let req = formb(&jwt).await;
  // debug!("OAuth2 request payload: {:?}", req);

  let res = CL
    .post("https://oauth2.googleapis.com/token")
    .form(&req)
    .send()
    .await
    .cwl("POST request to Google OAuth2 token endpoint failed")?;

  debug!("HTTP Response status: {}", res.status());
  debug!("HTTP Response headers: {:?}", res.headers());

  match res.status().as_u16() {
    200 => {
      let fin = res
        .json::<Tokens>()
        .await
        .cwl("Failed to parse JSON response from token endpoint")?;
      let token = fin.access_token().await;
      debug!("Successfully obtained OAuth2 token.");

      let bearer_token = format!("Bearer {token}");

      // Cache the token with expiry (expires_in is in seconds, subtract 60s for safety)
      let expires_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + fin.expires_in as u64
        - 60;

      {
        let mut cache = TOKEN_CACHE.lock().unwrap();
        cache.insert(
          cache_key,
          CachedToken {
            token: bearer_token.clone(),
            expires_at,
          },
        );
        debug!(
          "Cached OAuth2 token for user: {} (expires at: {})",
          usr, expires_at
        );
      }

      Ok(bearer_token)
    }
    status => {
      let error_text = res
        .text()
        .await
        .cwl("Failed to read error response body from token endpoint")?;

      error!("This is error_text in token {:#?}", error_text);
      error!(
        "OAuth2 token endpoint returned non-200 status ({}) - Body: {}",
        status, error_text
      );

      bail!("Token endpoint error");
    }
  }
}

pub async fn get_tok(usr: String, sub: &str) -> AppResult<String> {
  debug!("=== GET_TOK CALLED ===");
  debug!("usr: '{}', sub: '{}'", usr, sub);
  debug!("=== CALLING get_secret_json from get_tok ===");
  debug!("About to call get_secret_json with usr: '{}'", usr);
  let val = get_secret_json(usr.clone())
    .await
    .cwl(&format!("Failed to get secret JSON for user {usr}"))?;
  debug!("=== get_secret_json RETURNED from get_tok ===");
  info!("Finished getting token for secret access here");
  let ts = token(val.clone(), usr.clone(), sub)
    .await
    .cwl("Failed to generate token for user")?;
  Ok(ts)
}

// ---------------- ORDER OF FUNCTIONS AND EXPLAIN ---------------------

/*

 1. the get_tok calls get_secrets_json
 2. the get_secrets_json then sees if there is a jsonf for a domain in the database
 3. if not it calls store_replace_secret
 4. store_replace_secret calls create_empty_secret
 5. store_replace_secret then calls delete_secrets
 6. delete_secrets calls destroy_secret_version
 7. then store_replace_secret calls add_secret_json

For this to work the get_secrets_json function calls get_token_for_secrets
which calls get_cloud_token to get the project token, or if local gets the json file

*/

// ---------------- END OF ORDER OF FUNCTIONS AND EXPLAIN ---------------------

// NOTE this is exactly the same as getting a local json, just in the cloud
pub async fn get_secret_json(usr: String) -> AppResult<Value> {
  debug!("🔍 GET_SECRET_JSON: === CALLED with usr: {} ===", usr);

  let usr1 = usr.clone();
  // NOTE  when getting a secret it must always correspond to the domain without periods

  // Check if the user string contains an @ symbol (is an email)
  if !usr1.contains('@') {
    error!(
      user_string = %usr1,
      "Invalid user format: Expected email address but got domain name only. Please ensure the database contains a valid email address like 'someone@{}'",
      usr1
    );

    // Automatically call frshtsadmin to update the admin database
    warn!("Attempting to fix admin database by calling frshtsadmin...");
    match crate::sheets::frshtsadmin(None).await {
      Ok(_) => {
        info!("Successfully called frshtsadmin to update admin database");
        bail!(
          "Admin database has been updated. Please retry the command. The issue was: Expected format 'someone@{}' but got '{}'. The database should now contain the correct email address.",
          usr1,
          usr1
        );
      }
      Err(e) => {
        error!(error = ?e, "Failed to call frshtsadmin to update admin database");
        bail!(
          "No email found! Please add email. Expected format: 'someone@{}' but got '{}'. Check the database record and ensure it contains a valid email address. Also failed to automatically update admin database: {}",
          usr1,
          usr1,
          e
        );
      }
    }
  }

  let filename = usr1
    .split('@')
    .nth(1)
    .unwrap_or_default()
    .replace(['.', ' '], "")
    .to_string();

  // Subdomain mapping for multi-domain organizations - minimal intervention
  let filename = if let Some(mapped) =
    crate::surrealstart::map_subdomain_to_jsonf(
      usr1.split('@').nth(1).unwrap_or_default(),
    ) {
    debug!(
      "🔍 GET_SECRET_JSON: Subdomain mapping detected, using mapped filename: '{}'",
      mapped
    );
    mapped
  } else {
    filename
  };

  // Check cache first using the filename as key
  {
    let cache = DOMAIN_SECRET_CACHE.lock().unwrap();
    if let Some(cached_secret) = cache.get(&filename) {
      debug!(
        "🔍 GET_SECRET_JSON: Returning cached secret for domain: {}",
        filename
      );
      return Ok(cached_secret.clone());
    }
  }

  debug!(
    "🔍 GET_SECRET_JSON: Loading secret from Google Secret Manager for domain: {}",
    filename
  );
  debug!("🔍 GET_SECRET_JSON: About to call get_token_for_secrets()");
  let ep = Ep::Secrets;
  let tse = get_token_for_secrets()
    .await
    .cwl(&format!("Failed to get secrets token for {usr}"))?;
  debug!("🔍 GET_SECRET_JSON: Successfully got token for admin for secrets");

  debug!(
    "🔍 GET_SECRET_JSON: extracted filename '{}' from user '{}'",
    filename, usr
  );
  debug!("🔍 GET_SECRET_JSON: === DEBUGGING JSON FILE UPLOAD ===");
  debug!(
    "🔍 GET_SECRET_JSON: Expected secret name in Google Secrets Manager: '{}'",
    filename
  );
  debug!(
    "🔍 GET_SECRET_JSON: Expected JSON file in local folder if not already uploaded and deleted: '{}.json'",
    filename
  );

  // Add debugging for the local file search
  debug!("🔍 GET_SECRET_JSON: === DEBUGGING LOCAL FILE SEARCH ===");
  debug!(
    "🔍 GET_SECRET_JSON: About to search for local JSON file with filename: '{}'",
    filename
  );

  //------------------------- BEGINNING OF EXTRA PART ------------------------------

  // First, always check if the secret exists in Google Secrets Manager
  // If it exists, skip database operations and go directly to cloud retrieval
  // If it doesn't exist, proceed with database check and upload logic

  debug!(
    filename = %filename,
    "Checking if secret exists in Google Secrets Manager"
  );

  debug!("=== ABOUT TO CHECK GOOGLE SECRETS MANAGER ===");

  let url = format!(
    "{}{}/secrets/{}/versions/latest:access",
    ep.base_url(),
    &*PID,
    filename
  );

  let au_build = req_build("GET", &url, Some(&tse), None, None)
    .cwl("Could not create the auth_builder for checking secret existence")?;

  let check_res = au_build
    .send()
    .await
    .cwl("Failed to send request to check secret")?;

  debug!("=== GOOGLE SECRETS MANAGER RESPONSE ===");
  debug!("Status: {}", check_res.status());
  debug!("Status code: {}", check_res.status().as_u16());

  let secret_exists_in_cloud = check_res.status().as_u16() == 200;

  debug!("=== SECRET EXISTS CHECK RESULT ===");
  debug!(
    "secret_exists_in_cloud: {} for {filename}",
    secret_exists_in_cloud
  );

  if secret_exists_in_cloud {
    debug!(filename = %filename, "Secret for found in Google Secrets Manager, proceeding directly to cloud retrieval");

    // Even if secret exists in cloud, we should clean up any local JSON files
    debug!(
      "Checking for local JSON files to clean up since secret already exists in cloud"
    );
    let folder_path = match env::consts::OS {
      "macos" => "/Users/jasonjurotich/Documents/GCLOUD/",
      _ => "/home/jason_jurotich/adminbotbinaryv2/JSONTESTS",
    };

    if let Ok(json_files) = find_json_files(folder_path).await {
      for file_path in json_files {
        if let Some(stem) = file_path.file_stem().and_then(|s| s.to_str())
          && stem == filename
        {
          debug!("Found matching local JSON file to delete: {:?}", file_path);
          match handle_git_operations(file_path.clone(), filename.clone()).await
          {
            Ok(_) => {
              info!("Successfully cleaned up local JSON file: {:?}", file_path);
            }
            Err(e) => {
              warn!(
                "Failed to clean up local JSON file {:?}: {:?}",
                file_path, e
              );
            }
          }
          break;
        }
      }
    }

  // =================================== HERE WE START UPLOADING LOCAL JSON IF THERE WAS ONE ===========
  } else {
    debug!(
      filename = %filename,
      status = check_res.status().as_u16(),
      "Secret not found in Google Secrets Manager, checking database and upload logic"
    );
    debug!("=== ENTERING ELSE BRANCH - SECRET NOT IN CLOUD ===");

    // Only check database and do upload logic if secret doesn't exist in cloud
    // Skip database check for main domain (local development)
    debug!("=== CHECKING IF FILENAME IS MAIN DOMAIN ===");
    debug!("filename: '{}', FIL: '{}'", filename, &*FIL);
    debug!("filename == *FIL: {}", filename == *FIL);

    if filename == *FIL {
      debug!(
        filename = %filename,
        main_fil = %*FIL,
        "Main domain and secret not in cloud - this shouldn't happen in normal operation"
      );
    } else {
      let sql = "select value jsonf from type::table($table) where jsonf = $filename limit 1";

      debug!(sql = %sql, filename = %filename, table = %ep.table_sheet(), "Executing initial database query");

      let result = DB
        .query(sql)
        .bind(("table", ep.table_sheet()))
        .bind(("filename", filename.clone()))
        .await
        .cwl("Failed to execute database query")?
        .take::<Option<String>>(0)
        .cwl("Failed to extract query result")?
        .unwrap_or_default();

      debug!(result = %result, result_empty = result.is_empty(), "Initial query result");

      let final_result = if !result.is_empty() {
        result
      } else {
        warn!(
          "No credentials found in the secrets database for {}, attempting to add credentials",
          filename
        );

        debug!("=== ABOUT TO CALL store_replace_secret ===");
        debug!(
          "Looking for JSON file: '{}.json' in GCLOUD folder",
          filename
        );
        // Attempt to upload json file for new school
        match store_replace_secret(tse.clone()).await {
          Ok(_) => {
            debug!("Secret stored successfully, retrying database query");

            // Retry query after successful secret storage
            debug!(sql = %sql, filename = %filename, table = %ep.table_sheet(), "Retrying database query after store_replace_secret");
            match DB
              .query(sql)
              .bind(("table", ep.table_sheet()))
              .bind(("filename", filename.clone()))
              .await
            {
              Ok(mut res) => {
                debug!(
                  "Query executed successfully, attempting to take results"
                );

                match res.take::<Option<String>>(0) {
                  Ok(Some(taken)) => {
                    debug!(result = ?taken, "Successfully retrieved results from retry");
                    taken
                  }
                  Ok(None) => {
                    info!("No results found in retry query");
                    String::new()
                  }
                  Err(e) => {
                    error!(error = ?e, "Error taking results from query");
                    return Err(e).cwl("Failed to extract query results");
                  }
                }
              }
              Err(e) => {
                error!(error = ?e, "Error executing query");
                return Err(e).cwl("Failed to execute database query");
              }
            }
          }
          Err(e) => {
            error!(error = ?e, "Failed to store/replace secret");
            String::new()
          }
        }
      };

      // Check if we have results after retrying
      if final_result.is_empty() {
        error!(
          domain = %filename,
          "No JSON file found for domain"
        );
        bail!(
          "Someone tried to use the admin bot from the domain {}, check and see if the json file was created and added to the folder for that school, \
        or if the json file itself is corrupted or does not have text inside of it.",
          filename
        );
      }
    }
  }

  //------------------------- END OF EXTRA PART TO UPLOAD LOCAL JASON TO SECRETS ----------------------

  // Now proceed with cloud retrieval for all cases
  debug!(filename = %filename, "Proceeding with Google Secrets Manager retrieval");

  // NOTE  for this to work the service account must be given access to the cloud secret under its permissions. This was not necessary in the other code because in the other one, the same service account created and then accessed the secret

  let au_build = req_build("GET", &url, Some(&tse), None, None)
    .cwl("Could not create the auth_builder for secret retrieval")?;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request for secret retrieval")?;

  match res.status().as_u16() {
    200 => {
      debug!(secret_name = %filename, "Successfully accessed secret. Parsing payload.");

      let rfin: Value = res.json().await.cwl(&format!(
        "Failed to parse JSON response from Secrets Manager for secret: {filename}"
      ))?;

      match rfin.get("payload").and_then(|p| p.get("data")) {
        Some(encoded_data) => {
          if let Some(payload) = encoded_data.as_str() {
            let decoded_data = STANDARD
              .decode(payload)
              .cwl("Failed to decode base64 payload")?;

            // NOTE here is the json info, similar to the local json file
            let json_data: Value =
              serde_json::from_slice(&decoded_data).cwl(&format!(
                "Failed to parse decoded secret payload as JSON for secret: {filename}"
              ))?;

            debug!(secret_name = %filename, "Successfully decoded and parsed secret JSON.");

            // Cache the secret for future use
            {
              let mut cache = DOMAIN_SECRET_CACHE.lock().unwrap();
              cache.insert(filename.clone(), json_data.clone());
              debug!(
                "🔍 GET_SECRET_JSON: Cached secret for domain: {}",
                filename
              );
            }

            Ok(json_data)
          } else {
            error!(secret_name = %filename, "Secret payload data field was not a string.");
            bail!(
              "Secret payload 'data' field was not a string for secret: {}",
              filename
            )
          }
        }
        None => {
          error!(secret_name = %filename, response = ?rfin, "'payload.data' field not found in secret response.");
          bail!(
            "'payload.data' field not found in secret response for secret: {}",
            filename
          )
        }
      }
    }
    status => {
      let error_text = res.text().await
        .cwl(&format!("Failed to read error response body from Secrets Manager for secret: {filename}"))?;

      error!(
        "Secrets Manager API returned non-200 status ({}) for secret '{}': {}",
        status, filename, error_text
      );
      bail!("Failed to retrieve secret: HTTP {}", status)
    }
  }
}

pub async fn store_replace_secret(tse: String) -> AppResult<()> {
  debug!("=== STORE_REPLACE_SECRET CALLED ===");
  let ep = Ep::Secrets;
  let folder_path = match env::consts::OS {
    "macos" => "/Users/jasonjurotich/Documents/GCLOUD/",
    _ => "/home/jason_jurotich/adminbotbinaryv2/JSONTESTS",
  };
  debug!(
    "🔍 STEP 1: store_replace_secret folder_path = {}",
    folder_path
  );

  debug!("🔍 STEP 2: About to call find_json_files");
  let json_files = find_json_files(folder_path)
    .await
    .cwl("Failed to search for JSON files")?;

  debug!(
    "🔍 STEP 3: find_json_files returned {} files: {:?}",
    json_files.len(),
    json_files
  );

  for file in &json_files {
    if let Some(name) = file.file_stem().and_then(|s| s.to_str()) {
      debug!(
        "🔍 STEP 3a: Found JSON file: '{}' -> will create secret named: '{}'",
        file.display(),
        name
      );
    }
  }

  if json_files.is_empty() {
    warn!(
      "🚨 STEP 4: No JSON files found in local folder '{}'. This means no secrets will be stored in database.",
      folder_path
    );
    debug!(
      "🚨 STEP 4a: store_replace_secret returning early due to no files - this means the database won't be updated"
    );
    return Ok(());
  } else {
    debug!("🔍 STEP 4: Found {} JSON files to upload", json_files.len());
  }

  // Track files that need to be deleted due to being empty or invalid
  let mut files_to_delete = Vec::new();
  debug!(
    "🔍 STEP 5: Starting to process {} JSON files",
    json_files.len()
  );

  for (file_index, file_path) in json_files.iter().enumerate() {
    debug!(
      "🔍 STEP 6.{}: Processing file {} of {}: {:?}",
      file_index,
      file_index + 1,
      json_files.len(),
      file_path
    );

    let Some(stem) = file_path.file_stem() else {
      debug!(
        "🚨 STEP 6.{}.1: Skipping file with no stem: {:?}",
        file_index, file_path
      );
      continue;
    };
    let Some(file_name) = stem.to_str() else {
      debug!(
        "🚨 STEP 6.{}.2: Skipping file with invalid UTF-8 stem: {:?}",
        file_index, file_path
      );
      continue;
    };
    let file_name = file_name.to_string();
    debug!(
      "🔍 STEP 6.{}.3: Extracted file_name: '{}'",
      file_index, file_name
    );

    let url = format!("{}{}/secrets/{}", ep.base_url(), &*PID, file_name);
    debug!(
      "🔍 STEP 6.{}.4: Checking if secret exists at URL: {}",
      file_index, url
    );

    let au_build = req_build("GET", &url, Some(&tse), None, None)
      .cwl("Could not create the auth_builder for the lists function")?;

    let res = au_build
      .send()
      .await
      .cwl("Failed to send request for the lists function")?;

    debug!(
      "🔍 STEP 6.{}.5: Received status: {}",
      file_index,
      res.status().as_u16()
    );

    match res.status().as_u16() {
      403 => {
        debug!("🚨 STEP 6.{}.6a: Got 403 error", file_index);
        let error_text = res
          .text()
          .await
          .cwl("Failed to read error response body from secrets manager API")?;

        error!(
          "Google secrets manager API returned 403 Body: {}",
          error_text
        );

        bail!("Google secrets manager API returned 403 ")
      }
      404 => {
        debug!(
          "🔍 STEP 6.{}.6b: Secret doesn't exist (404), creating empty secret for {}...",
          file_index, file_name
        );
        create_empty_secret_container(tse.clone(), file_name.clone())
          .await
          .cwl("Could not run create_empty_secret 404")?;
        debug!(
          "🔍 STEP 6.{}.6b.1: Sleeping for 2 seconds after creating secret container",
          file_index
        );
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        debug!(
          "🔍 STEP 6.{}.6b.2: Finished sleeping after creating secret container",
          file_index
        );
      }
      200 => {
        debug!(
          "🔍 STEP 6.{}.6c: Secret exists (200), deleting previous secret versions for {}...",
          file_index, file_name
        );
        delete_secrets(tse.clone(), file_name.clone())
          .await
          .cwl(&format!(
            "Failed to delete existing secrets for {file_name}"
          ))?;
        debug!(
          "🔍 STEP 6.{}.6c.1: Sleeping for 2 seconds after deleting secrets",
          file_index
        );
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        debug!(
          "🔍 STEP 6.{}.6c.2: Finished sleeping after deleting secrets",
          file_index
        );
      }
      status => {
        debug!("🚨 STEP 6.{}.6d: Unexpected status: {}", file_index, status);
        let error_text = res
          .text()
          .await
          .cwl("Failed to access secrets manager api")?;

        error!(
          "Google Sheets API returned non-200 status: {} - Body: {}",
          status, error_text
        );
        bail!("API error")
      }
    }

    debug!("🔍 STEP 7.{}: About to read file content", file_index);

    debug!(
      "🔍 STEP 7.{}.1: Reading file at path: {:#?}",
      file_index, file_path
    );

    let pload = fs::read(&file_path).await.cwl("Could not read file")?;

    debug!(
      "🔍 STEP 7.{}.2: Read {} bytes from file",
      file_index,
      pload.len()
    );

    // Check if file is empty
    if pload.is_empty() {
      error!(
        "🚨 STEP 7.{}.4a: JSON file is empty, marking for deletion: {}",
        file_index,
        file_path.display()
      );
      files_to_delete.push(file_path.clone());
      continue;
    }

    debug!(
      "🔍 STEP 7.{}.4b: File is not empty, validating JSON",
      file_index
    );
    // Validate JSON content
    match serde_json::from_slice::<Value>(&pload) {
      Ok(_) => {
        debug!(
          "🔍 STEP 7.{}.5a: Successfully validated JSON content for {}",
          file_index,
          file_path.display()
        );
      }
      Err(e) => {
        error!(
          "🚨 STEP 7.{}.5b: Invalid JSON content in file {}, error: {}, marking for deletion",
          file_index,
          file_path.display(),
          e
        );
        files_to_delete.push(file_path.clone());
        continue;
      }
    };

    debug!("🔍 STEP 8.{}: About to call add_secret_json", file_index);
    add_secret_json(tse.clone(), file_name.clone(), pload, file_path.clone())
      .await
      .cwl("Could not add secret")?;
    debug!(
      "🔍 STEP 8.{}.1: add_secret_json completed successfully",
      file_index
    );

    debug!("🔍 STEP 9.{}: About to update database", file_index);
    let sql = "
          let $existing = (
            select id from type::table($table)
            where jsonf = $filename limit 1
          );
          if $existing {
            update $existing[0].id
            set
              jsonf = $filename,
              secret_id = $secret_id;
          } else {
            create type::thing($table, rand::uuid::v7())
            set
              jsonf = $filename,
              secret_id = $secret_id;
          };
        ";

    debug!(
      "🔍 STEP 9.{}.1: SQL update using filename: {} and table: {}",
      file_index,
      file_name,
      ep.table_sheet()
    );

    match DB
      .query(sql)
      .bind(("table", ep.table_sheet()))
      .bind(("filename", file_name.clone()))
      .bind(("secret_id", file_name.clone()))
      .await
    {
      Ok(_) => {
        info!(
          "🔍 STEP 9.{}.2a: Successfully processed and stored secret for file_name: {}",
          file_index, file_name
        );
      }
      Err(e) => {
        error!(
          "🚨 STEP 9.{}.2b: Could not update database for file_name: {}, error: {:?}",
          file_index, file_name, e
        );
      }
    }
    debug!(
      "🔍 STEP 10.{}: Finished processing file {}",
      file_index, file_name
    );
  }

  debug!(
    "🔍 STEP 11: Finished processing all {} files",
    json_files.len()
  );

  // Delete any files that were empty or invalid
  if !files_to_delete.is_empty() {
    info!(
      "🔍 STEP 12: Cleaning up {} invalid or empty JSON files",
      files_to_delete.len()
    );

    for (delete_index, file_path) in files_to_delete.iter().enumerate() {
      debug!(
        "🔍 STEP 12.{}: Attempting to delete file: {}",
        delete_index,
        file_path.display()
      );
      match tokio::fs::remove_file(&file_path).await {
        Ok(_) => {
          info!(
            "🔍 STEP 12.{}.1a: Successfully deleted invalid/empty file: {}",
            delete_index,
            file_path.display()
          );
        }
        Err(e) => {
          error!(
            "🚨 STEP 12.{}.1b: Failed to delete invalid/empty file: {}, error: {:?}",
            delete_index,
            file_path.display(),
            e
          );
        }
      }
    }
  } else {
    debug!("🔍 STEP 12: No files marked for deletion");
  }

  debug!("🔍 STEP 13: store_replace_secret function completed successfully");
  Ok(())
}

pub async fn create_empty_secret_container(
  tse: String,
  filename: String,
) -> AppResult<()> {
  let ep = Ep::Secrets;
  let url = format!("{}{}/secrets", ep.base_url(), &*PID);

  let qry = json!({
    "secretId": filename
  });

  let bdy = json!({
    "replication": {
      "automatic": {}
    },
  });

  let au_build = req_build("POST", &url, Some(&tse), Some(&qry), Some(&bdy))
    .cwl("Could not create the auth_builder")?;

  let res = au_build.send().await.cwl("Failed to send request")?;

  match res.status().as_u16() {
    200 => {}
    status => {
      let error_text = res
        .text()
        .await
        .cwl("Failed to access secrets manager api")?;

      error!(
        "Google Secrets manager API returned non-200 status: {} - Body: {}",
        status, error_text
      );
      bail!("API error")
    }
  }

  info!("Empty secret contain made for {}", filename);
  Ok(())
}

pub async fn delete_secrets(tse: String, sid: String) -> AppResult<()> {
  let ep = Ep::Secrets;
  let list_versions_url =
    format!("{}{}/secrets/{}/versions", ep.base_url(), &*PID, sid);

  debug!(
    "This is list_versions_url in delete_secrets {:#?}",
    list_versions_url
  );

  let au_build = req_build("GET", &list_versions_url, Some(&tse), None, None)
    .cwl("Could not create the auth_builder for delete_secrets")?;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request for the lists function")?;

  debug!("This is res  in delete_secrets {:#?}", res);

  match res.status().as_u16() {
    200 => {
      let rfin: Value = res
        .json()
        .await
        .cwl("Failed to parse JSON response from delete_secrets")?;

      if let Some(versions) = rfin.get("versions").and_then(|v| v.as_array()) {
        for version_num in versions
          .iter()
          .filter_map(|ver| ver.get("name").and_then(serde_json::Value::as_str))
          .filter_map(|name| name.rsplit('/').next())
        {
          if let Err(e) =
            destroy_secret_version(tse.clone(), sid.as_str(), version_num).await
          {
            warn!("Failed to destroy version {}: {:?}", version_num, e);
          }
        }
      }
    }
    404 => {
      warn!(
        "Secret {} doesn't exist yet, skipping version deletion",
        sid
      );
    }

    status => {
      let error_text = res.text().await.cwl(
        "Failed to read error response body from Google secrets manager API",
      )?;

      error!(
        "Google Sheets API returned non-200 status: {} - Body: {}",
        status, error_text
      );
      bail!("API error")
    }
  }

  Ok(())
}

pub async fn add_secret_json(
  tsn: String,
  filename: String,
  pload: Vec<u8>,
  path: PathBuf,
) -> AppResult<()> {
  debug!(
    "🔍 ADD_SECRET_JSON: Starting for filename: {}, path: {:?}",
    filename, path
  );
  let ep = Ep::Secrets;

  let add_version_url =
    format!("{}{}/secrets/{}:addVersion", ep.base_url(), &*PID, filename);

  debug!("🔍 ADD_SECRET_JSON: URL: {}", add_version_url);

  let qry = serde_json::json!({
      "payload": {
          "data": STANDARD.encode(pload)
      }
  });

  debug!(
    "🔍 ADD_SECRET_JSON: Payload size: {} bytes (base64 encoded)",
    qry["payload"]["data"].as_str().unwrap_or("").len()
  );

  let au_build =
    req_build("POST", &add_version_url, Some(&tsn), None, Some(&qry))
      .cwl("Could not create the auth_builder")?;

  debug!("🔍 ADD_SECRET_JSON: About to send request to Google Secrets Manager");
  let res = au_build.send().await.cwl("Failed to send request")?;

  debug!(
    "🔍 ADD_SECRET_JSON: Received response with status: {}",
    res.status().as_u16()
  );

  match res.status().as_u16() {
    200 => {
      debug!(
        "🔍 ADD_SECRET_JSON: Success (200), about to call handle_git_operations"
      );
      handle_git_operations(path, filename)
        .await
        .cwl("Could not handle git")?;
      debug!(
        "🔍 ADD_SECRET_JSON: handle_git_operations completed successfully"
      );
    }

    status => {
      debug!("🚨 ADD_SECRET_JSON: Non-200 status: {}", status);
      let error_text = res.text().await.cwl("Failed to read error response")?;

      error!(
        "Google secrets manager API returned non-200 status: {} - Body: {}",
        status, error_text
      );
      bail!("API error")
    }
  }

  debug!("🔍 ADD_SECRET_JSON: Function completed successfully");
  Ok(())
}

async fn destroy_secret_version(
  tsn: String,
  sid: &str,
  version: &str,
) -> AppResult<()> {
  let ep = Ep::Secrets;
  let url = format!(
    "{}{}/secrets/{}/versions/{}:destroy",
    ep.base_url(),
    &*PID,
    sid,
    version
  );
  debug!(url = %url, "Attempting to destroy secret version");

  let empty_body = json!({});

  let au_build = req_build("POST", &url, Some(&tsn), None, Some(&empty_body))
    .cwl("Could not create the auth_builder")?;

  let res = au_build.send().await.cwl("Failed to send request")?;

  match res.status().as_u16() {
    200 => {
      debug!(
        version = %version,
        secret_id = %sid,
        "Successfully destroyed secret version"
      );
      Ok(())
    }
    404 => {
      warn!(version = %version, "Version not found");
      bail!("Version {} not found", version)
    }
    411 => {
      error!(
        version = %version,
        secret_id = %sid,
        "Failed to destroy secret version"
      );
      bail!(
        "Version {} of secret {} could not be destroyed",
        version,
        sid
      )
    }
    403 => {
      error!(version = %version, "Permission denied to destroy version");
      bail!("Permission denied to destroy version {}", version)
    }
    status => {
      let error_text = res.text().await.cwl("Failed to read error response")?;
      error!(
        status = %status,
        error = %error_text,
        "Unexpected status code when destroying secret version"
      );
      bail!("Failed to destroy secret version: HTTP {}", status)
    }
  }
}

pub async fn find_json_files(folder: &str) -> AppResult<Vec<PathBuf>> {
  let mut files = Vec::new();
  let mut dir = fs::read_dir(folder)
    .await
    .cwl(&format!("Failed to read directory: {folder}"))?;

  while let Some(entry) = dir
    .next_entry()
    .await
    .cwl(&format!("Failed to read directory entry in: {folder}"))?
  {
    if entry.path().extension().and_then(|ext| ext.to_str()) == Some("json") {
      debug!(path = ?entry.path(), "Found JSON file");
      files.push(entry.path());
    }
  }

  if files.is_empty() {
    warn!(folder = %folder, "No JSON files found in directory");
  } else {
    debug!(count = files.len(), folder = %folder, "Found JSON files");
  }

  debug!("These are files in find_jons_files {:#?}", files);
  Ok(files)
}

pub async fn handle_git_operations(
  path: PathBuf,
  filename: String,
) -> AppResult<()> {
  debug!(
    "🔍 HANDLE_GIT_OPS: Starting for filename: {}, path: {:?}",
    filename, path
  );

  // Validate the file path is within allowed directories and is a JSON file
  debug!(
    "🔍 HANDLE_GIT_OPS: Attempting to canonicalize path: {:?}",
    path
  );
  let canonical_path = path.canonicalize().map_err(|e| {
    error!(
      "🚨 HANDLE_GIT_OPS: Failed to canonicalize path: {:?}, error: {:?}",
      path, e
    );
    anyhow::anyhow!("Invalid file path provided: {:?}", e)
  })?;

  debug!("🔍 HANDLE_GIT_OPS: Canonical path: {:?}", canonical_path);

  if canonical_path.extension().is_none_or(|ext| ext != "json") {
    error!(
      "🚨 HANDLE_GIT_OPS: File extension is not .json: {:?}",
      canonical_path
    );
    return Err(anyhow::anyhow!("Only JSON files are allowed for deletion"));
  }

  debug!("🔍 HANDLE_GIT_OPS: File extension validation passed");

  // Define allowed base directories
  let allowed_dirs = [
    "/Users/jasonjurotich/Documents/GCLOUD/", // Updated to match the actual folder path used
    "/Users/jasonjurotich/Documents/RUSTDEV/JSONSECRETSAPI",
    "/home/jason_jurotich/adminbotbinaryv2/JSONTESTS",
  ];

  debug!(
    "🔍 HANDLE_GIT_OPS: Checking if path is in allowed directories: {:?}",
    allowed_dirs
  );

  let is_allowed = allowed_dirs.iter().any(|&allowed| {
    debug!(
      "🔍 HANDLE_GIT_OPS: Checking against allowed dir: {}",
      allowed
    );
    if let Ok(allowed_canonical) = PathBuf::from(allowed).canonicalize() {
      let starts_with = canonical_path.starts_with(&allowed_canonical);
      debug!(
        "🔍 HANDLE_GIT_OPS: Canonical allowed dir: {:?}, starts_with: {}",
        allowed_canonical, starts_with
      );
      starts_with
    } else {
      debug!(
        "🚨 HANDLE_GIT_OPS: Could not canonicalize allowed dir: {}",
        allowed
      );
      false
    }
  });

  debug!("🔍 HANDLE_GIT_OPS: is_allowed = {}", is_allowed);

  if !is_allowed {
    error!(
      "🚨 HANDLE_GIT_OPS: File deletion not allowed outside of designated directories. Path: {:?}",
      canonical_path
    );
    return Err(anyhow::anyhow!(
      "File deletion not allowed outside of designated directories"
    ));
  }

  debug!(
    "🔍 HANDLE_GIT_OPS: Path validation passed, attempting to delete the local file: {:?}",
    path
  );

  match tokio::fs::remove_file(&path).await {
    Ok(_) => {
      debug!(
        "🔍 HANDLE_GIT_OPS: Successfully deleted local file: {:?}",
        path
      );
    }
    Err(e) => {
      error!(
        "🚨 HANDLE_GIT_OPS: Failed to delete file: {:?}, error: {:?}",
        path, e
      );
      return Err(e).cwl(&format!("Failed to delete file: {}", path.display()));
    }
  }

  let is_server = std::env::consts::OS == "linux"
    && PathBuf::from("/home/jason_jurotich/adminbotbinaryv2/JSONTESTS")
      .exists();

  if is_server {
    let git_dir =
      PathBuf::from("/home/jason_jurotich/adminbotbinaryv2/JSONTESTS");

    async fn run_git_command(
      dir: &PathBuf,
      args: &[&str],
      error_msg: &str,
    ) -> AppResult<()> {
      let output = tokio::process::Command::new("git")
        .current_dir(dir)
        .args(args)
        .output()
        .await
        .cwl(error_msg)?;

      if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        error!(
          command = ?args,
          error = %stderr,
          "Git command failed"
        );
        bail!("{}: {}", error_msg, stderr)
      }
      Ok(())
    }

    if git_dir.exists() {
      debug!("Fetching latest from GitHub");
      run_git_command(&git_dir, &["fetch"], "Failed to fetch updates")
        .await
        .cwl("Git fetch operation failed")?;

      let path_str = path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Invalid file path: {:?}", path))
        .cwl("Failed to convert path to string")?;

      debug!("Removing file from Git tracking");
      run_git_command(
        &git_dir,
        &["rm", "-f", path_str],
        "Failed to stage file deletion",
      )
      .await
      .cwl("Failed to remove file from Git tracking")?;

      debug!("Committing deletion");
      run_git_command(
        &git_dir,
        &["commit", "-m", &format!("Remove JSON file: {filename}")],
        "Failed to commit changes",
      )
      .await
      .cwl("Failed to commit file deletion")?;

      debug!("Pushing changes to GitHub");
      run_git_command(&git_dir, &["push"], "Failed to push changes")
        .await
        .cwl("Failed to push changes to remote")?;

      debug!(
        file = %filename,
        "Successfully completed all Git operations"
      );
    }
  }

  Ok(())
}
