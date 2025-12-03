// ============================================================================
// TOKEN NAMING CONVENTION FOR THIS PROJECT
// ============================================================================
// This project uses TWO types of tokens:
//
// 1. TSNS (Token Subject No, Sheets):
//    - For accessing Google Sheets directly
//    - Service Account: adminbotv3@adminbot-prod.iam.gserviceaccount.com
//    - No subject (no domain-wide delegation)
//    - No impersonation (adminbotv3 accesses sheets with its own identity)
//    - Cloud: Token from VM metadata
//    - Local: Token from JSON key file
//
// 2. TSNI (Token Subject No, Impersonate):
//    - For sending Google Chat messages
//    - Source SA: adminbotv3@adminbot-prod.iam.gserviceaccount.com
//    - Target SA: adminbot@adminbot-iedu-prod.iam.gserviceaccount.com
//    - No subject (no domain-wide delegation)
//    - WITH impersonation (adminbotv3 impersonates adminbot)
//    - Cloud: adminbotv3 (from metadata) impersonates adminbot
//    - Local: adminbotv3 (from JSON) impersonates adminbot
// ============================================================================

use crate::AppResult;
use crate::tracer::ContextExt;
use crate::{debug, error, info};
use crate::surrealstart::{CL, SDIR};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

// For local JSON key support
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::Utc;
use ring::rand::SystemRandom;
use ring::signature::{RSA_PKCS1_SHA256, RsaKeyPair};

// Scopes for different token types
#[allow(non_snake_case, non_upper_case_globals)]
pub mod scps {
  // Scopes for chat (includes multiple scopes like original)
  pub const C: [&str; 4] = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/chat.bot",
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/spreadsheets",
  ];
}

/// Local JSON key structure for adminbotv3@adminbot-prod.iam.gserviceaccount.com
#[derive(Debug, Serialize, Deserialize, Clone)]
struct LocalServiceAccountKey {
  #[serde(rename = "type")]
  key_type: String,
  pub project_id: String,
  pub private_key_id: String,
  pub private_key: String,
  pub client_email: String,
  pub client_id: String,
  pub token_uri: String,
}

/// IAM Credentials API response structure
#[derive(Debug, Serialize, Deserialize)]
struct IamAccessTokenResponse {
  #[serde(rename = "accessToken")]
  access_token: String,
  #[serde(rename = "expireTime")]
  expire_time: String,
}

/// Detects if running in Google Cloud environment by checking metadata service
pub async fn is_cloud_environment() -> bool {
  debug!("Checking if running in cloud environment");

  match CL
    .get("http://metadata.google.internal")
    .header("Metadata-Flavor", "Google")
    .timeout(std::time::Duration::from_secs(2))
    .send()
    .await
  {
    Ok(_) => {
      info!("Running in Google Cloud environment");
      true
    }
    Err(_) => {
      info!("Running in local environment");
      false
    }
  }
}

/// Loads the adminbotv3 JSON key from local file for testing
async fn load_adminbot_json_key() -> AppResult<LocalServiceAccountKey> {
  // Use SDIR (secrets directory) + hardcoded filename for adminbotv3
  let secrets_dir = &*SDIR;
  let filename = "adminbot-prodiamgserviceaccountcom.json";
  let json_path = format!("{}/{}", secrets_dir, filename);

  debug!("Loading adminbot JSON key from: {}", json_path);

  let content = tokio::fs::read_to_string(&json_path)
    .await
    .cwl(&format!("Failed to read adminbot JSON key file: {}", json_path))?;

  let key: LocalServiceAccountKey = serde_json::from_str(&content)
    .cwl("Failed to parse adminbot JSON key file")?;

  Ok(key)
}

/// Signs a JWT using the local JSON key (for local development)
async fn sign_jwt_with_local_key(key: &LocalServiceAccountKey, scopes: &[&str]) -> AppResult<String> {
  let now = Utc::now().timestamp();
  let expiry = now + (50 * 60); // 50 minutes

  let header = json!({
    "alg": "RS256",
    "typ": "JWT"
  });

  let claim = json!({
    "iss": key.client_email,
    "scope": scopes.join(" "),
    "aud": &key.token_uri,
    "iat": now,
    "exp": expiry,
  });

  // Encode header and claims
  let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
  let claims_b64 = URL_SAFE_NO_PAD.encode(claim.to_string().as_bytes());
  let signing_input = format!("{}.{}", header_b64, claims_b64);

  // Parse the private key
  let key_str = key.private_key
    .replace("-----BEGIN PRIVATE KEY-----", "")
    .replace("-----END PRIVATE KEY-----", "")
    .replace('\n', "");

  let key_bytes = base64::engine::general_purpose::STANDARD
    .decode(key_str)
    .cwl("Failed to decode base64 private key")?;

  let key_pair = RsaKeyPair::from_pkcs8(&key_bytes)
    .cwl("Failed to parse PKCS8 private key")?;

  // Sign the JWT
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

  Ok(format!("{}.{}.{}", header_b64, claims_b64, signature_b64))
}

/// Gets source token from VM metadata service (for cloud environment)
async fn get_source_token() -> AppResult<String> {
  debug!("Getting source token from VM metadata service");

  let url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

  let res = CL
    .get(url)
    .header("Metadata-Flavor", "Google")
    .send()
    .await
    .cwl("Failed to get source token from metadata service")?;

  let status = res.status();
  if status.is_success() {
    let rfin: Value = res.json().await.cwl("Failed to parse JSON response")?;

    let token_str = rfin["access_token"]
      .as_str()
      .ok_or_else(|| {
        anyhow::anyhow!("Access token not found in metadata response")
      })
      .cwl("Access token not found in metadata response")?;

    debug!("Successfully obtained source token from metadata service");
    Ok(token_str.to_string())
  } else {
    let error_text = res.text().await.cwl("Failed to read error response body")?;
    error!(status = %status, error = %error_text, "Failed to get source token");
    anyhow::bail!("Source token request failed")
  }
}

/// Gets source token from local JSON key (for local development)
async fn get_source_token_from_json() -> AppResult<String> {
  debug!("Getting source token from local JSON key file");

  // Load the adminbot JSON key
  let key = load_adminbot_json_key().await?;

  // IAM Credentials API scope needed to impersonate other SAs
  let scopes = ["https://www.googleapis.com/auth/cloud-platform"];

  // Sign JWT
  let jwt = sign_jwt_with_local_key(&key, &scopes).await?;

  // Exchange JWT for access token
  let form = vec![
    ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
    ("assertion", &jwt),
  ];

  let res = CL
    .post(&key.token_uri)
    .form(&form)
    .send()
    .await
    .cwl("Failed to exchange JWT for token")?;

  if !res.status().is_success() {
    let error_text = res.text().await.unwrap_or_default();
    anyhow::bail!("Failed to get token from JSON key: {}", error_text);
  }

  let token_response: Value = res.json().await.cwl("Failed to parse token response")?;

  let access_token = token_response["access_token"]
    .as_str()
    .ok_or_else(|| anyhow::anyhow!("No access_token in response"))?;

  debug!("Successfully obtained source token from JSON key");
  Ok(access_token.to_string())
}

/// Impersonates a service account using the IAM Credentials API
/// This is used for TSNI (chat token with impersonation)
///
/// # Arguments
/// * `target_sa` - The service account email to impersonate
/// * `scopes` - The scopes to request for the impersonated token
///
/// # Returns
/// A Bearer token for the target service account with the requested scopes
pub async fn impersonate_service_account(
  target_sa: &str,
  scopes: &[&str],
) -> AppResult<String> {
  debug!("Impersonating service account: {} with {} scopes", target_sa, scopes.len());

  // Get source token - use VM metadata in cloud, JSON key locally
  let source_token = if is_cloud_environment().await {
    debug!("Using VM metadata for source token (cloud)");
    get_source_token().await.cwl("Failed to get source token from VM metadata")?
  } else {
    debug!("Using JSON key file for source token (local)");
    get_source_token_from_json().await.cwl("Failed to get source token from JSON key")?
  };

  debug!("Source token obtained (length: {}, prefix: {}...)",
    source_token.len(),
    &source_token[..50.min(source_token.len())]
  );

  // Call IAM Credentials API
  let url = format!(
    "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{}:generateAccessToken",
    target_sa
  );

  let body = json!({
    "scope": scopes,
  });

  debug!("Calling IAM Credentials API at: {}", url);
  debug!("Requesting scopes: {:?}", scopes);

  let res = CL
    .post(&url)
    .bearer_auth(&source_token)
    .json(&body)
    .send()
    .await
    .cwl("Failed to call IAM Credentials API")?;

  debug!("IAM Credentials API response status: {}", res.status());

  match res.status().as_u16() {
    200 => {
      let iam_response: IamAccessTokenResponse = res
        .json()
        .await
        .cwl("Failed to parse IAM Credentials API response")?;

      let bearer_token = format!("Bearer {}", iam_response.access_token);

      info!("Successfully impersonated service account: {}", target_sa);
      Ok(bearer_token)
    }
    status => {
      let error_text = res
        .text()
        .await
        .cwl("Failed to read error response body from IAM Credentials API")?;

      error!("IAM Credentials API error: {:#?}", error_text);
      error!(
        "IAM Credentials API returned status {} for target SA '{}': {}",
        status, target_sa, error_text
      );

      anyhow::bail!("IAM Credentials API error: {}", status)
    }
  }
}

// ============================================================================
// MAIN TOKEN FUNCTIONS
// ============================================================================

/// Gets TSNS token (Token Subject No, Sheets)
/// adminbotv3@adminbot-prod.iam.gserviceaccount.com accesses Google Sheets directly
///
/// # How it works:
/// - **Production**: Gets token from VM metadata for adminbotv3 with sheets scopes
/// - **Local**: Gets token from JSON key for adminbotv3 with sheets scopes
/// - No subject, no impersonation - adminbotv3 uses its own identity
///
/// # Returns
/// Bearer token for Google Sheets API
pub async fn get_tsns() -> AppResult<String> {
  debug!("Getting TSNS token for Google Sheets access");

  if is_cloud_environment().await {
    debug!("Getting token from metadata (cloud)");
    // In cloud, get token from metadata with sheets scopes
    let url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

    let res = CL
      .get(url)
      .header("Metadata-Flavor", "Google")
      .send()
      .await
      .cwl("Failed to get token from metadata service")?;

    let status = res.status();
    if status.is_success() {
      let rfin: Value = res.json().await.cwl("Failed to parse JSON response")?;
      let token_str = rfin["access_token"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Access token not found in metadata response"))
        .cwl("Access token not found in metadata response")?;

      info!("Successfully obtained TSNS token from metadata for sheets");
      Ok(format!("Bearer {}", token_str))
    } else {
      let error_text = res.text().await.cwl("Failed to read error response body")?;
      error!(status = %status, error = %error_text, "Failed to get token from metadata");
      anyhow::bail!("Metadata token request failed with status {}: {}", status, error_text)
    }
  } else {
    debug!("Getting token from JSON key (local)");
    // In local, get token from JSON key with sheets scopes
    let key = load_adminbot_json_key().await?;

    // Scopes for sheets access
    let scopes = [
      "https://www.googleapis.com/auth/drive",
      "https://www.googleapis.com/auth/spreadsheets",
    ];

    let jwt = sign_jwt_with_local_key(&key, &scopes).await?;

    let form = vec![
      ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
      ("assertion", &jwt),
    ];

    let res = CL
      .post(&key.token_uri)
      .form(&form)
      .send()
      .await
      .cwl("Failed to exchange JWT for token")?;

    if !res.status().is_success() {
      let error_text = res.text().await.unwrap_or_default();
      anyhow::bail!("Failed to get token from JSON key: {}", error_text);
    }

    let token_response: Value = res.json().await.cwl("Failed to parse token response")?;
    let access_token = token_response["access_token"]
      .as_str()
      .ok_or_else(|| anyhow::anyhow!("No access_token in response"))?;

    info!("Successfully obtained TSNS token from JSON key for sheets");
    Ok(format!("Bearer {}", access_token))
  }
}

/// Gets TSNI token (Token Subject No, Impersonate)
/// adminbotv3@adminbot-prod.iam.gserviceaccount.com impersonates
/// adminbot@adminbot-iedu-prod.iam.gserviceaccount.com for Google Chat
///
/// # How it works:
/// - **Production**: adminbotv3 (from metadata) impersonates adminbot
/// - **Local**: adminbotv3 (from JSON key) impersonates adminbot
/// - No subject (no domain-wide delegation)
/// - WITH impersonation (adminbotv3 acts AS adminbot)
///
/// # Returns
/// Bearer token for Google Chat API with chat.bot scope
/// Main entry point for getting tokens via impersonation (like surrealdbadminbot)
///
/// # Arguments
/// * `usr` - User email (domain determines which SA to impersonate)
/// * `sub` - "no" for chat bot scope
pub async fn get_tok_impersonated(usr: &str, sub: &str) -> AppResult<String> {
  debug!("get_tok_impersonated: usr='{}', sub='{}'", usr, sub);

  // Extract domain from email
  let domain = usr.split('@').nth(1)
    .ok_or_else(|| anyhow::anyhow!("Invalid email: {}", usr))?;

  // Map domain to service account
  let target_sa = match domain {
    "ieducando.com" => "avisosbot@avisosbot.iam.gserviceaccount.com",
    _ => anyhow::bail!("No SA for domain: {}", domain),
  };

  debug!("Domain '{}' → SA '{}'", domain, target_sa);

  // Only support chat bot scope (no domain-wide delegation)
  if sub != "no" {
    anyhow::bail!("Only sub='no' supported");
  }

  let scopes = scps::C.as_slice();

  impersonate_service_account(target_sa, scopes)
    .await
    .cwl(&format!("Failed to impersonate {}", target_sa))
}

pub async fn get_tsni() -> AppResult<String> {
  debug!("Getting TSNI token for Google Chat");
  get_tok_impersonated("jason.jurotich@ieducando.com", "no")
    .await
    .cwl("Failed to get TSNI token")
}
