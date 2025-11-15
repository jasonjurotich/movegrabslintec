pub use super::aux_sur::*;
pub use super::limiters::*;
pub use super::sheets::*;
pub use super::surrealstart::*;
use crate::validate_required_fields;

use crate::AppResult;
use crate::apis::Ep;
use crate::aux_mods::*;
use crate::check_key;
use crate::error_utils::{get_status_code_name, parse_google_api_error};
use crate::extract_record_parts;
use crate::tracer::ContextExt;
use crate::{bail, debug, error, warn};
// use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::Body;
use serde_json::{Value, json};
use std::env;
use std::path::Path;
use std::sync::Arc;
use tokio::fs as tfs;
use tokio::fs::File as Fio;
use tokio::io::AsyncWriteExt;
use tokio_util::codec::{BytesCodec, FramedRead};

use governor::RateLimiter;
use governor::clock::DefaultClock;
use governor::state::InMemoryState;
use governor::state::NotKeyed;

#[allow(dead_code)]
pub async fn get_resource_etag(
  endpoint: &Ep,
  resource_id: &str,
  token: &str,
) -> AppResult<String> {
  let url = format!("{}{}", endpoint.base_url(), resource_id);

  debug!("Getting ETag for resource: {}", url);

  let get_build = req_build("GET", &url, Some(token), None, None)
    .cwl("Could not create GET request for resource ETag")?;

  let res = get_build
    .send()
    .await
    .cwl("Failed to GET resource for ETag")?;

  if res.status().as_u16() >= 200 && res.status().as_u16() < 300 {
    if let Some(etag) = res.headers().get("etag") {
      if let Ok(etag_str) = etag.to_str() {
        debug!("Retrieved ETag for {}: {}", resource_id, etag_str);
        Ok(etag_str.to_string())
      } else {
        bail!("Could not parse ETag header for resource: {}", resource_id);
      }
    } else {
      bail!("No ETag header found for resource: {}", resource_id);
    }
  } else {
    bail!(
      "Failed to GET resource {} for ETag: status {}",
      resource_id,
      res.status()
    );
  }
}

#[allow(dead_code)]
pub async fn retry_delete_with_etag(
  url: &str,
  endpoint: &Ep,
  resource_id: &str,
  token: &str,
) -> AppResult<reqwest::Response> {
  debug!("Retrying DELETE with ETag for URL: {}", url);

  for attempt in 1..=3 {
    debug!("ETag retry attempt {} for {}", attempt, url);

    let etag = get_resource_etag(endpoint, resource_id, token)
      .await
      .cwl("Failed to get ETag for retry DELETE")?;

    let mut retry_build = req_build("DELETE", url, Some(token), None, None)
      .cwl("Could not create retry DELETE request with ETag")?;

    let clean_etag = etag.trim_matches('"');
    debug!(
      "Adding If-Match header with membership ETag: {}",
      clean_etag
    );
    retry_build = retry_build.header("If-Match", clean_etag);

    get_global_delete_limiter().until_ready().await;

    let retry_res = retry_build
      .send()
      .await
      .cwl("Failed to send retry DELETE request with ETag")?;

    if retry_res.status().as_u16() != 412 {
      return Ok(retry_res);
    }

    if let Ok(error_text) = retry_res.text().await {
      debug!(
        "Got 412 on attempt {}: Status: 412, Details: {}",
        attempt, error_text
      );
    } else {
      debug!(
        "Got 412 on attempt {} (could not read error details)",
        attempt
      );
    }
    debug!("Retrying with fresh ETag...");
  }

  bail!("Failed to DELETE after 3 ETag retry attempts due to race conditions")
}

fn ex_str_val(rfin: &Value, path: &[&str]) -> Value {
  let mut current = rfin;
  for &key in path {
    current = if let Ok(index) = key.parse::<usize>() {
      // Handle array index
      match current.as_array() {
        Some(arr) => arr.get(index).unwrap_or(&Value::Null),
        None => return Value::String(String::new()),
      }
    } else {
      // Handle object key
      match current.get(key) {
        Some(val) => val,
        None => return Value::String(String::new()),
      }
    };
  }
  match current {
    Value::String(s) => Value::String(s.clone()),
    Value::Array(_) => Value::String(String::new()),
    _ => Value::String(current.as_str().unwrap_or_default().to_string()),
  }
}

fn ex_bool_val(rfin: &Value, path: &[&str]) -> Value {
  let mut current = rfin;
  for &key in path {
    current = match current.get(key) {
      Some(val) => val,
      None => return Value::Bool(false),
    };
  }

  // Handle different value types including Google Sheets string representations
  let boolean_result = match current {
    // Handle direct boolean values
    Value::Bool(b) => *b,
    // Handle string representations (including Google Sheets variations)
    Value::String(s) => {
      match s.to_lowercase().trim() {
        "true" | "TRUE" | "1" | "yes" | "YES" | "verdadero" | "VERDADERO" => {
          true
        }
        "false" | "FALSE" | "0" | "no" | "NO" | "falso" | "FALSO" => false,
        _ => false, // Default to false for invalid strings
      }
    }
    // Handle numbers (0 = false, anything else = true)
    Value::Number(n) => {
      if let Some(i) = n.as_i64() {
        i != 0
      } else if let Some(f) = n.as_f64() {
        f != 0.0
      } else {
        false
      }
    }
    // Default to false for other types
    _ => false,
  };

  Value::Bool(boolean_result)
}

// fn ex_arr_val(rfin: &Value, path: &[&str]) -> Value {
//   let mut current = rfin;
//   for &key in path {
//     current = match current.get(key) {
//       Some(val) => val,
//       None => return Value::Array(Vec::new()),
//     };
//   }
//   Value::Array(current.as_array().cloned().unwrap_or_default())
// }

fn ex_arr_as_str_val(rfin: &Value, path: &[&str]) -> Value {
  let mut current = rfin;
  for &key in path {
    current = match current.get(key) {
      Some(val) => val,
      None => return Value::String(String::new()),
    };
  }
  match current {
    Value::Array(arr) => {
      let string_items: Vec<String> = arr
        .iter()
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect();
      Value::String(string_items.join(", "))
    }
    _ => Value::String(String::new()),
  }
}

fn ex_arr_emails(rfin: &Value, path: &[&str]) -> Value {
  let mut current = rfin;
  for &key in path {
    current = match current.get(key) {
      Some(val) => val,
      None => return Value::String(String::new()),
    };
  }
  match current {
    Value::Array(arr) => {
      let emails: Vec<String> = arr
        .iter()
        .filter_map(|v| v.get("email"))
        .filter_map(|e| e.as_str())
        .map(|s| s.to_string())
        .collect();
      Value::String(emails.join(", "))
    }
    _ => Value::String(String::new()),
  }
}

fn ex_arr_file_urls(rfin: &Value, path: &[&str]) -> Value {
  let mut current = rfin;
  for &key in path {
    current = match current.get(key) {
      Some(val) => val,
      None => return Value::String(String::new()),
    };
  }
  match current {
    Value::Array(arr) => {
      let urls: Vec<String> = arr
        .iter()
        .filter_map(|v| v.get("fileUrl"))
        .filter_map(|u| u.as_str())
        .map(|s| s.to_string())
        .collect();
      Value::String(urls.join(", "))
    }
    _ => Value::String(String::new()),
  }
}

pub async fn change_one_sheet_in_all(record: Value) -> AppResult<()> {
  let ep = Ep::Sheets;
  let template_sheet_id = &*TSHID;

  let tsyusr = get_tok(
    "adminbot@adminbot-iedu-prod.iam.gserviceaccount.com".to_owned(),
    "yes",
  )
  .await
  .cwl("Failed to generate token for change_one_sheet_in_all")?;

  let (id, data) = extract_record_parts(record.clone())?;
  let mut er1 = String::new();

  let destspshid = check_key(&data, "spreadsheet_id", &mut er1)
    .and_then(|v| v.as_str())
    .unwrap_or("")
    .to_string();

  let p = PETS
    .get()
    .await
    .cwl("Could not get pet fields for list_lics")?;

  let args: Vec<&str> = p.params.split(", ").collect();
  let titl = args[0].trim(); // sheet to replace

  let ept = match Ep::strtoenum(titl, false) {
    Some(ep) => ep,
    None => bail!(
      "Invalid sheet title '{}' - no matching endpoint found",
      titl
    ),
  };

  let (tempshid, tempindex) = get_sheet_id_index(
    tsyusr.clone(),
    ept.clone(),
    template_sheet_id.to_string(),
  )
  .await
  .cwl("Failed to get sheet ID from template spreadsheet")?
  .ok_or_else(|| {
    anyhow::anyhow!("Sheet '{}' not found in template spreadsheet", titl)
  })?;

  debug!(
    "Using tempshid={} and tempindex={} from template spreadsheet for copy operation",
    tempshid, tempindex
  );

  if let Some((destshid, _)) =
    get_sheet_id_index(p.tsy.clone(), ept.clone(), destspshid.clone())
      .await
      .cwl("Failed to get sheet ID for checkbox operation")?
  {
    delete_sheet(p.tsy.clone(), &destspshid, destshid)
      .await
      .cwl("Could not delete sheet or sheet does not exist in the destination spreadsheet")?;
  }

  let url = format!(
    "{}{}/sheets/{}:copyTo",
    ep.base_url(),
    template_sheet_id, // hardcoded template spreadsheet ID
    tempshid           // this is the sheetid from template document
  );

  debug!("COPY OPERATION DEBUG:",);
  debug!("  url = {}", url);
  debug!("  source_spreadsheet_id = {}", template_sheet_id);
  debug!("  source_sheet_id = {}", tempshid);
  debug!("  dest_spreadsheet_id = {}", destspshid);
  debug!("  sheet_name = {}", titl);

  let mut er1 = String::new();

  // NOTE sheetid for us is actually the spreadsheetid, careful
  let body = json!({
    "destinationSpreadsheetId": destspshid,
  });

  debug!("Request body: {:#?}", body);

  if !er1.is_empty() {
    warn!(errors = %er1, "Found errors in change_one_sheet_in_all");
  } else {
    debug!("All required keys present in change_one_sheet_in_all");
  }

  let au_build = req_build(
    "POST",
    &url,
    Some(&tsyusr),
    None,        // Pass all current queries here
    Some(&body), // No JSON body for a GET "lists" operation
  )
  .cwl("Could not create the auth_builder for change_one_sheet_in_all")?;

  debug!("About to call global sheets limiter...");
  get_global_sheets_limiter().until_ready().await;
  debug!("Global sheets limiter released, sending request...");

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request for change_one_sheet_in_all")?;

  debug!("Received response from copy operation");
  debug!("Response status: {}", res.status());
  debug!("This is res in change_one_sheet_in_all {:#?}", res);

  match res.status().as_u16() {
    200 => {
      debug!("SUCCESS: Copy operation returned 200");
      let rfin: Value = res
        .json()
        .await
        .cwl("Failed to parse JSON response from Sheets API")?;

      debug!("Copy response JSON: {:#?}", rfin);

      update_db_good(id.clone()).await.cwl(
        "Failed to update database with successful change_one_sheet_in_all result",
      )?;

      debug!("Database updated with good status");

      // Extract the new sheet ID from the response
      if let Some(new_sheet_id) = rfin.get("sheetId").and_then(|v| v.as_i64()) {
        debug!(
          "Extracted new sheet ID: {}, about to rename to '{}' at index {}",
          new_sheet_id,
          titl.to_uppercase(),
          tempindex
        );
        rename_sheet_and_index(
          p.tsy,
          &destspshid,
          new_sheet_id as i32,
          &titl.to_uppercase(),
          tempindex,
        )
        .await
        .cwl("Could not rename new sheet")?;
        debug!("Successfully renamed sheet to '{}'", titl.to_uppercase());
      } else {
        error!("No sheetId found in API response, skipping rename operation");
      }
      debug!("change_one_sheet_in_all completed successfully");
    }
    status => {
      debug!("ERROR: Copy operation returned status {}", status);
      let error_text = res.text().await.cwl(
        "Failed to read error response body from Google Chrome Comercial API in change_one_sheet_in_all"
      )?;

      debug!("Error response body: {}", error_text);

      // Add API error to er1
      if !er1.is_empty() {
        er1.push('\n');
      }
      er1.push_str(&format!(
        "Status code: {status}\nError details: {error_text}"
      ));

      update_db_bad(er1, id.clone()).await.cwl(
        "Failed to update database with change_one_sheet_in_all error result",
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

#[allow(dead_code)]
pub async fn get_perms_file(
  fid: String,
  p: &Pets,
) -> AppResult<Vec<Vec<String>>> {
  let mut cors: Vec<Vec<String>> = Vec::new();
  let ep = Ep::Files;

  let url = format!("{}{fid}/permissions", ep.base_url());

  let qry = json!({
    "fields": "*"
  });

  let au_build = req_build("GET", &url, Some(&p.tsy.clone()), Some(&qry), None)
    .cwl("Could not create request builder in get_perms_file")?;

  get_global_drive_limiter().until_ready().await;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request in get_perms_file")?;

  match res.status().as_u16() {
    200..=299 => {
      let rfin: Value =
        res.json().await.cwl("Failed to parse JSON response")?;

      let empty_vec = Vec::new();

      let perms = rfin
        .get("permissions")
        .and_then(|c| c.as_array())
        .unwrap_or(&empty_vec);

      for perm in perms.iter() {
        let pif = perm
          .get("id")
          .and_then(|id| id.as_str())
          .map(|s| s.to_string());

        let eml = perm
          .get("emailAddress")
          .and_then(|eml| eml.as_str())
          .map(|s| s.to_string());

        let role = perm
          .get("role")
          .and_then(|role| role.as_str())
          .map(|s| s.to_string());

        let perm_type = perm
          .get("type")
          .and_then(|ptype| ptype.as_str())
          .map(|s| s.to_string());

        if let (Some(perm_id), Some(email), Some(role_val), Some(type_val)) =
          (pif, eml, role, perm_type)
        {
          cors.push(vec![perm_id, email, role_val, type_val]);
        }
      }
      Ok(cors)
    }
    status => {
      let error_text = res
        .text()
        .await
        .cwl("Failed to read error response body in get_perms_file")?;

      let clean_error_details = parse_google_api_error(&error_text);
      bail!(
        "File permissions retrieval failed - Status: {} {} - {}",
        status,
        get_status_code_name(status),
        clean_error_details
      )
    }
  }
}

pub async fn get_perm_id(
  fid: String,
  usr: String,
  tse: String,
  role: String,
) -> AppResult<String> {
  debug!("GET_PERM_ID DEBUG: fid={}, usr={}, role={}", fid, usr, role);

  let ep = Ep::Files;

  let url = format!("{}{fid}/permissions", ep.base_url());

  let qry = json!({
    "fields":"permissions(id,emailAddress,role)",
  });

  debug!("GET_PERM_ID DEBUG: Making request to URL: {}", url);
  debug!("GET_PERM_ID DEBUG: Query params: {:#?}", qry);

  let au_build = req_build("GET", &url, Some(&tse.clone()), Some(&qry), None)
    .cwl("Could not create request builder in get_perm_id")?;

  get_global_drive_limiter().until_ready().await;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request in get_perm_id")?;

  debug!("GET_PERM_ID DEBUG: Response status: {}", res.status());

  match res.status().as_u16() {
    200..=299 => {
      let rfin: Value =
        res.json().await.cwl("Failed to parse JSON response")?;

      debug!("GET_PERM_ID DEBUG: Response body: {:#?}", rfin);

      // Look for the user in the permissions list
      if let Some(permissions) =
        rfin.get("permissions").and_then(|p| p.as_array())
      {
        debug!("GET_PERM_ID DEBUG: Found {} permissions", permissions.len());
        for permission in permissions {
          if let (Some(email), Some(perm_role), Some(id)) = (
            permission.get("emailAddress").and_then(|e| e.as_str()),
            permission.get("role").and_then(|r| r.as_str()),
            permission.get("id").and_then(|i| i.as_str()),
          ) && email.eq_ignore_ascii_case(&usr)
            && perm_role == role
          {
            debug!(
              "GET_PERM_ID DEBUG: Found matching permission - email={}, role={}, id={}",
              email, perm_role, id
            );

            return Ok(id.to_string());
          } else {
            debug!(
              "GET_PERM_ID DEBUG: Permission doesn't match - email={:?}, role={:?}, looking for user={}, role={}",
              permission.get("emailAddress"),
              permission.get("role"),
              usr,
              role
            );
          }
        }
      } else {
        debug!("GET_PERM_ID DEBUG: No permissions array found in response");
      }

      bail!(
        "User {} does not have {} permissions for file {}",
        usr,
        role,
        fid
      )
    }
    status => {
      let error_text = res
        .text()
        .await
        .cwl("Failed to read error response body in get_perm_id")?;

      let clean_error_details = parse_google_api_error(&error_text);
      bail!(
        "Getting permissions failed - Status: {} {} - {}",
        status,
        get_status_code_name(status),
        clean_error_details
      )
    }
  }
}

pub async fn get_perm_driveid(fid: String, tse: String) -> AppResult<String> {
  debug!("GET_PERM_ID DEBUG: fid={}", fid);

  let ep = Ep::Files;

  let url = format!("{}{fid}/permissions", ep.base_url());

  let qry = json!({
    "supportsAllDrives":"true",
    "useDomainAdminAccess": "true",
    "fields": "*"
  });

  debug!("GET_PERM_ID DEBUG: Making request to URL: {}", url);
  debug!("GET_PERM_ID DEBUG: Query params: {:#?}", qry);

  let au_build = req_build("GET", &url, Some(&tse.clone()), Some(&qry), None)
    .cwl("Could not create request builder in get_perm_driveid")?;

  get_global_drive_limiter().until_ready().await;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request in get_perm_driveid")?;

  debug!("GET_PERM_ID DEBUG: Response status: {}", res.status());

  match res.status().as_u16() {
    200..=299 => {
      let rfin: Value =
        res.json().await.cwl("Failed to parse JSON response")?;

      debug!("GET_PERM_ID DEBUG: Response body: {:#?}", rfin);

      // Look for the user in the permissions list
      if let Some(permissions) =
        rfin.get("permissions").and_then(|p| p.as_array())
      {
        debug!("GET_PERM_ID DEBUG: Found {} permissions", permissions.len());
        for permission in permissions {
          if let (Some(email), Some(perm_role), Some(id)) = (
            permission.get("emailAddress").and_then(|e| e.as_str()),
            permission.get("role").and_then(|r| r.as_str()),
            permission.get("id").and_then(|i| i.as_str()),
          ) {
            debug!(
              "GET_PERM_ID DEBUG: Found permission - email={}, role={}, id={}",
              email, perm_role, id
            );

            if perm_role == "organizer" {
              debug!("GET_PERM_ID DEBUG: Found organizer - email={}", email);
              return Ok(email.to_string());
            } else {
              debug!(
                "GET_PERM_ID DEBUG: Skipping non-organizer - email={}, role={}",
                email, perm_role
              );
            }
          } else {
            debug!(
              "GET_PERM_ID DEBUG: Permission doesn't match - email={:?}, role={:?}",
              permission.get("emailAddress"),
              permission.get("role"),
            );
          }
        }
      } else {
        debug!("GET_PERM_ID DEBUG: No permissions array found in response");
      }

      bail!("No organizer found for shared drive {}", fid)
    }
    status => {
      let error_text = res
        .text()
        .await
        .cwl("Failed to read error response body in get_perm_driveid")?;

      let clean_error_details = parse_google_api_error(&error_text);
      bail!(
        "Getting permissions failed - Status: {} {} - {}",
        status,
        get_status_code_name(status),
        clean_error_details
      )
    }
  }
}

pub async fn add_perm_id(
  fid: String,
  usr: String,
  tse: String,
  role: String,
) -> AppResult<String> {
  let ep = Ep::Files;

  let url = format!("{}{fid}/permissions", ep.base_url());

  let body = json!({
    "type": "user",
    "role": role,
    "emailAddress": usr
  });

  let qry = json!({
    "fields":"id",
  });

  let au_build =
    req_build("POST", &url, Some(&tse.clone()), Some(&qry), Some(&body))
      .cwl("Could not create request builder in get_perm_id")?;

  get_global_drive_limiter().until_ready().await;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request in get_perm_id")?;

  match res.status().as_u16() {
    200..=299 => {
      let rfin: Value =
        res.json().await.cwl("Failed to parse JSON response")?;

      let perm_id = rfin
        .get("id")
        .and_then(|id| id.as_str())
        .ok_or_else(|| anyhow::anyhow!("Permission ID not found in response"))?
        .to_string();

      Ok(perm_id)
    }
    status => {
      let error_text = res
        .text()
        .await
        .cwl("Failed to read error response body in get_perm_id")?;

      let clean_error_details = parse_google_api_error(&error_text);
      bail!(
        "Adding user as writer failed - Status: {} {} - {}",
        status,
        get_status_code_name(status),
        clean_error_details
      )
    }
  }
}

pub async fn fix_body_usrs(record: Value, p: &Pets) -> AppResult<Value> {
  let (_id, data) = extract_record_parts(record.clone())?;
  let mut er1 = String::new();

  let nombres_raw = check_key(&data, "nombres", &mut er1)
    .and_then(|v| v.as_str())
    .unwrap_or("")
    .to_string();

  let apellidos_raw = check_key(&data, "apellidos", &mut er1)
    .and_then(|v| v.as_str())
    .unwrap_or("")
    .to_string();

  let grp = check_key(&data, "grupo", &mut er1)
    .and_then(|v| v.as_str())
    .unwrap_or("")
    .to_string();

  let rorg = check_key(&data, "org", &mut er1)
    .and_then(|v| v.as_str())
    .unwrap_or("/")
    .to_string();

  let first_names_for_email = process_first_names(&nombres_raw);
  let last_names_for_email = process_last_names(&apellidos_raw);
  debug!("This is last_names_for_email {:#?}", last_names_for_email);

  let first_names_for_display =
    process_first_names_for_display(&nombres_raw, &p.cmd);
  let last_names_for_display = process_last_names_for_display(&apellidos_raw);

  let cleaned_nombres = first_names_for_display.join(" ");
  let cleaned_apellidos = last_names_for_display.join(" ");

  let use_dots = matches!(
    p.abr.as_str(),
    "jeebmex"
      | "lopgon"
      | "cerver"
      | "lsopue"
      | "altvis"
      | "cenccvi"
      | "imexpue"
      | "damic"
      | "coldis"
      | "colalt"
      | "disacad"
      | "samteq"
      | "jsmmic"
      | "iplcoa"
  );

  let (usr, pwd, fixed) = generate_username_password(
    &data,
    &first_names_for_email,
    &last_names_for_email,
    use_dots,
    p,
  );

  if p.cmd == "cuf" && usr.is_empty() {
    bail!(
      "❌ El usuario no fue creado para el comando 'cuf' porque la celda del \
      correo fijo estaba vacía o mal escrita - favor de revisar esa fila."
    );
  }

  debug!("Generated email for user creation: {}", usr);

  let path = if ["cu", "cud", "cuf"].contains(&p.cmd.as_str()) {
    if grp.is_empty() {
      rorg
    } else {
      get_org_from_orgbase(p.abr.clone(), grp.to_owned())
        .await
        .cwl("Could not get org from group in orgbase")?
    }
  } else if p.cmd == "cup" {
    if grp.is_empty() {
      rorg
    } else {
      let pgrp = format!("pf{}", grp);
      get_org_from_orgbase(p.abr.clone(), pgrp.to_owned())
        .await
        .cwl("Could not get org from group in orgbase")?
    }
  } else {
    rorg
  };

  let mut body = json!({
    "primaryEmail": usr,
    "password": pwd,
    "changePasswordAtNextLogin": fixed,
    "orgUnitPath": path,
    "name": {
      "givenName": Value::String(cleaned_nombres),
      "familyName": Value::String(cleaned_apellidos),
    }
  });

  if let Some(name) = data.get("departamento")
    && !name.is_null()
  {
    body["organizations"] = json!([{
      "department": name.clone()
    }]);
  }

  debug!(
    "Generated body for Google API: {}",
    serde_json::to_string_pretty(&body)
      .unwrap_or_else(|_| "Failed to serialize body".to_string())
  );

  Ok(body)
}

pub async fn change_org_cb(
  cb: String,
  org: String,
  limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
  p: &Pets,
) -> AppResult<()> {
  let ep = Ep::Chromebooks;

  let url = format!("{}moveDevicesToOu", ep.base_url());

  let body = json!({
    "deviceIds": cb
  });

  let qry = json!({
    "orgUnitPath": org,
  });

  let au_build =
    req_build("POST", &url, Some(&p.tsy.clone()), Some(&qry), Some(&body))
      .cwl("Could not create request builder in change_org_cb")?;

  limiter.until_ready().await;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request in change_org_cb")?;

  match res.status().as_u16() {
    200..=299 => {
      debug!("Changed chromebook {cb} org");
    }
    status => {
      let error_text = res
        .text()
        .await
        .cwl("Failed to read error response body in change_org_cb")?;

      let clean_error_details = parse_google_api_error(&error_text);
      bail!(
        "change_org_cb failed - Status: {} {} - {}",
        status,
        get_status_code_name(status),
        clean_error_details
      )
    }
  }
  Ok(())
}

pub async fn download_file_from_drive(
  file_id: &str,
  tse: &str,
  output_path: &str,
) -> AppResult<()> {
  let ap3 = "https://www.googleapis.com/drive/v3/files/";
  let url = format!("{}{}?alt=media", ap3, file_id);

  let au_build = req_build("GET", &url, Some(tse), None, None)
    .cwl("Could not create request builder in download_file_from_drive")?;

  let res = au_build
    .send()
    .await
    .cwl("Failed to download file from Google Drive")?;

  let bytes = res.bytes().await.cwl("Failed to get response bytes")?;

  let mut file = Fio::create(output_path)
    .await
    .cwl("Failed to create output file")?;

  AsyncWriteExt::write_all(&mut file, &bytes)
    .await
    .cwl("Failed to write data to file")?;

  Ok(())
}

pub async fn add_row_to_database(
  rfin: Value,
  p: &Pets,
  _group: Option<String>,
) -> AppResult<String> {
  let cmd = p.cmd.as_str();
  debug!("add_row_to_database: Adding new row for cmd: {}", cmd);

  debug!("This is rfin in add_row_to_database {:#?}", rfin);

  if rfin.is_null() {
    bail!("Cannot add null data to database");
  }

  match cmd {
    "cup" => {
      let suspended = match cmd {
        "cogsu" => true,
        "cogusu" => false,
        "cogsua" => true,
        "cogusua" => false,
        "su" => true,
        "usu" => false,
        _ => false,
      };

      let orp = ex_str_val(&rfin, &["orgUnitPath"]);
      debug!("This is orp in add_row_to_database {:#?}", orp);

      let org_str = match &orp {
        Value::String(s) => s.clone(),
        _ => String::new(),
      };

      let grpp = if org_str == "/" || org_str.is_empty() {
        String::new()
      } else {
        get_grp_from_orgbase(p.abr.clone(), org_str)
          .await
          .cwl("Could not get org for group")?
      };

      debug!("This is grpp in add_row_to_database {:#?}", grpp);

      let idioma = {
        let lang_val = ex_str_val(&rfin, &["languages", "0", "languageCode"]);
        if let Value::String(s) = lang_val {
          if s.is_empty() {
            Value::String("es-419".to_string())
          } else {
            Value::String(s)
          }
        } else {
          Value::String("es-419".to_string())
        }
      };

      let record = serde_json::json!({
        "data": {
          "do": "x",
          "id": ex_str_val(&rfin, &["id"]),
          "correo": ex_str_val(&rfin, &["primaryEmail"]),
          "nombres": ex_str_val(&rfin, &["name", "givenName"]),
          "apellidos": ex_str_val(&rfin, &["name", "familyName"]),
          "org": orp,
          "suspendido": suspended,
          "archivado": ex_bool_val(&rfin, &["archived"]),
          "cambiar_pass": ex_bool_val(&rfin, &["changePasswordAtNextLogin"]),
          "grupo": grpp,
          "departamento": ex_str_val(&rfin, &["organizations", "0", "department"]),
          "cost_center": ex_str_val(&rfin, &["organizations", "0", "costCenter"]),
          "es_admin": ex_bool_val(&rfin, &["isAdmin"]),
          "es_sub_admin": ex_bool_val(&rfin, &["isDelegatedAdmin"]),
          "sv2_activo": ex_bool_val(&rfin, &["isEnrolledIn2Sv"]),
          "sv2_obligado": ex_bool_val(&rfin, &["isEnforcedIn2Sv"]),
          "idioma": idioma,
          "fecha_creada": ex_str_val(&rfin, &["creationTime"]),
          "aliases": ex_arr_as_str_val(&rfin, &["aliases"]),
          "modificar_grupo": null
        },
        "abr": p.abr,
        "ers": ""
      });

      let sql = "create type::thing($table, rand::uuid::v7()) content $record";

      let result = DB
        .query(sql)
        .bind(("table", Ep::Users.table_sheet()))
        .bind(("record", record))
        .await
        .cwl("Failed to add new cup user row to database")?;

      debug!("add_row_to_database: Insert result for cup: {:?}", result);

      Ok("success".to_string())
    }

    _ => bail!("add_row_to_database not implemented for command: {}", cmd),
  }
}

pub async fn update_rows_good(
  id: String,
  rfin: Value,
  p: &Pets,
  args: Option<String>,
) -> AppResult<()> {
  let cmd = p.cmd.as_str();
  debug!("update_rows_good: Attempting to update id: {}", id);

  // debug!("This is rfin in update rows good {:#?}", rfin);

  if rfin.is_null() {
    return Ok(());
  }

  let mut bindings: Vec<(&str, Value)> =
    vec![("id_string", Value::String(id.clone()))];

  let qry = match cmd {
    "cu" | "cud" | "cuf" | "cog" | "coga" | "cogc" | "cogal" | "cogalc"
    | "cogsu" | "cogusu" | "cogsua" | "cogusua" | "uu" | "uuo" | "su"
    | "usu" | "au" | "uau" | "giu" | "iden" | "ides" | "idfr" | "idge"
    | "cps" | "cpfs" => {
      let suspended = match cmd {
        "cogsu" => true,
        "cogusu" => false,
        "cogsua" => true,
        "cogusua" => false,
        "su" => true,
        "usu" => false,
        _ => false,
      };

      // Extract and bind in one step for comprehensive user update
      let orp = ex_str_val(&rfin, &["orgUnitPath"]);

      debug!("This is orp in update_rows_good {:#?}", orp);

      let org_str = match &orp {
        Value::String(s) => s.clone(),
        _ => String::new(),
      };

      let grpp = if org_str == "/" || org_str.is_empty() {
        String::new()
      } else {
        get_grp_from_orgbase(p.abr.clone(), org_str)
          .await
          .cwl("Could not get org for group")?
      };

      debug!("This is grpp in update_rows_good {:#?}", grpp);

      bindings.extend([
        ("data_id", ex_str_val(&rfin, &["id"])),
        ("correo", ex_str_val(&rfin, &["primaryEmail"])),
        ("nombres", ex_str_val(&rfin, &["name", "givenName"])),
        ("apellidos", ex_str_val(&rfin, &["name", "familyName"])),
        ("contrasena", ex_str_val(&rfin, &["password"])),
        ("org", orp),
        ("suspendido", Value::Bool(suspended)),
        ("archivado", ex_bool_val(&rfin, &["archived"])),
        (
          "cambiar_pass",
          ex_bool_val(&rfin, &["changePasswordAtNextLogin"]),
        ),
        ("grupo", Value::String(grpp)),
        (
          "departamento",
          ex_str_val(&rfin, &["organizations", "0", "department"]),
        ),
        (
          "cost_center",
          ex_str_val(&rfin, &["organizations", "0", "costCenter"]),
        ),
        ("es_admin", ex_bool_val(&rfin, &["isAdmin"])),
        ("es_sub_admin", ex_bool_val(&rfin, &["isDelegatedAdmin"])),
        ("sv2_activo", ex_bool_val(&rfin, &["isEnrolledIn2Sv"])),
        ("sv2_obligado", ex_bool_val(&rfin, &["isEnforcedIn2Sv"])),
        ("idioma", {
          let lang_val = ex_str_val(&rfin, &["languages", "0", "languageCode"]);
          if let Value::String(s) = lang_val {
            if s.is_empty() {
              Value::String("es-419".to_string())
            } else {
              Value::String(s)
            }
          } else {
            Value::String("es-419".to_string())
          }
        }),
        ("fecha_creada", ex_str_val(&rfin, &["creationTime"])),
        ("aliases", ex_arr_as_str_val(&rfin, &["aliases"])),
      ]);

      "
        update type::record($id_string)
        set 
          data.do = 'x',
          data.id = $data_id,
          data.correo = $correo,
          data.nombres = $nombres,
          data.apellidos = $apellidos,
          data.cambiar_pass = $cambiar_pass,
          data.grupo = $grupo,
          data.org = $org,
          data.suspendido = $suspendido,
          data.archivado = $archivado,
          data.departamento = $departamento,
          data.cost_center = $cost_center,
          data.es_admin = $es_admin,
          data.es_sub_admin = $es_sub_admin,
          data.sv2_activo = $sv2_activo,
          data.sv2_obligado = $sv2_obligado,
          data.idioma = $idioma,
          data.fecha_creada = $fecha_creada,
          data.aliases = $aliases,
          data.modificar_grupo = NONE,
          tse = NONE,
          cmd = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
    }

    "cc" | "ac" | "rac" | "uc" | "oc" | "racl" => {
      // Extract and bind course creation data

      let ownid = ex_str_val(&rfin, &["ownerId"]);
      let eml = get_eml_from_idusr(
        p.abr.clone(),
        match &ownid {
          Value::String(s) => s.clone(),
          _ => String::new(),
        },
      )
      .await
      .cwl("Could not get org for group")?;

      bindings.extend([
        ("data_id", ex_str_val(&rfin, &["id"])),
        ("fecha_creada", ex_str_val(&rfin, &["creationTime"])),
        ("fecha_actualizada", ex_str_val(&rfin, &["updateTime"])),
        ("nombre", ex_str_val(&rfin, &["name"])),
        ("sala", ex_str_val(&rfin, &["room"])),
        ("codigo_enrolar", ex_str_val(&rfin, &["enrollmentCode"])),
        ("seccion", ex_str_val(&rfin, &["section"])),
        ("desc_cabeza", ex_str_val(&rfin, &["descriptionHeading"])),
        ("descripcion", ex_str_val(&rfin, &["description"])),
        ("estado", ex_str_val(&rfin, &["courseState"])),
        ("correo_dueno", Value::String(eml)),
      ]);

      if cmd == "oc" {
        "
        update type::record($id_string)
        set 
          data.do = 'x',
          data.id = $data_id,
          data.nombre = $nombre,
          data.seccion = $seccion,
          data.desc_cabeza = $desc_cabeza,
          data.descripcion = $descripcion,
          data.sala = $sala,
          data.codigo_enrolar = $codigo_enrolar,
          data.estado = $estado,
          data.fecha_creada = $fecha_creada,
          data.fecha_actualizada = $fecha_actualizada,
          data.correo_dueno = $correo_dueno,
          data.correo_dos = NONE,
          tse = NONE,
          cmd = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
      } else {
        "
        update type::record($id_string)
        set 
          data.do = 'x',
          data.id = $data_id,
          data.nombre = $nombre,
          data.seccion = $seccion,
          data.desc_cabeza = $desc_cabeza,
          data.descripcion = $descripcion,
          data.sala = $sala,
          data.codigo_enrolar = $codigo_enrolar,
          data.estado = $estado,
          data.fecha_creada = $fecha_creada,
          data.fecha_actualizada = $fecha_actualizada,
          data.correo_dueno = $correo_dueno,
          tse = NONE,
          cmd = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
      }
    }

    "atu" => {
      // Extract student ID from guardian invitation response
      let student_id = ex_str_val(&rfin, &["studentId"]);

      // Get student's email from studentId
      let correo_alumno = get_eml_from_idusr(
        p.abr.clone(),
        match &student_id {
          Value::String(s) => s.clone(),
          _ => String::new(),
        },
      )
      .await
      .cwl("Could not get correo_alumno from studentId")?;

      // Get group from student's email
      let grpp = get_grp_from_correo(p.abr.clone(), correo_alumno.clone())
        .await
        .cwl("Could not get grupo from correo_alumno")?;

      bindings.extend([
        ("data_id", ex_str_val(&rfin, &["invitationId"])),
        ("correo_tutor", ex_str_val(&rfin, &["invitedEmailAddress"])),
        ("correo_alumno", Value::String(correo_alumno)),
        ("estado", ex_str_val(&rfin, &["state"])),
        ("grupo", Value::String(grpp)),
      ]);

      "
        update type::record($id_string)
        set
          data.do = 'x',
          data.id = $data_id,
          data.correo_tutor = $correo_tutor,
          data.correo_alumno = $correo_alumno,
          data.estado = $estado,
          data.grupo = $grupo,
          tse = NONE,
          cmd = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
    }

    "co" | "uo" => {
      let org_unit_id = ex_str_val(&rfin, &["orgUnitId"]);
      let cleaned_id = match &org_unit_id {
        Value::String(s) if s.starts_with("id:") => {
          Value::String(s[3..].to_string())
        }
        _ => org_unit_id,
      };

      bindings.extend([
        ("data_id", cleaned_id),
        ("nombre", ex_str_val(&rfin, &["name"])),
        ("descripcion", ex_str_val(&rfin, &["description"])),
        ("ruta_principal", ex_str_val(&rfin, &["parentOrgUnitPath"])),
        ("ruta", ex_str_val(&rfin, &["orgUnitPath"])),
      ]);

      "
        update type::record($id_string)
        set 
          data.do = 'x',
          data.id = $data_id,
          data.nombre = $nombre,
          data.descripcion = $descripcion,
          data.ruta = $ruta,
          data.ruta_principal = $ruta_principal,
          tse = NONE,
          cmd = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
    }

    "cg" | "ug" => {
      bindings.extend([
        ("data_id", ex_str_val(&rfin, &["id"])),
        ("correo", ex_str_val(&rfin, &["email"])),
        ("nombre", ex_str_val(&rfin, &["name"])),
        ("descripcion", ex_str_val(&rfin, &["description"])),
      ]);

      "
        update type::record($id_string)
        set 
          data.do = 'x',
          data.id = $data_id,
          data.nombre = $nombre,
          data.descripcion = $descripcion,
          data.correo = $correo,
          data.abrv = string::split(data.correo, '@')[0],
          tse = NONE,
          cmd = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
    }

    "amg" | "umg" => {
      bindings.extend([
        ("data_id", ex_str_val(&rfin, &["id"])),
        (
          "grupo",
          args
            .as_ref()
            .map(|s| Value::String(s.clone()))
            .unwrap_or(Value::Null),
        ),
        ("correo", ex_str_val(&rfin, &["email"])),
        ("clase", ex_str_val(&rfin, &["kind"])),
        ("rol", ex_str_val(&rfin, &["role"])),
        ("tipo", ex_str_val(&rfin, &["type"])),
        ("estatus", ex_str_val(&rfin, &["status"])),
      ]);

      "
        update type::record($id_string)
        set 
          data.do = 'x',
          data.id = $data_id,
          data.grupo = if type::is::null($grupo) then NONE else $grupo end,
          data.correo = $correo,
          data.clase = $clase,
          data.rol = $rol,
          data.tipo = $tipo,
          data.estatus = $estatus,
          data.modificar_grupo = NONE,
          tse = NONE,
          cmd = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
    }

    "cca" | "mac" => {
      bindings.extend([
        ("data_id", ex_str_val(&rfin, &["id"])),
        ("nombre", ex_str_val(&rfin, &["summary"])),
        ("descripcion", ex_str_val(&rfin, &["description"])),
        ("ubicacion", ex_str_val(&rfin, &["location"])),
        ("id_color", ex_str_val(&rfin, &["colorId"])),
        ("zona_horaria", ex_str_val(&rfin, &["timeZone"])),
      ]);

      "
        update type::record($id_string)
        set 
          data.do = 'x',
          data.id = $data_id,
          data.nombre = $nombre,
          data.ubicacion = $ubicacion,
          data.id_color = $id_color,
          data.descripcion = $descripcion,
          data.zona_horaria = $zona_horaria,
          tse = NONE,
          cmd = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
    }

    "cev" | "mev" => {
      bindings.extend([
        ("data_id", ex_str_val(&rfin, &["id"])),
        ("nombre", ex_str_val(&rfin, &["summary"])),
        ("descripcion", ex_str_val(&rfin, &["description"])),
        ("ubicacion", ex_str_val(&rfin, &["location"])),
        ("id_color", ex_str_val(&rfin, &["colorId"])),
        ("invitados", ex_arr_emails(&rfin, &["attendees"])),
        ("anexos", ex_arr_file_urls(&rfin, &["attachments"])),
      ]);

      "
        update type::record($id_string)
        set
          data.do = 'x',
          data.id = $data_id,
          data.nombre = $nombre,
          data.ubicacion = $ubicacion,
          data.id_color = $id_color,
          data.descripcion = $descripcion,
          data.invitados = $invitados,
          data.anexos = $anexos,
          tse = NONE,
          cmd = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
    }

    "ct" | "mt" => {
      bindings.extend([("data_id", ex_str_val(&rfin, &["id"]))]);

      "
        update type::record($id_string)
        set 
          data.do = 'x',
          data.id = $data_id,
          tse = NONE,
          cmd = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
    }

    "upcb" => {
      bindings.extend([
        ("usuario_anotado", ex_str_val(&rfin, &["annotatedUser"])),
        ("ruta_org", ex_str_val(&rfin, &["orgUnitPath"])),
      ]);

      "
        update type::record($id_string)
        set 
          data.do = 'x',
          data.usuario_anotado = $usuario_anotado,
          data.ruta_org = $ruta_org,
          tse = NONE,
          cmd = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
    }

    "scb" | "uscb" | "rcb" | "fcb" => {
      let estatus = match cmd {
        "scb" => "DISABLED",
        "uscb" => "ACTIVE",
        "rcb" => "RETIRED",
        "fcb" => "FACTORY_RESET",
        "upcb" => "PROVISIONED",
        _ => "",
      };

      bindings.extend([("estatus", Value::String(estatus.to_string()))]);

      "
        update type::record($id_string)
        set 
          data.do = 'x',
          data.estatus = $estatus,
          tse = NONE,
          cmd = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
    }

    // NOTE some domains have problems with service accounts and subdomains, it did not work for jjir.me
    "cod" => {
      // Extract permissions arrays and join them
      let empty_permissions = Vec::new();
      let permissions = rfin
        .get("permissions")
        .and_then(|p| p.as_array())
        .unwrap_or(&empty_permissions);

      let emails: Vec<String> = permissions
        .iter()
        .filter_map(|perm| perm.get("emailAddress").and_then(|e| e.as_str()))
        .map(|s| s.to_string())
        .collect();
      let roles: Vec<String> = permissions
        .iter()
        .filter_map(|perm| perm.get("role").and_then(|r| r.as_str()))
        .map(|s| s.to_string())
        .collect();

      bindings.extend([
        (
          "correo_dueno",
          ex_str_val(&rfin, &["owners", "0", "emailAddress"]),
        ),
        ("correos_compartidos", Value::String(emails.join(", "))),
        ("permisos_compartidos", Value::String(roles.join(", "))),
      ]);

      "
        update type::record($id_string)
        set 
          data.do = 'x',
          data.correo_dueno = $correo_dueno,
          data.correos_compartidos = $correos_compartidos,
          data.permisos_compartidos = $permisos_compartidos,
          tse = NONE,
          cmd = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
    }

    "gfe" => {
      let empty_vec = Vec::new();

      // Check if this is an empty response (normal for 90% of users)
      let is_empty_response = rfin
        .get("_empty_response")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

      let emails: Vec<String> = if is_empty_response {
        // Handle the common case: no forwarding emails
        debug!(
          "GFE: Processing empty response - no forwarding emails for this user"
        );
        Vec::new()
      } else {
        // Handle normal case with actual forwarding addresses
        let foremls = rfin
          .get("forwardingAddresses")
          .and_then(|c| c.as_array())
          .unwrap_or(&empty_vec);

        foremls
          .iter()
          .filter_map(|perm| {
            perm.get("forwardingEmail").and_then(|e| e.as_str())
          })
          .map(|s| s.to_string())
          .collect()
      };

      bindings.extend([("correos_reenvio", Value::String(emails.join(", ")))]);

      "
        update type::record($id_string)
        set 
          data.do = 'x',
          data.correos_reenvio = $correos_reenvio,
          tse = NONE,
          cmd = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
    }

    "aua" | "auaa" => {
      // Handle UserAlias API response - use the aliases string passed as args parameter
      let aliases_str = if cmd == "auaa" {
        // For multiple aliases, use the original comma-separated string from args
        args.unwrap_or_default()
      } else {
        // For single alias, extract from the API response
        rfin
          .get("alias")
          .and_then(|v| v.as_str())
          .unwrap_or("")
          .to_string()
      };

      bindings.extend([("aliases", Value::String(aliases_str))]);

      "
        update type::record($id_string)
        set
          data.do = 'x',
          data.aliases = $aliases,
          tse = NONE,
          cmd = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
    }

    "dua" | "duaa" => {
      "
        update type::record($id_string)
        set
          data.do = 'x',
          data.aliases = NONE,
          tse = NONE,
          cmd = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
    }

    "ag" | "rg" => {
      "
        update type::record($id_string)
        set 
          data.do = 'x',
          data.modificar_grupo = NONE,
          tse = NONE,
          cmd = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
    }

    "apc" | "adsc" | "dac" | "dpc" => {
      "
        update type::record($id_string)
        set 
          data.do = 'x',
          data.correo_dos = NONE,
          data.grupo_dos = NONE,
          tse = NONE,
          cmd = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
    }

    "dff" => {
      "
        update type::record($id_string)
        set
          data.do = 'x',
          tse = NONE,
          cmd = NONE,
          data.id_carpeta = NONE,
          data.nombre_carpeta = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
    }

    "ali" => {
      // Extract license data from rfin response
      let correo_usuario = ex_str_val(&rfin, &["userId"]);
      let skuid = ex_str_val(&rfin, &["skuId"]);

      debug!(
        "This is correo_usuario in update_rows_good {:#?}",
        correo_usuario
      );
      debug!("This is skuid in update_rows_good {:#?}", skuid);

      let grpp = get_grp_from_correo(
        p.abr.clone(),
        match &correo_usuario {
          Value::String(s) => s.clone(),
          _ => String::new(),
        },
      )
      .await
      .cwl("Could not get grupo from correo")?;

      let abrlicpp = get_abrlic_from_skuid(
        p.abr.clone(),
        match &skuid {
          Value::String(s) => s.clone(),
          _ => String::new(),
        },
      )
      .await
      .cwl("Could not get abrlic from skuid")?;

      debug!("This is grpp in update_rows_good {:#?}", grpp);
      debug!("This is abrlicpp in update_rows_good {:#?}", abrlicpp);

      bindings.extend([
        ("data_id", ex_str_val(&rfin, &["id"])),
        ("correo_usuario", correo_usuario),
        ("nombre_producto", ex_str_val(&rfin, &["productName"])),
        ("pid", ex_str_val(&rfin, &["productId"])),
        ("nombre_sku", ex_str_val(&rfin, &["skuName"])),
        ("skuid", skuid),
        ("abrlic", Value::String(abrlicpp)),
        ("grupo", Value::String(grpp)),
      ]);

      "
        update type::record($id_string)
        set
          data.do = 'x',
          data.id = if string::len($data_id) > 0 then $data_id else rand::guid(10) end,
          data.correo_usuario = $correo_usuario,
          data.nombre_producto = $nombre_producto,
          data.pid = $pid,
          data.nombre_sku = $nombre_sku,
          data.skuid = $skuid,
          data.abrlic = $abrlic,
          data.grupo = $grupo,
          tse = NONE,
          cmd = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
    }

    _ => {
      "
        update type::record($id_string)
        set 
          data.do = 'x',
          tse = NONE,
          cmd = NONE,
          data.shid = NONE,
          data.index = NONE;
      "
    }
  };

  debug!("update_rows_good: Executing query for cmd: {}", cmd);

  let mut query_builder = DB.query(qry);
  for (key, value) in bindings {
    query_builder = query_builder.bind((key, value));
  }

  query_builder
    .await
    .cwl("Failed to update database with good status")?;

  Ok(())
}

#[allow(dead_code)]
pub async fn create_pdf_from_sheet(
  idshee: &str,
  namsh: &str,
  p: &Pets,
  tse: String,
) -> AppResult<()> {
  let folpdfs = "FOL_PDFS_GRADES";

  let fullpath = if env::consts::OS == "macos" {
    format!("/Users/jj/Desktop/{folpdfs}_{}", &p.dom.to_uppercase())
  } else {
    format!(
      "/home/jason_jurotich/adminbotbinaryv2/{folpdfs}_{}",
      &p.dom.to_uppercase()
    )
  };

  let path = Path::new(&fullpath);
  if !path.exists() {
    tfs::create_dir_all(&fullpath)
      .await
      .cwl("Failed to create PDF output directory")?;
  }

  println!("DEBUG this is path in boletas {:#?}", path);
  let qry = json!({"mimeType":"application/pdf"});

  // this transforms the google spreadsheet into a pdf with all the sheets inside with a particular format
  let url = format!(
    "https://docs.google.com/spreadsheets/d/{idshee}/export?format=pdf&portrait=true&top_margin=0.00&bottom_margin=0.00&left_margin=0.00&right_margin=0.00&pagenum=false&size=letter&scale=1&gridlines=false&horizontal_alignment=CENTER&vertical_alignment=TOP&printtitle=false"
  );

  let au_build =
    req_build("GET", &url, Some(&tse.clone()), Some(&qry), None)
      .cwl("Could not create request builder in create_pdf_from_sheet")?;

  get_global_drive_limiter().until_ready().await;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request in create_pdf_from_sheet")?;

  match res.status().as_u16() {
    200..=299 => {
      let data = res
        .bytes()
        .await
        .cwl("Failed to get PDF data from response")?;

      let file_path = format!("{fullpath}/{}.pdf", namsh.to_uppercase());
      let mut file = Fio::create(&file_path)
        .await
        .cwl("Failed to create PDF file")?;

      file
        .write_all(&data)
        .await
        .cwl("Failed to write PDF data to file")?;

      debug!("Successfully created PDF: {}", file_path);
      Ok(())
    }
    status => {
      let error_text = res
        .text()
        .await
        .cwl("Failed to read error response body in create_pdf_from_sheet")?;

      let clean_error_details = parse_google_api_error(&error_text);
      bail!(
        "PDF export from sheet failed - Status: {} {} - {}",
        status,
        get_status_code_name(status),
        clean_error_details
      )
    }
  }
}

#[allow(dead_code)]
pub async fn upload_pdf_drive(
  file_path: &str,
  file_name: &str,
  folder_id: &str,
  tse: &str,
) -> AppResult<String> {
  let ep = Ep::Files;

  let file = Fio::open(file_path)
    .await
    .cwl("Failed to open PDF file for upload")?;

  let stream = FramedRead::new(file, BytesCodec::new());
  let body = Body::wrap_stream(stream);

  let upload_url = format!("{}?uploadType=multipart", ep.base_url());

  let au_build = req_build("POST", &upload_url, Some(tse), None, None)
    .cwl("Could not create request builder in upload_pdf_drive")?
    .header("Content-Type", "application/pdf")
    .body(body);

  get_global_drive_limiter().until_ready().await;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send upload request in upload_pdf_drive")?;

  match res.status().as_u16() {
    200..=299 => {
      let rfin: Value = res
        .json()
        .await
        .cwl("Failed to parse JSON response from Drive upload")?;

      let file_id =
        rfin.get("id").and_then(|id| id.as_str()).ok_or_else(|| {
          anyhow::anyhow!("File ID not found in upload response")
        })?;

      debug!("Successfully uploaded PDF to Drive with ID: {}", file_id);

      // Move the file to the correct folder and set name
      let patch_url = format!("{}{}", ep.base_url(), file_id);
      let name_body = json!({"name": file_name});
      let folder_query = json!({"addParents": folder_id});

      let patch_build = req_build(
        "PATCH",
        &patch_url,
        Some(tse),
        Some(&folder_query),
        Some(&name_body),
      )
      .cwl("Could not create request builder for moving PDF to folder")?;

      get_global_drive_limiter().until_ready().await;

      let patch_res = patch_build
        .send()
        .await
        .cwl("Failed to move PDF to folder")?;

      match patch_res.status().as_u16() {
        200..=299 => {
          let patch_rfin: Value = patch_res
            .json()
            .await
            .cwl("Failed to parse JSON response from file move")?;

          debug!("Successfully moved PDF to folder: {:?}", patch_rfin);
          Ok(file_id.to_string())
        }
        status => {
          let error_text = patch_res
            .text()
            .await
            .cwl("Failed to read error response from file move")?;

          let clean_error_details = parse_google_api_error(&error_text);
          bail!(
            "Failed to move PDF to folder - Status: {} {} - {}",
            status,
            get_status_code_name(status),
            clean_error_details
          )
        }
      }
    }
    status => {
      let error_text = res
        .text()
        .await
        .cwl("Failed to read error response body in upload_pdf_drive")?;

      let clean_error_details = parse_google_api_error(&error_text);
      bail!(
        "PDF upload to Drive failed - Status: {} {} - {}",
        status,
        get_status_code_name(status),
        clean_error_details
      )
    }
  }
}

pub async fn create_orgs_orgbase(
  rows: Vec<Value>,
  p: &Pets,
  errs1: &mut Vec<String>,
) -> AppResult<()> {
  let ep = Ep::Orgs;

  if !rows.is_empty() {
    let org_limiter = create_rate_limiter(Ep::Orgbase.modvlow());

    // Sort rows by data.id to ensure proper dependency order
    let mut sorted_rows = rows;
    sorted_rows.sort_by(|a, b| {
      let a_id = a
        .get("data")
        .and_then(|data| data.get("id"))
        .and_then(|id| id.as_i64())
        .unwrap_or(0);
      let b_id = b
        .get("data")
        .and_then(|data| data.get("id"))
        .and_then(|id| id.as_i64())
        .unwrap_or(0);
      a_id.cmp(&b_id)
    });

    // Process rows synchronously (one by one) to respect dependencies
    for record in sorted_rows {
      let (id, data) = extract_record_parts(record.clone())?;
      let mut er1 = String::new();

      let name = check_key(&data, "nombre", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let parent_path = check_key(&data, "org_principal", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("/");

      validate_required_fields!(data, er1, "create_ors_orgbase", id);

      // if name.is_empty() {
      //   let error_msg = format!("Skipping record {} - empty name", id);
      //   error!(error = %error_msg, "Record validation failed");
      //   errs1.push(error_msg);
      //   continue;
      // }

      let body = json!({
        "name": name,
        "description": name,
        "parentOrgUnitPath": parent_path,
        "blockInheritance": false,
      });

      let au_build = req_build(
        "POST",
        ep.base_url(),
        Some(&p.tsy.clone()),
        None,
        Some(&body),
      )
      .cwl("Could not create request builder in create_orgs_orgbase")?;

      org_limiter.until_ready().await;

      let res = au_build
        .send()
        .await
        .cwl("Failed to send request in create_orgs_orgbase")?;

      match res.status().as_u16() {
        200..=299 => {
          debug!("Successfully created org: {}", name);
          let rfin: Value = res
            .json()
            .await
            .cwl("Failed to parse JSON response from Orgs API")?;

          update_rows_good(id, rfin, p, None).await.cwl(
            "Failed to update database with successful create_orgs_orgbase result",
          )?;
        }
        409 => {
          warn!(
            "Org {} already exists - continuing with command execution.",
            name
          );
          // Mark as successful since org exists
          let rfin =
            json!({"id": name, "name": name, "parentOrgUnitPath": parent_path});
          update_rows_good(id, rfin, p, None)
            .await
            .cwl("Failed to update database with existing org result")?;
        }
        status => {
          let error_text = res
            .text()
            .await
            .cwl("Failed to read error response body in create_orgs_orgbase")?;

          let clean_error_details = parse_google_api_error(&error_text);

          let error_msg = format!(
            "create_orgs_orgbase failed for org {} - Status: {} {} - {}",
            name,
            status,
            get_status_code_name(status),
            clean_error_details
          );

          error!(error = %error_msg, "Organization creation failed");
          errs1.push(error_msg.clone());

          update_db_bad(error_msg.clone(), id)
            .await
            .cwl("Failed to update database with create_orgs_orgbase error")?;
        }
      }
    }
  }

  Ok(())
}

pub async fn pet_sum(p: &Pets) -> AppResult<()> {
  let txs = get_petition_summary_by_domain(p)
    .await
    .cwl("Could not get petition summary.")?;

  chres(txs, "".to_string(), p.tsn.clone())
    .await
    .cwl("Could not send chat to space.")?;
  Ok(())
}

pub async fn list_members(
  record: Value,
  limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
  p: &Pets,
) -> AppResult<()> {
  let ep = Ep::Members;

  let (id, data) = extract_record_parts(record.clone())?;
  let mut er1 = String::new();

  let grp = check_key(&data, "correo", &mut er1)
    .and_then(|v| v.as_str())
    .unwrap_or("")
    .to_string();

  let ap = format!("{}{}/{}", ep.base_url(), grp, ep.res_obs());

  debug!("This is ap in list_members {:#?}", ap);

  // let p = PETS
  //   .get()
  //   .await
  //   .cwl("Could not get pet fields for list_members")?;

  let qrys = json! ({
    "maxResults": "200",
  });

  debug!("This is qrys in list_members {:#?}", qrys);

  lists(
    ep,
    Some(&qrys),
    Some(ap.clone()),
    p.tsy.clone(),
    p.abr.clone(),
    limiter,
    Some(json!({"key1": grp.clone()})),
    p,
  )
  .await
  .cwl("Failed to process lists for lists_members")?;

  debug!("Successfully got list of members for {:?}", grp);

  update_db_good(id.clone())
    .await
    .cwl("Failed to update database with successful list_members result")?;

  Ok(())
}

pub async fn get_grps_usr(
  usr: String,
  p: &Pets,
) -> AppResult<Vec<Vec<String>>> {
  let ep = Ep::Groups;

  let qry = json!({
    "userKey": usr
  });

  let au_build =
    req_build("GET", ep.base_url(), Some(&p.tsy.clone()), Some(&qry), None)
      .cwl("Could not create request builder in get_grps_usr")?;

  // Use global Groups listing limiter to prevent rate multiplication in concurrent operations
  crate::limiters::get_global_groups_list_limiter()
    .until_ready()
    .await;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request in get_grps_usr")?;

  match res.status().as_u16() {
    200..=299 => {
      let rfin: Value =
        res.json().await.cwl("Failed to parse JSON response")?;

      let empty_vec = Vec::new();

      let grps = rfin
        .get("groups")
        .and_then(|c| c.as_array())
        .unwrap_or(&empty_vec);

      let mut groups_list = Vec::new();

      for group in grps.iter() {
        let gid = group
          .get("id")
          .and_then(|id| id.as_str())
          .map(|s| s.to_string());

        let gemail = group
          .get("email")
          .and_then(|room| room.as_str())
          .map(|s| s.to_string());

        if let (Some(group_id), Some(group_email)) = (gid, gemail) {
          groups_list.push(vec![group_id, group_email]);
        }
      }
      Ok(groups_list)
    }
    status => {
      let error_text = res
        .text()
        .await
        .cwl("Failed to read error response body in get_grps_usr")?;

      let clean_error_details = parse_google_api_error(&error_text);
      bail!(
        "Course retrieval failed - Status: {} {} - {}",
        status,
        get_status_code_name(status),
        clean_error_details
      )
    }
  }
}

pub async fn get_membs_grp(
  grp: String,
  limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
  p: &Pets,
) -> AppResult<Vec<Vec<String>>> {
  let mut membs: Vec<Vec<String>> = Vec::new();
  let ep = Ep::Groups;

  // let url = format!("{}{}", ep.base_url(), usr);

  // NOTE will not get provisioned classes, you MUST update all classes to active to get all the classes first
  // "courseStates":"ACTIVE" (use this query only in the future for getting grades from one student)

  let grp = if grp.contains("@") {
    grp
  } else {
    format!("{}@{}", grp.trim(), p.dom)
  };

  let url = format!("{}{grp}/members", ep.base_url());

  debug!("This is url in get_membs_grp {:#?}", url);

  let au_build = req_build("GET", &url, Some(&p.tsy.clone()), None, None)
    .cwl("Could not create request builder in get_membs_grp")?;

  limiter.until_ready().await;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request in get_membs_grp")?;

  match res.status().as_u16() {
    200..=299 => {
      let rfin: Value =
        res.json().await.cwl("Failed to parse JSON response")?;

      debug!("This is rfin in get_membs_grp {:#?}", rfin);

      let empty_vec = Vec::new();

      let mems = rfin
        .get("members")
        .and_then(|c| c.as_array())
        .unwrap_or(&empty_vec);

      for member in mems.iter() {
        let eml = member
          .get("email")
          .and_then(|id| id.as_str())
          .map(|s| s.to_string());

        let rol = member
          .get("role")
          .and_then(|rol| rol.as_str())
          .map(|s| s.to_string());

        if let (Some(eml), Some(rol)) = (eml, rol) {
          membs.push(vec![eml, rol]);
        }
      }

      debug!("This is membs in get_membs_grp {:#?}", membs);
      Ok(membs)
    }
    status => {
      let error_text = res
        .text()
        .await
        .cwl("Failed to read error response body in get_membs_grp")?;

      bail!(format_google_api_error(
        "Group members retrieval",
        "admin",
        status,
        &error_text,
        Some(&format!("group {}", grp))
      ))
    }
  }
}
