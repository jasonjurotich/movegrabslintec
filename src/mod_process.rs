use crate::AppResult;
use crate::apis::Ep;
use crate::aux_process::*;
use crate::aux_sur::check_is_admin;
use crate::check_key;
use crate::error_utils::{get_status_code_name, parse_google_api_error};
use crate::extract_record_parts;
use crate::goauth::get_tok;
use crate::limiters::{
  get_global_delete_limiter, get_global_drive_limiter,
  get_global_email_delete_limiter, get_global_licensing_limiter,
};
use crate::surrealstart::{
  DB, Pets, del_row_good, get_lic_info, req_build, update_db_bad,
};
use crate::tracer::ContextExt;
use crate::validate_required_fields;
use crate::{bail, debug, warn};
use governor::RateLimiter;
use governor::clock::DefaultClock;
use governor::state::InMemoryState;
use governor::state::NotKeyed;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

async fn get_admin_email_for_abr(abr: &str) -> AppResult<String> {
  debug!("🔍 Getting admin email for abr: {}", abr);

  let ep = Ep::Admins;
  let qry = "
    select value data.admins
    from type::table($table)
    where abr = $abr
    limit 1
  ";

  let mut result = DB
    .query(qry)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr.to_string()))
    .await
    .cwl("Failed to query admin email from petitions")?;

  let admin_email: Option<String> = result
    .take(0)
    .cwl("Failed to extract admin email from query result")?;

  match admin_email {
    Some(email) if !email.is_empty() => {
      debug!("✅ Found admin email for abr {}: {}", abr, email);
      Ok(email)
    }
    _ => {
      debug!("❌ No admin email found for abr: {}", abr);
      bail!("No admin email found for abbreviation: {}", abr)
    }
  }
}

async fn retry_with_fresh_token(
  method: &str,
  url: &str,
  query_params: Option<&Value>,
  request_json_body: Option<&Value>,
  p: &Pets,
  id: String,
) -> AppResult<()> {
  debug!(
    "🔄 [RETRY] Attempting OAuth token refresh for cmd={}",
    p.cmd
  );

  let admin_email = get_admin_email_for_abr(&p.abr).await?;
  debug!("🔄 [RETRY] Found admin email: {}", admin_email);

  // Clear expired token and get fresh one
  use crate::goauth::clear_token_cache;
  clear_token_cache();

  let fresh_token = crate::goauth::get_tok(admin_email, "yes")
    .await
    .cwl("Failed to refresh OAuth token")?;
  debug!("🔄 [RETRY] Successfully obtained fresh token");

  // Rebuild and retry request
  let fresh_au_build = req_build(
    method,
    url,
    Some(&fresh_token),
    query_params,
    request_json_body,
  )
  .cwl("Could not create request builder with fresh token")?;

  let retry_res = fresh_au_build
    .send()
    .await
    .cwl("Failed to send retry request")?;

  debug!(
    "🔄 [RETRY] Retry completed with status: {}",
    retry_res.status()
  );

  if retry_res.status().is_success() {
    debug!("✅ [RETRY] SUCCESS! Fresh token worked for cmd={}", p.cmd);
    Ok(())
  } else {
    let retry_error = format!(
      "Retry failed - Status: {} - Fresh token rejected",
      retry_res.status()
    );
    update_db_bad(retry_error.clone(), id)
      .await
      .cwl("Failed to update database with retry failure")?;
    bail!("RETRY_FAILED: {}", retry_error);
  }
}

// Helper function to build query parameters for mt command with updateMask
// Commented out - not currently used but may be needed for future functionality
#[allow(dead_code)]
fn build_mt_query_params(data: &serde_json::Map<String, Value>) -> Value {
  let mut query_obj = json!({
    "fields": "*",
  });

  let mut update_fields = Vec::new();

  // Check which fields are present in the data and add them to updateMask
  if data.get("titulo").is_some() && !data["titulo"].is_null() {
    update_fields.push("title");
  }
  if data.get("descripcion").is_some() && !data["descripcion"].is_null() {
    update_fields.push("description");
  }
  if data.get("puntos").is_some() && !data["puntos"].is_null() {
    update_fields.push("maxPoints");
  }
  if data.get("estado").is_some() && !data["estado"].is_null() {
    update_fields.push("state");
  }
  if data.get("id_tema").is_some() && !data["id_tema"].is_null() {
    update_fields.push("topicId");
  }
  // NOTE: assigneeMode is NOT supported in updateMask according to Google Classroom API docs
  // Removed: if data.get("modo").is_some() && !data["modo"].is_null() {
  //   update_fields.push("assigneeMode");
  // }
  if data.get("modificable").is_some() && !data["modificable"].is_null() {
    let modificable_str = data["modificable"].as_str().unwrap_or("");
    if !modificable_str.trim().is_empty() {
      update_fields.push("submissionModificationMode");
    }
  }
  if data.get("fecha_programada").is_some()
    && !data["fecha_programada"].is_null()
  {
    let fecha_str = data["fecha_programada"].as_str().unwrap_or("");
    if !fecha_str.trim().is_empty() {
      update_fields.push("scheduledTime");
    }
  }
  if data.get("fecha_vencida").is_some() && !data["fecha_vencida"].is_null() {
    let fecha_str = data["fecha_vencida"].as_str().unwrap_or("");
    if !fecha_str.trim().is_empty() {
      update_fields.push("dueDate");
      update_fields.push("dueTime");
    }
  }
  if data.get("id_periodo").is_some() && !data["id_periodo"].is_null() {
    update_fields.push("gradingPeriodId");
  }
  // NOTE: multipleChoiceQuestion is NOT updatable after creation according to Google Classroom API
  // Removed: if data.get("opciones").is_some() && !data["opciones"].is_null() {
  //   update_fields.push("multipleChoiceQuestion");
  // }

  if !update_fields.is_empty() {
    let update_mask = update_fields.join(",");
    debug!("MT DEBUG: Generated updateMask: {}", update_mask);
    query_obj["updateMask"] = json!(update_mask);
  } else {
    debug!("MT DEBUG: No update fields found, no updateMask added");
  }

  debug!("MT DEBUG: Final query params: {}", query_obj);
  query_obj
}

// NOTE if you switch PUT with post in commands that modify and not create, it will say the user does not exist!

#[allow(clippy::too_many_arguments)]
pub async fn process_elements<T: Into<Ep>>(
  ep: T,
  method: &str, // "POST" | "PUT" | "PATCH" | "DELETE"
  rbod: Option<Value>,
  uarg: Option<String>, // If None and POST, defaults to base + res_obs
  id: String,           // SurrealDB record with { id, data }
  tse: String,          // Token
  limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
  p: &Pets,
  oauth_cancellation: Option<Arc<AtomicBool>>, // Optional cancellation token for OAuth expiration
  custom_query_params: Option<Value>, // Custom query parameters for specific commands
) -> AppResult<()> {
  let ep: Ep = ep.into();

  let url = if uarg.is_none() {
    ep.base_url().to_string()
  } else {
    uarg.unwrap_or("".to_string())
  };

  // For DELETE methods, we don't send a JSON body in the request
  let request_json_body: Option<Value> =
    if method.eq_ignore_ascii_case("DELETE") {
      None
    } else {
      rbod
    };

  // Use custom query parameters if provided, otherwise empty
  let query_params = custom_query_params.as_ref();

  let au_build = req_build(
    method,
    &url,
    Some(&tse),
    query_params,
    request_json_body.as_ref(),
  )
  .cwl("Could not create request builder in process_elements")?;

  let valid_drive_commands = ["cod", "ddm", "dff"];
  let email_delete_commands = ["du", "duni"];
  let delete_commands =
    ["dc", "dca", "dev", "dor", "ddm", "dg", "rmg", "dt", "dtu"];
  let licensing_commands = ["ali", "uli", "qli"];

  // Track timing for rate limiting debugging
  let rate_limit_start = std::time::Instant::now();

  if valid_drive_commands.contains(&p.cmd.as_str()) {
    debug!("Using global drive limiter for cmd={}", p.cmd);
    get_global_drive_limiter().until_ready().await;
  } else if method == "DELETE"
    && email_delete_commands.contains(&p.cmd.as_str())
  {
    debug!("Using global email delete limiter for DELETE cmd={}", p.cmd);
    get_global_email_delete_limiter().until_ready().await;
  } else if method == "DELETE" && delete_commands.contains(&p.cmd.as_str()) {
    debug!("Using global delete limiter for DELETE cmd={}", p.cmd);
    get_global_delete_limiter().until_ready().await;
  } else if licensing_commands.contains(&p.cmd.as_str()) {
    debug!("Using global licensing limiter for cmd={}", p.cmd);
    get_global_licensing_limiter().until_ready().await;
  } else {
    debug!(
      "Using shared rate limiter for cmd={} - waiting for permit",
      p.cmd
    );
    limiter.until_ready().await;
  }

  let rate_limit_duration = rate_limit_start.elapsed();
  debug!(
    "Rate limiter wait completed for cmd={}, waited: {:?}, now sending HTTP request",
    p.cmd, rate_limit_duration
  );

  // Check if OAuth has expired before making the request
  if let Some(cancellation) = &oauth_cancellation {
    let cancellation_state = cancellation.load(Ordering::Relaxed);
    debug!(
      "🔍 Checking OAuth cancellation token for cmd={}, state: {}",
      p.cmd, cancellation_state
    );

    if cancellation_state {
      debug!(
        "🛑 OAuth cancellation detected, skipping request for cmd={}",
        p.cmd
      );
      let oauth_skip_error = format!(
        "Command '{}' skipped - OAuth token expired, process was cancelled",
        p.cmd
      );
      update_db_bad(oauth_skip_error.clone(), id)
        .await
        .cwl("Failed to update database with OAuth skip error")?;
      bail!(
        "OAUTH_TOKEN_EXPIRED: Request skipped due to detected OAuth expiration."
      )
    }
  } else {
    debug!("🔍 No OAuth cancellation token available for cmd={}", p.cmd);
  }

  let request_start = std::time::Instant::now();
  let res = au_build
    .send()
    .await
    .cwl("Failed to send request in process_elements")?;

  let request_duration = request_start.elapsed();
  debug!(
    "HTTP request completed for cmd={}, request took: {:?}, total time: {:?}",
    p.cmd,
    request_duration,
    rate_limit_start.elapsed()
  );

  debug!("HTTP response status={}", res.status().as_u16());

  // Additional debugging for dff command
  if p.cmd == "dff" {
    debug!("DFF DEBUG: HTTP response status: {}", res.status());
    debug!("DFF DEBUG: HTTP response headers: {:#?}", res.headers());
  }

  // Additional debugging for cev/mev commands
  if p.cmd == "cev" || p.cmd == "mev" {
    debug!("CEV/MEV DEBUG: HTTP response status: {}", res.status());
    debug!("CEV/MEV DEBUG: HTTP response headers: {:#?}", res.headers());
    if let Some(ref body) = request_json_body {
      debug!("CEV/MEV DEBUG: Request body sent: {:#?}", body);
    }
  }

  match res.status().as_u16() {
    200..=299 => {
      if (method == "DELETE"
        && [
          "du", "duni", "dc", "dca", "dev", "dor", "ddm", "dg", "rmg", "dt",
          "dtu",
        ]
        .contains(&p.cmd.as_str()))
        || (method == "PATCH" && p.cmd == "dtui")
      {
        del_row_good(id)
          .await
          .cwl("Failed to delete row for process_elements")?;
      // NOTE this is a superficial way to fix this problem, find a better one later.
      } else if res.status().as_u16() == 204
        || (method == "POST" && p.cmd == "udu")
      {
        // Handle 204 No Content responses (like undelete users) with dummy JSON
        let rfin: Value = serde_json::json!({"success": true, "action": "undeleted", "_empty_response": true});

        debug!("This is rfin for empty response {:#?}", rfin);

        update_rows_good(id, rfin, p, None).await.cwl(
          "Failed to update database with successful process_elements result",
        )?;
      } else {
        let rfin: Value = res
          .json()
          .await
          .cwl("Failed to parse JSON response from Sheets API")?;

        debug!("This is rfin in provision_create {:#?}", rfin);

        // Additional debugging for dff command
        if p.cmd == "dff" {
          debug!("DFF DEBUG: API Response received: {:#?}", rfin);

          // Check if parents field exists in response
          if let Some(parents) = rfin.get("parents") {
            debug!("DFF DEBUG: File parents after operation: {:#?}", parents);
          } else {
            debug!(
              "DFF DEBUG: No parents field in response - this might indicate the operation succeeded"
            );
          }

          // Log file ID and name if available
          if let Some(file_id) = rfin.get("id") {
            debug!("DFF DEBUG: Operated on file ID: {:#?}", file_id);
          }
          if let Some(file_name) = rfin.get("name") {
            debug!("DFF DEBUG: Operated on file name: {:#?}", file_name);
          }

          // Most importantly: log if the parents array is empty or contains the removed parent
          if let Some(Value::Array(parents_array)) = rfin.get("parents") {
            if parents_array.is_empty() {
              debug!(
                "DFF DEBUG: SUCCESS - File now has no parents (removed from all folders)"
              );
            } else {
              debug!(
                "DFF DEBUG: WARNING - File still has parents: {:#?}",
                parents_array
              );
              debug!(
                "DFF DEBUG: Check the parents list above to see if the folder was actually removed"
              );
            }
          }
        }

        // For cup command, add new user row to database first, then unmark original row
        if p.cmd == "cup" {
          add_row_to_database(rfin.clone(), p, None)
            .await
            .cwl("Failed to add new user row to database for cup command")?;
        }

        update_rows_good(id, rfin.clone(), p, None).await.cwl(
          "Failed to update database with successful process_elements result",
        )?;
      }
      Ok(())
    }
    429 => {
      let error_text = res
        .text()
        .await
        .cwl("Failed to read 429 error response body in process_elements")?;

      let clean_error_details = parse_google_api_error(&error_text);

      // Extract element ID for context
      let element_context =
        if let Ok(record) = serde_json::from_str::<Value>(&id) {
          if let Ok((_, data)) = extract_record_parts(record) {
            if let Some(element_id) = data.get("id").and_then(|v| v.as_str()) {
              format!(" [ID: {}]", element_id)
            } else {
              String::new()
            }
          } else {
            String::new()
          }
        } else {
          String::new()
        };

      let enhanced_error = format!(
        "Command '{}' failed{} - Status: {} {} - Operation: {} {} - {}",
        p.cmd,
        element_context,
        429,
        get_status_code_name(429),
        method,
        url,
        clean_error_details
      );

      update_db_bad(enhanced_error.clone(), id)
        .await
        .cwl("Failed to update database with process_elements error result")?;

      bail!(enhanced_error)
    }
    status => {
      let error_text = res
        .text()
        .await
        .cwl("Failed to read error response body in process_elements")?;

      // Handle OAuth token expiration (status 401 or others with OAuth error message)
      if (status == 401
        || error_text
          .contains("Request had invalid authentication credentials"))
        && error_text.contains("Expected OAuth 2 access token")
      {
        // Signal cancellation to prevent other requests from executing
        if let Some(cancellation) = &oauth_cancellation {
          let old_state = cancellation.load(Ordering::Relaxed);
          cancellation.store(true, Ordering::Relaxed);
          debug!(
            "🚨 OAuth expiration detected - cancellation token set from {} to {} for cmd={}",
            old_state,
            cancellation.load(Ordering::Relaxed),
            p.cmd
          );
        } else {
          debug!(
            "🚨 OAuth expiration detected but no cancellation token available for cmd={}",
            p.cmd
          );
        }

        // Update the current record as failed due to OAuth expiration
        let oauth_enhanced_error = format!(
          "Command '{}' failed - Status: {} {} - OAuth token expired after 90+ minutes",
          p.cmd,
          status,
          get_status_code_name(status)
        );

        update_db_bad(oauth_enhanced_error.clone(), id.clone())
          .await
          .cwl("Failed to update database with OAuth expiration error")?;

        // 🔄 EXPERIMENTAL: Try token refresh and retry for 'ali' command only
        if p.cmd == "ali"
          && retry_with_fresh_token(
            method,
            &url,
            query_params,
            request_json_body.as_ref(),
            p,
            id,
          )
          .await
          .is_ok()
        {
          return Ok(()); // Success with fresh token!
        }

        // Return a special error that mods() can detect and handle
        bail!(
          "OAUTH_TOKEN_EXPIRED: Google rejecting all requests after 90+ minutes. Process must be restarted."
        )
      }

      // OLD CODE - commented out for testing
      // let mut er1 = String::new();
      // er1.push_str(&format!(
      //   "Status code: {status}\nError details: {error_text}"
      // ));

      // Special handling for 409 CONFLICT on user creation commands
      if status == 409 && ["cu", "cup", "cud", "cuf"].contains(&p.cmd.as_str())
      {
        debug!("409 CONFLICT detected for user creation command: {}", p.cmd);

        // Extract user email from request body (not original data)
        let user_email = if let Some(ref body) = request_json_body {
          body
            .get("primaryEmail")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string()
        } else {
          "unknown".to_string()
        };

        debug!("User already exists: {}", user_email);

        // Return a special error that can be detected by the caller
        // Don't call update_db_bad here - let the caller handle it
        bail!("USER_ALREADY_EXISTS:{}", user_email)
      }

      // Additional debugging for cev/mev commands
      if p.cmd == "cev" || p.cmd == "mev" {
        debug!("CEV/MEV DEBUG: Error response body: {}", error_text);
        if let Some(ref body) = request_json_body {
          debug!("CEV/MEV DEBUG: Request body that caused error: {:#?}", body);
        }
      }

      // Standard error handling for all other cases
      let clean_error_details = parse_google_api_error(&error_text);

      // Extract element ID for context
      let element_context =
        if let Ok(record) = serde_json::from_str::<Value>(&id) {
          if let Ok((_, data)) = extract_record_parts(record) {
            if let Some(element_id) = data.get("id").and_then(|v| v.as_str()) {
              format!(" [ID: {}]", element_id)
            } else {
              String::new()
            }
          } else {
            String::new()
          }
        } else {
          String::new()
        };

      // Special handling for mt command with unsupported updateMask fields
      let enhanced_error = if p.cmd == "mt"
        && clean_error_details.contains("Non-supported update mask fields")
      {
        format!(
          "Command '{}' failed{} - Status: {} {} - {}\n\nCampos que SÍ se pueden actualizar: titulo, descripcion, estado, fecha_vencida, puntos, fecha_programada, modificable, id_tema, id_periodo\n\nCampos que NO se pueden actualizar: modo, opciones, links_drive, links_url, links_youtube, links_formulario, tipo_trabajo",
          p.cmd,
          element_context,
          status,
          get_status_code_name(status),
          clean_error_details
        )
      } else {
        format!(
          "Command '{}' failed{} - Status: {} {} - {}",
          p.cmd,
          element_context,
          status,
          get_status_code_name(status),
          clean_error_details
        )
      };

      update_db_bad(enhanced_error.clone(), id)
        .await
        .cwl("Failed to update database with process_elements error result")?;

      bail!(enhanced_error)
    }
  }
}

/// Extracts the 'id' field from data with error accumulation
macro_rules! get_id {
  ($data:expr, $er1:expr) => {
    check_key($data, "id", $er1)
      .and_then(|v| v.as_str())
      .unwrap_or("")
  };
}

macro_rules! get_usr {
  ($data:expr, $er1:expr) => {{
    // First try correo_dueno without adding errors
    $data
      .get("correo_dueno")
      .and_then(|v| v.as_str())
      .filter(|s| !s.is_empty())
      .unwrap_or_else(|| {
        // If correo_dueno fails, try correo and add error only if both fail
        check_key($data, "correo", $er1)
          .and_then(|v| v.as_str())
          .unwrap_or("")
      })
  }};
}

macro_rules! get_admin {
  ($data:expr, $er1:expr) => {
    check_key($data, "es_admin", $er1)
      .and_then(|v| v.as_str())
      .map(|s| s.to_uppercase() == "TRUE")
      .unwrap_or(false)
  };
}

// macro_rules! skip_if_admin_usr {
//   ($data:expr, $er1:expr, $msg:expr) => {
//     if get_admin!($data, $er1) {
//       warn!($msg);
//       return Ok(());
//     }
//   };
// }

macro_rules! skip_if_admin {
  ($abr:expr, $usr:expr) => {
    if check_is_admin($abr.clone(), $usr.to_owned())
      .await
      .unwrap_or(false)
    {
      warn!("skipping admin");
      return Ok(());
    }
  };
}

pub async fn process_command(
  record: Value,
  limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
  p: &Pets,
  oauth_cancellation: Option<Arc<AtomicBool>>, // Optional cancellation token for OAuth expiration
) -> AppResult<()> {
  debug!("Process_command called with cmd={}", p.cmd);

  // Shared boilerplate logic
  let ep = Ep::enumcmd(&p.cmd);

  let (id, data) = extract_record_parts(record.clone())?;

  let mut er1 = String::new();
  let cmd = p.cmd.as_str();

  match cmd {
    "dor" | "du" | "duni" | "dc" | "dua" | "dfo" | "dac" | "dpc" | "qli"
    | "ddm" | "dca" | "dev" | "dfe" | "dg" | "dt" | "dtu" => {
      let gen_id = if cmd == "duni" {
        check_key(&data, "correo", &mut er1)
          .and_then(|v| v.as_str())
          .unwrap_or("")
      } else {
        get_id!(&data, &mut er1)
      };

      if cmd == "du" {
        let usr = get_usr!(&data, &mut er1);

        // Debug the raw data structure
        warn!("DU DEBUG: Full data structure for user {}: {:?}", usr, data);

        // Check the raw es_admin field
        let raw_admin = check_key(&data, "es_admin", &mut er1);
        warn!("DU DEBUG: Raw es_admin field: {:?}", raw_admin);

        let is_admin = get_admin!(&data, &mut er1);
        warn!("DU DEBUG: cmd={}, is_admin={}, usr={}", cmd, is_admin, usr);

        if is_admin {
          let has_wadmintemp = usr.contains("wadmintemp");
          let has_wrunstemp = usr.contains("wrunstemp");
          warn!(
            "DU DEBUG: has_wadmintemp={}, has_wrunstemp={}",
            has_wadmintemp, has_wrunstemp
          );

          if !has_wadmintemp && !has_wrunstemp {
            warn!(
              "BLOCKING: Admin user {} does not contain temp patterns - DELETION BLOCKED",
              usr
            );
            return Ok(());
          } else {
            warn!("ALLOWING: Temp admin user {} deletion allowed", usr);
          }
        } else {
          warn!("DU DEBUG: Non-admin user {} deletion proceeding", usr);
        }
      }

      let secval = if cmd == "dua" {
        check_key(&data, "aliases", &mut er1)
          .and_then(|v| v.as_str())
          .unwrap_or("")
          .trim()
      } else if ["dac", "dpc"].contains(&cmd) {
        check_key(&data, "correo_dos", &mut er1)
          .and_then(|v| v.as_str())
          .unwrap_or("")
      } else if cmd == "dev" {
        check_key(&data, "id_calendario", &mut er1)
          .and_then(|v| v.as_str())
          .unwrap_or("")
      } else if cmd == "qli" {
        check_key(&data, "abrlic", &mut er1)
          .and_then(|v| v.as_str())
          .unwrap_or("")
      } else if cmd == "dt" {
        check_key(&data, "id_curso", &mut er1)
          .and_then(|v| v.as_str())
          .unwrap_or("")
      } else if cmd == "dtu" {
        check_key(&data, "correo_alumno", &mut er1)
          .and_then(|v| v.as_str())
          .unwrap_or("")
      } else if cmd == "duni" {
        check_key(&data, "correo", &mut er1)
          .and_then(|v| v.as_str())
          .unwrap_or("")
      } else {
        ""
      };

      // Only validate if there are errors, or if secval is required but empty
      if !er1.is_empty()
        || (["dua", "dac", "dpc", "dev", "qli", "dt", "dtu"].contains(&cmd)
          && secval.is_empty())
      {
        validate_required_fields!(data, er1, "delete_elements", id);
      }

      if cmd == "duni" && (gen_id.is_empty() || !er1.is_empty()) {
        validate_required_fields!(
          data,
          er1,
          "delete_elements",
          gen_id.to_string()
        );
      }

      // CRITICAL: Do NOT create a new limiter here!
      // Each process_command call would get its own independent rate limiter,
      // effectively multiplying the request rate by the number of concurrent operations.
      // For example: 10 deletions × 7 ops/sec each = 70 ops/sec total instead of 7 ops/sec.
      // The limiter passed from mods() is shared across all operations to enforce the true rate limit.
      debug!(
        "Using shared rate limiter for delete operation - cmd={}",
        cmd
      );

      let url = if cmd == "dua" {
        format!("{}{gen_id}/aliases/{secval}", ep.base_url())
      } else if cmd == "dfo" {
        format!("{}{gen_id}/photos/thumbnail", ep.base_url())
      } else if cmd == "dac" {
        format!("{}{gen_id}/students/{secval}", ep.base_url())
      } else if cmd == "dpc" {
        format!("{}{gen_id}/teachers/{secval}", ep.base_url())
      } else if cmd == "qli" {
        let usr = check_key(&data, "correo_usuario", &mut er1)
          .and_then(|v| v.as_str())
          .unwrap_or("");

        let (opid, osid) = get_lic_info(&p.abr, secval)
          .await
          .cwl("Could not get new lics info")?;

        debug!(
          "qli command: Deleting license for user='{}', opid='{}', osid='{}'",
          usr, opid, osid
        );
        format!("{}{}/sku/{}/user/{}", ep.base_url(), opid, osid, usr)
      } else if cmd == "dfe" {
        let suburl = ep
          .base_url()
          .replace("messages/", "settings/forwardingAddresses");
        format!("{}/{}", suburl, gen_id)
      } else if cmd == "dor" {
        format!("{}id:{}", ep.base_url(), gen_id)
      } else if cmd == "dev" {
        format!("{}{}/events/{}", ep.base_url(), secval, gen_id)
      } else if cmd == "dt" {
        format!("{}{}/courseWork/{}", ep.base_url(), secval, gen_id)
      } else if cmd == "duni" {
        format!("{}{}", ep.base_url(), secval)
      } else if cmd == "dtu" {
        format!("{}{}/guardians/{}", ep.base_url(), secval, gen_id)
      } else {
        format!("{}{}", ep.base_url(), gen_id)
      };

      let tsy = if ["ddm", "dca", "dfe"].contains(&cmd) {
        let usr = get_usr!(&data, &mut er1);

        skip_if_admin!(p.abr, usr);

        get_tok(usr.to_owned(), "yes")
          .await
          .cwl("Failed to generate token for user for files")?
      } else {
        p.tsy.clone()
      };

      // Use the shared limiter to enforce true rate limiting across all concurrent operations
      let delete_query_params = if cmd == "ddm" {
        Some(json!({
          "supportsAllDrives": "true",
        }))
      } else {
        None
      };

      process_elements(
        ep,
        "DELETE",
        None,
        Some(url),
        id,
        tsy,
        limiter,
        p,
        oauth_cancellation.clone(),
        delete_query_params,
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for delete_elements")
      })?;
    }

    "cod" => {
      let gen_id = get_id!(&data, &mut er1);

      let usr = get_usr!(&data, &mut er1);

      let permusr = check_key(&data, "correos_compartidos", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let permrol = check_key(&data, "permisos_compartidos", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      debug!(
        "COD DEBUG: gen_id={}, usr={}, permusr={}, permrol={}",
        gen_id, usr, permusr, permrol
      );
      debug!("COD DEBUG: Full data structure: {:#?}", data);

      validate_required_fields!(data, er1, "change_owner_file", id);

      skip_if_admin!(p.abr, usr);

      debug!("COD DEBUG: Getting token for user: {}", usr);
      let tsyusr = get_tok(usr.to_owned(), "yes")
        .await
        .cwl("Failed to generate token for user for files")?;

      get_global_drive_limiter().until_ready().await;

      // Try to get permission ID if user already has writer permissions
      let permid = match get_perm_id(
        gen_id.to_owned(),
        permusr.to_owned(),
        tsyusr.clone(),
        "writer".to_owned(),
      )
      .await
      {
        Ok(id) => {
          debug!(
            "User {} already has writer permissions for file {}",
            permusr, gen_id
          );
          id
        }
        Err(_) => {
          debug!(
            "User {} doesn't have writer permissions, adding as writer first",
            permusr
          );
          // User doesn't have writer permissions, add them as writer first
          add_perm_id(
            gen_id.to_owned(),
            permusr.to_owned(),
            tsyusr.clone(),
            "writer".to_owned(),
          )
          .await
          .cwl("Could not add user as writer before making them owner")?
        }
      };

      let url = format!("{}{}/permissions/{}", ep.base_url(), gen_id, permid);

      let body = json!({
        "role": permrol,
      });

      let cod_query_params = Some(json!({
        "transferOwnership": "true",
        "fields": "*",
      }));

      process_elements(
        ep,
        "PATCH",
        Some(body),
        Some(url),
        id,
        tsyusr,
        limiter,
        p,
        oauth_cancellation.clone(),
        cod_query_params,
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for change_owner_file")
      })?;
    }

    "dff" => {
      let gen_id = get_id!(&data, &mut er1);

      let usr = get_usr!(&data, &mut er1);

      let parent = check_key(&data, "id_carpeta", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      debug!("DFF DEBUG: Starting remove file from folder operation");
      debug!("DFF DEBUG: File ID (gen_id): {}", gen_id);
      debug!("DFF DEBUG: Parent folder ID to remove from: {}", parent);
      debug!("DFF DEBUG: File owner (usr): {}", usr);

      validate_required_fields!(data, er1, "remove_file_folder", id);

      skip_if_admin!(p.abr, usr);

      let tsyusr = get_tok(usr.to_owned(), "yes")
        .await
        .cwl("Failed to generate token for user for files")?;

      get_global_drive_limiter().until_ready().await;

      let url = format!("{}{}", ep.base_url(), gen_id);
      debug!("DFF DEBUG: API URL: {}", url);

      let body = json!({
        "removeParents": parent
      });
      debug!("DFF DEBUG: Request body: {}", body);

      let dff_query_params = Some(json!({
        "supportsAllDrives": "true",
        "fields": "id,name,parents",
      }));

      process_elements(
        ep,
        "PATCH",
        Some(body),
        Some(url),
        id,
        tsyusr,
        limiter,
        p,
        oauth_cancellation.clone(),
        dff_query_params,
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for remove_file_folder")
      })?;
    }

    _ => {
      return Ok(());
    }
  }

  Ok(())
}
