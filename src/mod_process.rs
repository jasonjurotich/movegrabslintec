use crate::AppResult;
use crate::apis::Ep;
use crate::aux_mods::*;
use crate::aux_sur::check_is_admin;
use crate::check_key;
use crate::error_utils::{get_status_code_name, parse_google_api_error};
use crate::extract_record_parts;
use crate::limiters::{
  create_rate_limiter, get_global_delete_limiter, get_global_drive_limiter,
  get_global_email_delete_limiter, get_global_licensing_limiter,
};
use crate::tracer::ContextExt;
use crate::validate_required_fields;
use crate::{bail, debug, warn};
use chrono::{Datelike, Duration, Local, NaiveDate, Timelike};
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

macro_rules! skip_if_admin_usr {
  ($data:expr, $er1:expr, $msg:expr) => {
    if get_admin!($data, $er1) {
      warn!($msg);
      return Ok(());
    }
  };
}

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

    "co" => {
      let body = json!({
        "name": check_key(&data, "nombre", &mut er1).unwrap_or(&Value::Null),
        "description": check_key(&data, "descripcion", &mut er1).unwrap_or(&Value::Null),
        "parentOrgUnitPath": check_key(&data, "ruta_principal", &mut er1).unwrap_or(&Value::Null),
        "blockInheritance": false,
      });

      validate_required_fields!(data, er1, "create_orgs", id);

      process_elements(
        ep,
        "POST",
        Some(body),
        None,
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for create_orgs")
      })?;
    }

    "cobg" => {
      let grp = check_key(&data, "grupo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let nombre = check_key(&data, "nombre", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      validate_required_fields!(data, er1, "create_groups_orgbase", id);

      let eml = format!("{}@{}", grp, p.dom);

      let body = json!({
        "email": eml,
        "name": nombre,
        "description": nombre,
      });

      process_elements(
        Ep::Groups,
        "POST",
        Some(body),
        None,
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for create_groups_base")
      })?;
    }

    "cobcg" => {
      let grp = check_key(&data, "grupo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      validate_required_fields!(data, er1, "change_config_grp", id);

      change_config_grp(grp.to_owned(), limiter, p, Some(id))
        .await
        .cwl("Could not chang the config of the group")?;
    }

    "aobg" => {
      let pgrp = check_key(&data, "grupo_principal", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let grp = check_key(&data, "grupo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      validate_required_fields!(data, er1, "add_groups_base", id);

      add_usr_grp(
        grp.to_owned(),
        pgrp.to_owned(),
        limiter.clone(),
        p,
        None,
        None,
        Some(id),
      )
      .await
      .cwl("Could not add user to group")?;
    }

    "uo" => {
      let gen_id = get_id!(&data, &mut er1);

      let mut body = json!({
        "name": check_key(&data, "nombre", &mut er1).unwrap_or(&Value::Null),
        "parentOrgUnitPath": check_key(&data, "ruta_principal", &mut er1).unwrap_or(&Value::Null),
        "blockInheritance": false,
      });

      validate_required_fields!(data, er1, "update_orgs", id);

      if let Some(name) = data.get("descripcion")
        && !name.is_null()
      {
        body["description"] = name.clone();
      }

      let url = format!("{}id:{}", ep.base_url(), gen_id);

      process_elements(
        ep,
        "PUT",
        Some(body),
        Some(url),
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for update_orgs")
      })?;
    }

    "cg" => {
      let body = json!({
        "email": check_key(&data, "correo", &mut er1).unwrap_or(&Value::Null),
        "name": check_key(&data, "nombre", &mut er1).unwrap_or(&Value::Null),
        "description": check_key(&data, "descripcion", &mut er1).unwrap_or(&Value::Null),
      });

      validate_required_fields!(data, er1, "create_groups", id);

      process_elements(
        ep,
        "POST",
        Some(body),
        None,
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for create_groups")
      })?;
    }

    "ug" => {
      let gen_id = get_id!(&data, &mut er1);

      let body = json!({
        "email": check_key(&data, "correo", &mut er1).unwrap_or(&Value::Null),
        "name": check_key(&data, "nombre", &mut er1).unwrap_or(&Value::Null),
        "description": check_key(&data, "descripcion", &mut er1).unwrap_or(&Value::Null),
      });

      validate_required_fields!(data, er1, "update_groups", id);
      let url = format!("{}{}", ep.base_url(), gen_id);

      process_elements(
        ep,
        "PUT",
        Some(body),
        Some(url),
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for update_groups")
      })?;
    }

    "ccg" | "ccgex" => {
      let grp = check_key(&data, "abrv", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      validate_required_fields!(data, er1, "change_config_grp", id);

      change_config_grp(grp.to_owned(), limiter, p, Some(id))
        .await
        .cwl("Could not chang the config of the group")?;
    }

    "cu" | "cup" | "cud" | "cuf" => {
      let body = fix_body_usrs(record.clone(), p)
        .await
        .cwl("Failed to process user body")?;

      validate_required_fields!(data, er1, "create_users", id);

      let id_clone = id.clone();
      match process_elements(
        ep,
        "POST",
        Some(body),
        None,
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      {
        Ok(()) => {}
        Err(e) => {
          let error_msg = e.to_string();
          if error_msg.starts_with("USER_ALREADY_EXISTS:") {
            // Extract email from the error message
            let user_email = error_msg.replace("USER_ALREADY_EXISTS:", "");
            let friendly_message = format!(
              "✅ El usuario '{}' ya existe - favor de eliminar esa fila.",
              user_email
            );

            // Update database with the friendly error message
            update_db_bad(friendly_message.clone(), id_clone)
              .await
              .cwl("Failed to update database with existing user error")?;

            // bail!(friendly_message);
          } else {
            return Err(
              e.context("Failed to run process_elements for create_users"),
            );
          }
        }
      }
    }

    "uu" => {
      let gen_id = get_id!(&data, &mut er1);
      skip_if_admin_usr!(&data, &mut er1, "skipping admin");

      let mut body = json!({
        "primaryEmail": check_key(&data, "correo", &mut er1).unwrap_or(&Value::Null),
        "name": {
          "givenName": check_key(&data, "nombres", &mut er1).unwrap_or(&Value::Null),
          "familyName": check_key(&data, "apellidos", &mut er1).unwrap_or(&Value::Null),
        }
      });

      validate_required_fields!(data, er1, "update_users", id);

      // Only add organizations if cost_center or departamento are present (can be empty)
      // NOTE, check if leaving these as empty spaces will work or if they should rather be null like the rest,
      // check git history from 9 oct 2025 to see the previous version (this was changed on 10 oct)
      let cost_center = data.get("cost_center");
      let departamento = data.get("departamento");

      if cost_center.is_some() || departamento.is_some() {
        let mut org = json!({});

        if let Some(cc) = cost_center {
          org["costCenter"] = cc.clone();
        }

        if let Some(dept) = departamento {
          org["department"] = dept.clone();
        }

        body["organizations"] = json!([org]);
      }

      let url = format!("{}{}", ep.base_url(), gen_id);

      process_elements(
        ep,
        "PUT",
        Some(body),
        Some(url),
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for update_users")
      })?;
    }

    "uuo" => {
      let usr = get_usr!(&data, &mut er1);

      let org_path = check_key(&data, "org", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("/");

      validate_required_fields!(data, er1, "update_user_org", id);

      let url = format!("{}{}", ep.base_url(), usr);

      let body = json!({
        "orgUnitPath": org_path,
      });

      process_elements(
        ep,
        "PUT",
        Some(body),
        Some(url),
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None,
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for update_user_org")
      })?;
    }

    "udu" => {
      let gen_id = get_id!(&data, &mut er1);
      // let usr = get_usr!(&data, &mut er1);

      validate_required_fields!(data, er1, "undelete_users", id);
      let url = format!("{}{}/undelete", ep.base_url(), gen_id);

      let body = json!({});

      process_elements(
        ep,
        "POST",
        Some(body),
        Some(url),
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for undelete_users")
      })?;
    }

    // FIX has irish format but is pulling again from the database, just get them here.
    "cp" | "cpf" | "cps" | "cpfs" => {
      // let gen_id = get_id!(&data, &mut er1);
      let usr = get_usr!(&data, &mut er1);

      validate_required_fields!(data, er1, "password_users", id);

      change_pass(record.clone(), Some(id), usr.to_owned(), p, limiter)
        .await
        .map_err(|e| {
          e.context("Failed to run process_elements for change_pass")
        })?;
    }

    "su" | "usu" => {
      skip_if_admin_usr!(&data, &mut er1, "skipping admin");
      let usr = get_usr!(&data, &mut er1);
      validate_required_fields!(data, er1, "(un)suspend_users", id);

      suspend_usrs(Some(id), usr.to_owned(), p, limiter)
        .await
        .map_err(|e| {
          e.context("Failed to run process_elements for (un)suspend_users")
        })?;
    }

    "au" | "uau" => {
      skip_if_admin_usr!(&data, &mut er1, "skipping admin");
      let usr = get_usr!(&data, &mut er1);
      validate_required_fields!(data, er1, "(un)archive_users", id);

      archive_usrs(Some(id), usr.to_owned(), p, limiter)
        .await
        .map_err(|e| {
          e.context("Failed to run process_elements for (un)archive_users")
        })?;
    }

    "gfe" => {
      let usr = get_usr!(&data, &mut er1);

      validate_required_fields!(data, er1, "forward_emails", id);

      get_forward_emls(usr.to_owned(), limiter, p, Some(id))
        .await
        .cwl("could not get forwarded emails")?;
    }

    "giu" => {
      let usr = get_usr!(&data, &mut er1);

      validate_required_fields!(data, er1, "info_users", id);

      get_info_usrs(usr.to_owned(), limiter, p, Some(id))
        .await
        .cwl("could not get user info")?;
    }

    "iden" | "ides" | "idfr" | "idge" | "idit" => {
      // let gen_id = get_id!(&data, &mut er1);
      let usr = get_usr!(&data, &mut er1);
      validate_required_fields!(data, er1, "change_language", id);
      let url = format!("{}{}", ep.base_url(), usr);

      let idiom = match cmd {
        "iden" => "en-US",
        "ides" => "es-MX",
        "idit" => "it-IT",
        "idge" => "de-DE",
        "idfr" => "fr-FR",
        _ => "es",
      };

      let body = json!({
        "languages": [
          {
            "languageCode": idiom,
            "preference": "preferred"
          }
        ]
      });

      // Use generic abstraction for delete
      process_elements(
        ep,
        "PUT",
        Some(body),
        Some(url),
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for change_language")
      })?;
    }

    "aua" => {
      let gen_id = get_id!(&data, &mut er1);
      let usr = get_usr!(&data, &mut er1);

      let alias = check_key(&data, "aliases", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();

      debug!(
        "AUA DEBUG: gen_id={}, usr={}, alias='{}'",
        gen_id, usr, alias
      );
      debug!(
        "AUA DEBUG: Raw aliases field: {:?}",
        check_key(&data, "aliases", &mut String::new())
      );

      validate_required_fields!(data, er1, "change_aliases", id);

      let url = format!("{}{}/aliases", ep.base_url(), usr);

      let body = json!({
        "id": gen_id,
        "primaryEmail": usr,
        "alias": alias
      });

      process_elements(
        ep,
        "POST",
        Some(body),
        Some(url),
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for change_aliases")
      })?;
    }

    "auaa" => {
      let gen_id = get_id!(&data, &mut er1);
      let usr = get_usr!(&data, &mut er1);

      let alias = check_key(&data, "aliases", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();

      validate_required_fields!(data, er1, "change_aliases", id);

      add_multiple_aliases(
        Some(id),
        gen_id.to_owned(),
        usr.to_owned(),
        alias.to_owned(),
        p,
        limiter,
      )
      .await
      .cwl("Could not add aliases to user")?;
    }

    "duaa" => {
      let gen_id = get_id!(&data, &mut er1);

      let alias = check_key(&data, "aliases", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();

      validate_required_fields!(data, er1, "change_aliases", id);

      delete_multiple_aliases(
        Some(id),
        gen_id.to_owned(),
        alias.to_owned(),
        p,
        limiter,
      )
      .await
      .cwl("Could not delete aliases from user")?;
    }

    "afo" => {
      let usr = get_usr!(&data, &mut er1);

      let fid = check_key(&data, "foto_perfil", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

      validate_required_fields!(data, er1, "add photo", id);

      let url = format!("{}{usr}/photos/thumbnail", ep.base_url());

      let dir = downfotos(usr.to_owned(), &fid, p)
        .await
        .cwl("Could not get file downloaded")?;

      let bod = getbytes(&dir).await.cwl("Could not get file in bytes")?;

      let body = json!({
        "photoData": bod
      });

      // Use generic abstraction for create
      let result = process_elements(
        ep,
        "PUT",
        Some(body),
        Some(url),
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for change_photos")
      });

      // Delete the temporary file immediately after upload attempt
      if let Err(e) = tokio::fs::remove_file(&dir).await {
        debug!(
          "Warning: Failed to delete temporary photo file {}: {}",
          dir, e
        );
      }

      result?;
    }

    "csc" => {
      // let gen_id = get_id!(&data, &mut er1);
      let usr = get_usr!(&data, &mut er1);

      let grp = check_key(&data, "grupo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      validate_required_fields!(data, er1, "change_courses_usr", id);

      // let del_limiter = create_rate_limiter(Ep::Courses.modvlow()); // For deletions

      let cors = get_courses_stu(usr.to_owned(), limiter.clone(), p)
        .await
        .cwl("Could not get courses for student")?;

      debug!("This is cors in csc {:#?}", cors);

      del_courses_stu(usr.to_owned(), cors, p)
        .await
        .cwl("Could not remove student from courses")?;

      add_courses_stu(usr.to_owned(), grp.to_owned(), p)
        .await
        .cwl("Could not add student to courses")?;

      update_db_good(id)
        .await
        .cwl("Failed to update database with process_elements result")?;
    }

    "asc" => {
      // let gen_id = get_id!(&data, &mut er1);
      let usr = get_usr!(&data, &mut er1);

      let grp = check_key(&data, "grupo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      validate_required_fields!(data, er1, "add_courses_usr", id);

      add_courses_stu(usr.to_owned(), grp.to_owned(), p)
        .await
        .cwl("Could not add student to courses")?;

      update_db_good(id)
        .await
        .cwl("Failed to update database with process_elements result")?;
    }

    "dsc" => {
      // let gen_id = get_id!(&data, &mut er1);
      let usr = get_usr!(&data, &mut er1);

      validate_required_fields!(data, er1, "delete_courses_usr", id);

      // let del_limiter = create_rate_limiter(Ep::Courses.modvlow()); // For deletions

      let cors = get_courses_stu(usr.to_owned(), limiter.clone(), p)
        .await
        .cwl("Could not get courses for student")?;

      debug!("This is cors in csc {:#?}", cors);

      del_courses_stu(usr.to_owned(), cors, p)
        .await
        .cwl("Could not remove student from courses")?;

      update_db_good(id)
        .await
        .cwl("Failed to update database with process_elements result")?;
    }

    "ag" => {
      skip_if_admin_usr!(&data, &mut er1, "skipping admin");
      let usr = get_usr!(&data, &mut er1);

      let ngrp = check_key(&data, "modificar_grupo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      validate_required_fields!(data, er1, "add_group", id);

      add_usr_grp(
        usr.to_owned(),
        ngrp.to_owned(),
        limiter.clone(),
        p,
        None,
        None,
        Some(id),
      )
      .await
      .cwl("Could not add user to group")?;
    }

    "amg" => {
      // skip_if_admin_usr!(&data, &mut er1, "skipping admin");

      let grp = check_key(&data, "grupo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let eml = check_key(&data, "correo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let rol = check_key(&data, "rol", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let typ = check_key(&data, "tipo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      validate_required_fields!(data, er1, "add_external_members_group", id);

      add_usr_grp(
        eml.to_owned(),
        grp.to_owned(),
        limiter.clone(),
        p,
        Some(rol),
        Some(typ),
        Some(id),
      )
      .await
      .cwl("Could not add external user to group")?;
    }

    "rg" => {
      skip_if_admin_usr!(&data, &mut er1, "skipping admin");
      let usr = get_usr!(&data, &mut er1);

      let ngrp = check_key(&data, "modificar_grupo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      validate_required_fields!(data, er1, "add_group", id);

      // del_usr_grp now uses global Groups deletion limiter internally
      del_usr_grp(usr.to_owned(), ngrp.to_owned(), p, Some(id))
        .await
        .cwl("Could not remove user from group")?;
    }

    "rmg" => {
      let grp = check_key(&data, "grupo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let eml = check_key(&data, "correo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      validate_required_fields!(data, er1, "remove_external_from_group", id);

      // del_usr_grp now uses global Groups deletion limiter internally
      del_usr_grp(eml.to_owned(), grp.to_owned(), p, Some(id))
        .await
        .cwl("Could not remove user from group")?;
    }

    "umg" => {
      // skip_if_admin_usr!(&data, &mut er1, "skipping admin");

      let grp = check_key(&data, "grupo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let ngrp = check_key(&data, "modificar_grupo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let eml = check_key(&data, "correo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let rol = check_key(&data, "rol", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let typ = check_key(&data, "tipo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      validate_required_fields!(data, er1, "update_external_from_group", id);

      // del_usr_grp now uses global Groups deletion limiter internally
      del_usr_grp(eml.to_owned(), grp.to_owned(), p, Some(id.clone()))
        .await
        .cwl("Could not remove user from group")?;

      add_usr_grp(
        eml.to_owned(),
        ngrp.to_owned(),
        limiter.clone(),
        p,
        Some(rol),
        Some(typ),
        Some(id),
      )
      .await
      .cwl("Could not add external user to group")?;
    }

    "rga" => {
      skip_if_admin_usr!(&data, &mut er1, "skipping admin");
      let usr = get_usr!(&data, &mut er1);

      validate_required_fields!(data, er1, "delete_all_grps_usr", id);

      // get_grps_usr now uses global Groups listing limiter internally
      let grps = get_grps_usr(usr.to_owned(), p)
        .await
        .cwl("Could not get groups for user")?;

      debug!("COGAL DEBUG: Groups found for user {}: {:?}", usr, grps);

      del_usr_grps_all(usr.to_owned(), grps, p)
        .await
        .cwl("Could not remove user from group")?;

      update_db_good(id)
        .await
        .cwl("Failed to update database with process_elements result")?;
    }

    // NOTE cogalc is to remove user from all groups and does NOT work for lsopue for error in google with their domain
    "cog" | "cogal" | "cogc" | "cogalc" => {
      skip_if_admin_usr!(&data, &mut er1, "skipping admin");

      let usr = get_usr!(&data, &mut er1);

      let ngrp = check_key(&data, "modificar_grupo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let validation_field = match cmd {
        "cog" => "change_orggrp",
        "cogc" => "change_orggrpcls",
        "cogal" => "change_orgallgrp",
        "cogalc" => "change_orgallgrpcls",
        _ => "change_orggrp",
      };

      validate_required_fields!(data, er1, validation_field, id);

      let grp = check_key(&data, "grupo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      if ["cogalc", "cogal"].contains(&cmd) {
        // get_grps_usr now uses global Groups listing limiter internally
        let grps = get_grps_usr(usr.to_owned(), p)
          .await
          .cwl("Could not get groups for user")?;
        debug!("COGAL DEBUG: Groups found for user {}: {:?}", usr, grps);
        // NOTE the for loop here in delete all users does not work for lsopue
        del_usr_grps_all(usr.to_owned(), grps, p)
          .await
          .cwl("Could not remove user from group")?;
      } else if !grp.is_empty() {
        // del_usr_grp now uses global Groups deletion limiter internally
        del_usr_grp(usr.to_owned(), grp.to_owned(), p, None)
          .await
          .cwl("Could not remove user from group")?;
      }

      add_usr_grp(
        usr.to_owned(),
        ngrp.to_owned(),
        limiter.clone(),
        p,
        None,
        None,
        None,
      )
      .await
      .cwl("Could not add user to group")?;

      if ngrp.contains("profes") {
        add_usr_grpprof(usr.to_owned(), p)
          .await
          .cwl("Could not add user to group")?;
      }

      if ["cogc", "cogalc"].contains(&cmd) {
        // Use appropriate Courses API rate limiter for listing courses
        // This command is Users endpoint but needs to call Courses API for sub-operations
        let list_limiter = create_rate_limiter(Ep::Courses.modlst()); // 40 ops/sec for Courses listing
        let cors = get_courses_stu(usr.to_owned(), list_limiter.clone(), p)
          .await
          .cwl("Could not get courses for student")?;

        del_courses_stu(usr.to_owned(), cors, p)
          .await
          .cwl("Could not remove student from courses")?;

        add_courses_stu(usr.to_owned(), ngrp.to_owned(), p)
          .await
          .cwl("Could not add student to courses")?;
      }

      let path = get_org_from_orgbase(p.abr.clone(), ngrp.to_owned())
        .await
        .cwl("Could not get org from group in orgbase")?;

      if !path.is_empty() {
        let url = format!("{}{}", ep.base_url(), usr);

        let body = json!({
          "orgUnitPath": path,
        });

        process_elements(
          ep,
          "PUT",
          Some(body),
          Some(url),
          id,
          p.tsy.clone(),
          limiter,
          p,
          oauth_cancellation.clone(),
          None, // No custom query params
        )
        .await
        .map_err(|e| {
          e.context("Failed to run process_elements for change_orggrp")
        })?;
      } else {
        warn!(
          "No orgpath found for group '{}', request will be skipped",
          ngrp
        );
      }
    }

    "coga" => {
      skip_if_admin_usr!(&data, &mut er1, "skipping admin");
      let usr = get_usr!(&data, &mut er1);

      validate_required_fields!(data, er1, "change_orggrpyear", id);

      let grp = check_key(&data, "grupo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      if !grp.is_empty() {
        // del_usr_grp now uses global Groups deletion limiter internally
        del_usr_grp(usr.to_owned(), grp.to_owned(), p, None)
          .await
          .cwl("Could not remove user from group")?;
      }

      let ngrade = get_next_grade(grp);
      let ngrade_str = ngrade.unwrap_or_default();

      if !ngrade_str.is_empty() {
        add_usr_grp(
          usr.to_owned(),
          ngrade_str.clone(),
          limiter.clone(),
          p,
          None,
          None,
          None,
        )
        .await
        .cwl("Could not add user to group")?;

        let path =
          get_org_from_orgbase(p.abr.clone(), ngrade_str.clone()).await?;

        if !path.is_empty() {
          let url = format!("{}{}", ep.base_url(), usr);

          let body = json!({
            "orgUnitPath": path,
          });

          process_elements(
            ep,
            "PUT",
            Some(body),
            Some(url),
            id,
            p.tsy.clone(),
            limiter,
            p,
            oauth_cancellation.clone(),
            None, // No custom query params
          )
          .await
          .map_err(|e| {
            e.context("Failed to run process_elements for change_orggrpyear")
          })?;
        } else {
          warn!(
            "No orgpath found for group '{}', request will be skipped",
            ngrade_str
          );
        }
      } else {
        warn!(
          "User no longer in a group that corresponds \
          to preconfigured list for group '{}', \
          request will be skipped",
          grp
        );
      }
    }

    "cogsu" | "cogusu" | "cogsua" | "cogusua" => {
      skip_if_admin_usr!(&data, &mut er1, "skipping admin");
      let usr = get_usr!(&data, &mut er1);

      let ngrp = check_key(&data, "modificar_grupo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      validate_required_fields!(data, er1, "change_orggrpsu", id);

      let grp = check_key(&data, "grupo", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      if ["cogsua", "cogusua"].contains(&cmd) {
        // get_grps_usr now uses global Groups listing limiter internally
        let grps = get_grps_usr(usr.to_owned(), p)
          .await
          .cwl("Could not get groups for user")?;
        del_usr_grps_all(usr.to_owned(), grps, p)
          .await
          .cwl("Could not remove user from group")?;
      } else if !grp.is_empty() {
        // del_usr_grp now uses global Groups deletion limiter internally
        del_usr_grp(usr.to_owned(), grp.to_owned(), p, None)
          .await
          .cwl("Could not remove user from group")?;
      }

      add_usr_grp(
        usr.to_owned(),
        ngrp.to_owned(),
        limiter.clone(),
        p,
        None,
        None,
        None,
      )
      .await
      .cwl("Could not get file downloaded")?;

      if ngrp.contains("profes") {
        add_usr_grpprof(usr.to_owned(), p)
          .await
          .cwl("Could not add user to group")?;
      }

      suspend_usrs(None, usr.to_owned(), p, limiter.clone())
        .await
        .map_err(|e| {
          e.context("Failed to run process_elements for change_org in cog(u)su")
        })?;

      let path = get_org_from_orgbase(p.abr.clone(), ngrp.to_owned()).await?;

      if !path.is_empty() {
        let url = format!("{}{}", ep.base_url(), usr);

        let body = json!({
          "orgUnitPath": path,
        });

        process_elements(
          ep.clone(),
          "PUT",
          Some(body),
          Some(url),
          id.clone(),
          p.tsy.clone(),
          limiter.clone(),
          p,
          oauth_cancellation.clone(),
          None, // No custom query params
        )
        .await
        .map_err(|e| {
          e.context("Failed to run process_elements for change_org in cog(u)su")
        })?;
      } else {
        warn!(
          "No orgpath found for group '{}', org change will be skipped",
          ngrp
        );
      }
    }

    "ali" => {
      // change to ali
      let usr = check_key(&data, "correo_usuario", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let abrlic = check_key(&data, "abrlic", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      debug!(
        "ali command: Processing record id='{}', abr='{}', abrlic='{}', correo_usuario='{}'",
        id, p.abr, abrlic, usr
      );

      validate_required_fields!(data, er1, "add_license", id);

      let (npid, nsid) = get_lic_info(&p.abr, abrlic)
        .await
        .cwl("Could not get new lics info")?;

      let url = format!("{}{}/sku/{}/user", ep.base_url(), npid, nsid);

      let body = json!({"userId": usr});

      process_elements(
        ep,
        "POST",
        Some(body),
        Some(url),
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for add_license")
      })?;
    }

    "uli" => {
      let usr = check_key(&data, "correo_usuario", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let abrlic = check_key(&data, "abrlic", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let opid = check_key(&data, "pid", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let osid = check_key(&data, "skuid", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      debug!(
        "uli command: Processing record id='{}', abr='{}', abrlic='{}', correo_usuario='{}', opid='{}', osid='{}'",
        id, p.abr, abrlic, usr, opid, osid
      );

      validate_required_fields!(data, er1, "update_license", id);

      let (npid, nsid) = get_lic_info(&p.abr, abrlic)
        .await
        .cwl("Could not get new lics info")?;

      debug!(
        "uli command: Got new license info - npid='{}', nsid='{}'",
        npid, nsid
      );

      let url = format!("{}{}/sku/{}/user/{}", ep.base_url(), opid, osid, usr);

      debug!("uli command: URL for license update: {}", url);

      let body = json!({
          "userId": usr,
          "productId": npid,
          "skuId": nsid
      });

      debug!("uli command: Request body: {:?}", body);

      process_elements(
        ep,
        "PUT",
        Some(body),
        Some(url),
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for update_license")
      })?;
    }

    "cc" => {
      let mut body = json!({
        "name": check_key(&data, "nombre", &mut er1).unwrap_or(&Value::Null),
        "ownerId": Value::String(get_usr!(&data, &mut er1).to_string()),
        "courseState":"ACTIVE",
      });

      validate_required_fields!(data, er1, "create class", id);

      if let Some(desc_heading) = data.get("seccion")
        && !desc_heading.is_null()
      {
        body["section"] = desc_heading.clone();
      }

      if let Some(desc_heading) = data.get("desc_cabeza")
        && !desc_heading.is_null()
      {
        body["descriptionHeading"] = desc_heading.clone();
      }
      if let Some(description) = data.get("descripcion")
        && !description.is_null()
      {
        body["description"] = description.clone();
      }
      if let Some(room) = data.get("sala")
        && !room.is_null()
      {
        body["room"] = room.clone();
      }

      process_elements(
        ep,
        "POST",
        Some(body),
        None,
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for create_groups")
      })?;
    }

    "atu" => {
      let usr = check_key(&data, "correo_alumno", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let body = json!({
        "studentId": usr,
        "invitedEmailAddress": check_key(&data, "correo_tutor", &mut er1).unwrap_or(&Value::Null),
      });

      validate_required_fields!(data, er1, "create class", id);

      let url = format!("{}{}/guardianInvitations", ep.base_url(), usr);

      process_elements(
        ep,
        "POST",
        Some(body),
        Some(url),
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for create_groups")
      })?;
    }

    "dtui" => {
      let usr = check_key(&data, "correo_alumno", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let invitation_id = check_key(&data, "id", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      validate_required_fields!(data, er1, "update guardian invitation", id);

      let body = json!({
        "state": "COMPLETE"
      });

      let qry = json!({
        "updateMask":"state",
      });

      let url = format!(
        "{}{}/guardianInvitations/{}",
        ep.base_url(),
        usr,
        invitation_id
      );

      process_elements(
        ep,
        "PATCH",
        Some(body),
        Some(url),
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        Some(qry),
      )
      .await
      .map_err(|e| {
        e.context(
          "Failed to run process_elements for update guardian invitation",
        )
      })?;
    }

    "ac" | "rac" => {
      let gen_id = get_id!(&data, &mut er1);

      let nom = check_key(&data, "nombre", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      validate_required_fields!(data, er1, "archive class", id);

      let archive = match cmd {
        "ac" => "ARCHIVED",
        "rac" => "ACTIVE",
        _ => "ACTIVE",
      };

      let body = json!({
        "courseState":archive,
        "name":nom,
      });

      let url = format!("{}{}", ep.base_url(), gen_id);

      process_elements(
        ep,
        "PUT",
        Some(body),
        Some(url),
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for (un)archive courses")
      })?;
    }

    "uc" => {
      let gen_id = get_id!(&data, &mut er1);

      let mut body = json!({
        "courseState":"ACTIVE",
      });

      if let Some(name) = data.get("nombre")
        && !name.is_null()
      {
        body["name"] = name.clone();
      }
      if let Some(section) = data.get("seccion")
        && !section.is_null()
      {
        body["section"] = section.clone();
      }
      if let Some(desc_heading) = data.get("desc_cabeza")
        && !desc_heading.is_null()
      {
        body["descriptionHeading"] = desc_heading.clone();
      }
      if let Some(description) = data.get("descripcion")
        && !description.is_null()
      {
        body["description"] = description.clone();
      }
      if let Some(room) = data.get("sala")
        && !room.is_null()
      {
        body["room"] = room.clone();
      }

      let url = format!("{}{}", ep.base_url(), gen_id);

      process_elements(
        ep,
        "PUT",
        Some(body),
        Some(url),
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for upddate_courses")
      })?;
    }

    "oc" => {
      let gen_id = get_id!(&data, &mut er1);

      let cordos = check_key(&data, "correo_dos", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      validate_required_fields!(data, er1, "change_owner", id);

      add_teacher_cor(cordos.to_owned(), gen_id.to_owned(), limiter.clone(), p)
        .await
        .cwl("Could not get file downloaded")?;

      let body = json!({
        "ownerId": cordos,
      });

      let url = format!("{}{}", ep.base_url(), gen_id);

      let oc_query_params = Some(json!({
        "updateMask": "ownerId",
      }));

      process_elements(
        ep,
        "PATCH",
        Some(body),
        Some(url),
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        oc_query_params,
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for change_owner")
      })?;
    }

    // NOTE aguas!!! each sub function many times needs a global limiter, or the limiter passed gets mulitplied by the join all method!
    "racl" => {
      let gen_id = get_id!(&data, &mut er1);
      // let usr = get_usr!(&data, &mut er1);
      // let del_limiter = create_rate_limiter(Ep::Courses.modvlow()); // For deletions

      let grp = check_key(&data, "sala", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      debug!("This is grp in racl {:#?}", grp);
      validate_required_fields!(data, er1, "add_rem_studs_cour", id);

      let stus = get_stus_course(gen_id.to_owned(), limiter.clone(), p)
        .await
        .cwl("Could not get courses for student")?;

      debug!("This is stus in csc {:#?}", stus);

      del_stus_course(gen_id.to_owned(), stus, p)
        .await
        .cwl("Could not remove student from courses")?;

      let membs = get_membs_grp(grp.to_owned(), limiter.clone(), p)
        .await
        .cwl("Could not get members from group")?;

      add_stus_course(gen_id.to_owned(), membs, p)
        .await
        .cwl(&format!("Could not add students to course {}", gen_id))?;

      update_db_good(id)
        .await
        .cwl("Failed to update database with process_elements result")?;
    }

    "apc" | "adsc" => {
      let gen_id = get_id!(&data, &mut er1);

      let cordos = check_key(&data, "correo_dos", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      validate_required_fields!(data, er1, "add_prostu", id);

      let body = json!({
        "courseId": gen_id,
        "userId": cordos
      });

      let url = if cmd == "apc" {
        format!("{}{}/teachers", ep.base_url(), gen_id)
      } else {
        format!("{}{}/students", ep.base_url(), gen_id)
      };

      process_elements(
        ep,
        "POST",
        Some(body),
        Some(url),
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for addprostu to courses")
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

    "scb" | "uscb" | "rcb" | "fcb" => {
      let gen_id = get_id!(&data, &mut er1);

      validate_required_fields!(data, er1, "modify_chromebooks", id);

      let url =
        format!("{}:batchChangeStatus", ep.base_url().trim_end_matches('/'));

      debug!("Chromebook batch status change URL: {}", url);

      let reason = if cmd == "rcb" {
        "DEPROVISION_REASON_RETIRING_DEVICE"
      } else if cmd == "fcb" {
        "DEPROVISION_REASON_SAME_MODEL_REPLACEMENT"
      } else {
        ""
      };

      let action = if cmd == "scb" {
        "CHANGE_CHROME_OS_DEVICE_STATUS_ACTION_DISABLE"
      } else if cmd == "uscb" {
        "CHANGE_CHROME_OS_DEVICE_STATUS_ACTION_REENABLE"
      } else if ["rcb", "fcb"].contains(&cmd) {
        "CHANGE_CHROME_OS_DEVICE_STATUS_ACTION_DEPROVISION"
      } else {
        ""
      };

      let mut body = json!({
        "deviceIds": gen_id,
        "changeChromeOsDeviceStatusAction": action,
      });

      if ["rcb", "fcb"].contains(&cmd) {
        body["deprovisionReason"] = json!(reason);
      }

      process_elements(
        ep,
        "POST",
        Some(body),
        Some(url),
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for modifying chromebooks")
      })?;
    }

    "upcb" => {
      let gen_id = get_id!(&data, &mut er1);

      let org = check_key(&data, "ruta_org", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let usra = check_key(&data, "usuario_anotado", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      validate_required_fields!(data, er1, "modify_chromebooks", id);

      let asset = check_key(&data, "id_asset", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      change_org_cb(gen_id.to_owned(), org.to_owned(), limiter.clone(), p)
        .await
        .map_err(|e| {
          e.context("Failed to run process_elements for change_org_cb")
        })?;

      let url = format!("{}{}", ep.base_url(), gen_id);

      let body = json!({
        "annotatedUser": usra,
        "annotatedAssetId": asset,
      });

      process_elements(
        ep,
        "PUT",
        Some(body),
        Some(url),
        id,
        p.tsy.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for modifying chromebooks")
      })?;
    }

    "cca" | "mca" => {
      let gen_id = if cmd == "mca" {
        get_id!(&data, &mut er1)
      } else {
        ""
      };

      let usr = get_usr!(&data, &mut er1);

      let mut body = json!({
        "summary": check_key(&data, "nombre", &mut er1).unwrap_or(&Value::Null),
      });

      validate_required_fields!(data, er1, "create/modify calendars", id);

      let tsyusr = get_tok(usr.to_owned(), "yes")
        .await
        .cwl("Failed to generate token for user for files")?;

      if let Some(description) = data.get("descripcion")
        && !description.is_null()
      {
        body["description"] = description.clone();
      }

      if let Some(loc) = data.get("ubicacion")
        && !loc.is_null()
      {
        body["location"] = loc.clone();
      }

      // if let Some(loc) = data.get("id_color")
      //   && !loc.is_null()
      // {
      //   body["colorId"] = loc.clone();
      // }

      let default_timezone = Value::String("America/Mexico_City".to_string());
      let timezone = data
        .get("zona_horaria")
        .filter(|z| !z.is_null())
        .unwrap_or(&default_timezone);
      body["timeZone"] = timezone.clone();

      let (method, url) = if cmd == "cca" {
        ("POST", None)
      } else {
        ("PATCH", Some(format!("{}{}", ep.base_url(), gen_id)))
      };

      process_elements(
        ep,
        method,
        Some(body),
        url,
        id,
        tsyusr,
        limiter,
        p,
        oauth_cancellation.clone(),
        None, // No custom query params
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for create/modify calendars")
      })?;
    }

    "cev" | "mev" => {
      let gen_id = if cmd == "mev" {
        get_id!(&data, &mut er1)
      } else {
        ""
      };

      let cal_id = check_key(&data, "id_calendario", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let mut body = json!({
        "summary": check_key(&data, "nombre_evento", &mut er1).unwrap_or(&Value::Null),
      });

      validate_required_fields!(data, er1, "create/modify events", id);

      // Set guest permissions with defaults after validation (don't add to error list for optional fields)
      body["guestsCanInviteOthers"] = data
        .get("pueden_invitar")
        .filter(|v| {
          !(v.is_null() || v.is_string() && v.as_str().unwrap_or("").is_empty())
        })
        .unwrap_or(&Value::Bool(true))
        .clone();
      body["guestsCanModify"] = data
        .get("pueden_modificar")
        .filter(|v| {
          !(v.is_null() || v.is_string() && v.as_str().unwrap_or("").is_empty())
        })
        .unwrap_or(&Value::Bool(true))
        .clone();
      body["guestsCanSeeOtherGuests"] = data
        .get("pueden_verles")
        .filter(|v| {
          !(v.is_null() || v.is_string() && v.as_str().unwrap_or("").is_empty())
        })
        .unwrap_or(&Value::Bool(true))
        .clone();
      body["anyoneCanAddSelf"] = data
        .get("pueden_agregarse")
        .filter(|v| {
          !(v.is_null() || v.is_string() && v.as_str().unwrap_or("").is_empty())
        })
        .unwrap_or(&Value::Bool(true))
        .clone();

      let usr = get_usr!(&data, &mut er1);

      let tsyusr = get_tok(usr.to_owned(), "yes")
        .await
        .cwl("Failed to generate token for user for files")?;

      if let Some(description) = data.get("descripcion")
        && !description.is_null()
      {
        body["description"] = description.clone();
      }

      if let Some(loc) = data.get("ubicacion")
        && !loc.is_null()
      {
        body["location"] = loc.clone();
      }

      body["transparency"] = data
        .get("transparencia")
        .filter(|v| {
          !(v.is_null() || v.is_string() && v.as_str().unwrap_or("").is_empty())
        })
        .unwrap_or(&Value::String("opaque".to_string()))
        .clone();

      body["visibility"] = data
        .get("visibilidad")
        .filter(|v| {
          !(v.is_null() || v.is_string() && v.as_str().unwrap_or("").is_empty())
        })
        .unwrap_or(&Value::String("default".to_string()))
        .clone();

      if let Some(colid) = data.get("id_color")
        && !(colid.is_null()
          || colid.is_string() && colid.as_str().unwrap_or("").is_empty())
      {
        body["colorId"] = colid.clone();
      }

      body["status"] = data
        .get("estatus")
        .filter(|v| {
          !(v.is_null() || v.is_string() && v.as_str().unwrap_or("").is_empty())
        })
        .unwrap_or(&Value::String("confirmed".to_string()))
        .clone();

      // Handle date and time for calendar events
      let default_timezone = Value::String("America/Mexico_City".to_string());
      let timezone = data
        .get("zona_horaria")
        .filter(|z| !z.is_null())
        .unwrap_or(&default_timezone);

      // Get current date in YYYY-MM-DD format if dates are empty
      let current_date = Local::now().format("%Y-%m-%d").to_string();

      let fecha_inicia = data
        .get("fecha_inicia")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or(&current_date);

      let fecha_termina = data
        .get("fecha_termina")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or(&current_date);

      // Check if dates contain time (look for 'T' or time patterns)
      let start_has_time = fecha_inicia.contains('T')
        || fecha_inicia.matches(':').count() >= 2
        || (fecha_inicia.matches(':').count() == 1 && fecha_inicia.len() > 10);

      let end_has_time = fecha_termina.contains('T')
        || fecha_termina.matches(':').count() >= 2
        || (fecha_termina.matches(':').count() == 1
          && fecha_termina.len() > 10);

      // Create start and end objects based on whether time is specified
      if start_has_time || end_has_time {
        // Timed event - use dateTime format
        body["start"] = json!({
          "dateTime": fecha_inicia,
          "timeZone": timezone
        });
        body["end"] = json!({
          "dateTime": fecha_termina,
          "timeZone": timezone
        });
      } else {
        // All-day event - use date format only
        body["start"] = json!({
          "date": fecha_inicia
        });

        // For all-day events, end date should be the next day
        let end_date = if fecha_termina == fecha_inicia {
          // If same day, make it next day for all-day event
          let date = NaiveDate::parse_from_str(fecha_inicia, "%Y-%m-%d")
            .unwrap_or_else(|_| Local::now().date_naive());
          (date + Duration::days(1)).format("%Y-%m-%d").to_string()
        } else {
          fecha_termina.to_string()
        };

        body["end"] = json!({
          "date": end_date
        });
      }

      // Handle attendees
      let mut attendees_array: Vec<Value> = Vec::new();
      if let Some(attendees_str) = data
        .get("invitados")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
      {
        for email in attendees_str.split(',') {
          let email = email.trim();
          if !email.is_empty() {
            let attendee = json!({
              "email": email
            });
            attendees_array.push(attendee);
          }
        }
      }
      body["attendees"] = Value::Array(attendees_array);

      // Handle attachments
      let mut attachments_array: Vec<Value> = Vec::new();
      if let Some(attachments_str) = data
        .get("anexos")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
      {
        for file_url in attachments_str.split(',') {
          let file_url = file_url.trim();
          if !file_url.is_empty() {
            let attachment = json!({
              "fileUrl": file_url
            });
            attachments_array.push(attachment);
          }
        }
      }
      body["attachments"] = Value::Array(attachments_array);

      let (method, url) = if cmd == "cev" {
        ("POST", Some(format!("{}{}/events", ep.base_url(), &cal_id)))
      } else {
        (
          "PATCH",
          Some(format!("{}{}/events/{}", ep.base_url(), &cal_id, &gen_id)),
        )
      };

      let cev_mev_query_params = Some(json!({
        "sendUpdates": "all",
        "supportsAttachments": "true",
      }));

      process_elements(
        ep,
        method,
        Some(body),
        url,
        id,
        tsyusr,
        limiter,
        p,
        oauth_cancellation.clone(),
        cev_mev_query_params,
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for create/modify calendars")
      })?;
    }

    "ct" | "mt" => {
      let gen_id = if cmd == "mt" {
        get_id!(&data, &mut er1)
      } else {
        ""
      };

      let course_id = check_key(&data, "id_curso", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let usr = get_usr!(&data, &mut er1);

      let tsyusr = get_tok(usr.to_owned(), "yes")
        .await
        .cwl("Failed to generate token for user for files")?;

      let mut body = json!({
        "title": check_key(&data, "titulo", &mut er1).unwrap_or(&Value::Null),
        "workType": "ASSIGNMENT",
      });

      validate_required_fields!(data, er1, "create/modify assignments", id);

      if let Some(description) = data.get("descripcion")
        && !description.is_null()
      {
        body["description"] = description.clone();
      }

      if let Some(max_points) = data.get("puntos")
        && !max_points.is_null()
      {
        body["maxPoints"] = max_points.clone();
      }

      if let Some(state) = data.get("estado")
        && !state.is_null()
      {
        body["state"] = state.clone();
      }

      if let Some(topic_id) = data.get("id_tema")
        && !topic_id.is_null()
      {
        body["topicId"] = topic_id.clone();
      }

      // NOTE: assigneeMode is NOT supported in updateMask according to Google Classroom API docs
      // Only include for ct (create), default to ALL_STUDENTS if empty/missing
      if cmd == "ct" {
        if let Some(assignee_mode) = data.get("modo")
          && !assignee_mode.is_null()
          && !assignee_mode.as_str().unwrap_or("").trim().is_empty()
        {
          body["assigneeMode"] = assignee_mode.clone();
        } else {
          body["assigneeMode"] = json!("ALL_STUDENTS");
        }
      }

      if let Some(submission_mode) = data.get("modificable")
        && !submission_mode.is_null()
        && !submission_mode.as_str().unwrap_or("").trim().is_empty()
      {
        body["submissionModificationMode"] = submission_mode.clone();
      }

      // Handle scheduled time (RFC 3339 format with Z suffix required)
      if let Some(scheduled_time) = data.get("fecha_programada")
        && !scheduled_time.is_null()
        && !scheduled_time.as_str().unwrap_or("").trim().is_empty()
      {
        let time_str = scheduled_time.as_str().unwrap_or("");
        // Ensure timestamp ends with Z if it doesn't have a timezone
        let formatted_time = if !time_str.ends_with('Z')
          && !time_str.contains('+')
          && !time_str.contains("UTC")
        {
          format!("{}Z", time_str)
        } else {
          time_str.to_string()
        };
        body["scheduledTime"] = json!(formatted_time);
      }

      // Handle due date and time - parse combined string into separate date/time objects
      if let Some(fecha_vencida) = data.get("fecha_vencida")
        && !fecha_vencida.is_null()
        && !fecha_vencida.as_str().unwrap_or("").trim().is_empty()
      {
        let fecha_str = fecha_vencida.as_str().unwrap_or("");

        // Parse "YYYY-MM-DD HH:MM:SS" format from database
        if let Some((date_part, time_part)) = fecha_str.split_once('T') {
          // Parse date part "YYYY-MM-DD"
          if let Ok(parsed_date) =
            chrono::NaiveDate::parse_from_str(date_part, "%Y-%m-%d")
          {
            body["dueDate"] = json!({
              "year": parsed_date.year(),
              "month": parsed_date.month(),
              "day": parsed_date.day()
            });

            // Parse time part "HH:MM:SS"
            if let Ok(parsed_time) =
              chrono::NaiveTime::parse_from_str(time_part, "%H:%M:%S")
            {
              body["dueTime"] = json!({
                "hours": parsed_time.hour(),
                "minutes": parsed_time.minute(),
                "seconds": parsed_time.second()
              });
            }
          }
        } else if let Ok(parsed_date) =
          chrono::NaiveDate::parse_from_str(fecha_str, "%Y-%m-%d")
        {
          // Date only format
          body["dueDate"] = json!({
            "year": parsed_date.year(),
            "month": parsed_date.month(),
            "day": parsed_date.day()
          });
        }
      }

      // Handle grade category
      if let Some(category_name) = data.get("nombre_categoria")
        && !category_name.is_null()
      {
        let mut grade_category = json!({
          "name": category_name
        });

        if let Some(category_weight) = data.get("peso_categoria")
          && !category_weight.is_null()
        {
          // Convert percentage to weight (multiply by 10000 as per API)
          if let Some(weight_num) = category_weight.as_f64() {
            grade_category["weight"] = json!(weight_num * 10000.0);
          }
        }

        body["gradeCategory"] = grade_category;
      }

      // Handle grading period
      if let Some(grading_period) = data.get("id_periodo")
        && !grading_period.is_null()
      {
        body["gradingPeriodId"] = grading_period.clone();
      }

      // Handle materials array (links, forms, youtube, drive files)
      let mut materials: Vec<Value> = Vec::new();

      if let Some(links_url) = data.get("links_url")
        && !links_url.is_null()
        && !links_url.as_str().unwrap_or("").trim().is_empty()
      {
        for url in links_url.as_str().unwrap_or("").split(',') {
          let url = url.trim();
          if !url.is_empty() {
            materials.push(json!({
              "link": {
                "url": url
              }
            }));
          }
        }
      }

      // BUG careful this could still be broken in googles servers, use drive file link instead
      if let Some(links_form) = data.get("links_formulario")
        && !links_form.is_null()
        && !links_form.as_str().unwrap_or("").trim().is_empty()
      {
        for form_url in links_form.as_str().unwrap_or("").split(',') {
          let form_url = form_url.trim();
          if !form_url.is_empty() {
            materials.push(json!({
              "form": {
                "formUrl": form_url
              }
            }));
          }
        }
      }

      if let Some(links_youtube) = data.get("links_youtube")
        && !links_youtube.is_null()
        && !links_youtube.as_str().unwrap_or("").trim().is_empty()
      {
        for youtube_url in links_youtube.as_str().unwrap_or("").split(',') {
          let youtube_url = youtube_url.trim();
          if !youtube_url.is_empty() {
            // Extract YouTube video ID from URL
            let video_id = if youtube_url.contains("youtube.com/watch?v=") {
              youtube_url
                .split("youtube.com/watch?v=")
                .nth(1)
                .and_then(|s| s.split('&').next())
                .unwrap_or("")
            } else if youtube_url.contains("youtu.be/") {
              youtube_url
                .split("youtu.be/")
                .nth(1)
                .and_then(|s| s.split('?').next())
                .unwrap_or("")
            } else {
              // If no standard pattern found, use the URL as-is as video ID
              youtube_url
            };

            materials.push(json!({
              "youtubeVideo": {
                "id": video_id,
                "alternateLink": youtube_url
              }
            }));
          }
        }
      }

      // NOTE you CANNOT modify the share_mode after creating the assignment!!!!
      if let Some(links_drive) = data.get("links_drive")
        && !links_drive.is_null()
        && !links_drive.as_str().unwrap_or("").trim().is_empty()
      {
        for drive_url in links_drive.as_str().unwrap_or("").split(',') {
          let drive_url = drive_url.trim();
          if !drive_url.is_empty() {
            // Check if URL ends with <<v>> for VIEW mode, <<e>> for EDIT mode
            let (clean_url, share_mode) = if drive_url.ends_with("<<v>>") {
              (drive_url.trim_end_matches("<<v>>"), "VIEW")
            } else if drive_url.ends_with("<<e>>") {
              (drive_url.trim_end_matches("<<e>>"), "EDIT")
            } else {
              (drive_url, "STUDENT_COPY")
            };

            // Extract file ID from drive URL if possible
            let file_id = if clean_url.contains("/file/d/") {
              // Handle drive.google.com/file/d/FILEID/ URLs
              clean_url
                .split("/file/d/")
                .nth(1)
                .and_then(|s| s.split('/').next())
                .unwrap_or(clean_url)
            } else if clean_url.contains("/d/") {
              // Handle docs.google.com/*/d/FILEID/* URLs (docs, sheets, presentations)
              clean_url
                .split("/d/")
                .nth(1)
                .and_then(|s| s.split('/').next())
                .unwrap_or(clean_url)
            } else {
              clean_url
            };

            materials.push(json!({
              "driveFile": {
                "driveFile": {
                  "id": file_id,
                  "alternateLink": clean_url
                },
                "shareMode": share_mode
              }
            }));
          }
        }
      }

      if !materials.is_empty() {
        body["materials"] = Value::Array(materials);
      }

      // Handle multiple choice options
      if let Some(opciones) = data.get("opciones")
        && !opciones.is_null()
        && !opciones.as_str().unwrap_or("").trim().is_empty()
      {
        let choices: Vec<Value> = opciones
          .as_str()
          .unwrap_or("")
          .split(',')
          .map(|choice| Value::String(choice.trim().to_string()))
          .filter(|choice| !choice.as_str().unwrap_or("").is_empty())
          .collect();

        if !choices.is_empty() {
          body["multipleChoiceQuestion"] = json!({
            "choices": choices
          });
          body["workType"] = json!("MULTIPLE_CHOICE_QUESTION");
        }
      }

      let (method, url) = if cmd == "ct" {
        (
          "POST",
          Some(format!("{}{}/courseWork", ep.base_url(), &course_id)),
        )
      } else {
        (
          "PATCH",
          Some(format!(
            "{}{}/courseWork/{}",
            ep.base_url(),
            &course_id,
            &gen_id
          )),
        )
      };

      // Build query parameters - different for ct vs mt
      let ct_mt_query_params = if cmd == "mt" {
        Some(build_mt_query_params(&data))
      } else {
        // For ct (create), just fields
        Some(json!({
          "fields": "*",
        }))
      };

      process_elements(
        ep,
        method,
        Some(body),
        url,
        id,
        tsyusr.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        ct_mt_query_params,
      )
      .await
      .map_err(|e| {
        e.context(
          "Failed to run process_elements for create/modify assignments",
        )
      })?;
    }

    "mgr" => {
      let gen_id = get_id!(&data, &mut er1);
      let usr = get_usr!(&data, &mut er1);

      let course_id = check_key(&data, "id_curso", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      let course_work_id = check_key(&data, "id_tarea", &mut er1)
        .and_then(|v| v.as_str())
        .unwrap_or("");

      validate_required_fields!(data, er1, "modify grades", id);

      // Check if assignment was created by developer (bot)
      // if let Some(bot_created) = data.get("bot_hecho") {
      //   if bot_created != &Value::Bool(true) {
      //     er1.push_str(
      //       "Cannot modify grades for assignments not created by the bot; ",
      //     );
      //   }
      // } else {
      //   er1.push_str(
      //     "bot_hecho field is required to validate assignment ownership; ",
      //   );
      // }

      // if !er1.is_empty() {
      //   bail!(er1);
      // }

      let tsyusr = get_tok(usr.to_owned(), "yes")
        .await
        .cwl("Failed to generate token for user for grades")?;

      let mut body = json!({});

      if let Some(assigned_grade) = data.get("calificacion")
        && !assigned_grade.is_null()
      {
        body["assignedGrade"] = assigned_grade.clone();
      }

      let method = "PATCH";
      let url = Some(format!(
        "{}{}/courseWork/{}/studentSubmissions/{}",
        ep.base_url(),
        &course_id,
        &course_work_id,
        &gen_id
      ));

      let mgrqry = json!({
        "updateMask": "assignedGrade"
      });

      process_elements(
        ep,
        method,
        Some(body),
        url,
        id,
        tsyusr.clone(),
        limiter,
        p,
        oauth_cancellation.clone(),
        Some(mgrqry),
      )
      .await
      .map_err(|e| {
        e.context("Failed to run process_elements for modify grades")
      })?;
    }

    _ => {
      return Ok(());
    }
  }

  Ok(())
}
