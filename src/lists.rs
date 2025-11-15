#![allow(dead_code)]

pub use super::aux_sur::*;
pub use super::sheets::*;
use crate::AppResult;
use crate::aux_process::get_perm_driveid;
use crate::goauth::get_tok;
use crate::limiters::get_global_drive_limiter;
use crate::surrealstart::Pets;
use crate::tracer::ContextExt;
use crate::{bail, debug, error};
use governor::RateLimiter;
use governor::clock::DefaultClock;
use governor::state::InMemoryState;
use governor::state::NotKeyed;
use serde_json::{Value, json};
use std::error::Error;
use std::sync::Arc;

#[allow(clippy::too_many_arguments)]
pub async fn lists<T: Into<Ep>>(
  ep: T,
  rqry: Option<&Value>,
  uarg: Option<String>, // url with args if needed
  tse: String,
  abr: String,
  limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
  extra: Option<Value>,
  p: &Pets,
) -> AppResult<()> {
  let ep = ep.into();

  let url = if uarg.is_none() {
    ep.base_url().to_string()
  } else {
    uarg.unwrap_or("".to_string())
  };

  debug!("This is url in modslists {:?}", url);

  let mut tok = Option::<String>::None;
  let mut all_records = Vec::new(); // Accumulate all paginated results

  loop {
    // Debug current token info before request
    if let Some(ref t) = tok {
      let is_bearer = t.starts_with("Bearer ");
      let preview: String = t.chars().take(18).collect();
      debug!(
        is_bearer = is_bearer,
        token_len = t.len(),
        token_preview = %preview,
        "lists: using page token for pagination"
      );
    }
    let mut query_params = rqry.cloned().unwrap_or_else(|| json!({}));
    if let Some(ref token) = tok {
      query_params["pageToken"] = json!(token);
    }

    debug!("Making API request to URL: {}", url);
    debug!("Query parameters: {:#?}", query_params);

    let au_build =
      req_build("GET", &url, Some(&tse), Some(&query_params), None)
        .cwl("Could not create the auth_builder for the lists function")?;

    debug!("Final request URL base: {}", url);
    let auth_is_bearer = tse.starts_with("Bearer ");
    let auth_preview: String = tse.chars().take(18).collect();
    debug!(
      auth_is_bearer = auth_is_bearer,
      auth_len = tse.len(),
      auth_preview = %auth_preview,
      "lists: auth header metadata"
    );

    // debug!("This is auth_builder {:#?}", au_build);

    // For Drive API (Files), use global rate limiter instead of per-user limiter
    // NOTE it appears that the root folder does not get an id until google drive has at least one folder in it.
    let start_wait = std::time::Instant::now();
    if ["lv", "ldu", "ldd", "lca", "le"].contains(&p.cmd.as_str()) {
      get_global_drive_limiter().until_ready().await;
    } else if ["ls", "lsa", "lp", "lpa", "lt", "lta", "la", "lc"]
      .contains(&p.cmd.as_str())
    {
      // COMMENTED OUT - CLASSROOM LIMITER NOT NEEDED FOR THIS PROJECT
      // get_global_classroom_limiter().until_ready().await;
      limiter.until_ready().await;
    } else {
      limiter.until_ready().await;
    }
    let wait_time = start_wait.elapsed();
    if wait_time.as_millis() > 10 {
      debug!("Rate limiter wait time: {:?} for cmd={}", wait_time, p.cmd);
    }

    // Retry logic for timeout errors with exponential backoff
    let mut retry_count = 0;
    let max_retries = 3;
    let base_delay = std::time::Duration::from_secs(2);

    let api_start = std::time::Instant::now();
    let res = loop {
      match au_build.try_clone().unwrap().send().await {
        Ok(response) => {
          let api_time = api_start.elapsed();
          debug!(
            "Google API request completed in {:?} for cmd={}",
            api_time, p.cmd
          );
          break response;
        }
        Err(e) => {
          // Capture more detailed error information
          let error_details = format!("{:?}", e);
          let error_source = e.source().map(|s| format!("{:?}", s));
          let error_kind = if e.is_timeout() {
            "timeout"
          } else if e.is_connect() {
            "connection"
          } else if e.is_request() {
            "request"
          } else if e.is_redirect() {
            "redirect"
          } else if e.is_decode() {
            "decode"
          } else {
            "unknown"
          };

          // Only retry on timeout errors
          if e.is_timeout() && retry_count < max_retries {
            retry_count += 1;
            let delay = base_delay * 2_u32.pow(retry_count - 1);

            debug!(
              url = %url,
              retry_count = retry_count,
              delay_secs = delay.as_secs(),
              "Retrying request after timeout"
            );

            tokio::time::sleep(delay).await;
            continue;
          }

          error!(
            url = %url,
            error = %e,
            error_details = %error_details,
            error_source = ?error_source,
            error_kind = %error_kind,
            retry_count = retry_count,
            "Failed to send HTTP request to Google API after retries"
          );
          return Err(e.into());
        }
      }
    };

    match res.status().as_u16() {
      200 => {
        let response_text =
          res.text().await.cwl("Failed to get response text")?;
        // debug!("Raw API response text: {}", response_text);

        let rfin: Value = serde_json::from_str(&response_text)
          .cwl("Failed to parse JSON response for lists function")?;

        // debug!("This is rfin in lists {:#?}", rfin);

        // careful, the order here matters dont change this!
        let epres = if url.contains(ep.res_obs_guar(true)) {
          ep.res_obs_guar(true)
        } else if url.contains(ep.res_obs_guar(false)) {
          ep.res_obs_guar(false)
        } else {
          ep.res_obs()
        };

        let res_data = rfin[epres].as_array().cloned().unwrap_or_else(Vec::new);

        debug!(
          "Google API returned {} records on this page",
          res_data.len()
        );

        // Accumulate records instead of inserting immediately
        all_records.extend(res_data);

        tok = rfin["nextPageToken"].as_str().map(String::from);
      }
      500 => {
        let error_text = match res.text().await {
          Ok(text) => text,
          Err(e) => {
            error!(
              url = %url,
              status = 500,
              error = %e,
              "Failed to read error response body"
            );
            format!("Could not read response body: {}", e)
          }
        };
        error!(
          url = %url,
          status = 500,
          error_body = %error_text,
          "Google API request failed with status 500"
        );
        bail!(
          "La solicitud a la API de Google falló con estado 500: {}. Este es un error temporal de la API de Google. Por favor ejecuta el comando de nuevo.",
          error_text
        );
      }
      status => {
        let error_text = match res.text().await {
          Ok(text) => text,
          Err(e) => {
            error!(
              url = %url,
              status = %status,
              error = %e,
              "Failed to read error response body"
            );
            format!("Could not read response body: {}", e)
          }
        };
        error!(
          url = %url,
          status = %status,
          error_body = %error_text,
          "Google API request failed with non-200 status"
        );
        bail!(
          "Google API request failed with status {}: {}",
          status,
          error_text
        );
      }
    };

    if tok.is_none() {
      break;
    }
  }

  // After all pages are fetched, do a single batch insert
  if !all_records.is_empty() {
    debug!(
      "All pagination complete. Inserting {} total records in batch",
      all_records.len()
    );
    google_to_gdatabase(all_records, abr.clone(), ep.clone(), extra)
      .await
      .cwl("Failed to batch insert all paginated records")?;
  }

  let commands_requiring_db_processing = [
    "lo", "lg", "lu", "lud", "ly", "lc", "lgrap", "lgrav", "lcb", "lre", "ldr",
    "lsp",
  ];
  if commands_requiring_db_processing.contains(&p.cmd.as_str()) {
    gdatabase_to_sheetsdb(abr.clone(), ep.clone()).await.cwl(
      "Failed to process database to sheets database for lists function",
    )?;
  }

  Ok(())
}

pub async fn list_orgs(
  limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
  p: &Pets,
) -> AppResult<()> {
  let ep = Ep::Orgs;

  let qrys = json!({
    "type": "all",
  });

  lists(
    ep,
    Some(&qrys),
    None,
    p.tsy.clone(),
    p.abr.clone(),
    limiter,
    None,
    p,
  )
  .await
  .cwl("Failed to process list for list_orgs")?;
  Ok(())
}

pub async fn list_groups(
  limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
  p: &Pets,
) -> AppResult<()> {
  let ep = Ep::Groups;

  let qrys = json! ({
    "customer": "my_customer",
    "maxResults": "500",
  });

  lists(
    ep,
    Some(&qrys),
    None,
    p.tsy.clone(),
    p.abr.clone(),
    limiter,
    None,
    p,
  )
  .await
  .cwl("Failed to process lists for lists_groups")?;
  Ok(())
}

pub async fn list_users(
  limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
  p: &Pets,
) -> AppResult<()> {
  let ep = Ep::Users;

  let numorgs = count_db(p.abr.clone(), Ep::Orgs)
    .await
    .cwl("Could not get num orgs")?;

  if numorgs == 0 {
    list_orgs(limiter.clone(), p)
      .await
      .cwl("Could not get num orgs")?;
  }

  let numgrps = count_db(p.abr.clone(), Ep::Orgs)
    .await
    .cwl("Couldnt get num orgs")?;

  if numgrps == 0 {
    list_groups(limiter.clone(), p)
      .await
      .cwl("Could not get num grps")?;
  }

  let qrys = qrys_users().await.cwl("Failed to get qrys for users")?;

  debug!("This is qrys in list_users {:#?}", qrys);

  lists(
    ep,
    Some(&qrys),
    None,
    p.tsy.clone(),
    p.abr.clone(),
    limiter,
    None,
    p,
  )
  // lists(Some(&qrys), None, ep, None, "yes")
  .await
  .cwl("Failed to process lists for lists_usuarios")?;

  refresh_orgs_and_clear_paths(p)
    .await
    .cwl("Failed to unmarks rows in orgs")?;

  Ok(())
}

// Refresh orgs and get selected org path (data.do = 'd') if present
async fn refresh_orgs_and_get_selected_path(
  p: &Pets,
) -> AppResult<Option<String>> {
  // Refresh orgs from Sheets so the marked row is current
  frshts(p.tsy.clone(), p.abr.clone(), Ep::Orgs, p.spshid.clone())
    .await
    .cwl("Failed to refresh orgs from sheet")?;

  // Then get the selected org path (row with data.do = 'd')
  let path = get_org_from_orgbase(p.abr.clone(), "".to_string())
    .await
    .cwl("Failed to get selected org path after refresh")?;

  if path.is_empty() {
    Ok(None)
  } else {
    Ok(Some(path))
  }
}

async fn refresh_orgs_and_clear_paths(p: &Pets) -> AppResult<Option<String>> {
  let ep: Ep = Ep::Orgs;

  let qry = "
    update type::table($table)
    set data.do = 'x';
  ";

  DB.query(qry)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", p.abr.clone()))
    .await
    .cwl("Failed to update orgs to put all unmarked")?;

  toshs(p.tsy.clone(), p.abr.clone(), ep, p.spshid.clone())
    .await
    .cwl("Failed to save to sheets")?;

  // Then get the selected org path (row with data.do = 'd')
  let path = get_org_from_orgbase(p.abr.clone(), "".to_string())
    .await
    .cwl("Failed to get selected org path after refresh")?;

  if path.is_empty() {
    Ok(None)
  } else {
    Ok(Some(path))
  }
}

pub async fn qrys_users() -> AppResult<Value> {
  // NOTE for meets recordings, you have to make two petitions or a different complex query, there is no way to get meet recording from one simple petition.

  let p = PETS
    .get()
    .await
    .cwl("Could not get pet fields for get_fol_id")?;

  let args: Vec<String> = p
    .params
    .split(',')
    .map(|s| s.trim().to_lowercase())
    .collect();

  debug!("qrys_users: params = {:?}", p.params);
  debug!("qrys_users: args = {:?}, length = {}", args, args.len());

  let qry = if args.is_empty() {
    debug!("qrys_users: taking empty args branch");

    let sel_path = refresh_orgs_and_get_selected_path(&p)
      .await
      .cwl("Failed to refresh and get selected org for qrys_users")?;

    if let Some(path) = sel_path {
      debug!(
        "qrys_users: 1 arg suspension with selected org path = {:?}",
        path
      );
      format!("orgUnitPath='{path}'")
    } else {
      String::new()
    }
  } else if args.len() == 1 {
    // Check if this is a suspension command first
    if ["su", "usu"].contains(&args[0].as_str()) {
      debug!(
        "qrys_users: taking 1 arg suspension branch, arg[0] = {:?}",
        args[0]
      );
      let suspended = if args[0] == "su" { "true" } else { "false" };

      // Prefer using selected org if available
      let sel_path = refresh_orgs_and_get_selected_path(&p)
        .await
        .cwl("Failed to refresh and get selected org for qrys_users")?;

      if let Some(path) = sel_path {
        debug!(
          "qrys_users: 1 arg suspension with selected org path = {:?}",
          path
        );
        format!("isSuspended={suspended} orgUnitPath='{path}'")
      } else {
        // Fall back to only suspension filter
        format!("isSuspended={suspended}")
      }
    } else {
      // This is an org command
      debug!(
        "qrys_users: taking 1 arg org branch, arg[0] = {:?}",
        args[0]
      );
      let path = get_org_from_orgbase(p.abr.clone(), args[0].clone())
        .await
        .cwl("Failed to get org for qrys")?;
      debug!("qrys_users: 1 arg org path = {:?}", path);
      if !path.is_empty() {
        format!("orgUnitPath='{path}'")
      } else {
        String::new()
      }
    }
  } else if args.len() == 2 {
    debug!(
      "qrys_users: taking 2 arg branch, args[0] = {:?}, args[1] = {:?}",
      args[0], args[1]
    );

    // Check if first arg is suspension status
    if ["su", "usu"].contains(&args[0].as_str()) {
      let suspended = if args[0] == "su" { "true" } else { "false" };

      // Check if second arg contains "/" (org path) or not (group code)
      debug!("qrys_users: 2 arg looking up group code: {}", args[1]);
      let path = get_org_from_orgbase(p.abr, args[1].clone())
        .await
        .cwl("Failed to get org for qrys")?;

      debug!(
        "qrys_users: 2 arg final path = {:?}, suspended = {}",
        path, suspended
      );

      if !path.is_empty() {
        format!("isSuspended={suspended} orgUnitPath='{path}'")
      } else {
        format!("isSuspended={suspended}")
      }
    } else {
      // Handle other 2-arg cases if needed
      String::new()
    }
  } else if args.len() == 3 {
    debug!(
      "qrys_users: taking 3 arg branch, args[0] = {:?}, args[1] = {:?}, args[2] = {:?}",
      args[0], args[1], args[2]
    );
    let suspended = if args[1] == "su" { "true" } else { "false" };
    let path = get_org_from_orgbase(p.abr, args[2].clone())
      .await
      .cwl("Failed to get org for qrys")?;
    debug!(
      "qrys_users: 3 arg path = {:?}, suspended = {}",
      path, suspended
    );
    format!("isSuspended={suspended} orgUnitPath='{path}'")
  } else {
    debug!("qrys_users: taking default branch for {} args", args.len());
    String::new()
  };

  // "showDeleted":"true",

  let mut qrys = if p.cmd == "lu" {
    json!({
      "customer": "my_customer",
      "maxResults": "500",
      "viewType": "admin_view",
      "projection": "FULL",
    })
  } else {
    json!({
      "customer": "my_customer",
      "maxResults": "500",
      "viewType": "admin_view",
      "showDeleted":"true",
      "projection": "FULL",
    })
  };

  if !qry.is_empty() {
    // Use the query format without "and": isSuspended=false orgUnitPath='/path'
    qrys["query"] = json!(qry);
  }

  debug!("qrys_users: final qry = {:?}", qry);
  debug!("qrys_users: final qrys = {:?}", qrys);

  Ok(qrys)
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

pub async fn list_files(
  record: Value,
  limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
  p: &Pets,
) -> AppResult<()> {
  let ep = Ep::Files;

  // let p = PETS
  //   .get()
  //   .await
  //   .cwl("Could not get pet fields for list_files")?;

  let (id, data) = extract_record_parts(record.clone())?;
  let mut er1 = String::new();

  debug!("=== LIST_FILES DEBUG ===");
  debug!("Full record: {:#?}", record);
  debug!("Extracted ID: {:?}", id);
  debug!("Extracted data: {:#?}", data);

  let drive_id = check_key(&data, "id", &mut er1)
    .and_then(|v| v.as_str())
    .unwrap_or("")
    .to_string();

  let usr = if p.cmd != "ldd" {
    check_key(&data, "correo", &mut er1)
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string()
  } else {
    get_perm_driveid(drive_id.to_owned(), p.tsy.clone())
      .await
      .cwl("Could not get email organizer")?
  };

  debug!("=== EMAIL EXTRACTION DEBUG ===");
  debug!("Extracted usr from correo field: '{}'", usr);
  debug!("Error string er1: '{}'", er1);

  let tsyusr = get_tok(usr.clone(), "yes")
    .await
    .cwl("Failed to generate token for user for files")?;

  let (qrys, extra_data) = if p.cmd == "lv" {
    let namfol = "Meet Recordings".to_string();
    let filid =
      get_fol_id_from_name(usr.clone(), tsyusr.clone(), namfol.clone(), p)
        .await
        .cwl("Failed to get folder ID for lmeets")?;

    if filid.is_none() {
      debug!(
        "No folder ID found for lv command, skipping file listing for user: {}",
        usr
      );
      return Ok(());
    }

    let qrys = qrys_files(Some(usr.clone()), filid, None, p, None)
      .await
      .cwl("Failed to get qrys for files")?;

    let extra = Some(json!({"key1": namfol}));
    (qrys, extra)
  } else if p.cmd == "ldd" {
    debug!("Processing ldd command with drive_id: '{}'", drive_id);
    let qrys = qrys_files(None, None, None, p, Some(drive_id.clone()))
      .await
      .cwl("Failed to get qrys for files")?;
    let extra = Some(json!({"key1": drive_id.clone()}));
    (qrys, extra)
  } else {
    let qrys = qrys_files(Some(usr.clone()), None, None, p, None)
      .await
      .cwl("Failed to get qrys for files")?;
    (qrys, None)
  };

  debug!("This is qrys in list_files {:#?}", qrys);

  lists(
    ep,
    Some(&qrys),
    None,
    tsyusr,
    p.abr.clone(),
    limiter,
    extra_data,
    p,
  )
  .await
  .cwl("Failed to process lists for lists_files")?;

  debug!("Successfully got list of files for {:?}", usr);

  update_db_good(id.clone())
    .await
    .cwl("Failed to update database with successful list_members result")?;

  Ok(())
}

pub async fn get_fol_id_from_name(
  usr: String,
  tsy: String,
  name: String,
  p: &Pets,
) -> AppResult<Option<String>> {
  let ep = Ep::Files;

  let qrys = qrys_files(Some(usr), None, Some(name), p, None)
    .await
    .cwl("Failed to get qrys for get_fol_id")?;

  debug!("This is qrys in get_fol_id {:#?}", qrys);

  let au_build = req_build("GET", ep.base_url(), Some(&tsy), Some(&qrys), None)
    .cwl("Could not create the auth_builder for the get_fol_id")?;

  get_global_drive_limiter().until_ready().await;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request for get_fol_id")?;

  match res.status().as_u16() {
    200 => {
      let rfin: Value = res
        .json()
        .await
        .cwl("Failed to parse JSON response for get_fol_id")?;

      debug!("This is rfin in get_fol_id {:#?}", rfin);

      let res_data = rfin[ep.res_obs()]
        .as_array()
        .cloned()
        .unwrap_or_else(Vec::new);

      let filid = res_data
        .first()
        .and_then(|item| item.get("id"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
      Ok(filid)
    }
    status => {
      let error_text = res.text().await.cwl("Failed to read error response")?;
      error!(status = %status, error = %error_text, "HTTP request failed");
      Ok(None)
    }
  }
}

pub async fn qrys_files(
  usr: Option<String>,
  id: Option<String>,
  name: Option<String>,
  p: &Pets,
  drive_id: Option<String>,
) -> AppResult<Value> {
  // NOTE for meets recordings, you have to make two petitions or a different complex query, there is no way to get meet recording from one simple petition.

  // let p = PETS
  //   .get()
  //   .await
  //   .cwl("Could not get pet fields for get_fol_id")?;

  let typs: Vec<&str> = p.params.split(',').collect();

  let typ = match typs[0] {
    "docs" => "application/vnd.google-apps.document",
    "sheets" => "application/vnd.google-apps.spreadsheet",
    "slides" => "application/vnd.google-apps.presentation",
    "forms" => "application/vnd.google-apps.form",
    "sites" => "application/vnd.google-apps.site",
    "images" => "image/png, image/jpeg",
    "videos" => "video/mp4",
    "fols" => "application/vnd.google-apps.folder",
    _ => "NO MYMETYPE",
  };

  let q = if typ != "NO MYMETYPE" && usr.is_some() {
    format!(
      "mimeType='{typ}' and '{}' in owners",
      usr.unwrap_or_default()
    )
  } else if typs[0].contains("-") {
    format!("modifiedTime > '{}T12:00:00-00:00'", typs[0])
  } else if name.is_some() && usr.is_some() {
    format!(
      "name = '{}' and mimeType = 'application/vnd.google-apps.folder' and '{}' in owners",
      name.unwrap_or_default(),
      usr.unwrap_or_default()
    )
  } else if p.cmd == "lv" && usr.is_some() && id.is_some() {
    format!(
      "'{}' in parents and '{}' in owners",
      id.unwrap_or_default(),
      usr.unwrap_or_default()
    )
  } else if usr.is_some() {
    // For ldu command and other cases where we want to list all files owned by user
    format!("'{}' in owners", usr.unwrap_or_default())
  } else {
    // handle default case or return empty query
    String::new() // or some default query
  };

  let mut qrys = json! ({
    "supportsAllDrives":"true",
    "includeItemsFromAllDrives":"true",
    "fields":"*",
    "spaces":"drive",
    "orderBy":"quotaBytesUsed desc",
    "pageSize": "1000",
  });

  if !q.is_empty() {
    qrys["q"] = json!(q);
  }

  if drive_id.is_some() {
    qrys["driveId"] = json!(drive_id);
    qrys["corpora"] = json!("drive");
  } else {
    qrys["corpora"] = json!("allDrives");
  }

  Ok(qrys)
}

pub async fn list_drives(
  limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
  p: &Pets,
) -> AppResult<()> {
  let ep = Ep::Drives;

  // let p = PETS
  //   .get()
  //   .await
  //   .cwl("Could not get pet fields for list_drives")?;

  let qrys = json! ({
    "pageSize": "100",
    "useDomainAdminAccess": "true",
    "fields": "drives(\
      id,name,kind,createdTime,colorRgb,themeId,\
      backgroundImageLink,hidden,orgUnitId,\
      restrictions(\
        adminManagedRestrictions,driveMembersOnly,domainUsersOnly,\
        sharingFoldersRequiresOrganizerPermission,downloadRestriction\
      )\
    )",
  });

  lists(
    ep,
    Some(&qrys),
    None,
    p.tsy.clone(),
    p.abr.clone(),
    limiter,
    None,
    p,
  )
  .await
  .cwl("Failed to process lists for list_drives")?;

  Ok(())
}
