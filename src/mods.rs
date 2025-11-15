#![allow(dead_code)]

pub use super::apis::*;
pub use super::lists::*;
pub use super::mod_process::*;
use crate::AppResult;
use crate::jsonresfil::get_template_id_index;
use crate::limiters::*;
use crate::surrealstart::{
  DB, PETS, Pets, SP, TSHID, getnumqry, is_list_command,
};
use crate::tracer::ContextExt;
use crate::{bail, debug, error, info, warn};
use futures::future::join_all;
use governor::RateLimiter;
use governor::clock::DefaultClock;
use governor::state::InMemoryState;
use governor::state::NotKeyed;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

async fn update_sheets_with_remaining_rows<T: Into<Ep>>(
  ep: T,
  p: &Pets,
) -> AppResult<()> {
  let ep = ep.into();

  // Query for rows that still need to be processed (data.do = 'd')
  let remaining_qry = "
    select data from type::table($table)
    where abr = $abr and data.do = 'd'
  ";

  let mut response = DB
    .query(remaining_qry)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", p.abr.clone()))
    .await
    .cwl("Failed to query remaining rows for Google Sheets update")?;

  let remaining_rows: Vec<Value> = response
    .take(0)
    .cwl("Failed to take remaining rows query result")?;

  debug!(
    "Found {} remaining rows that need to be processed for cmd={}",
    remaining_rows.len(),
    p.cmd
  );

  if !remaining_rows.is_empty() {
    // Call toshs to update Google Sheets with the remaining rows
    toshs(p.tsy.clone(), p.abr.clone(), ep, p.spshid.clone())
      .await
      .cwl("Failed to update Google Sheets with remaining rows")?;

    debug!(
      "Successfully updated Google Sheets with {} remaining rows for cmd={}",
      remaining_rows.len(),
      p.cmd
    );
  }

  Ok(())
}

pub async fn runmods(
  record: Value,
  limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
  p: &Pets,
  oauth_cancellation: Option<Arc<AtomicBool>>, // Optional cancellation token for OAuth expiration
) -> AppResult<()> {
  // debug!("SUSPEND DEBUG: runmods called with record: {:#?}", record);
  debug!("p.cmd = {}", p.cmd);

  match record.get("cmd").and_then(Value::as_str) {
    Some(command) => {
      // Validate command using centralized lists
      if !crate::surrealstart::is_valid_command(command) {
        debug!("Invalid command in runmods: {}", command);
        return Ok(());
      }
      debug!(
        "Processing valid command in runmods: {} (is_list_command: {})",
        command,
        is_list_command(command)
      );

      match command {
        "lo" => list_orgs(limiter, p).await.cwl("Failed to list orgs")?,
        "lg" => list_groups(limiter, p).await.cwl("Failed to list groups")?,
        "lu" => list_users(limiter, p).await.cwl("Failed to list users")?,
        "ldr" => list_drives(limiter, p).await.cwl("Failed to list drives")?,
        "lm" => list_members(record.clone(), limiter, p)
          .await
          .cwl("Failed to list members")?,
        "lv" => list_files(record.clone(), limiter, p)
          .await
          .cwl("Failed to list meet recordings")?,
        "ldu" => {
          list_files(record.clone(), limiter, p)
            .await
            .cwl("Failed to list files for user")?;
        }
        "ldd" => {
          list_files(record.clone(), limiter, p)
            .await
            .cwl("Failed to list files for drive")?;
        }
        "lma" => list_members(record.clone(), limiter, p)
          .await
          .cwl("Failed to list all members")?,

        // "cadsh" => change_one_sheet_in_all(record.clone())
        //   .await
        //   .cwl("Failed to copy admin sheet to other spreadsheets")?,
        cmd if crate::surrealstart::get_other_commands().contains(&cmd) => {
          if let Err(e) = process_command(
            record.clone(),
            limiter,
            p,
            oauth_cancellation.clone(),
          )
          .await
          {
            let error_msg =
              format!("Failed to process command '{}': {:#}", command, e);
            error!(command = %command, error = %e, "Failed to process command");
            return Err(anyhow::anyhow!(error_msg));
          }
        }

        _ => {
          error!(command = %command, "Unknown command");
          bail!("Unknown command: {}", command);
        }
      }
    }
    _ => {
      error!(record = ?record, "Missing command in record");
      bail!("Missing command in record: {:?}", record);
    }
  }

  Ok(())
}

pub async fn get_updatable_rows<T: Into<Ep>>(
  ep: T,
  all_rows: &str,
) -> AppResult<Vec<Value>> {
  let ep = ep.into();

  debug!("This is ep in get_updatable_rows {:#?}", ep);

  // Get pet fields at the top to use throughout the function
  let p = PETS
    .get()
    .await
    .cwl("Could not get pet fields for get_updatable_rows")?;

  debug!(
    "get_updatable_rows called with: cmd={}, abr={}, all_rows={}",
    p.cmd, p.abr, all_rows
  );

  let template_data = if p.cmd == "cadsh" {
    get_template_id_index()
      .await
      .cwl("Could not get sheet id and index for template")?
  } else {
    None
  };

  debug!(
    "This is template_data in get_updatable_rows {:#?}",
    template_data
  );

  let update_qry = if all_rows == "yes" {
    "
      update type::table($table)
      set cmd = $cmd, data.do = 'd'
      where abr = $abr
      return abr, cmd, data, ers, type::string(id) as id;
    "
  } else if p.cmd == "cadsh" && template_data.is_some() {
    "
      update type::table($table)
      set cmd = $cmd, data.shid = $shid, data.index = $index
      where data.do = 'd'
        and data.spreadsheet_id != $template_spreadsheet_id
      return abr, cmd, data, ers, type::string(id) as id;
    "
  } else {
    "
      update type::table($table)
      set cmd = $cmd
      where abr = $abr and data.do = 'd'
      return abr, cmd, data, ers, type::string(id) as id;
    "
  };

  let mut response = if p.cmd == "cadsh" {
    if let Some((shid, index)) = template_data {
      DB.query(update_qry)
        .bind(("table", ep.table_sheet()))
        .bind(("cmd", p.cmd.clone()))
        .bind(("shid", shid))
        .bind(("index", index))
        .bind(("template_spreadsheet_id", &*TSHID))
        .await
        .cwl("Failed to add shid and index to petition")?
    } else {
      debug!(
        "NOT adding shid and index - cmd={}, template_data.is_some()={}",
        p.cmd, false
      );
      debug!("Using update query for non-cadsh: {}", update_qry);
      DB.query(update_qry)
        .bind(("table", ep.table_sheet()))
        .bind(("abr", p.abr.clone()))
        .bind(("cmd", p.cmd.clone()))
        .await
        .cwl("Failed to update all rows")?
    }
  } else {
    debug!(
      "NOT adding shid and index - cmd={}, template_data.is_some()={}",
      p.cmd,
      template_data.is_some()
    );
    debug!("Using update query for non-cadsh: {}", update_qry);
    DB.query(update_qry)
      .bind(("table", ep.table_sheet()))
      .bind(("abr", p.abr.clone()))
      .bind(("cmd", p.cmd.clone()))
      .await
      .cwl("Failed to update all rows")?
  };

  let updated_rows: Vec<Value> =
    response.take(0).cwl("Failed to take select query result")?;

  debug!(
    "get_updatable_rows: Found {} rows marked for processing",
    updated_rows.len()
  );

  let rcou3 = count_db(p.abr.clone(), ep.clone())
    .await
    .cwl("Could not get count from database")?;

  debug!(
    "get_updatable_rows: Total database count after update: {}",
    rcou3
  );

  debug!(
    "get_updatable_rows: Returning {} updatable rows",
    updated_rows.len()
  );

  Ok(updated_rows)
}

pub async fn mods<T: Into<Ep>>(
  ep: T,
  lom: &str,
  all: &str,
  p: &Pets,
) -> AppResult<()> {
  let start = Instant::now();

  let ep = ep.into();

  let qps = if lom == "mod" {
    ep.modvel()
  } else if lom == "modl" {
    // this is for deleting when the velocity has to be lower sometimes
    ep.modvlow()
  } else {
    ep.modlst()
  };

  // COMMENTED OUT - CHAT NOT NEEDED FOR THIS PROJECT
  // let epch1 = if ["mod", "modl"].contains(&lom) {
  //   ep.chstmod(p.cmd.clone())
  // } else {
  //   chstlinit(ep.clone(), p.cmd.clone())
  //     .await
  //     .cwl("Could not get chat init text")?
  // };
  //
  // chres(epch1, p.sp.clone(), p.tsn.clone())
  //   .await
  //   .cwl("Failed to send start mod chat message")?;

  // NOTE deldbs empties both databases, you do NOT empty the google database for just mods
  if lom == "l_one" {
    deldbs(p.abr.clone(), ep.clone())
      .await
      .cwl("could not delete rows from databases")?;
  }

  if lom == "lmul" {
    let epts = chtoshep(ep.clone(), p.cmd.clone())
      .await
      .cwl("Could not get ep for toshs")?;
    deldbs(p.abr.clone(), epts.clone())
      .await
      .cwl("could not delete rows from databases")?;

    let rcou22 = count_db(p.abr.clone(), epts.clone())
      .await
      .cwl("Could not get count from database")?;

    debug!("This is rcou22 in mods {:#?}", rcou22);
  }

  let mut errs1: Vec<String> = Vec::new();

  if ["mod", "lmul", "modl"].contains(&lom) {
    // only for debugging, rcou1 not used itself
    let rcou1 = count_db(p.abr.clone(), ep.clone())
      .await
      .cwl("Could not get count from database")?;
    debug!("This is rcou1 in mods before frshts {:#?}", rcou1);

    if p.cmd == "cadsh" {
      // Authorization filter: only allow cadsh command from authorized space
      if p.sp != format!("spaces/{}", &*SP) {
        error!("Unauthorized cadsh command attempted from space: {}", p.sp);
        bail!(
          "Unauthorized: cadsh command can only be executed from the authorized space"
        );
      }
      debug!("Running frshtsadmin for cadsh");
      frshtsadmin(Some("cadsh"))
        .await
        .cwl("Failed to get rows from admin sheet")?;
    // } else if p.cmd == "petsum" {
    //   let txs = get_petition_summary_by_domain(p)
    //     .await
    //     .cwl("Could not get petition summary.")?;

    //   chres(txs, "".to_string(), p.tsn.clone())
    //     .await
    //     .cwl("Could not send chat to space.")?;
    } else {
      debug!("Running frshts with cmd={}, abr={}", p.cmd, p.abr);
      frshts(p.tsy.clone(), p.abr.clone(), ep.clone(), p.spshid.clone())
        .await
        .cwl("Failed to refresh sheets")?;
    }

    // NOTE all is a variable here, not fixed.
    debug!("Calling get_updatable_rows with ep={:?}, all={}", ep, all);
    let updated_rows = get_updatable_rows(ep.clone(), all)
      .await
      .cwl("Failed to get updatable rows")?;

    debug!("get_updatable_rows returned {} rows", updated_rows.len());

    // Special handling for create orgs from orgbase command - must be processed synchronously
    if p.cmd == "cobo" {
      debug!(
        "Processing create orgs from orgbase command synchronously with {} rows",
        updated_rows.len()
      );
      // COMMENTED OUT - NOT NEEDED FOR THIS PROJECT
      // if let Err(e) = create_orgs_orgbase(updated_rows, p, &mut errs1).await {
      //   let error_msg = format!("create_orgs_orgbase failed: {}", e);
      //   error!(error = %error_msg, "Create orgs orgbase failed");
      //   errs1.push(error_msg);
      // }
      // Skip the parallel processing since we handled the rows synchronously
    } else {
      // debug!("This is results from database in mods {:#?}", updated_rows);

      // let results = DB
      //   .query("select * from type::table($table);")
      //   .bind(("table", ep.table_sheet()))
      //   .await?;
      // debug!("This is results from database in mods {:#?}", results);

      let num = updated_rows.len();
      debug!("Found {} rows to process for cmd={}", num, p.cmd);

      // ============== parallel part starts here ========================
      if num != 0 {
        use crate::aux_sur::get_all_org_group_mappings;
        use crate::goauth::{
          check_token_needs_refresh, clear_token_cache, get_tok,
        };
        use crate::surrealstart::ORG_GROUP_CACHE;

        // Proactively refresh OAuth token if it will expire soon (within 10 minutes)
        if check_token_needs_refresh(
          &format!("{}@{}", p.abr, p.dom),
          "yes",
          600,
        ) {
          debug!(
            "OAuth token expiring soon for {}, proactively refreshing",
            p.abr
          );
          clear_token_cache();
          match get_tok(format!("{}@{}", p.abr, p.dom), "yes").await {
            Ok(_) => {
              debug!("Successfully refreshed OAuth token for {}", p.abr);
            }
            Err(e) => {
              warn!("Failed to proactively refresh OAuth token: {}", e);
            }
          }
        }

        if matches!(
          p.cmd.as_str(),
          "giu"
            | "cu"
            | "cud"
            | "cup"
            | "cuf"
            | "uu"
            | "uuo"
            | "cog"
            | "coga"
            | "cogc"
            | "cogal"
            | "cogalc"
            | "cogsu"
            | "cogusu"
            | "cogsua"
            | "cogusua"
        ) {
          debug!("Pre-loading org-group mappings for cmd={}", p.cmd);
          match get_all_org_group_mappings(p.abr.clone()).await {
            Ok(mappings) => {
              let mut cache = ORG_GROUP_CACHE.lock().await;
              cache.insert(p.abr.clone(), mappings);
              debug!(
                "Cached {} org-group mappings for abr {}",
                cache.get(&p.abr).map(|m| m.len()).unwrap_or(0),
                p.abr
              );
            }
            Err(e) => {
              warn!("Failed to pre-load org-group mappings: {}", e);
            }
          }
        }

        let limiter = create_rate_limiter(qps);
        debug!(
          qps = %qps,
          num_operations = %num,
          cmd = %p.cmd,
          "Initialized SHARED rate limiter for {} concurrent operations at {} ops/sec",
          num, qps
        );

        // Create shared OAuth cancellation token for all futures
        let oauth_cancellation = Arc::new(AtomicBool::new(false));
        debug!(
          "Created OAuth cancellation token for cmd={}, initial state: {}",
          p.cmd,
          oauth_cancellation.load(std::sync::atomic::Ordering::Relaxed)
        );

        let mut futures = Vec::new();

        // Create futures for non-blocking concurrent execution
        // CRITICAL: All futures share the same rate limiter to enforce the true rate limit
        for (idx, record) in updated_rows.into_iter().enumerate() {
          debug!("Creating future #{} for cmd={}", idx + 1, p.cmd);
          let limclon = limiter.clone(); // This shares the same underlying rate limiter
          let oauth_cancel = oauth_cancellation.clone(); // Share the same cancellation token

          debug!(
            "Future #{} created with OAuth cancellation token state: {}",
            idx + 1,
            oauth_cancel.load(std::sync::atomic::Ordering::Relaxed)
          );

          let future = runmods(record, limclon, p, Some(oauth_cancel));
          futures.push(future);
        }

        let futures_count = futures.len();
        debug!(
          "Starting {} concurrent operations for cmd={} - each will wait for shared rate limiter permit",
          futures_count, p.cmd
        );
        let concurrent_start = std::time::Instant::now();

        debug!(
          "About to start {} concurrent operations, OAuth token state before execution: {}",
          futures_count,
          oauth_cancellation.load(std::sync::atomic::Ordering::Relaxed)
        );

        // Execute all futures concurrently - the shared rate limiter enforces the true rate
        // Spawn a progress tracker
        let progress_handle = {
          let cmd = p.cmd.clone();
          tokio::spawn(async move {
            let start = std::time::Instant::now();
            loop {
              tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
              let elapsed = start.elapsed();
              info!(
                "⏳ Still processing {} operations for cmd={} - elapsed: {:?}",
                futures_count, cmd, elapsed
              );
            }
          })
        };

        let results = join_all(futures).await;

        // Cancel progress tracker
        progress_handle.abort();

        debug!(
          "All {} concurrent operations completed, OAuth token state after execution: {}",
          futures_count,
          oauth_cancellation.load(std::sync::atomic::Ordering::Relaxed)
        );

        let concurrent_duration = concurrent_start.elapsed();
        let actual_rate = if concurrent_duration.as_secs_f64() > 0.0 {
          futures_count as f64 / concurrent_duration.as_secs_f64()
        } else {
          0.0
        };
        debug!(
          "Completed {} operations for cmd={} in {:?} - actual rate: {:.2} ops/sec (target: {} ops/sec)",
          futures_count, p.cmd, concurrent_duration, actual_rate, qps
        );

        // Collect errors from concurrent operations and check for OAuth expiration
        let mut oauth_expired = false;
        for (idx, result) in results.iter().enumerate() {
          if let Err(e) = result {
            let error_msg = format!("Task {idx} failed: {e}");
            error!(error = %error_msg, "Concurrent task failed");

            // Check if this is an OAuth token expiration error
            if error_msg.contains("OAUTH_TOKEN_EXPIRED") {
              oauth_expired = true;
            }

            errs1.push(error_msg);
          }
        }

        // Handle OAuth token expiration - this runs once after all concurrent operations
        if oauth_expired {
          let oauth_error_msg = "🚫 **PROCESO INTERRUMPIDO: Token de Google expirado**\n\n\
            El token de autenticación de Google ha expirado después de más de 90 minutos de procesamiento. \
            Google ya no acepta peticiones y el proceso debe reiniciarse.\n\n\
            **Acción requerida:** Por favor, ejecuta nuevamente el comando para continuar con las filas restantes.";

          // COMMENTED OUT - CHAT NOT NEEDED FOR THIS PROJECT
          // // Send chat notification about the OAuth expiration
          // if let Err(e) =
          //   chres(oauth_error_msg.to_string(), p.sp.clone(), p.tsn.clone())
          //     .await
          // {
          //   warn!("Failed to send OAuth expiration chat notification: {}", e);
          // }

          // Update Google Sheets with remaining rows that need to be processed
          if let Err(e) = update_sheets_with_remaining_rows(ep.clone(), p).await
          {
            warn!("Failed to update sheets with remaining rows: {}", e);
          }

          debug!(
            "OAuth token expiration handled - chat sent and sheets updated with remaining rows"
          );
        }

        // After all individual lists complete, process the accumulated data for sub list commands
        if lom == "lmul" {
          debug!("Processing accumulated data for command: {}", p.cmd);
          let target_ep = chtoshep(ep.clone(), p.cmd.clone())
            .await
            .cwl("Could not get ep for gdatabase_to_sheetsdb")?;
          gdatabase_to_sheetsdb(p.abr.clone(), target_ep).await.cwl(
            &format!(
              "Failed to process database to sheets database for {} command",
              p.cmd
            ),
          )?;
        }

      // ============== parallel part ends here ========================
      } else {
        // Commands like cpc and cpfc work with parameters directly, not selected rows
        if ["cpc", "cpfc", "suc", "usuc"].contains(&p.cmd.as_str()) {
          debug!("Processing {} command with parameters directly", p.cmd);
          let limiter = create_rate_limiter(qps);

          // Create a dummy record since these commands don't need actual record data
          let dummy_record = serde_json::json!({
            "id": "dummy",
            "data": {},
            "cmd": p.cmd.clone()
          });

          match runmods(dummy_record, limiter, p, None).await {
            Ok(_) => {
              debug!("{} command completed successfully", p.cmd)
            }
            Err(e) => {
              let error_msg = format!("{} command failed: {}", p.cmd, e);
              error!(error = %error_msg, "Parameter-based command failed");
              errs1.push(error_msg);
            }
          }
        } else {
          let er = format!(
            "No hay filas seleccionadas para los procesos para modificar {}",
            ep.table_sheet()
          );
          errs1.push(er);
        }
      }
    }
  } else if lom == "l_one" {
    let limiter = create_rate_limiter(qps);

    // Create a dummy record
    let record = json!({
      "cmd": p.cmd.clone()
    });

    match runmods(record, limiter, p, None).await {
      Ok(_) => {
        debug!("Successfully executed match runmods in mods");

        // Special handling for ltu command - process accumulated guardian data
        if p.cmd == "ltu" {
          gdatabase_to_sheetsdb(p.abr.clone(), ep.clone()).await.cwl(
            "Failed to process database to sheets database for ltu command",
          )?;
        }
      }
      Err(e) => {
        let error_msg = format!("match runmods in mods failed: {e}");
        error!(error = %error_msg, "match runmods execution in mods failed");
        errs1.push(error_msg);
      }
    }
  }

  let finers: String = geterrsdb(ep.clone(), p.abr.clone())
    .await
    .cwl("Failed to get errors from database")?;

  let errs2 = errs1.join("\n\n");
  let mut tx;

  if !finers.is_empty() {
    tx = "Estos eran los errores que salieron: \n\n".to_string();
    tx.push_str(&errs2);
    tx.push_str(&finers);
  } else if !errs2.is_empty() {
    tx = "Estos eran los errores que salieron: \n\n".to_string();
    tx.push_str(&errs2);
  } else {
    tx = "No había errores registrados.".to_string();
  }

  // NOTE NEVER TOUCH THIS! when there are two sheets involved, the first toshs gets the actual lists
  // the other toshs in the condition redoes the sheet where rows were selected.
  // except for cadsh, which does not use the first toshs
  if p.cmd != "cadsh" {
    if p.cmd == "lv" {
      debug!("lv command: Unmarking all users (data.do = 'x') before toshs");
      let unmark_query = "
        update type::table($table)
        set data.do = 'x'
        where abr = $abr and data.do = 'd'
      ";

      let _unmark_result = DB
        .query(unmark_query)
        .bind(("table", ep.table_sheet()))
        .bind(("abr", p.abr.clone()))
        .await
        .cwl("Failed to unmark users for lv command")?;

      debug!("lv command: Successfully unmarked all users with data.do = 'd'");
    }

    toshs(p.tsy.clone(), p.abr.clone(), ep.clone(), p.spshid.clone())
      .await
      .cwl("Failed to save to sheets")?;
  }

  if lom == "lmul" {
    let epts = chtoshep(ep.clone(), p.cmd.clone())
      .await
      .cwl("Could not get ep for toshs")?;
    toshs(p.tsy.clone(), p.abr.clone(), epts, p.spshid.clone())
      .await
      .cwl("Failed to save to sheets")?;
  } else if p.cmd == "cadsh" {
    debug!("cadsh: About to call toshs with SID: {}", SID.to_string());

    // Debug: Check total record count before toshs call
    let count_query = "select count() from type::table($table) group all";
    let mut count_response = DB
      .query(count_query)
      .bind(("table", ep.table_sheet()))
      .await
      .cwl("Failed to query record count before toshs")?;

    let count_result: Vec<Value> = count_response
      .take(0)
      .cwl("Failed to take count query result")?;

    if let Some(count_val) =
      count_result.first().and_then(|row| row.get("count"))
    {
      debug!(
        "cadsh: Total records in database before toshs call: {}",
        count_val
      );
    }

    toshs(p.tsy.clone(), "".to_string(), ep.clone(), SID.to_string())
      .await
      .cwl("Failed to save to sheets")?;
  }

  let duration = start.elapsed().as_millis();
  let duration_usize: usize = duration
    .try_into()
    .cwl("Failed to convert duration to usize")?;
  let tims = durs(duration_usize).await;

  // let epch2 = if ["mod", "modl"].contains(&lom) {
  //   ep.chendmod(p.cmd.clone(), tims, tx)
  // } else if p.cmd == "li" && lom == "lmul" {
  //   getnumqrylics(p.abr.clone(), tims)
  //     .await
  //     .cwl("Failed to get number query")?
  // } else {
  //   getnumqry(ep.clone(), p.abr.clone(), p.cmd.clone(), tims)
  //     .await
  //     .cwl("Failed to get number query")?
  // };

  // COMMENTED OUT - CHAT NOT NEEDED FOR THIS PROJECT
  // chres(epch2, p.sp.clone(), p.tsn.clone())
  //   .await
  //   .cwl("Failed to send end mod chat message")?;

  // Send email notification for completed mod commands (not list commands)
  if ["mod", "modl"].contains(&lom) {
    debug!(
      "Sending email notification for completed mod command: {}",
      p.cmd
    );
    // COMMENTED OUT - EMAIL NOTIFICATION NOT NEEDED FOR THIS PROJECT
    // if let Err(e) = sendgmsop(p).await {
    //   warn!("Failed to send email notification: {}", e);
    // }
  }

  // debug!("{}", ep.chendmod(cmd, tims, tx));

  Ok(())
}
