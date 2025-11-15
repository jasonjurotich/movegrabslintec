// pub use super::apis::*;
pub use super::sheets::*;
use crate::AppResult;
use crate::aux_sur::{check_user_in_admin_list, get_abr_from_email_domain};
use crate::surrealstart::{SP, get_all_commands};
use crate::tracer::ContextExt;
use crate::{bail, debug, info, warn};

// use crate::chres;

use chrono::Utc;
// use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
// use surrealdb::sql::Uuid;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[allow(non_snake_case, non_upper_case_globals)]
pub struct Event {
  // New nested format
  pub chat: Option<Value>,
  pub commonEventObject: Option<Value>,

  // Legacy format
  pub eventTime: Option<String>,
  pub message: Option<Value>,
  pub space: Option<Value>,
  #[serde(rename = "type")]
  pub typ: Option<String>,
  pub user: Option<Value>,
  pub action: Option<Value>,
  pub isDialogEvent: Option<bool>,
  pub dialogEventType: Option<String>,
  pub common: Option<Value>,
}

pub async fn from_event(evs: &Event, args: String) -> AppResult<String> {
  // Check if we have the new format (chat field) and normalize to old format
  let (mes, coms, event_type, space_obj, ev_usr) = if let Some(chat) = &evs.chat
  {
    debug!("Using NEW Google Chat API format (chat object)");

    let message_payload = chat["messagePayload"].clone();
    let mes = message_payload["message"].clone();
    let coms = evs.commonEventObject.clone().unwrap_or_default();
    let event_type = chat["type"].as_str().unwrap_or("").to_string();
    let space_obj = message_payload["space"].clone();
    let ev_usr = chat["user"].clone();

    (mes, coms, event_type, space_obj, ev_usr)
  } else {
    debug!("Using LEGACY Google Chat API format");

    let mes = evs.message.clone().unwrap_or_default();
    let coms = evs.common.clone().unwrap_or_default();
    let event_type = evs.typ.clone().unwrap_or_default();
    let space_obj = evs.space.clone().unwrap_or_default();
    let ev_usr = evs.user.clone().unwrap_or_default();

    (mes, coms, event_type, space_obj, ev_usr)
  };

  debug!(event_type = %event_type, "Event type received from Google Chat");

  let mesid = mes["name"]
    .as_str()
    .unwrap_or("")
    .split("messages/")
    .nth(1)
    .unwrap_or("")
    .to_string()
    .replace('\"', "");

  let space = if event_type == "ADDED_TO_SPACE" {
    space_obj["name"]
      .as_str()
      .unwrap_or(&format!("spaces/{}", &*SP))
      .to_string()
      .replace('\"', "")
  } else {
    mes["space"]["name"]
      .as_str()
      .unwrap_or(&format!("spaces/{}", &*SP))
      .to_string()
      .replace('\"', "")
  };

  // TODO: For CLI commands, mesid and space might be empty or default values
  // This causes petition ID reuse. We need to add unique identifiers for CLI.

  let creatime = mes["createTime"]
    .as_str()
    .map(|s| s.to_string().replace('\"', ""))
    .unwrap_or_else(|| Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string());

  // For CLI commands, mesid is empty, so add timestamp to make petition ID unique
  let is_cli_command = !args.is_empty();

  let argtext = if args.is_empty() {
    mes["argumentText"]
      .as_str()
      .unwrap_or("")
      .to_string()
      .replace('\"', "")
  } else {
    args
  };

  // NOTE this finally gives the cli the possibility to add unique identifiers
  let final_mesid = if is_cli_command && mesid.is_empty() {
    let timestamp_nanos = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    format!("cli-{timestamp_nanos}")
  } else {
    mesid.clone()
  };

  debug!(
    is_cli = is_cli_command,
    original_mesid = %mesid,
    final_mesid = %final_mesid,
    "Petition ID generation for CLI vs Chat"
  );

  // For ADDED_TO_SPACE events, user info is in evs.user, not mes.sender
  let usr = if event_type == "ADDED_TO_SPACE" {
    ev_usr["email"]
      .as_str()
      .unwrap_or(&*EM)
      .to_string()
      .replace('\"', "")
  } else {
    mes["sender"]["email"]
      .as_str()
      .unwrap_or(&*EM)
      .to_string()
      .replace('\"', "")
  };

  let nam = if event_type == "ADDED_TO_SPACE" {
    ev_usr["displayName"]
      .as_str()
      .unwrap_or("")
      .to_string()
      .replace('\"', "")
  } else {
    mes["sender"]["displayName"]
      .as_str()
      .unwrap_or("")
      .to_string()
      .replace('\"', "")
  };

  let slcom = mes["slashCommand"]["commandId"]
    .as_str()
    .unwrap_or("")
    .to_string()
    .replace('\"', "");

  let cardid = mes["cardsV2"][0]["cardId"]
    .as_str()
    .unwrap_or("")
    .to_string()
    .replace('\"', "");

  let cardhed = mes["cardsV2"][0]["card"]["header"]["title"]
    .as_str()
    .unwrap_or("")
    .to_string()
    .replace('\"', "");

  let invfunc = coms["invokedFunction"]
    .as_str()
    .unwrap_or("")
    .to_string()
    .replace('\"', "");

  // NOTE this only divides up the text from what comes in, there is no tsn here yet
  // when it is ADDED_TO_SPACE the result of this will be this: PARSED ARGUMENTS (empty input) command=no abbreviation=iedu parameters=

  let (fincom, finabr, finparams) = finalargs(argtext.clone(), usr.clone())
    .await
    .cwl("Failed to parse command arguments")?;

  // NOTE this only sees really if the domain of the person sending the petition, no matter its typs, (ADDED_TO_SPACE or MESSAGE, is in the admins sheet, and if so get the row to be able to create tokens later. If there is no domain, it should send a message to the default chat indicating that a specific domain tried to use the bot but was not in the list of domains permitted.)

  // NOTE this does NOT add the new admin if the domain of the admin was already in the list or if there was ANY admin in the admins column so useless up to now to see if the new admin is there or not. IT DOES NOT REFRESH THE ADMINS TABLE IN THAT CASE AND STILL DOES NOT EXIST IN THE ADMINS TABLE HERE IN THAT CASE!

  let (findom, finjsonf, finadmin, finspshid, finabr) = match get_admin_info(
    finabr.clone(),
    is_cli_command,
    if event_type == "ADDED_TO_SPACE" {
      Some(usr.clone())
    } else {
      None
    },
  )
  .await
  {
    Ok(info) => info,
    Err(e) => {
      if e.to_string().contains("UNAUTHORIZED_DOMAIN") {
        warn!("Unauthorized domain detected: {}", e);
        chreserrs(format!(
          "UNAUTHORIZED DOMAIN ATTEMPT: {} | User: {} | Event type: {}",
          e, usr, event_type
        ))
        .await
        .cwl("Failed to send unauthorized domain alert to superadmin")?;
        return Ok("".to_string());
      } else {
        return Err(e).cwl("Failed to get admin info");
      }
    }
  };

  debug!(
    finabr = %finabr,
    findom = %findom,
    finjsonf = %finjsonf,
    finadmin = %finadmin,
    finspshid = %finspshid,
    "DEBUG: ADMIN INFO RETURND FROM get_admin_info"
  );

  // ----------- FROM HERE WE HAVE THE ARGS DATA AND THE ADMIN ROW FROM THE ADMINS TABLE, NO TOKEN YET -------------

  // these would come from the admin sheet in the admin bot
  // let finadmin = &*EM;
  // let finspshid = &*SID;

  // let finjsonf = finadmin
  //   .split('@')
  //   .nth(1)
  //   .unwrap_or_default()
  //   .replace('.', "")
  //   .to_string();

  // let findom = finadmin.split('@').nth(1).unwrap_or_default().to_string();

  debug!(
    message_id = %final_mesid,
    space = %space,
    user = %usr,
    "Processing event"
  );

  let data = json!({
    "mesid":final_mesid,
    "space":space,
    "time":creatime,
    "argtext":argtext,
    "usr":usr,
    "nam":nam,
    "slcom":slcom,
    "cardid":cardid,
    "cardhed":cardhed,
    "invfunc":invfunc,
    "fincom":fincom,
    "finabr":finabr,
    "finparams":finparams,
    "findom":findom,
    "finjsonf":finjsonf,
    "finadmin":finadmin,
    "finspshid":finspshid,
  });

  debug!("This is data {:#?}", data);

  // Use CREATE ONLY to prevent duplicates atomically
  let query = "
    let $result = (
      create only type::thing(
        'petitions', string::join('', [$space, '-', $mesid, '-', $usr])
      )
      content {
        mesid: $mesid,
        space: $space,
        time: $time,
        argtext: $argtext,
        usr: $usr,
        nam: $nam,
        slcom: $slcom,
        cardid: $cardid,
        cardhed: $cardhed,
        invfunc: $invfunc,
        fincom: $fincom,
        finabr: $finabr,
        finparams: $finparams,
        findom: $findom,
        finjsonf: $finjsonf,
        finadmin: $finadmin,
        finspshid: $finspshid
      }
    );
    
    if $result != null {
      return type::string($result.id);
    } else {
      return none;
    }
  ";

  let mut result = DB
    .query(query)
    .bind(data.clone())
    .await
    .cwl("Failed to execute petition creation query")?;

  // --------- HERE WE HAVE A RECORD IN THE PETTIONS TABLE, BU NO TOKEN YET -----------------

  // FIXED: The query returns two results: index 0 is the variable assignment (None), index 1 is the return value
  // We need to read from index 1 to get the actual record ID returned by the query
  let record_id_result = result.take::<Option<String>>(1);
  debug!(record_id_result = ?record_id_result, "Record ID extraction from index 1");

  let record_id = match record_id_result {
    Ok(Some(id)) => {
      debug!(record_id = %id, "Successfully created new petition");
      id
    }
    Ok(None) => {
      debug!("Query returned None - no record created (likely already exists)");
      "".to_string()
    }
    Err(e) => {
      let error_str = format!("{e:?}");
      if error_str.contains("RecordExists") {
        debug!(
          "Petition already exists (RecordExists error) - this is expected for duplicates"
        );
        "".to_string()
      } else {
        debug!(error = ?e, "Unexpected error extracting record ID");
        "".to_string()
      }
    }
  };

  // -------------- FROM HERE IS WHERE WE GET TOKENS AND ADD THEM TO THE PETITION ----------------

  if !record_id.is_empty() {
    let should_continue = add_tokens_to_petitions(record_id.clone())
      .await
      .cwl("Failed to add tokens to petition")?;

    if !should_continue {
      debug!(record_id = %record_id, "Petition was deleted due to unauthorized access");
      return Ok("".to_string());
    }

    // NOTE we should not pass this point if the user is not an admin, no matter what the type of event

    debug!(record_id = %record_id, "Created new petition WITH BOTH TOKENS");
  } else {
    info!("Petition already exists");
  }

  Ok(record_id)
}

pub async fn add_tokens_to_petitions(id: String) -> AppResult<bool> {
  info!("WE ARE STARTING ADD_TOKENS_TO_PETITIONS");

  let qry = "
    select usr, finadmin, fincom, finabr, space from type::record($record_id);
  ";

  debug!(query = %qry, record_id = %id, "Executing add_tokens_to_petitions query");

  let mut result = DB
    .query(qry)
    .bind(("record_id", id.clone()))
    .await
    .cwl("Failed to query user from petitions")?;

  debug!(result = ?result, "RAW RESULT FROM add_tokens_to_petitions query");

  // Try to see what we can actually extract
  let result_as_string = format!("{result:?}");
  debug!(raw_response = %result_as_string, "add_tokens_to_petitions: Raw response structure");

  // Try extracting with different types
  let record: Option<serde_json::Value> = match result
    .take::<Vec<serde_json::Value>>(0)
  {
    Ok(vec) => {
      debug!(success_vec = ?vec, "add_tokens_to_petitions: Successfully got Vec<serde_json::Value>");
      vec.first().cloned()
    }
    Err(e) => {
      debug!(error = ?e, "add_tokens_to_petitions: Failed to get Vec<serde_json::Value>, trying alternatives");
      None
    }
  };

  let (usr, finadmin, fincom, finabr, space) = match record {
    Some(data) => (
      data["usr"].as_str().unwrap_or("NO USER").to_string(),
      data["finadmin"].as_str().unwrap_or("NO ADMIN").to_string(),
      data["fincom"].as_str().unwrap_or("").to_string(),
      data["finabr"].as_str().unwrap_or("").to_string(),
      data["space"].as_str().unwrap_or("").to_string(),
    ),
    None => (
      "".to_string(),
      "".to_string(),
      "".to_string(),
      "".to_string(),
      "".to_string(),
    ),
  };

  debug!(user = %usr, finadmin = %finadmin, command = %fincom, abr = %finabr, space = %space, "FOUND USER, FINADMIN, COMMAND, ABR, AND SPACE FOR PETITION");
  if !usr.is_empty() {
    let tsn = get_tok(usr.clone(), "no")
      .await
      .cwl("Failed to get chat token")?;

    debug!("This is tsn {tsn} for usr {usr}");

    // Commands that don't need admin tokens
    let commands_without_admin = ["ayuda", "", "no"];
    let needs_admin_token = !commands_without_admin.contains(&fincom.as_str());

    debug!(
      command = %fincom,
      finadmin = %finadmin,
      needs_admin_token = needs_admin_token,
      finadmin_is_empty = finadmin.is_empty(),
      "DEBUG: Token decision logic"
    );

    if !finabr.is_empty() {
      let mut is_authorized =
        check_user_in_admin_list(finabr.clone(), usr.clone())
          .await
          .cwl("Failed to check if user is in admin list")?;

      debug!("This is is_authorized {:#?}", is_authorized);

      if !is_authorized {
        warn!(user = %usr, abr = %finabr, "User is NOT in admin list initially, running frshtsadmin to refresh");

        frshtsadmin(None)
          .await
          .cwl("Could not run command to refresh admin list")?;

        is_authorized = check_user_in_admin_list(finabr.clone(), usr.clone())
          .await
          .cwl("Failed to re-check if user is in admin list after refresh")?;

        debug!("This is second is_authorized {:#?}", is_authorized);

        if !is_authorized {
          warn!(user = %usr, abr = %finabr, "User is STILL NOT in admin list after refresh, sending unauthorized message and deleting petition");
          let unauthorized_msg = format!(
            "⛔ Unauthorized: {} No estas autorizado para usar este bot para el dominio {}. Habla con el admin de este servicio para tener acceso.",
            usr, finabr
          );
          chres(unauthorized_msg, space.clone(), tsn.clone())
            .await
            .cwl("Failed to send unauthorized message to user")?;

          let delete_query = "delete type::record($record_id);";
          DB.query(delete_query)
            .bind(("record_id", id.clone()))
            .await
            .cwl("Failed to delete unauthorized petition record")?;

          debug!(petition_id = %id, user = %usr, "Deleted unauthorized petition record");
          return Ok(false);
        }

        debug!(user = %usr, abr = %finabr, "User IS NOW authorized in admin list after refresh");
      } else {
        debug!(user = %usr, abr = %finabr, "User IS authorized in admin list");
      }
    }

    let tsy = if needs_admin_token && !finadmin.is_empty() {
      info!("STARTING THE TSY PROCESS");

      let admin_token = get_tok(finadmin.clone(), "yes")
        .await
        .cwl("Failed to get normal token")?;

      debug!("This is this tsy admin_token {admin_token} for usr {finadmin}");

      admin_token
    } else {
      debug!(
        "DEBUG: Setting tokadmin to NO because needs_admin_token={} and finadmin_is_empty={}",
        needs_admin_token,
        finadmin.is_empty()
      );
      "NO".to_string()
    };

    // this would come from checking the users by a request in the admin bot
    // BUG this presupposes the chat is the same as the admin being used, but in the case of ieducando, the chat is not the same as the admin being used for the command and this here below will always fail because we dont have a tsy of ieducando here, just the tsn

    // FIX we need a sub function here that compares the usr domain to the finadmin domain and if they are different, then create a tsy for the usr and then that needs to be used for the confirm2au function, presupposing that the usr in this case is a superadmin... for now, just put an exception for ieducando.com

    // NOTE the usr here is the user of the chat, not the finadmin being used for the commands
    let twoauth = if tsy != "NO" {
      let domain = usr.split('@').nth(1).unwrap_or("").to_string();

      let auth_result = if domain == "ieducando.com"
        || domain == "adminbot-iedu-prod.iam.gserviceaccount.com"
      {
        "true".to_string()
      } else {
        confirm2au(usr.clone(), tsy.clone())
          .await
          .cwl("Could not get two auth confirmation from google server")?
      };

      debug!("This is auth_result: {:#?}", auth_result);

      if auth_result == "false" {
        warn!(user = %usr, admin = %finadmin, "Admin does not have 2FA enabled, sending unauthorized message and deleting petition");
        let unauthorized_msg = format!(
          "⛔ Unauthorized: {} no tiene autenticación de dos pasos (2FA) activada. Debe activar 2FA para usar este bot.",
          finadmin
        );
        chres(unauthorized_msg, space.clone(), tsn.clone())
          .await
          .cwl("Failed to send 2FA unauthorized message to user")?;

        let delete_query = "delete type::record($record_id);";
        DB.query(delete_query)
          .bind(("record_id", id.clone()))
          .await
          .cwl("Failed to delete unauthorized petition record")?;

        debug!(petition_id = %id, admin = %finadmin, "Deleted petition due to missing 2FA");
        return Ok(false);
      }

      auth_result
    } else {
      "NO".to_string()
    };

    // let twoauth = "YES";

    let data2 = json!({
      "tokchat":tsn,
      "tokadmin":tsy,
      "twoauth":twoauth
    });

    let query2 = "
      update petitions set
        tokchat = $tokchat,
        tokadmin = $tokadmin,
        twoauth = $twoauth
      where type::string(id) contains $id
    ;";

    DB.query(query2)
      .bind(data2)
      .bind(("id", id.clone()))
      .await
      .cwl("Failed to update petition with tokens")?;

    debug!(
      petition_id = %id,
      user = %usr,
      "Updated petition with tokens"
    );
  }

  Ok(true)
}

// OLD FINALARGS FUNCTION - COMMENTED OUT FOR COMPARISON
// #[allow(unused_assignments)]
// pub async fn finalargs_old(args: String) -> (String, String, String) {
//   let mut cm = "no".to_string();
//   let mut finabr = ABR.to_string();
//   let mut params = String::new();

//   if args.contains(',') {
//     let func: Vec<&str> = args.split(',').collect();
//     if func[0].contains(' ') {
//       let spl: Vec<&str> = func[0].split_whitespace().collect();
//       finabr = spl[0].to_string().to_lowercase();
//       cm = spl.get(1).unwrap_or(&"no").to_string().to_lowercase();
//     } else {
//       cm = func[0].to_string().to_lowercase();
//     }
//     params = func[1..].join(",").trim().to_string().to_lowercase();
//   } else if args.contains(' ') {
//     let spl: Vec<&str> = args.split_whitespace().collect();
//     finabr = spl[0].to_string().to_lowercase();
//     cm = spl.get(1).unwrap_or(&"no").to_string().to_lowercase();
//   } else {
//     cm = args.to_string().to_lowercase();
//   }

//   debug!(
//     command = %cm,
//     abbreviation = %finabr,
//     parameters = %params,
//     "PARSED ARGUMENTS"
//   );

//   (cm, finabr, params)
// }

#[allow(unused_assignments)]
pub async fn finalargs(
  args: String,
  user_email: String,
) -> AppResult<(String, String, String)> {
  let mut cm = "no".to_string();
  let mut finabr = ABR.to_string();
  let mut params = String::new();

  // Handle empty or whitespace-only input
  let args_trimmed = args.trim();

  if args_trimmed.is_empty() {
    debug!(
      command = %cm,
      abbreviation = %finabr,
      parameters = %params,
      "PARSED ARGUMENTS (empty input)"
    );
    return Ok((cm, finabr, params));
  }

  // Use centralized command list from surrealstart
  let valid_commands = get_all_commands();

  // Handle comma-separated input (has parameters)
  if args_trimmed.contains(',') {
    let parts: Vec<&str> = args_trimmed.split(',').collect();
    let first_part = parts[0].trim();

    // Extract parameters (everything after first comma)
    params = parts[1..].join(",").trim().to_string().to_lowercase();

    // Parse the first part (could be "command" or "school command")
    if first_part.contains(' ') {
      // Format: "school command, params"
      let tokens: Vec<&str> = first_part.split_whitespace().collect();
      if tokens.len() >= 2 {
        let potential_school = tokens[0].trim().to_lowercase();
        let potential_command = tokens[1].trim().to_lowercase();

        // Check if second token is a valid command
        if valid_commands.contains(&potential_command.as_str()) {
          finabr = potential_school;
          cm = potential_command;
        } else {
          // Fallback: treat first token as command if it's valid
          if valid_commands.contains(&potential_school.as_str()) {
            cm = potential_school;
            // Determine ABR from user email when not provided
            finabr = get_abr_from_email_domain(user_email.clone()).await?;
          } else {
            cm = "no".to_string();
            finabr = get_abr_from_email_domain(user_email.clone()).await?;
          }
        }
      }
    } else {
      // Format: "command, params"
      let potential_command = first_part.to_lowercase();
      if valid_commands.contains(&potential_command.as_str()) {
        cm = potential_command;
        // Determine ABR from user email when not provided
        finabr = get_abr_from_email_domain(user_email.clone()).await?;
      } else {
        cm = "no".to_string();
        finabr = get_abr_from_email_domain(user_email.clone()).await?;
      }
    }
  } else if args_trimmed.contains(' ') {
    // Handle space-separated input without parameters
    let tokens: Vec<&str> = args_trimmed.split_whitespace().collect();

    if tokens.len() >= 2 {
      let first_token = tokens[0].trim().to_lowercase();
      let second_token = tokens[1].trim().to_lowercase();

      // Check if second token is a valid command
      if valid_commands.contains(&second_token.as_str()) {
        // Format: "school command"
        finabr = first_token;
        cm = second_token;
      } else if valid_commands.contains(&first_token.as_str()) {
        // Format: "command something_else" - treat as command only
        cm = first_token;
        // Determine ABR from user email when not provided
        finabr = get_abr_from_email_domain(user_email.clone()).await?;
      } else {
        // Neither token is a recognized command
        cm = "no".to_string();
        finabr = get_abr_from_email_domain(user_email.clone()).await?;
      }
    } else if tokens.len() == 1 {
      // Single token with spaces somehow - treat as single token
      let token = tokens[0].trim().to_lowercase();
      if valid_commands.contains(&token.as_str()) {
        cm = token;
      } else {
        cm = "no".to_string();
      }
    }
  } else {
    // Single token without spaces or commas
    let token = args_trimmed.to_lowercase();
    if valid_commands.contains(&token.as_str()) {
      cm = token;
      // Determine ABR from user email when not provided
      finabr = get_abr_from_email_domain(user_email.clone()).await?;
    } else {
      // Could be a school abbreviation with default command, but more likely invalid
      cm = "no".to_string();
      finabr = get_abr_from_email_domain(user_email.clone()).await?;
    }
  }

  debug!(
    command = %cm,
    abbreviation = %finabr,
    parameters = %params,
    input = %args_trimmed,
    "PARSED ARGUMENTS"
  );

  Ok((cm, finabr, params))
}

pub async fn get_admin_info(
  abr: String, // NOTE this is iedu when the app is first installed, means nothing here for that case
  is_cli: bool,
  user_email: Option<String>,
) -> AppResult<(String, String, String, String, String)> {
  let ep = Ep::Admins;

  frshtsadmin(None)
    .await
    .cwl("Could not run command to populate admins column")?;

  // If user_email is provided (ADDED_TO_SPACE event), query by domain instead of abr
  let (query, search_param) = if let Some(ref email) = user_email {
    let domain = email.split('@').nth(1).unwrap_or("").to_string();
    (
      "
        select data.dominio, data.jsonf,
          data.correo, data.spreadsheet_id, data.admins, data.abr
        from type::table($table)
        where data.dominio = $domain
        limit 1
      ",
      domain,
    )
  } else {
    (
      "
        select data.dominio, data.jsonf,
          data.correo, data.spreadsheet_id, data.admins, data.abr
        from type::table($table)
        where data.abr = $abr
        limit 1
      ",
      abr.clone(),
    )
  };

  debug!(
    query = %query,
    table = %ep.table_sheet(),
    search_param = %search_param,
    searching_by = if user_email.is_some() { "domain" } else { "abr" },
    "Executing get_admin_info query"
  );

  let result = if user_email.is_some() {
    DB.query(query)
      .bind(("table", ep.table_sheet()))
      .bind(("domain", search_param.clone()))
      .await
      .cwl("Could not get admin info from database")
  } else {
    DB.query(query)
      .bind(("table", ep.table_sheet()))
      .bind(("abr", search_param.clone()))
      .await
      .cwl("Could not get admin info from database")
  };

  debug!(result = ?result, "Raw query result for admin table");

  // Let's see ALL records in the table
  // let all_query = "select * from type::table($table);";
  // let all_result = DB.query(all_query).bind(("table", ep.table_sheet())).await;

  // debug!(
  //   all_query = %all_query,
  //   all_result = ?all_result,
  //   "DEBUG: All records in admin table"
  // );

  match result {
    Ok(mut response) => {
      let records: Vec<serde_json::Value> = response
        .take::<Vec<serde_json::Value>>(0)
        .cwl("Could not get records from response")?;

      debug!(
        records_count = records.len(),
        records = ?records,
        "Extracted records from response"
      );

      if let Some(data) = records.first() {
        debug!(data = ?data, "Found matching record");

        // Check if admins column is empty or null
        let admins_value = &data["data"]["admins"];
        let admins_abr = &data["data"]["abr"];
        let admins_empty = admins_value.is_null()
          || admins_value.as_str().is_none_or(|s| s.trim().is_empty());

        // Check if the user is in the admin list
        let user_not_in_list = if let Some(ref email) = user_email {
          !check_user_in_admin_list(
            admins_abr.as_str().unwrap_or("").to_string(),
            email.clone(),
          )
          .await
          .unwrap_or(false)
        } else {
          false
        };

        if admins_empty || user_not_in_list {
          warn!(
            "Admin domain record found for {} but admins column is empty or user not in list, running frshtsadmin to populate it",
            admins_abr
          );

          frshtsadmin(None)
            .await
            .cwl("Could not run command to populate admins column")?;

          // Re-query after refresh
          let mut result = if user_email.is_some() {
            DB.query(query)
              .bind(("table", ep.table_sheet()))
              .bind(("domain", search_param.clone()))
              .await
              .cwl("Failed to execute query after admins refresh")?
          } else {
            DB.query(query)
              .bind(("table", ep.table_sheet()))
              .bind(("abr", search_param.clone()))
              .await
              .cwl("Failed to execute query after admins refresh")?
          };

          let records: Vec<serde_json::Value> = result
            .take::<Vec<serde_json::Value>>(0)
            .cwl("Could not get records from response after admins refresh")?;
          // FIX from here on down we need to add the abr column as well
          if let Some(data) = records.first() {
            debug!(data = ?data, "Found matching record after admins refresh");
            Ok((
              data["data"]["dominio"].as_str().unwrap_or("").to_string(),
              data["data"]["jsonf"].as_str().unwrap_or("").to_string(),
              data["data"]["correo"].as_str().unwrap_or("").to_string(),
              data["data"]["spreadsheet_id"]
                .as_str()
                .unwrap_or("")
                .to_string(),
              data["data"]["abr"].as_str().unwrap_or("").to_string(),
            ))
          } else {
            Ok((
              EM.split('@').nth(1).unwrap_or("").to_string(),
              "".to_string(),
              EM.to_string(),
              SID.to_string(),
              "iedu".to_string(),
            ))
          }
        } else {
          Ok((
            data["data"]["dominio"].as_str().unwrap_or("").to_string(),
            data["data"]["jsonf"].as_str().unwrap_or("").to_string(),
            data["data"]["correo"].as_str().unwrap_or("").to_string(),
            data["data"]["spreadsheet_id"]
              .as_str()
              .unwrap_or("")
              .to_string(),
            data["data"]["abr"].as_str().unwrap_or("").to_string(),
          ))
        }
      } else {
        // NOTE No domain records found here at all, so running again to add new domain if there was one
        warn!(
          "No admin info found for {}, running frshtsadmin to refresh list",
          abr
        );

        frshtsadmin(None)
          .await
          .cwl("Could not run command to get new admin")?;

        let mut result = if user_email.is_some() {
          DB.query(query)
            .bind(("table", ep.table_sheet()))
            .bind(("domain", search_param.clone()))
            .await
            .cwl("Failed to execute query after refresh")?
        } else {
          DB.query(query)
            .bind(("table", ep.table_sheet()))
            .bind(("abr", search_param.clone()))
            .await
            .cwl("Failed to execute query after refresh")?
        };

        let records: Vec<serde_json::Value> = result
          .take::<Vec<serde_json::Value>>(0)
          .cwl("Could not get records from response after refresh")?;

        if let Some(data) = records.first() {
          debug!(data = ?data, "Found matching record after refresh (error path)");
          Ok((
            data["data"]["dominio"].as_str().unwrap_or("").to_string(),
            data["data"]["jsonf"].as_str().unwrap_or("").to_string(),
            data["data"]["correo"].as_str().unwrap_or("").to_string(),
            data["data"]["spreadsheet_id"]
              .as_str()
              .unwrap_or("")
              .to_string(),
            data["data"]["abr"].as_str().unwrap_or("").to_string(),
          ))
        } else if is_cli {
          warn!(
            "CLI mode (error path): No admin record for '{}', returning defaults",
            abr
          );
          Ok((
            EM.split('@').nth(1).unwrap_or("").to_string(),
            "".to_string(),
            EM.to_string(),
            SID.to_string(),
            "iedu".to_string(),
          ))
        } else {
          bail!(
            "UNAUTHORIZED_DOMAIN: Domain with abbreviation '{}' is not authorized to use this bot (error path). Contact administrator to add domain to approved list.",
            abr
          )
        }
      }
    }
    Err(e) => {
      // NOTE Once again, there was no record at all, so running again to add new domain if there was one
      warn!(
        "Error getting admin info for {}, running frshtsadmin to refresh list: {:?}",
        abr, e
      );

      frshtsadmin(None)
        .await
        .cwl("Could not run command to get new admins")?;

      let mut result = if user_email.is_some() {
        DB.query(query)
          .bind(("table", ep.table_sheet()))
          .bind(("domain", search_param.clone()))
          .await
          .cwl("Failed to execute query after refresh")?
      } else {
        DB.query(query)
          .bind(("table", ep.table_sheet()))
          .bind(("abr", search_param.clone()))
          .await
          .cwl("Failed to execute query after refresh")?
      };

      let records: Vec<serde_json::Value> = result
        .take::<Vec<serde_json::Value>>(0)
        .cwl("Could not get records from response after refresh")?;

      if let Some(data) = records.first() {
        debug!(data = ?data, "Found matching record after refresh (error path)");
        Ok((
          data["data"]["dominio"].as_str().unwrap_or("").to_string(),
          data["data"]["jsonf"].as_str().unwrap_or("").to_string(),
          data["data"]["correo"].as_str().unwrap_or("").to_string(),
          data["data"]["spreadsheet_id"]
            .as_str()
            .unwrap_or("")
            .to_string(),
          data["data"]["abr"].as_str().unwrap_or("").to_string(),
        ))
      } else if is_cli {
        warn!(
          "CLI mode (error path): No admin record for '{}', returning defaults",
          abr
        );
        Ok((
          EM.split('@').nth(1).unwrap_or("").to_string(),
          "".to_string(),
          EM.to_string(),
          SID.to_string(),
          "iedu".to_string(),
        ))
      } else {
        bail!(
          "UNAUTHORIZED_DOMAIN: Domain with abbreviation '{}' is not authorized to use this bot (error path). Contact administrator to add domain to approved list.",
          abr
        )
      }
    }
  }
}

// NOTE not needed anymore, we check in another function, will be moved or deleted later
#[allow(dead_code)]
pub async fn confirm_as_admin(tsy: String, email: String) -> AppResult<String> {
  debug!("THIS IS EMAIL IN CONFIRM AS ADMIN {:#?}", email);
  let ep = Ep::Admins;

  let results = DB
    .query("select * from type::table($table);")
    .bind(("table", ep.table_sheet()))
    .await?;

  debug!("This is results from database in mods {:#?}", results);

  let debug_qry = "select * from type::table($table) limit 3";

  let debug_result =
    DB.query(debug_qry).bind(("table", ep.table_sheet())).await;

  debug!(debug_result = ?debug_result, "Debug: Full records structure");

  let qry = "
    select value data.admins
    from type::table($table)
    where 
      if data.admins is not none and type::is::string(data.admins) then
        string::contains(data.admins, $email)
      else
        false
      end
  ";

  debug!(
    query = %qry,
    table = %ep.table_sheet(),
    email = %email,
    "Executing confirm_as_admin query"
  );

  let result = DB
    .query(qry)
    .bind(("table", ep.table_sheet()))
    .bind(("email", email.clone()))
    .await
    .cwl("Failed to execute query");

  debug!(result = ?result, "Raw query result from confirm_as_admin");

  match result {
    Ok(mut response) => {
      let records: Vec<Value> = response
        .take(0)
        .cwl("Failed to take first result from query")?;

      debug!(
        records_count = records.len(),
        records = ?records,
        email = %email,
        "Extracted records from confirm_as_admin query"
      );

      if !records.is_empty() {
        Ok(tsy)
      } else {
        warn!(
          "NO ADMIN RECORDS FOUND FOR {}, RUNNING frshtsadmin TO REFRESH LIST",
          email
        );

        frshtsadmin(None)
          .await
          .cwl("Could not run command to get new admins")?;

        let records: Vec<Value> = DB
          .query(qry)
          .bind(("table", ep.table_sheet()))
          .bind(("email", email.clone()))
          .await
          .cwl("Failed to execute query after refresh")?
          .take(0)
          .cwl("Failed to take first result from query after refresh")?;

        if !records.is_empty() {
          Ok(tsy)
        } else {
          debug!(
            "THE EMAIL {:#?} IS NOT IN THE LIST OF ADMINS IN THE SQL DATA ADMIN BOT SHEET, EITHER MANUALLY OR AUTOMATICALLY PUT, TSY WILL BE NO",
            email
          );
          Ok("NO".to_string())
        }
      }
    }
    Err(e) => {
      warn!(
        "ERROR CHECKING ADMIN RUNNING frshtsadmin TO REFRESH LIST: {:?}",
        e
      );

      frshtsadmin(None)
        .await
        .cwl("Could not run command to get new admins")?;

      let records: Vec<Value> = DB
        .query(qry)
        .bind(("table", ep.table_sheet()))
        .bind(("email", email.clone()))
        .await
        .cwl("Failed to execute query")?
        .take(0)
        .cwl("Failed to take first result from query")?;

      if !records.is_empty() {
        // If admin is found, return the token string
        let tsy = get_tok(email.clone(), "yes")
          .await
          .cwl("Could not get normal token")?;
        Ok(tsy)
      } else {
        debug!(
          "THE EMAIL {:#?} IS NOT IN THE LIST OF ADMINS IN THE SQL DATA ADMIN BOT SHEET, EITHER MANUALLY OR AUTOMATICALLY PUT, TSY WILL BE NO",
          email
        );
        // If no admin is found, return "NO"
        Ok("NO".to_string())
      }
    }
  }
}

pub async fn confirm2au(em: String, tsy: String) -> AppResult<String> {
  let ep = Ep::Users;

  let qr = json!({
    "projection":"BASIC",
  });

  // NOTE  need to ad query from surreal to get email and create token from id
  let url = format!("{}{}", ep.base_url(), em);

  debug!("This is url in confirm2au {:#?}", url);

  let au_build = req_build("GET", &url, Some(&tsy), Some(&qr), None)
    .cwl("Could not create the auth_builder for confirm2au")?;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request for the lists function")?;

  let rfin: Value = res
    .json()
    .await
    .cwl("Failed to parse JSON response from Sheets API")?;

  let verdict = rfin["isEnrolledIn2Sv"]
    .as_bool()
    .unwrap_or(false)
    .to_string();

  Ok(verdict)
}

// NOTE this really does not need the domain argument as before, we will use the domain from which we run the command like jjme cadsh, ORG
pub async fn get_template_id_index() -> AppResult<Option<(i32, i32)>> {
  let p = PETS
    .get()
    .await
    .cwl("Could not get pet fields to check command")?;

  let args: Vec<&str> = p.params.split(", ").collect();
  let titl = args[0].trim(); // sheet to replace

  debug!("This is titl in get_template_id_index {titl}");

  let ep = match Ep::strtoenum(titl, false) {
    Some(ep) => ep,
    None => return Ok(None),
  };

  debug!("This is ep in get_template_id_index {:#?}", ep);

  let template_sheet_id = &*TSHID;
  debug!("Using template spreadsheet ID: {}", template_sheet_id);

  let Some((shid, index)) =
    get_sheet_id_index(p.tsy.clone(), ep, template_sheet_id.to_string())
      .await
      .cwl("Failed to get sheet ID from template spreadsheet")?
  else {
    debug!(
      "get_template_id_index: sheet '{}' not found in template spreadsheet",
      titl
    );
    return Ok(None);
  };

  debug!(
    "get_template_id_index: found sheet '{}' in template with shid={}, index={}",
    titl, shid, index
  );
  Ok(Some((shid, index)))
}
