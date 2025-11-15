#![allow(dead_code)]

use super::apis::*;
use crate::AppResult;
use crate::goauth::get_token_for_secrets;
use crate::limiters::get_global_sheets_limiter;
use crate::surrealstart::{
  DB, EM, PETS, getnumqry, google_to_sheetsdb, req_build,
};
use crate::tracer::ContextExt;
use crate::{bail, debug, error, info, warn};
use regex::Regex;
use serde_json::{Map, Value, json};
use std::time::Instant;

//https://gemini.google.com/app/3f9756a96e8d2631

pub async fn frshts<T: Into<Ep>>(
  tse: String,
  abr: String,
  ep: T,
  spshid: String, // SID for avisosbot, zerotouchbot
) -> AppResult<()> {
  let ep: Ep = ep.into();
  let eps = Ep::Sheets;

  let url = format!(
    "{}{}/values/{}!A1:AI",
    eps.base_url(),
    spshid,
    ep.table_sheet().to_uppercase()
  );

  let au_build = req_build("GET", &url, Some(&tse), None, None)
    .cwl("Could not create the auth_builder for the lists function")?;

  // debug!("This is auth_builder {:#?}", au_build);

  // Apply rate limiting for Google Sheets API
  get_global_sheets_limiter().until_ready().await;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request for the lists function")?;
  // Log token metadata for Sheets GET
  debug!(
    token_kind = "sheets_get",
    is_bearer = tse.starts_with("Bearer "),
    token_len = tse.len(),
    token_preview = %tse.chars().take(18).collect::<String>(),
    url = %url,
    "frshts: request auth metadata"
  );

  match res.status().as_u16() {
    200 => {
      let rfin: Value = res
        .json()
        .await
        .cwl("Failed to parse JSON response from Sheets API")?;

      // debug!("This is rfin in frshts {:#?}", rfin);

      let res_array =
        rfin[eps.res_obs()].as_array().cloned().unwrap_or_default();

      debug!(
        "frshts: Got {} raw rows from Google Sheets API",
        res_array.len()
      );

      if res_array.is_empty() {
        warn!("No rows found in sheet data, skipping processing");
        return Ok(());
      }

      let rows: Vec<Vec<String>> = res_array
        .into_iter()
        .filter_map(|i| match serde_json::from_value(i) {
          Ok(row) => Some(row),
          Err(e) => {
            warn!(error = %e, "Failed to deserialize row, skipping");
            None
          }
        })
        .collect();

      debug!("frshts: Deserialized {} rows successfully", rows.len());

      let rws: Vec<Vec<String>> =
        rows.into_iter().filter(|row| !row.is_empty()).collect();

      debug!("frshts: Filtered to {} non-empty rows", rws.len());

      if rws.is_empty() {
        warn!("No non-empty rows found after filtering, skipping processing");
        return Ok(());
      }

      let transformed_values = spreadsheet_format_to_values(rws);

      debug!(
        "frshts: Transformed {} rows into structured values",
        transformed_values.len()
      );

      if transformed_values.is_empty() {
        warn!(
          "No valid records after transformation, skipping database operations"
        );
        return Ok(());
      }

      debug!(
        "frshts: Sending {} records to google_to_sheetsdb",
        transformed_values.len()
      );

      google_to_sheetsdb(abr.clone(), ep.clone(), transformed_values)
        .await
        .cwl(
          "Failed to process database to sheets database for lists function",
        )?;

      Ok(())
    }
    403 => {
      let error_text = res
        .text()
        .await
        .cwl("Failed to read error response body from Google Sheets API")?;

      if tse == "NO" {
        warn!(
          "User not yet authorized - token is 'NO', needs admin table refresh"
        );
        bail!("NEW_USER_PENDING")
      } else {
        error!(
          "Google Sheets API returned 403 status: Body: {}",
          error_text
        );
        bail!(
          "Google Sheets API error (HTTP 403 Forbidden): {}. This may be due to authentication issues like 2-step verification or insufficient permissions.",
          error_text
        )
      }
    }
    status => {
      let error_text = res
        .text()
        .await
        .cwl("Failed to read error response body from Google Sheets API")?;

      error!(
        "Google Sheets API returned non-200 status: {} - Body: {}",
        status, error_text
      );
      bail!("Google Sheets API error (HTTP {}): {}", status, error_text)
    }
  }
}

// NOTE here is where you put ORDERED BY for each query if needed
pub async fn toshs<T: Into<Ep>>(
  tse: String,
  abr: String,
  ep: T,
  spshid: String,
) -> AppResult<()> {
  let ep: Ep = ep.into();
  let eps = Ep::Sheets;

  debug!(
    "toshs: Starting to send data to Google Sheets for abr: {}, endpoint: {:?}",
    abr, ep
  );

  let (base_query, order_clause) = if matches!(ep, Ep::Users) {
    (
      "select data from (select * from type::table($table)",
      " order by data.suspendido asc, data.grupo asc)",
    )
  } else if matches!(ep, Ep::Courses) {
    (
      "select data from (select * from type::table($table)",
      " order by data.estado asc, data.sala desc)",
    )
  } else {
    ("select data from type::table($table)", "")
  };

  let qry = if !abr.is_empty() {
    format!("{} where abr = $abr{}", base_query, order_clause)
  } else {
    format!("{}{}", base_query, order_clause)
  };

  debug!(
    "toshs: Query details - qry: '{}', table: '{}', abr: '{}', spshid: '{}'",
    qry,
    ep.table_sheet(),
    abr,
    spshid
  );
  let result: Vec<Value> = DB
    .query(qry)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr.clone()))
    .await
    .cwl("Failed to execute query")?
    .take(0)
    .cwl("Failed to take query result")?;
  debug!("Database query returned {} raw records", result.len());

  // Debug: For Files endpoint, show exactly what we got from the database
  if matches!(ep, Ep::Files) {
    debug!("toshs: RAW database result for Files:");
    for (i, raw_record) in result.iter().take(2).enumerate() {
      debug!("toshs: Raw record {}: {:?}", i + 1, raw_record);
    }
  }

  // notes
  // debug!("toshs: Raw database records: {:#?}", result);

  // let data_values: Vec<Value> = result
  //   .into_iter()
  //   .filter_map(|record| record.get("data").cloned())
  //   .collect();

  // debug!(
  //   "toshs: Extracted {} data records from database",
  //   data_values.len()
  // );

  let mut data_values: Vec<Value> = Vec::new();
  for (i, record) in result.into_iter().enumerate() {
    // debug!("toshs: Processing record {}: {:?}", i + 1, record);
    if let Some(data) = record.get("data").cloned() {
      // debug!("toshs: Record {} has valid data: {:?}", i + 1, data);
      data_values.push(data);
    } else {
      debug!("toshs: Record {} has no 'data' field, skipping", i + 1);
    }
  }

  debug!(
    "toshs: Final result - Extracted {} data records from database",
    data_values.len()
  );

  // Debug: For Files endpoint, show what we got for nombre_carpeta
  if matches!(ep, Ep::Files) {
    for (i, record) in data_values.iter().take(3).enumerate() {
      if let Some(nombre_carpeta) = record.get("nombre_carpeta") {
        debug!(
          "toshs: Files record {} nombre_carpeta: {:?}",
          i + 1,
          nombre_carpeta
        );
      } else {
        debug!(
          "toshs: Files record {} MISSING nombre_carpeta field!",
          i + 1
        );
      }
    }
  }

  let mut ve = values_to_spreadsheet_format(&data_values, &ep);
  debug!(
    "toshs: Formatted {} rows for Google Sheets (including header)",
    ve.len()
  );

  // If no data but we still need headers, add them
  if data_values.is_empty() {
    ve = get_headers_for_endpoint(&ep);
    debug!("No data found, adding headers only for endpoint: {:?}", ep);
  }

  clshs(tse.clone(), ep.clone(), spshid.clone(), "A1:AI".to_string())
    .await
    .cwl("Could not clear the sheet.")?;

  let url = format!(
    "{}{}/values/{}!A1:AI:append",
    eps.base_url(),
    spshid.clone(),
    ep.table_sheet().to_uppercase()
  );

  let qr = json!({
    "valueInputOption":"USER_ENTERED",
    "insertDataOption": "OVERWRITE"
  });

  let vuls = json!({ "values": ve });

  debug!("toshs: Sending {} rows to Google Sheets API", ve.len());

  let au_build = req_build("POST", &url, Some(&tse), Some(&qr), Some(&vuls))
    .cwl("Could not create the auth_builder for chres")?;

  // debug!("This is au_build in toshs {:#?}", au_build);

  // Apply rate limiting for Google Sheets API
  get_global_sheets_limiter().until_ready().await;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request for the lists function")?;
  debug!(
    token_kind = "sheets_append",
    is_bearer = tse.starts_with("Bearer "),
    token_len = tse.len(),
    token_preview = %tse.chars().take(18).collect::<String>(),
    url = %url,
    rows = ve.len(),
    "toshs: request auth metadata"
  );

  match res.status().as_u16() {
    200 => {
      debug!(
        "toshs: Successfully sent {} rows to Google Sheets",
        ve.len()
      );
      shchechboxes(tse.clone(), ep.clone(), spshid.clone())
        .await
        .cwl("Failed to add checkboxes to sheet.")?;
      Ok(())
    }
    status => {
      let error_text = res
        .text()
        .await
        .cwl("Failed to read error response body from Google Sheets API")?;

      error!(
        "Google Sheets API returned non-200 status: {} - Body: {}",
        status, error_text
      );
      bail!("Google Sheets API error (HTTP {}): {}", status, error_text)
    }
  }
}

pub async fn clshs<T: Into<Ep>>(
  tse: String,
  ep: T,
  spshid: String,
  range: String,
) -> AppResult<()> {
  let ep: Ep = ep.into();
  let eps = Ep::Sheets;

  let url = format!(
    "{}{}/values/{}!{}:clear",
    eps.base_url(),
    &spshid,
    ep.table_sheet().to_uppercase(),
    range
  );

  debug!("This is url in chshs {:#?}", url);

  // NOTE  DO NOT ERASE, google server error, needs empty body
  let body = json!({});

  let au_build = req_build("POST", &url, Some(&tse), None, Some(&body))
    .cwl("Could not create the auth_builder for clshs")?;

  // debug!("This is au_build clshs {:#?}", au_build);

  debug!("about to send request to Google Sheets API for clshs");

  // Apply rate limiting for Google Sheets API
  get_global_sheets_limiter().until_ready().await;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request for the lists function")?;
  debug!(
    token_kind = "sheets_clear",
    is_bearer = tse.starts_with("Bearer "),
    token_len = tse.len(),
    token_preview = %tse.chars().take(18).collect::<String>(),
    url = %url,
    range = %range,
    "clshs: request auth metadata"
  );

  debug!(
    "received response from Google Sheets API for clshs - status: {}",
    res.status()
  );
  // debug!("response headers: {:#?}", res.headers());

  match res.status().as_u16() {
    200 => {
      let response_body =
        res.text().await.cwl("Failed to read response body")?;
      debug!("successful response body: {}", response_body);
      debug!("Successfully cleared sheet history");
      Ok(())
    }
    status => {
      let error_text = res.text().await.cwl(
        "Failed to read error response body from Google Sheets API for clshs",
      )?;

      error!("Error response status: {} - Body: {}", status, error_text);
      bail!("Google Sheets API error (HTTP {}): {}", status, error_text)
    }
  }
}

pub async fn shchechboxes<T: Into<Ep>>(
  tse: String,
  ep: T,
  spshid: String,
) -> AppResult<()> {
  let ep: Ep = ep.into();
  let eps = Ep::Sheets;

  let Some((shid, _)) =
    get_sheet_id_index(tse.clone(), ep.clone(), spshid.clone())
      .await
      .cwl("Failed to get sheet ID for checkbox operation")?
  else {
    return Ok(());
  };

  let mut vecs: Vec<Value> = Vec::new();

  // checkboxes
  let ju9 = json!({
    "setDataValidation": {
      "range":{
        "sheetId": &shid,
        "startRowIndex":1,
        "startColumnIndex":0,
        "endColumnIndex":1
      },
      "rule":{
        "condition":{
          "type":"BOOLEAN",
          "values": [
            {"userEnteredValue": "d"},
            {"userEnteredValue": "x"}
            ]
        },
        "showCustomUi":"true",
        "strict":"true"
      }
    }
  });

  // freeze one column and row
  let ju4 = json!({
    "updateSheetProperties": {
      "properties":{
        "sheetId": &shid,
        "gridProperties":{
          "frozenRowCount":1,
          "frozenColumnCount":1
        }
      },
      "fields": "gridProperties(frozenRowCount,frozenColumnCount)"
    }
  });

  // center checkboxes
  let ju3 = json!({
    "repeatCell": {
      "range":{
        "sheetId": &shid,
        "startRowIndex":0,
        "startColumnIndex":0,
        "endColumnIndex":1
      },
      "cell":{
        "userEnteredFormat":{
          "horizontalAlignment" : "CENTER"
        }
      },
      "fields":"userEnteredFormat(horizontalAlignment)"
    }
  });

  vecs.push(ju3);
  vecs.push(ju4);
  vecs.push(ju9);

  let qr4 = json!({ "requests": vecs });
  let url = format!("{}{}:batchUpdate", eps.base_url(), &spshid);

  let au_build = req_build("POST", &url, Some(&tse), None, Some(&qr4))
    .cwl("Could not create the auth_builder for chres")?;

  // debug!("This is au_build in boxes {:#?}", au_build);

  // Apply rate limiting for Google Sheets API
  get_global_sheets_limiter().until_ready().await;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request for the lists function")?;

  match res.status().as_u16() {
    200 => {
      // let rfin: Value = res
      //   .json()
      //   .await
      //   .cwl("Failed to parse JSON response from shchechboxes")?;
      Ok(())
    }
    status => {
      let error_text = res
        .text()
        .await
        .cwl("Failed to read error response body from Google Sheets API for shchechboxe")?;

      error!(
        "Google Sheets API returned non-200 status: {} - Body: {}",
        status, error_text
      );
      bail!("Google Sheets API error (HTTP {}): {}", status, error_text)
    }
  }
}

pub async fn get_sheet_id_index<T: Into<Ep>>(
  tse: String,
  ep: T,
  spshid: String,
) -> AppResult<Option<(i32, i32)>> {
  let ep: Ep = ep.into();
  let eps = Ep::Sheets;
  let sheet_name = ep.table_sheet().to_uppercase();
  let url = format!("{}{}", eps.base_url(), &spshid);
  debug!(url = %url, spreadsheet_id = %spshid, target_sheet = %sheet_name, "Requesting spreadsheet metadata to find sheet ID.");

  let qry = json!({
      "fields": "sheets(properties(title,sheetId,index))"
  });

  let au_build = req_build("GET", &url, Some(&tse), Some(&qry), None)
    .cwl("Could not create the auth_builder for get_sheet_id")?;

  // Apply rate limiting for Google Sheets API
  get_global_sheets_limiter().until_ready().await;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request for the lists function")?;
  debug!(
    token_kind = "sheets_metadata",
    is_bearer = tse.starts_with("Bearer "),
    token_len = tse.len(),
    token_preview = %tse.chars().take(18).collect::<String>(),
    url = %url,
    "get_sheet_id_index: request auth metadata"
  );

  match res.status().as_u16() {
    200 => {
      let rfin: Value = res
        .json()
        .await
        .cwl("Failed to parse JSON response from get_sheet_id")?;

      if let Some(sheets) = rfin["sheets"].as_array() {
        for sheet in sheets {
          if let Some(title) = sheet
            .get("properties")
            .and_then(|p| p.get("title"))
            .and_then(Value::as_str)
          {
            if title == sheet_name {
              if let (Some(sheet_id), Some(index)) = (
                sheet
                  .get("properties")
                  .and_then(|p| p.get("sheetId"))
                  .and_then(Value::as_i64),
                sheet
                  .get("properties")
                  .and_then(|p| p.get("index"))
                  .and_then(Value::as_i64),
              ) {
                debug!(sheet_name = %sheet_name, sheet_id = sheet_id, index = index, spreadsheet_id = spshid, "Found matching sheet ID and index.");
                return Ok(Some((sheet_id as i32, index as i32))); // Found it! Return Ok.
              } else {
                warn!(sheet_name = %sheet_name, spreadsheet_id = spshid, sheet_data = ?sheet, "Sheet found by title, but sheetId or index was missing or not an integer.");
              }
            }
          } else {
            warn!(spreadsheet_id = spshid, sheet_data = ?sheet, "Skipping sheet with missing properties or title.");
          }
        }
      } else {
        warn!(spreadsheet_id = spshid, response_data = ?rfin, "Response JSON did not contain a 'sheets' array or it was null/invalid.");
      }

      debug!(target_sheet = %sheet_name, spreadsheet_id = spshid, "Target sheet not found within the spreadsheet.");
      Ok(None)
    }
    status => {
      let error_text = res
        .text()
        .await
        .cwl("Failed to read error response body from Google Sheets API for get_sheet_id")?;

      error!(
        "Google Sheets API returned non-200 status: {} - Body: {} - Spreadsheet ID: {}",
        status, error_text, spshid
      );
      bail!("API error for spreadsheet ID: {}", spshid)
    }
  }
}

pub fn values_to_spreadsheet_format(
  data: &[Value],
  ep: &Ep,
) -> Vec<Vec<String>> {
  // Input is now a slice
  if data.is_empty() {
    return vec![];
  }

  let first_obj = match data.first().and_then(Value::as_object) {
    Some(obj) => obj,
    None => return vec![],
  };

  // Get ordered keys based on the endpoint type
  let ordered_keys = get_ordered_keys_for_endpoint(ep, first_obj);

  // Create uppercase header row based on ordered keys
  let headers: Vec<String> =
    ordered_keys.iter().map(|k| k.to_uppercase()).collect();

  let mut rows = Vec::with_capacity(data.len() + 1);
  rows.push(headers); // Add uppercase headers row

  for value in data.iter() {
    let obj = match value.as_object() {
      Some(o) => o,
      None => {
        // Skip non-object values instead of adding empty rows
        continue;
      }
    };

    // Create data row by looking up values using the ordered keys
    let row: Vec<String> = ordered_keys
      .iter()
      .map(|key| {
        // Iterate ordered keys
        match obj.get(key) {
          // Lookup using the key
          Some(Value::String(s)) => s.clone(),
          Some(Value::Number(n)) => n.to_string(),
          Some(Value::Bool(b)) => b.to_string(),
          Some(Value::Null) => String::new(),
          Some(other_value) => other_value.to_string(), // Fallback for complex types
          None => String::new(),                        // Key not found
        }
      })
      .collect();

    rows.push(row);
  }

  rows
}

fn get_headers_for_endpoint<T: Into<Ep> + Clone>(ep: &T) -> Vec<Vec<String>> {
  let ep_clone = ep.clone().into();
  let query_text = ep_clone.qrys();
  let mut keys = extract_keys_from_query(&query_text);

  if keys.is_empty() {
    keys = vec![];
  }

  let headers: Vec<String> = keys.iter().map(|k| k.to_uppercase()).collect();
  vec![headers]
}

fn extract_keys_from_query(query: &str) -> Vec<String> {
  let mut keys = Vec::new();
  let select_list_content_to_parse: &str;
  let re = Regex::new(r"(?i)select\s+('x'\s+as\s+do)").unwrap();
  let mut last_match_start_of_list_content: Option<usize> = None;

  for captures in re.captures_iter(query) {
    if let Some(list_start_match) = captures.get(1) {
      last_match_start_of_list_content = Some(list_start_match.start());
    }
  }

  if let Some(list_actual_start_pos) = last_match_start_of_list_content {
    let query_portion_containing_list = &query[list_actual_start_pos..];
    let lower_query_portion_containing_list =
      query_portion_containing_list.to_lowercase();

    // Find the main " from " clause, ignoring any inside parentheses
    let mut paren_depth = 0;
    let mut from_pos = None;
    let chars: Vec<char> = query_portion_containing_list.chars().collect();
    let lower_chars: Vec<char> =
      lower_query_portion_containing_list.chars().collect();

    for i in 0..chars.len() {
      match chars[i] {
        '(' => paren_depth += 1,
        ')' => paren_depth -= 1,
        _ => {}
      }

      if paren_depth == 0 && i + 6 < chars.len() {
        let slice = &lower_chars[i..i + 6];
        if slice == [' ', 'f', 'r', 'o', 'm', ' '] {
          from_pos = Some(i);
          break;
        }
      }
    }

    if let Some(pos) = from_pos {
      select_list_content_to_parse = &query_portion_containing_list[..pos];
    } else {
      select_list_content_to_parse = query_portion_containing_list;
    }
  } else if query.trim_start().to_lowercase().starts_with("'x' as do") {
    select_list_content_to_parse = query.trim_start(); // Use trimmed version
  } else {
    return keys; // Return empty
  }

  if select_list_content_to_parse.trim().is_empty() {
    return keys;
  }

  for field_definition in select_list_content_to_parse.split(',') {
    let trimmed_definition = field_definition.trim();
    if trimmed_definition.is_empty() {
      continue;
    }

    if let Some(as_pos) = trimmed_definition.to_lowercase().rfind(" as ") {
      let alias_part_candidate =
        trimmed_definition[as_pos + " as ".len()..].trim_start();
      if let Some(alias) = alias_part_candidate.split_whitespace().next() {
        keys.push(alias.to_string());
      }
    }
  }

  keys
}

// fn extract_keys_from_query(query: &str) -> Vec<String> {
//   let mut keys = Vec::new();

//   for line in query.lines() {
//     let line = line.trim();
//     if line.contains(" as ") {
//       if let Some(key_part) = line.split(" as ").nth(1) {
//         if let Some(key) = key_part.split_whitespace().next() {
//           let clean_key = key.trim_end_matches(',');
//           keys.push(clean_key.to_string());
//         }
//       }
//     }
//   }

//   keys
// }

// Helper function to get ordered keys for an endpoint
fn get_ordered_keys_for_endpoint(
  ep: &Ep,
  obj: &Map<String, Value>,
) -> Vec<String> {
  // Get the query text for this endpoint
  let query_text = ep.qrys();

  // Extract keys from the query
  let keys = extract_keys_from_query(&query_text);

  // Log what we found to help with debugging
  debug!(
    "Extracted {} keys from query for endpoint {:?}: {:?}",
    keys.len(),
    ep,
    keys
  );

  // If we found keys in the query, use that order
  if !keys.is_empty() {
    return keys;
  }

  // Fallback: use the keys from the object in their original order
  warn!(
    "No column order found in query for endpoint {:?}, using default object key order",
    ep
  );
  obj.keys().cloned().collect()
}

pub fn spreadsheet_format_to_values(data: Vec<Vec<String>>) -> Vec<Value> {
  if data.is_empty() || data[0].is_empty() {
    return vec![];
  }
  let keys: Vec<String> = data[0].iter().map(|s| s.to_lowercase()).collect();

  data
    .iter()
    .skip(1)
    .map(|row| {
      let mut map = Map::new();
      for (key, value) in keys.iter().zip(row.iter()) {
        // Special handling for the "do" field to convert checkbox values
        let processed_value = if key == "do" {
          match value.as_str() {
            "TRUE" => "d".to_string(),  // checked = marked row
            "FALSE" => "x".to_string(), // unchecked = unmarked row
            other => other.to_string(), // keep as is for other values
          }
        } else {
          value.clone()
        };
        map.insert(key.clone(), Value::String(processed_value));
      }
      Value::Object(map)
    })
    .collect()
}

pub async fn frshtsadmin(cmd_override: Option<&str>) -> AppResult<()> {
  let tsn = get_token_for_secrets()
    .await
    .cwl("Failed to get token for secrets")?;

  let ep = Ep::Admins;

  // NOTE this is the original sheet id for admins
  // let shi = "1_oVdctSf2OkJbp8z8kmwxaxC-K5-XaHqDlIpW6jWXn4";

  let shi = "1HnXYhMJZsM-_DYR0SPzU236D0aKVRxNfEUrxU295bW4";
  let url = format!(
    "{}{shi}/values/{}!A1:AI",
    ep.base_url(),
    ep.table_sheet().to_uppercase()
  );

  debug!(url = %url, "Fetching admin data from Google Sheets");

  let au_build = req_build("GET", &url, Some(&tsn), None, None)
    .cwl("Failed to create auth builder for frshtsadmin")?;

  // Apply rate limiting for Google Sheets API
  get_global_sheets_limiter().until_ready().await;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request for frshtsadmin")?;

  match res.status().as_u16() {
    200 => {
      let rfin: Value = res
        .json()
        .await
        .cwl("Failed to parse JSON response from Sheets API")?;

      let res = rfin[ep.res_obs()].as_array().cloned().unwrap_or_default();

      if res.is_empty() {
        warn!("No rows found in sheet data, skipping processing");
        return Ok(());
      }

      let rows: Vec<Vec<String>> = res
        .into_iter()
        .filter_map(|i| match serde_json::from_value(i) {
          Ok(row) => Some(row),
          Err(e) => {
            warn!(error = %e, "Failed to deserialize row, skipping");
            None
          }
        })
        .collect();

      let rws: Vec<Vec<String>> =
        rows.into_iter().filter(|row| !row.is_empty()).collect();

      if rws.is_empty() {
        warn!("No non-empty rows found after filtering, skipping processing");
        return Ok(());
      }

      let transformed_values = spreadsheet_format_to_values(rws);

      // debug!(
      //   record_count = transformed_values.len(),
      //   "frshtsadmin: Processing admin records"
      // );

      // for (i, record) in transformed_values.iter().enumerate() {
      //   let do_status = record
      //     .get("do")
      //     .and_then(|v| v.as_str())
      //     .unwrap_or("unknown");
      //   let domain = record
      //     .get("dominio")
      //     .and_then(|v| v.as_str())
      //     .unwrap_or("unknown");
      //   debug!(
      //     "frshtsadmin: Record {} - domain: '{}', do status: '{}', full record: {:?}",
      //     i + 1,
      //     domain,
      //     do_status,
      //     record
      //   );
      // }

      let mut valid_records = Vec::new();
      let mut skipped_count = 0;

      for mut admin_record in transformed_values {
        let domain = match admin_record.as_object_mut() {
          Some(obj) => match obj.get("dominio").and_then(|d| d.as_str()) {
            Some(d) if !d.is_empty() => d.to_string(),
            _ => {
              warn!(record = ?obj, "Skipping record due to missing or empty domain");
              skipped_count += 1;
              continue;
            }
          },
          _ => {
            warn!(record = ?admin_record, "Skipping record due to invalid format");
            skipped_count += 1;
            continue;
          }
        };

        // debug!(domain = %domain, "Processing domain");

        if let Some(obj) = admin_record.as_object_mut() {
          let current_admins = obj
            .get("admins")
            .and_then(|v| v.as_str())
            .filter(|s| !s.trim().is_empty())
            .unwrap_or("");

          let ieducando_admins = format!(
            "juanjose.arroyo@ieducando.com, victor.segura@ieducando.com, \
             guadalupe.yanez@ieducando.com, jason.jurotich@ieducando.com, {}, ieducando@{domain}",
            &*EM
          );

          let extended_admins = if current_admins.is_empty() {
            ieducando_admins
          } else if current_admins.contains("juanjose.arroyo@ieducando.com") {
            current_admins.to_string()
          } else {
            format!("{current_admins}, {ieducando_admins}")
          };

          obj.insert("admins".to_string(), Value::String(extended_admins));
          // debug!(domain = %domain, "Updated admins list for domain");
        }

        // let do_status = admin_record
        //   .get("do")
        //   .and_then(|v| v.as_str())
        //   .unwrap_or("unknown");
        // debug!(
        //   "frshtsadmin: Record do status: '{}' for domain '{}'",
        //   do_status, domain
        // );

        valid_records.push(admin_record);
      }

      debug!(
        "frshtsadmin: Processing {} valid records, skipped {}",
        valid_records.len(),
        skipped_count
      );

      if !valid_records.is_empty() {
        let batch_size: usize = std::env::var("SURREAL_BATCH_SIZE")
          .unwrap_or_else(|_| "2500".to_string())
          .parse()
          .unwrap_or(2500);
        let total_chunks = valid_records.len().div_ceil(batch_size);

        debug!(
          "frshtsadmin: Processing {} records in {} chunks of {} each",
          valid_records.len(),
          total_chunks,
          batch_size
        );

        if cmd_override == Some("cadsh") {
          debug!("frshtsadmin: Using optimized DELETE + CREATE for cadsh");

          DB.query("delete type::table($table);")
            .bind(("table", ep.table_sheet()))
            .await
            .cwl("Failed to delete existing admin records")?;

          for (chunk_idx, chunk) in valid_records.chunks(batch_size).enumerate()
          {
            debug!(
              "frshtsadmin: Processing chunk {}/{} ({} records)",
              chunk_idx + 1,
              total_chunks,
              chunk.len()
            );

            let chunk_vec: Vec<_> = chunk.to_vec();

            DB.query(
              "
              for $record in $records {
                create type::thing($table, rand::uuid::v7())
                set data = $record, ers = '';
              }
              ",
            )
            .bind(("table", ep.table_sheet()))
            .bind(("records", chunk_vec))
            .await
            .cwl(&format!(
              "Failed to batch insert records in chunk {}",
              chunk_idx + 1
            ))?;
          }
        } else {
          for (chunk_idx, chunk) in valid_records.chunks(batch_size).enumerate()
          {
            debug!(
              "frshtsadmin: Processing chunk {}/{} ({} records)",
              chunk_idx + 1,
              total_chunks,
              chunk.len()
            );

            let chunk_vec: Vec<_> = chunk.to_vec();

            DB.query(
              "
              for $record in $records {
                if $record.do = 'd' {
                  let $existing = (
                    select id from type::table($table)
                    where data.dominio = $record.dominio
                    limit 1
                  );
                  if $existing {
                    update $existing[0].id
                    set data = $record, ers = '';
                  } else {
                    create type::thing($table, rand::uuid::v7())
                    set data = $record, ers = '';
                  };
                };
              }
              ",
            )
            .bind(("table", ep.table_sheet()))
            .bind(("records", chunk_vec))
            .await
            .cwl(&format!(
              "Failed to batch upsert records in chunk {}",
              chunk_idx + 1
            ))?;
          }
        }
      }

      // Check final database state after all upserts - use count query
      let count_query = "select count() from type::table($table) group all";
      let mut count_response = DB
        .query(count_query)
        .bind(("table", ep.table_sheet()))
        .await
        .cwl("Failed to query final record count")?;

      let count_result: Vec<Value> = count_response
        .take(0)
        .cwl("Failed to take count query result")?;

      if let Some(count_obj) = count_result.first()
        && let Some(count_val) = count_obj.get("count")
      {
        debug!(
          "frshtsadmin: Final database state after all upserts - {} records total",
          count_val
        );
      }

      info!("ALL ADMIN RECORDS WERE PROCESSED CORRECTLY");
      Ok(())
    }
    status => {
      let error_text = res
        .text()
        .await
        .cwl("Failed to read error response from API")?;

      error!(
        status = status,
        error = %error_text,
        "Google Sheets API returned error status"
      );

      Err(anyhow::anyhow!("API error: {} - {}", status, error_text))
    }
  }
}

pub async fn count_db<T: Into<Ep>>(abr: String, ep: T) -> AppResult<u64> {
  let ep = ep.into();

  let qry = "
    select count() from type::table($table)
    where abr = $abr group all;
    ";

  let mut result = DB
    .query(qry)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr.clone()))
    .await
    .cwl("Could not get admin info from database")?;

  let record_count: u64 = result
    .take::<Option<serde_json::Map<String, serde_json::Value>>>(0)?
    .and_then(|obj| obj.get("count")?.as_u64())
    .unwrap_or(0);

  Ok(record_count)
}

pub async fn setup_orgbase(
  tse: String,
  abr: String,
  spshid: String,
) -> AppResult<()> {
  let ep = Ep::Orgbase;

  let record_count = count_db(abr.clone(), ep.clone())
    .await
    .cwl("Could not get count from database")?;

  if record_count == 0 {
    frshts(tse.clone(), abr.clone(), ep.clone(), spshid)
      .await
      .cwl("Failed to refresh sheets")?;
  } else {
    // The table has records
    debug!("Table '{}' has {} records.", ep.table_sheet(), record_count);
  }

  // let results = DB
  //   .query("select * from type::table($table) limit 5;")
  //   .bind(("table", ep.table_sheet()))
  //   .await?;
  // debug!("This is results from orgbase database {:#?}", results);

  Ok(())
}

pub async fn setup_lictyp() -> AppResult<()> {
  let start = Instant::now();
  let ep = Ep::Lictyp;

  let p = PETS
    .get()
    .await
    .cwl("Could not get pet fields for function_one")?;

  // COMMENTED OUT - CHAT NOT NEEDED FOR THIS PROJECT
  // let epch1 = chstlinit(ep.clone(), p.cmd.clone())
  //   .await
  //   .cwl("Could not get chat init text")?;
  //
  // chres(epch1, p.sp.clone(), p.tsn.clone())
  //   .await
  //   .cwl("Failed to send start mod chat message")?;

  let record_count = count_db(p.abr.clone(), ep.clone())
    .await
    .cwl("Could not get count from database")?;

  if record_count == 0 {
    debug!(
      "Inserting {} license type records for domain {}",
      lcs().len(),
      p.abr.clone()
    );
    for record in lcs().iter().cloned() {
      let sql = r#"
        create type::thing($table, rand::uuid::v7())
        set data = $record, abr = $abr,
        ers = '', data.do = 'x';
      "#;

      DB.query(sql)
        .bind(("table", ep.table_sheet()))
        .bind(("record", record))
        .bind(("abr", p.abr.clone()))
        .await
        .cwl("Failed to insert license type record")?;
    }
  }

  // let results = DB
  //   .query("select * from type::table($table) limit 5;")
  //   .bind(("table", ep.table_sheet()))
  //   .await?;
  // debug!("This is results from orgbase database {:#?}", results);

  toshs(p.tsy, p.abr.clone(), ep.clone(), p.spshid)
    .await
    .cwl("Failed to process data to sheets for setup_lictyp")?;

  let duration = start.elapsed().as_millis();
  let duration_usize: usize = duration
    .try_into()
    .cwl("Failed to convert duration to usize")?;
  let tims = durs(duration_usize).await;

  let _epch2 = getnumqry(ep.clone(), p.abr.clone(), p.cmd.clone(), tims)
    .await
    .cwl("Failed to get number query")?;

  // COMMENTED OUT - CHAT NOT NEEDED FOR THIS PROJECT
  // chres(epch2, p.sp, p.tsn)
  //   .await
  //   .cwl("Failed to send end mod chat message")?;

  Ok(())
}

pub async fn delete_sheet(
  tse: String,
  spshid: &str,
  shid: i32,
) -> AppResult<()> {
  let ep = Ep::Sheets;

  let url = format!("{}{}:batchUpdate", ep.base_url(), spshid);

  debug!(
    url = %url,
    ss_id = %spshid,
    sheet_id = %shid,
    "Preparing to delete sheet."
  );

  let bod = json!({
    "requests": [
      {
        "deleteSheet": {
            "sheetId": shid
        }
      }
    ]
  });

  let au_build = req_build("POST", &url, Some(&tse), None, Some(&bod))
    .cwl("Could not create the auth_builder for delete_sheet")?;

  // Apply rate limiting for Google Sheets API
  get_global_sheets_limiter().until_ready().await;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request for delete_sheet")?;

  debug!("This is res in delete_sheet {:#?}", res);

  match res.status().as_u16() {
    404 => {
      warn!("Sheet doesn't exist (404), skipping deletion");
      Ok(())
    }
    400 => {
      let error_text = res.text().await.cwl(
        "Failed to read error response body from Google Sheets API in delete_sheet",
      )?;

      if error_text.contains("does not exist") {
        warn!("Sheet doesn't exist (400), skipping deletion");
        Ok(())
      } else {
        error!(
          "Google Sheets API returned 400 status: {} - Body: {}",
          400, error_text
        );
        bail!(
          "Google Sheets API error (HTTP 400 Bad Request): {}",
          error_text
        )
      }
    }
    200 => {
      let rfin: Value = res
        .json()
        .await
        .cwl("Failed to parse JSON response from Sheets API")?;
      debug!("This is rfin in delete_sheet {:#?}", rfin);
      Ok(())
    }
    status => {
      let error_text = res.text().await.cwl(
        "Failed to read error response body from Google Sheets API in delete_sheet",
      )?;

      error!(
        "Google Sheets API returned non-200 status: {} - Body: {}",
        status, error_text
      );
      bail!("Google Sheets API error (HTTP {}): {}", status, error_text)
    }
  }
}

pub async fn rename_sheet_and_index(
  tse: String,
  spshid: &str,
  shid: i32,
  new_sheet_name: &str,
  index: i32,
) -> AppResult<()> {
  let ep = Ep::Sheets;

  let url = format!("{}{}:batchUpdate", ep.base_url(), spshid);

  // debug!(
  //   url = %url,
  //   ss_id = %spreadsheet_id,
  //   sheet_id = sheet_id,
  //   new_name = %new_sheet_name,
  //   "Preparing to rename sheet."
  // );

  let bod = json!({
    "requests": [
      {
        "updateSheetProperties": {
          "properties": {
            "sheetId": shid,
            "title": new_sheet_name,
            "index": index
          },
          "fields": "title,index"
        }
      }
    ]
  });

  let au_build = req_build("POST", &url, Some(&tse), None, Some(&bod))
    .cwl("Could not create the auth_builder for rename_sheet")?;

  // Apply rate limiting for Google Sheets API
  get_global_sheets_limiter().until_ready().await;

  let res = au_build
    .send()
    .await
    .cwl("Failed to send request for rename_sheet")?;

  debug!("This is res in rename_sheet {:#?}", res);

  match res.status().as_u16() {
    200 => {
      let rfin: Value = res
        .json()
        .await
        .cwl("Failed to parse JSON response from Sheets API")?;
      debug!("This is rfin in rename_sheet {:#?}", rfin);
    }
    status => {
      let error_text = res.text().await.cwl(
        "Failed to read error response body from Google Sheets API in rename_sheet",
      )?;

      error!(
        "Google Sheets API returned non-200 status: {} - Body: {}",
        status, error_text
      );
      bail!("Google Sheets API error (HTTP {}): {}", status, error_text)
    }
  }
  Ok(())
}

// pub async fn check_num_db(
//   tse: String,
//   abr: String,
//   check: &str,
// ) -> AppResult<()> {
//   let ep = match check {
//     "org" => Ep::Orgs,
//     "grp" => Ep::Groups,
//     "usr" => Ep::Users,
//     _ => Ep::Orgbase,
//   };

//   let record_count = count_db(abr.clone(), ep.clone())
//     .await
//     .cwl("Could not get count from database")?;

//   if record_count == 0 {
//     frshts(tse.clone(), abr.clone(), ep.clone(), "no".to_string())
//       .await
//       .cwl("Failed to refresh sheets")?;
//   } else {
//     // The table has records
//     debug!("Table '{}' has {} records.", ep.table_sheet(), record_count);
//   }

//   let results = DB
//     .query("select * from type::table($table) limit 5;")
//     .bind(("table", ep.table_sheet()))
//     .await?;
//   debug!("This is results from orgbase database {:#?}", results);

//   Ok(())
// }
