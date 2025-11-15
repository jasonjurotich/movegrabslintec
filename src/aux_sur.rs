pub use super::apis::*;
pub use super::surrealstart::*;
use crate::AppResult;
use crate::sheets::frshts;
use crate::tracer::ContextExt;
use crate::{debug, error};
use serde_json::Value;
use std::collections::HashMap;

pub async fn get_org_from_orgbase(
  abr: String,
  grp: String,
) -> AppResult<String> {
  let table = if !grp.is_empty() {
    Ep::Orgbase
  } else {
    Ep::Orgs
  };

  let qry = if !grp.is_empty() {
    "select value data.org from type::table($table)
    where abr = $abr and data.grupo = $grp;"
  } else {
    "select value data.ruta from type::table($table)
    where abr = $abr and data.do = 'd';"
  };

  let mut query = DB
    .query(qry)
    .bind(("table", table.table_sheet()))
    .bind(("abr", abr.clone()));

  if !grp.is_empty() {
    query = query.bind(("grp", grp.clone()));
  }

  let orgstr: String = query
    .await
    .map(|mut result| {
      result
        .take::<Option<String>>(0)
        .unwrap_or(None)
        .unwrap_or_default()
    })
    .unwrap_or_default();

  // If we didn't find an org path and we're looking for a group, try refreshing orgbase
  if orgstr.is_empty() && !grp.is_empty() {
    debug!(
      "No orgpath found for group '{}' in abr '{}', refreshing orgbase and retrying",
      grp, abr
    );

    // Get the PETS to access tokens and sheet info
    let pets = crate::lists::PETS
      .get()
      .await
      .cwl("Could not get PETS for orgbase refresh")?;

    // Refresh the orgbase table from Google Sheets
    frshts(
      pets.tsy.clone(),
      abr.clone(),
      Ep::Orgbase,
      pets.spshid.clone(),
    )
    .await
    .cwl("Failed to refresh orgbase from sheets")?;

    // Retry the query after refresh
    let retry_query = DB
      .query(qry)
      .bind(("table", table.table_sheet()))
      .bind(("abr", abr.clone()))
      .bind(("grp", grp.clone()));

    let retry_orgstr: String = retry_query
      .await
      .map(|mut result| {
        result
          .take::<Option<String>>(0)
          .unwrap_or(None)
          .unwrap_or_default()
      })
      .unwrap_or_default();

    if !retry_orgstr.is_empty() {
      debug!(
        "Successfully found orgpath '{}' for group '{}' after refresh",
        retry_orgstr, grp
      );
    } else {
      debug!(
        "Group '{}' still not found in orgbase after refresh for abr '{}'",
        grp, abr
      );
    }

    Ok(retry_orgstr)
  } else {
    Ok(orgstr)
  }
}

pub async fn get_all_org_group_mappings(abr: String) -> AppResult<HashMap<String, String>> {
  let ep = Ep::Orgbase;

  let qry = "select data.org, data.grupo from type::table($table) where abr = $abr;";

  let mut response = DB
    .query(qry)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr.clone()))
    .await
    .cwl("Failed to fetch org-group mappings")?;

  let records: Vec<Value> = response
    .take(0)
    .cwl("Failed to extract org-group mapping results")?;

  let mut map = HashMap::new();
  for record in records {
    if let (Some(org), Some(grupo)) = (
      record.get("data").and_then(|d| d.get("org")).and_then(|o| o.as_str()),
      record.get("data").and_then(|d| d.get("grupo")).and_then(|g| g.as_str()),
    ) {
      map.insert(org.to_string(), grupo.to_string());
    }
  }

  debug!("Loaded {} org-group mappings for abr {}", map.len(), abr);
  Ok(map)
}

pub async fn get_grp_from_orgbase(
  abr: String,
  org: String,
) -> AppResult<String> {
  if org.is_empty() {
    debug!("get_grp_from_orgbase org is empty, returning empty string");
    return Ok(String::new());
  }

  use crate::surrealstart::ORG_GROUP_CACHE;

  {
    let cache = ORG_GROUP_CACHE.lock().await;
    if let Some(mappings) = cache.get(&abr) {
      if let Some(grupo) = mappings.get(&org) {
        debug!("get_grp_from_orgbase cache hit for abr={}, org={}: {}", abr, org, grupo);
        return Ok(grupo.clone());
      } else {
        debug!("get_grp_from_orgbase cache hit for abr={} but org '{}' not found in {} mappings", abr, org, mappings.len());
        return Ok(String::new());
      }
    }
  }

  debug!("get_grp_from_orgbase cache miss for abr={}, org={}, falling back to database", abr, org);

  let ep = Ep::Orgbase;

  let qry = "select value data.grupo from type::table($table)
    where abr = $abr and data.org = $org;";

  debug!("get_grp_from_orgbase query: {}", qry);
  debug!("get_grp_from_orgbase table: {}", ep.table_sheet());
  debug!("get_grp_from_orgbase abr: {}", abr);

  let query = DB
    .query(qry)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr.clone()))
    .bind(("org", org.clone()));

  let result = query.await;
  debug!("get_grp_from_orgbase raw result for org '{}': {:?}", org, result);

  let orgstr: String = result
    .map(|mut res| {
      let taken = res.take::<Option<String>>(0);
      debug!("get_grp_from_orgbase taken result: {:?}", taken);
      taken.unwrap_or(None).unwrap_or_default()
    })
    .unwrap_or_default();

  debug!("get_grp_from_orgbase final result: {}", orgstr);
  Ok(orgstr)
}

pub async fn get_grp_from_correo(
  abr: String,
  correo: String,
) -> AppResult<String> {
  let ep = Ep::Users;

  debug!("This is correo in get_grp_from_correo {:#?}", correo);

  let qry = "select value data.grupo from type::table($table)
    where abr = $abr and data.correo = $correo;";

  debug!("get_grp_from_correo query: {}", qry);
  debug!("get_grp_from_correo table: {}", ep.table_sheet());
  debug!("get_grp_from_correo abr: {}", abr);

  let mut query = DB
    .query(qry)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr.clone()));

  if !correo.is_empty() {
    query = query.bind(("correo", correo.clone()));
    debug!("get_grp_from_correo bound correo: {}", correo);
  } else {
    debug!("get_grp_from_correo correo is empty, not binding");
  }

  let result = query.await;
  debug!("get_grp_from_correo raw result: {:?}", result);

  let grpstr: String = result
    .map(|mut res| {
      let taken = res.take::<Option<String>>(0);
      debug!("get_grp_from_correo taken result: {:?}", taken);
      taken.unwrap_or(None).unwrap_or_default()
    })
    .unwrap_or_default();

  debug!("get_grp_from_correo final result: {}", grpstr);
  Ok(grpstr)
}

pub async fn get_eml_from_idusr(
  abr: String,
  idusr: String,
) -> AppResult<String> {
  let ep = Ep::Users;

  let qry = "select value data.correo from type::table($table)
    where abr = $abr and data.id = $idusr;";

  let mut query = DB
    .query(qry)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr.clone()));

  if !idusr.is_empty() {
    query = query.bind(("idusr", idusr.clone()));
    debug!("get_eml_from_idusr bound org: {}", idusr);
  } else {
    debug!("get_eml_from_idusr org is empty, not binding");
  }

  let result = query.await;
  debug!("get_eml_from_idusr raw result: {:?}", result);

  let eml: String = result
    .map(|mut res| {
      let taken = res.take::<Option<String>>(0);
      debug!("get_eml_from_idusr taken result: {:?}", taken);
      taken.unwrap_or(None).unwrap_or_default()
    })
    .unwrap_or_default();

  debug!("get_eml_from_idusr final result: {}", eml);
  Ok(eml)
}

pub async fn get_courses_grp(
  abr: &str,
  grp: String,
) -> AppResult<Vec<Vec<String>>> {
  let ep = Ep::Courses;

  debug!("This is grp in get courses grp {:#?}", grp);

  let qry = "
    select
      data.id,
      data.correo_dueno,
      data.codigo_enrolar
    from type::table($table) 
    where abr = $abr and data.sala = $grp
      and data.estado = 'ACTIVE';
    ";

  let results: Vec<Value> = DB
    .query(qry)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr.to_owned()))
    .bind(("grp", grp.clone()))
    .await
    .cwl("Failed to execute courses query")?
    .take(0)
    .cwl("Failed to take results from courses query")?;

  let mut ids = Vec::new();
  for result in results {
    if let Some(obj) = result.as_object() {
      let mut row = Vec::new();
      if let Some(data) = obj.get("data").and_then(|d| d.as_object()) {
        row.push(
          data
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        );
        row.push(
          data
            .get("correo_dueno")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        );
        row.push(
          data
            .get("codigo_enrolar")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        );
      }
      ids.push(row);
    }
  }

  Ok(ids)
}

pub async fn get_group_profs(abr: &str) -> AppResult<Vec<String>> {
  let ep = Ep::Groups;

  debug!("Getting group emails for abr: {}", abr);

  let qry = "
    select data.correo from type::table($table)
    where abr = $abr
      and (data.correo ~ '_classroom'
        or data.correo ~ '_clase'
        or data.correo ~ '_teachers')
      and data.correo !~ 'gtrainerdemo'";

  let results: Vec<Value> = DB
    .query(qry)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr.to_owned()))
    .await
    .cwl("Failed to execute group emails query")?
    .take(0)
    .cwl("Failed to take results from group emails query")?;

  let mut emails = Vec::new();
  for result in results {
    if let Some(obj) = result.as_object()
      && let Some(data) = obj.get("data").and_then(|d| d.as_object())
      && let Some(email) = data.get("correo").and_then(|v| v.as_str())
    {
      emails.push(email.to_string());
    }
  }

  debug!("Found {} group emails for abr: {}", emails.len(), abr);

  Ok(emails)
}

pub async fn check_is_admin(abr: String, usr: String) -> AppResult<bool> {
  let ep = Ep::Users;

  let qry = "
    select data.es_admin
    from type::table($table)
    where abr = $abr and data.correo = $usr
    limit 1
  ";

  let results: Vec<Value> = DB
    .query(qry)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr.clone()))
    .bind(("usr", usr.clone()))
    .await
    .cwl("Failed to execute admin check query")?
    .take(0)
    .cwl("Failed to take results from admin check query")?;

  if results.is_empty() {
    return Ok(false);
  }

  let is_admin = results[0]
    .get("data")
    .and_then(|data| data.get("es_admin"))
    .and_then(|v| v.as_bool())
    .unwrap_or(false);

  Ok(is_admin)
}

pub async fn check_user_in_admin_list(
  abr: String,
  usr: String,
) -> AppResult<bool> {
  let ep = Ep::Admins;

  let qry = "
    select value data.admins
    from type::table($table)
    where data.abr = $abr
      and if data.admins is not none and type::is::string(data.admins) then
        string::contains(data.admins, $usr)
      else
        false
      end
    limit 1
  ";

  debug!(
    query = %qry,
    table = %ep.table_sheet(),
    abr = %abr,
    user = %usr,
    "Checking if user is in admin list"
  );

  let results: Vec<Value> = DB
    .query(qry)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr.clone()))
    .bind(("usr", usr.clone()))
    .await
    .cwl("Failed to execute user in admin list check query")?
    .take(0)
    .cwl("Failed to take results from admin list check query")?;

  debug!(
    results_count = results.len(),
    "User in admin list check results"
  );

  Ok(!results.is_empty())
}

#[allow(dead_code)]
pub async fn get_orgs_orgbase(abr: &str) -> AppResult<Vec<Vec<String>>> {
  let ep = Ep::Orgbase;

  let qry = "
    select
      data.id,
      data.nombre,
      data.org_principal
    from type::table($table) 
    where abr = $abr
    order by data.id;
    ";

  let results: Vec<Value> = DB
    .query(qry)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr.to_owned()))
    .await
    .cwl("Failed to execute orgbase query")?
    .take(0)
    .cwl("Failed to take results from orbase query")?;

  let mut ids = Vec::new();
  for result in results {
    if let Some(obj) = result.as_object() {
      let mut row = Vec::new();
      if let Some(data) = obj.get("data").and_then(|d| d.as_object()) {
        row.push(
          data
            .get("nombre")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        );
        row.push(
          data
            .get("org_principal")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        );
      }
      ids.push(row);
    }
  }

  Ok(ids)
}

pub async fn get_petition_summary_by_domain(p: &Pets) -> AppResult<String> {
  debug!("=== PETITION SUMMARY FUNCTION CALLED ===");
  debug!("Input params: {}", p.params);

  let args: Vec<&str> = p.params.split(", ").collect();

  let date_range = if args.is_empty() || args[0].trim().is_empty() {
    return Ok(
      "Error: Se requiere un rango de fechas en formato YYYY-MM-DD:YYYY-MM-DD"
        .to_string(),
    );
  } else {
    args[0].to_string()
  };

  // Validate the date range using existing function
  let (start_date, end_date) = match validate_date_range(&date_range) {
    Ok((start, end)) => (start, end),
    Err(error_msg) => {
      error!("Date validation failed: {}", error_msg);
      return Ok(error_msg);
    }
  };

  debug!(
    "Getting petition summary from {} to {}",
    start_date, end_date
  );

  // IMPORTANT: SurrealDB GROUP BY requirement - field names in SELECT and GROUP BY must match exactly!
  // Every non-aggregate field in SELECT must appear in GROUP BY with identical naming.
  // Using "findom as domain" in SELECT with "group by findom" will fail.
  // Correct: SELECT findom, count() GROUP BY findom
  // Wrong:   SELECT findom as domain, count() GROUP BY findom
  let qry = "
    select
      findom,
      count() as petition_count
    from petitions
    where time >= $start_date and time <= $end_date
    group by findom;
  ";

  let results: Vec<Value> = DB
    .query(qry)
    .bind(("start_date", format!("{}T00:00:00Z", start_date)))
    .bind(("end_date", format!("{}T23:59:59Z", end_date)))
    .await
    .cwl("Failed to execute petition summary query")?
    .take(0)
    .cwl("Failed to take results from petition summary query")?;

  debug!("Found {} domain groups in petition summary", results.len());
  debug!("Query results: {:#?}", results);

  let mut total_petitions = 0;
  let mut summary_lines = Vec::new();

  for result in results {
    if let Some(obj) = result.as_object() {
      // Note: Using "findom" field name to match the SELECT field (not "domain")
      let domain = obj
        .get("findom")
        .and_then(|v| v.as_str())
        .unwrap_or("Unknown")
        .to_string();

      let count = obj
        .get("petition_count")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

      total_petitions += count;
      summary_lines.push(format!("{}: {} peticiones", domain, count));
    }
  }

  // Add total at the end
  summary_lines.push(format!("\n**Total: {} peticiones**", total_petitions));

  // Format for Google Chat with newlines
  let summary = if summary_lines.is_empty() {
    format!(
      "No se encontraron peticiones en el rango de fechas {} a {}",
      start_date, end_date
    )
  } else {
    format!(
      "📊 **Resumen de peticiones por dominio** ({})\n\n{}",
      date_range,
      summary_lines.join("\n")
    )
  };

  debug!("Generated petition summary: {}", summary);
  Ok(summary)
}

pub async fn get_unique_stus_from_gradsav(
  abr: String,
) -> AppResult<Vec<String>> {
  let ep = Ep::Gradsav;

  debug!("Getting unique students from gradsav for abr: {}", abr);

  let qry = "
    select data.correo_alumno from type::table($table)
    where abr = $abr
      and data.correo_alumno != null
      and data.correo_alumno != ''
    group by data.correo_alumno";

  let results: Vec<Value> = DB
    .query(qry)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr))
    .await
    .cwl("Failed to execute unique students query")?
    .take(0)
    .cwl("Failed to take results from unique students query")?;

  let mut students = Vec::new();
  for result in results {
    if let Some(obj) = result.as_object()
      && let Some(data) = obj.get("data").and_then(|d| d.as_object())
      && let Some(user_id) = data.get("correo_alumno").and_then(|v| v.as_str())
    {
      students.push(user_id.to_string());
    }
  }

  debug!("Found {} unique students", students.len());
  Ok(students)
}

pub async fn get_tutors_usr(usr: String, abr: String) -> AppResult<String> {
  let ep = Ep::Guardians;

  debug!("Getting tutors for student: {} in abr: {}", usr, abr);

  let qry = "
    select data.correo_tutor from type::table($table)
    where abr = $abr
      and data.correo_alumno = $correo_alumno
      and data.correo_tutor != null
      and data.correo_tutor != ''";

  let results: Vec<Value> = DB
    .query(qry)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr))
    .bind(("correo_alumno", usr))
    .await
    .cwl("Failed to execute tutors query")?
    .take(0)
    .cwl("Failed to take results from tutors query")?;

  let mut emails = Vec::new();
  for result in results {
    if let Some(obj) = result.as_object()
      && let Some(data) = obj.get("data").and_then(|d| d.as_object())
      && let Some(email) = data.get("correo_tutor").and_then(|v| v.as_str())
    {
      emails.push(email.to_string());
    }
  }

  let tutors_string = emails.join(",");
  debug!("Found tutors: {}", tutors_string);
  Ok(tutors_string)
}

pub async fn get_name_usr(usr: String, abr: String) -> AppResult<String> {
  let ep = Ep::Gradsav;

  debug!("Getting name for student: {} in abr: {}", usr, abr);

  let qry = "
    select data.nombre_alumno from type::table($table)
    where abr = $abr
      and data.correo_alumno = $correo_alumno
      and data.nombre_alumno != null
      and data.nombre_alumno != ''
    limit 1";

  let results: Vec<Value> = DB
    .query(qry)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr))
    .bind(("correo_alumno", usr))
    .await
    .cwl("Failed to execute student name query")?
    .take(0)
    .cwl("Failed to take results from student name query")?;

  for result in results {
    if let Some(obj) = result.as_object()
      && let Some(data) = obj.get("data").and_then(|d| d.as_object())
      && let Some(student_name) =
        data.get("nombre_alumno").and_then(|v| v.as_str())
    {
      // Convert to title case for proper formatting
      let formatted_name = student_name
        .split_whitespace()
        .map(|word| {
          let mut chars = word.chars();
          match chars.next() {
            None => String::new(),
            Some(first) => {
              first.to_uppercase().collect::<String>()
                + &chars.as_str().to_lowercase()
            }
          }
        })
        .collect::<Vec<String>>()
        .join(" ");

      debug!("Found student name: {}", formatted_name);
      return Ok(formatted_name);
    }
  }

  Ok("Unknown Student".to_string())
}

pub async fn get_abr_from_email_domain(email: String) -> AppResult<String> {
  let ep = Ep::Admins;

  // Extract domain from email
  let domain = email.split('@').nth(1).unwrap_or("").to_string();
  if domain.is_empty() {
    debug!("No domain found in email '{}', using default ABR", email);
    return Ok(ABR.to_string());
  }

  debug!("Getting ABR for domain: {}", domain);

  let qry = "
    select value data.abr
    from type::table($table)
    where data.dominio = $domain
    limit 1
    ";

  let domain_clone = domain.clone();
  let abr: String = DB
    .query(qry)
    .bind(("table", ep.table_sheet()))
    .bind(("domain", domain))
    .await
    .map(|mut result| {
      result
        .take::<Option<String>>(0)
        .unwrap_or(None)
        .unwrap_or_else(|| {
          debug!("No ABR found for domain '{}', using default", domain_clone);
          ABR.to_string()
        })
    })
    .unwrap_or_else(|e| {
      debug!(
        "Error querying ABR for domain '{}': {:?}, using default",
        domain_clone, e
      );
      ABR.to_string()
    });

  debug!("Found ABR '{}' for domain '{}'", abr, domain_clone);
  Ok(abr)
}

pub async fn get_grades_from_gradsav_stu(
  usr: String,
  abr: String,
) -> AppResult<Vec<Vec<String>>> {
  let ep = Ep::Gradsav;

  debug!("Getting grades for student: {} in abr: {}", usr, abr);

  let qry = "
    select data.nombre_curso,
      math::fixed(data.promedio_total, 1)
      as promedio_total from type::table($table)
    where abr = $abr
      and data.correo_alumno = $correo_alumno
      and data.nombre_curso != null
      and data.nombre_curso != ''
      and data.promedio_total != null";

  let results: Vec<Value> = DB
    .query(qry)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr))
    .bind(("correo_alumno", usr))
    .await
    .cwl("Failed to execute grades query")?
    .take(0)
    .cwl("Failed to take results from grades query")?;

  let mut grades = Vec::new();
  for result in results {
    debug!("DEBUG: Raw grade result object: {:#?}", result);
    if let Some(obj) = result.as_object() {
      let course_name = obj
        .get("data")
        .and_then(|d| d.as_object())
        .and_then(|data| data.get("nombre_curso"))
        .and_then(|v| v.as_str())
        .unwrap_or("N/A")
        .to_string();

      let average_grade = obj
        .get("promedio_total")
        .and_then(|v| {
          v.as_str()
            .map(|s| s.to_string())
            .or_else(|| v.as_f64().map(|f| f.to_string()))
        })
        .unwrap_or("0".to_string());

      debug!(
        "DEBUG: Extracted course_name: '{}', average_grade: '{}'",
        course_name, average_grade
      );
      grades.push(vec![course_name, average_grade]);
    }
  }

  debug!("Found {} course grades for student", grades.len());
  Ok(grades)
}
