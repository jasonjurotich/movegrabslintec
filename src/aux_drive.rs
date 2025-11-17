#![allow(dead_code)]

use crate::AppResult;
use crate::apis::Ep;
use crate::error_utils::{parse_google_api_error};
use crate::limiters::{get_global_drive_limiter, get_global_delete_limiter, get_global_groups_list_limiter};
use crate::surrealstart::{req_build, DB};
use crate::tracer::ContextExt;
use crate::{bail, debug};
use serde_json::{Value, json};
use serde::{Deserialize, Serialize};

// ==================== DATA STRUCTURES ====================

/// Represents a user/professor from Google Workspace
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Professor {
  pub id: String,
  pub email: String,
  pub full_name: Option<String>,
  pub org_unit: Option<String>,
  pub suspended: Option<bool>,
}

/// Represents a video recording file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoFile {
  pub id: String,
  pub name: String,
  pub mime_type: String,
  pub created_time: Option<String>,
  pub owner_email: Option<String>,
  pub size_bytes: Option<i64>,
  pub parent_folder_id: Option<String>,
  pub web_view_link: Option<String>,
}

/// Represents a "Meet Recordings" folder
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeetFolder {
  pub id: String,
  pub name: String,
  pub owner_email: String,
}

/// Represents a file/folder in a shared drive
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedDriveItem {
  pub id: String,
  pub name: String,
  pub mime_type: String,
  pub drive_id: Option<String>,
  pub parent_folder_id: Option<String>,
  pub web_view_link: Option<String>,
  pub created_time: Option<String>,
}

/// Represents a permission entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Permission {
  pub id: String,
  pub email: Option<String>,
  pub role: String,
  pub perm_type: String,
}

// ==================== HELPER FUNCTIONS ====================

/// Extracts a string value from nested JSON path
fn ex_str_val(rfin: &Value, path: &[&str]) -> Option<String> {
  let mut current = rfin;
  for key in path {
    current = current.get(key)?;
  }
  current.as_str().map(|s| s.to_string())
}

/// Extracts an integer value from nested JSON path
fn ex_i64_val(rfin: &Value, path: &[&str]) -> Option<i64> {
  let mut current = rfin;
  for key in path {
    current = current.get(key)?;
  }
  current.as_i64()
}

// ==================== GROUP MEMBER FUNCTIONS ====================

/// Gets all members of a Google Group
///
/// # Arguments
/// * `group_email` - Email address of the group (e.g., "grabacionesmeet@domain.com")
/// * `tsy` - Bearer token with subject for authentication (domain-wide delegation)
///
/// # Returns
/// Vector of email addresses
pub async fn get_group_members(
  group_email: &str,
  tsy: &str,
) -> AppResult<Vec<String>> {
  debug!("Getting members for group: {}", group_email);

  let mut all_members = Vec::new();
  let mut page_token: Option<String> = None;

  loop {
    let url = format!(
      "https://admin.googleapis.com/admin/directory/v1/groups/{}/members",
      group_email
    );

    let mut query = json!({
      "maxResults": 200
    });

    if let Some(ref token_val) = page_token {
      query["pageToken"] = json!(token_val);
    }

    let au_build = req_build("GET", &url, Some(tsy), Some(&query), None)
      .cwl("Failed to build request for group members")?;

    get_global_groups_list_limiter().until_ready().await;

    let res = au_build.send().await.cwl("Failed to send group members request")?;

    match res.status().as_u16() {
      200..=299 => {
        let rfin: Value = res.json().await.cwl("Failed to parse group members response")?;

        // Extract members from response
        if let Some(members) = rfin.get("members").and_then(|m| m.as_array()) {
          debug!("Processing {} members in current page for group: {}", members.len(), group_email);
          for member in members {
            if let Some(email) = member.get("email").and_then(|e| e.as_str()) {
              debug!("  - Found member: {}", email);
              all_members.push(email.to_string());
            }
          }
        }

        // Check for next page
        page_token = rfin
          .get("nextPageToken")
          .and_then(|t| t.as_str())
          .map(|s| s.to_string());

        if page_token.is_none() {
          break;
        } else {
          debug!("Fetching next page of members for group: {}", group_email);
        }
      }
      status => {
        let error_text = res.text().await.unwrap_or_else(|_| "Unknown error".to_string());
        let clean_error = parse_google_api_error(&error_text);
        bail!("Failed to get group members - Status: {} - {}", status, clean_error);
      }
    }
  }

  debug!("Found {} members in group {}", all_members.len(), group_email);
  Ok(all_members)
}

// ==================== PROFESSOR/USER FUNCTIONS ====================

/// Gets list of professors from SurrealDB
///
/// # Arguments
/// * `abr` - Database abbreviation (e.g., "linguatec")
/// * `filter_emails` - Optional list of emails to filter by
///
/// # Returns
/// Vector of Professor structs
pub async fn get_professors(
  abr: &str,
  filter_emails: Option<Vec<String>>,
) -> AppResult<Vec<Professor>> {
  debug!("Getting professors for abr: {}", abr);

  let query = if let Some(emails) = filter_emails {
    format!(
      "SELECT * FROM usrs WHERE abr = '{}' AND id IS NOT NULL AND email IN {:?} ORDER BY email",
      abr, emails
    )
  } else {
    format!(
      "SELECT * FROM usrs WHERE abr = '{}' AND id IS NOT NULL ORDER BY email",
      abr
    )
  };

  let mut response = DB
    .query(&query)
    .await
    .cwl("Failed to query professors from database")?;

  let results: Vec<Value> = response.take(0).cwl("Failed to extract professor results")?;

  let mut professors = Vec::new();
  for result in results {
    if let Some(id) = ex_str_val(&result, &["id"])
      && let Some(email) = ex_str_val(&result, &["email"])
    {
      let full_name = ex_str_val(&result, &["full_name"]);
      debug!("  - Found professor: {} ({})", email, full_name.as_deref().unwrap_or("No name"));
      professors.push(Professor {
        id,
        email,
        full_name,
        org_unit: ex_str_val(&result, &["org_unit"]),
        suspended: result.get("suspended").and_then(|s| s.as_bool()),
      });
    }
  }

  debug!("Found {} professors for abr: {}", professors.len(), abr);
  Ok(professors)
}

// ==================== MEET RECORDINGS FOLDER FUNCTIONS ====================

/// Finds the "Meet Recordings" folder for a specific professor
///
/// # Arguments
/// * `professor_email` - Email of the professor
/// * `tsy` - Bearer token with subject for authentication (domain-wide delegation)
///
/// # Returns
/// Option containing MeetFolder if found
pub async fn find_meet_recordings_folder(
  professor_email: &str,
  tsy: &str,
) -> AppResult<Option<MeetFolder>> {
  debug!("Finding Meet Recordings folder for: {}", professor_email);

  let mut page_token: Option<String> = None;

  loop {
    let query_str = format!(
      "mimeType='application/vnd.google-apps.folder' and name='Meet Recordings' and '{}' in owners",
      professor_email
    );

    let mut query = json!({
      "q": query_str,
      "fields": "nextPageToken,files(id,name)",
      "pageSize": 100
    });

    if let Some(ref token_val) = page_token {
      query["pageToken"] = json!(token_val);
    }

    let url = Ep::Files.base_url();
    let au_build = req_build("GET", url, Some(tsy), Some(&query), None)
      .cwl("Failed to build request for Meet Recordings folder")?;

    get_global_drive_limiter().until_ready().await;

    let res = au_build.send().await.cwl("Failed to send Meet Recordings folder request")?;

    match res.status().as_u16() {
      200..=299 => {
        let rfin: Value = res.json().await.cwl("Failed to parse Meet Recordings response")?;

        // Check if we found any files
        if let Some(files) = rfin.get("files").and_then(|f| f.as_array()) {
          debug!("Found {} potential Meet Recordings folders for {}", files.len(), professor_email);
          if !files.is_empty()
            && let Some(folder) = files.first()
            && let (Some(id), Some(name)) = (
              ex_str_val(folder, &["id"]),
              ex_str_val(folder, &["name"])
            )
          {
            debug!("✓ Meet Recordings folder exists: {} (ID: {}) for {}", name, id, professor_email);
            return Ok(Some(MeetFolder {
              id,
              name,
              owner_email: professor_email.to_string(),
            }));
          }
        } else {
          debug!("No folders found in this page for {}", professor_email);
        }

        // Check for next page
        page_token = rfin
          .get("nextPageToken")
          .and_then(|t| t.as_str())
          .map(|s| s.to_string());

        if page_token.is_none() {
          break;
        } else {
          debug!("Checking next page for Meet Recordings folder for {}", professor_email);
        }
      }
      status => {
        let error_text = res.text().await.unwrap_or_else(|_| "Unknown error".to_string());
        let clean_error = parse_google_api_error(&error_text);
        bail!("Failed to find Meet Recordings folder - Status: {} - {}", status, clean_error);
      }
    }
  }

  debug!("No Meet Recordings folder found for {}", professor_email);
  Ok(None)
}

// ==================== VIDEO FILE FUNCTIONS ====================

/// Gets all MP4 video files from a folder
///
/// # Arguments
/// * `folder_id` - ID of the folder containing videos
/// * `owner_email` - Email of the folder owner (for reference)
/// * `tsy` - Bearer token with subject for authentication (domain-wide delegation)
///
/// # Returns
/// Vector of VideoFile structs
pub async fn get_videos_from_folder(
  folder_id: &str,
  owner_email: &str,
  tsy: &str,
) -> AppResult<Vec<VideoFile>> {
  debug!("Getting videos from folder: {} for owner: {}", folder_id, owner_email);

  let mut all_videos = Vec::new();
  let mut page_token: Option<String> = None;

  loop {
    let query_str = format!(
      "'{}' in parents and mimeType='video/mp4'",
      folder_id
    );

    let mut query = json!({
      "q": query_str,
      "fields": "nextPageToken,files(id,name,mimeType,createdTime,size,webViewLink,parents)",
      "pageSize": 100
    });

    if let Some(ref token_val) = page_token {
      query["pageToken"] = json!(token_val);
    }

    let url = Ep::Files.base_url();
    let au_build = req_build("GET", url, Some(tsy), Some(&query), None)
      .cwl("Failed to build request for video files")?;

    get_global_drive_limiter().until_ready().await;

    let res = au_build.send().await.cwl("Failed to send video files request")?;

    match res.status().as_u16() {
      200..=299 => {
        let rfin: Value = res.json().await.cwl("Failed to parse video files response")?;

        // Extract files
        if let Some(files) = rfin.get("files").and_then(|f| f.as_array()) {
          debug!("Found {} video files in current page for folder {} (owner: {})", files.len(), folder_id, owner_email);
          for file in files {
            if let Some(id) = ex_str_val(file, &["id"])
              && let Some(name) = ex_str_val(file, &["name"])
            {
              let parent_id = file
                .get("parents")
                .and_then(|p| p.as_array())
                .and_then(|arr| arr.first())
                .and_then(|p| p.as_str())
                .map(|s| s.to_string());

              let size_bytes = ex_i64_val(file, &["size"]);
              let size_mb = size_bytes.map(|b| b as f64 / 1_048_576.0);

              debug!("  - Video: {} (ID: {}, Size: {:.2} MB) - Owner: {}",
                name, id,
                size_mb.unwrap_or(0.0),
                owner_email
              );

              all_videos.push(VideoFile {
                id,
                name,
                mime_type: ex_str_val(file, &["mimeType"]).unwrap_or_else(|| "video/mp4".to_string()),
                created_time: ex_str_val(file, &["createdTime"]),
                owner_email: Some(owner_email.to_string()),
                size_bytes,
                parent_folder_id: parent_id,
                web_view_link: ex_str_val(file, &["webViewLink"]),
              });
            }
          }
        }

        // Check for next page
        page_token = rfin
          .get("nextPageToken")
          .and_then(|t| t.as_str())
          .map(|s| s.to_string());

        if page_token.is_none() {
          break;
        } else {
          debug!("Fetching next page of videos for folder {} (owner: {})", folder_id, owner_email);
        }
      }
      status => {
        let error_text = res.text().await.unwrap_or_else(|_| "Unknown error".to_string());
        let clean_error = parse_google_api_error(&error_text);
        bail!("Failed to get video files - Status: {} - {}", status, clean_error);
      }
    }
  }

  debug!("Found {} videos in folder {}", all_videos.len(), folder_id);
  Ok(all_videos)
}

// ==================== SHARED DRIVE FUNCTIONS ====================

/// Creates a new shared drive
///
/// # Arguments
/// * `name` - Name of the shared drive
/// * `tsy` - Bearer token with subject for authentication (domain-wide delegation)
///
/// # Returns
/// ID of the created shared drive
pub async fn create_shared_drive(
  name: &str,
  tsy: &str,
) -> AppResult<String> {
  debug!("Creating shared drive: {}", name);

  let url = "https://www.googleapis.com/drive/v3/drives";
  let request_id = format!("create-{}-{}", name, chrono::Utc::now().timestamp());

  let query = json!({
    "requestId": request_id
  });

  let body = json!({
    "name": name
  });

  let au_build = req_build("POST", url, Some(tsy), Some(&query), Some(&body))
    .cwl("Failed to build request for creating shared drive")?;

  get_global_drive_limiter().until_ready().await;

  let res = au_build.send().await.cwl("Failed to send create shared drive request")?;

  match res.status().as_u16() {
    200..=299 => {
      let rfin: Value = res.json().await.cwl("Failed to parse create shared drive response")?;

      if let Some(id) = ex_str_val(&rfin, &["id"]) {
        debug!("Created shared drive with ID: {}", id);
        Ok(id)
      } else {
        bail!("No ID returned from create shared drive request");
      }
    }
    status => {
      let error_text = res.text().await.unwrap_or_else(|_| "Unknown error".to_string());
      let clean_error = parse_google_api_error(&error_text);
      bail!("Failed to create shared drive - Status: {} - {}", status, clean_error);
    }
  }
}

/// Finds a shared drive by name
///
/// # Arguments
/// * `name` - Name of the shared drive to search for
/// * `tsy` - Bearer token with subject for authentication (domain-wide delegation)
///
/// # Returns
/// Option containing the drive ID if found
pub async fn find_shared_drive_by_name(
  name: &str,
  tsy: &str,
) -> AppResult<Option<String>> {
  debug!("Searching for shared drive: {}", name);

  let mut page_token: Option<String> = None;

  loop {
    let mut query = json!({
      "q": format!("name contains '{}'", name),
      "pageSize": 100
    });

    if let Some(ref token_val) = page_token {
      query["pageToken"] = json!(token_val);
    }

    let url = "https://www.googleapis.com/drive/v3/drives";
    let au_build = req_build("GET", url, Some(tsy), Some(&query), None)
      .cwl("Failed to build request for finding shared drive")?;

    get_global_drive_limiter().until_ready().await;

    let res = au_build.send().await.cwl("Failed to send find shared drive request")?;

    match res.status().as_u16() {
      200..=299 => {
        let rfin: Value = res.json().await.cwl("Failed to parse find shared drive response")?;

        // Check if we found any drives
        if let Some(drives) = rfin.get("drives").and_then(|d| d.as_array()) {
          for drive in drives {
            if let Some(drive_name) = ex_str_val(drive, &["name"])
              && drive_name == name
              && let Some(id) = ex_str_val(drive, &["id"])
            {
              debug!("Found shared drive '{}' with ID: {}", name, id);
              return Ok(Some(id));
            }
          }
        }

        // Check for next page
        page_token = rfin
          .get("nextPageToken")
          .and_then(|t| t.as_str())
          .map(|s| s.to_string());

        if page_token.is_none() {
          break;
        }
      }
      status => {
        let error_text = res.text().await.unwrap_or_else(|_| "Unknown error".to_string());
        let clean_error = parse_google_api_error(&error_text);
        bail!("Failed to find shared drive - Status: {} - {}", status, clean_error);
      }
    }
  }

  debug!("Shared drive '{}' not found", name);
  Ok(None)
}

/// Creates a folder inside a shared drive
///
/// # Arguments
/// * `folder_name` - Name of the folder to create
/// * `parent_drive_id` - ID of the parent shared drive
/// * `tsy` - Bearer token with subject for authentication (domain-wide delegation)
///
/// # Returns
/// ID of the created folder
pub async fn create_folder_in_shared_drive(
  folder_name: &str,
  parent_drive_id: &str,
  tsy: &str,
) -> AppResult<String> {
  debug!("Creating folder '{}' in shared drive: {}", folder_name, parent_drive_id);

  let url = "https://www.googleapis.com/drive/v3/files";

  let query = json!({
    "supportsAllDrives": "true"
  });

  let body = json!({
    "name": folder_name,
    "mimeType": "application/vnd.google-apps.folder",
    "parents": [parent_drive_id]
  });

  let au_build = req_build("POST", url, Some(tsy), Some(&query), Some(&body))
    .cwl("Failed to build request for creating folder in shared drive")?;

  get_global_drive_limiter().until_ready().await;

  let res = au_build.send().await.cwl("Failed to send create folder request")?;

  match res.status().as_u16() {
    200..=299 => {
      let rfin: Value = res.json().await.cwl("Failed to parse create folder response")?;

      if let Some(id) = ex_str_val(&rfin, &["id"]) {
        debug!("Created folder '{}' with ID: {}", folder_name, id);
        Ok(id)
      } else {
        bail!("No ID returned from create folder request");
      }
    }
    status => {
      let error_text = res.text().await.unwrap_or_else(|_| "Unknown error".to_string());
      let clean_error = parse_google_api_error(&error_text);
      bail!("Failed to create folder in shared drive - Status: {} - {}", status, clean_error);
    }
  }
}

// ==================== PERMISSION FUNCTIONS ====================

/// Gets all permissions for a file/folder/drive
///
/// # Arguments
/// * `file_id` - ID of the file/folder/drive
/// * `tsy` - Bearer token with subject for authentication (domain-wide delegation)
///
/// # Returns
/// Vector of Permission structs
pub async fn get_file_permissions(
  file_id: &str,
  tsy: &str,
) -> AppResult<Vec<Permission>> {
  debug!("Getting permissions for file: {}", file_id);

  let url = format!("https://www.googleapis.com/drive/v3/files/{}/permissions", file_id);

  let query = json!({
    "supportsAllDrives": "true",
    "fields": "permissions(id,emailAddress,role,type)"
  });

  let au_build = req_build("GET", &url, Some(tsy), Some(&query), None)
    .cwl("Failed to build request for getting file permissions")?;

  get_global_drive_limiter().until_ready().await;

  let res = au_build.send().await.cwl("Failed to send get permissions request")?;

  match res.status().as_u16() {
    200..=299 => {
      let rfin: Value = res.json().await.cwl("Failed to parse get permissions response")?;

      let mut permissions = Vec::new();

      if let Some(perms) = rfin.get("permissions").and_then(|p| p.as_array()) {
        for perm in perms {
          if let (Some(id), Some(role), Some(perm_type)) = (
            ex_str_val(perm, &["id"]),
            ex_str_val(perm, &["role"]),
            ex_str_val(perm, &["type"])
          ) {
            let email = ex_str_val(perm, &["emailAddress"]);
            debug!("  - Permission: {} ({}) - {} - File: {}",
              email.as_deref().unwrap_or("No email"),
              perm_type,
              role,
              file_id
            );
            permissions.push(Permission {
              id,
              email,
              role,
              perm_type,
            });
          }
        }
      }

      debug!("Found {} permissions for file {}", permissions.len(), file_id);
      Ok(permissions)
    }
    status => {
      let error_text = res.text().await.unwrap_or_else(|_| "Unknown error".to_string());
      let clean_error = parse_google_api_error(&error_text);
      bail!("Failed to get file permissions - Status: {} - {}", status, clean_error);
    }
  }
}

/// Adds a permission to a file/folder/drive
///
/// # Arguments
/// * `file_id` - ID of the file/folder/drive
/// * `email` - Email address to grant permission to
/// * `role` - Role to grant (reader, writer, commenter, etc.)
/// * `tsy` - Bearer token with subject for authentication (domain-wide delegation)
pub async fn add_permission_to_file(
  file_id: &str,
  email: &str,
  role: &str,
  tsy: &str,
) -> AppResult<()> {
  debug!("Adding {} permission for {} to file: {}", role, email, file_id);

  let url = format!("https://www.googleapis.com/drive/v3/files/{}/permissions", file_id);

  let query = json!({
    "supportsAllDrives": "true"
  });

  let body = json!({
    "type": "user",
    "role": role,
    "emailAddress": email
  });

  let au_build = req_build("POST", &url, Some(tsy), Some(&query), Some(&body))
    .cwl("Failed to build request for adding permission")?;

  get_global_drive_limiter().until_ready().await;

  let res = au_build.send().await.cwl("Failed to send add permission request")?;

  match res.status().as_u16() {
    200..=299 => {
      debug!("✓ Successfully added {} permission for {} to file {}", role, email, file_id);
      Ok(())
    }
    status => {
      let error_text = res.text().await.unwrap_or_else(|_| "Unknown error".to_string());
      let clean_error = parse_google_api_error(&error_text);
      bail!("Failed to add {} permission for {} to file {} - Status: {} - {}", role, email, file_id, status, clean_error);
    }
  }
}

/// Deletes a permission from a file/folder/drive
///
/// # Arguments
/// * `file_id` - ID of the file/folder/drive
/// * `permission_id` - ID of the permission to delete
/// * `tsy` - Bearer token with subject for authentication (domain-wide delegation)
pub async fn delete_permission_from_file(
  file_id: &str,
  permission_id: &str,
  tsy: &str,
) -> AppResult<()> {
  debug!("Deleting permission {} from file: {}", permission_id, file_id);

  let url = format!(
    "https://www.googleapis.com/drive/v3/files/{}/permissions/{}",
    file_id, permission_id
  );

  let query = json!({
    "supportsAllDrives": "true"
  });

  let au_build = req_build("DELETE", &url, Some(tsy), Some(&query), None)
    .cwl("Failed to build request for deleting permission")?;

  get_global_delete_limiter().until_ready().await;

  let res = au_build.send().await.cwl("Failed to send delete permission request")?;

  match res.status().as_u16() {
    200..=299 => {
      debug!("✓ Successfully deleted permission {} from file {}", permission_id, file_id);
      Ok(())
    }
    status => {
      let error_text = res.text().await.unwrap_or_else(|_| "Unknown error".to_string());
      let clean_error = parse_google_api_error(&error_text);
      bail!("Failed to delete permission {} from file {} - Status: {} - {}", permission_id, file_id, status, clean_error);
    }
  }
}

/// Deletes a group's permission from a file/folder/drive by email
///
/// # Arguments
/// * `file_id` - ID of the file/folder/drive
/// * `group_email` - Email of the group to remove
/// * `tsy` - Bearer token with subject for authentication (domain-wide delegation)
pub async fn delete_group_permission(
  file_id: &str,
  group_email: &str,
  tsy: &str,
) -> AppResult<()> {
  debug!("Removing group permission for {} from file: {}", group_email, file_id);

  // First get all permissions
  let permissions = get_file_permissions(file_id, tsy).await?;

  // Find the permission matching the group email
  for perm in permissions {
    if let Some(ref email) = perm.email
      && email == group_email
    {
      debug!("Found matching group permission for {} on file {}, deleting...", group_email, file_id);
      delete_permission_from_file(file_id, &perm.id, tsy).await?;
      debug!("✓ Successfully removed group permission for {} from file {}", group_email, file_id);
      return Ok(());
    }
  }

  debug!("No permission found for group {} on file {} (already removed or never existed)", group_email, file_id);
  Ok(())
}

// ==================== FILE MOVING FUNCTIONS ====================

/// Moves a video file to a new parent folder
///
/// # Arguments
/// * `file_id` - ID of the file to move
/// * `current_parent_id` - Current parent folder ID
/// * `new_parent_id` - New parent folder ID
/// * `tsy` - Bearer token with subject for authentication (domain-wide delegation)
pub async fn move_video_file(
  file_id: &str,
  current_parent_id: &str,
  new_parent_id: &str,
  tsy: &str,
) -> AppResult<()> {
  debug!("Moving file {} from {} to {}", file_id, current_parent_id, new_parent_id);

  let url = format!("https://www.googleapis.com/drive/v3/files/{}", file_id);

  let query = json!({
    "addParents": new_parent_id,
    "removeParents": current_parent_id,
    "supportsAllDrives": "true"
  });

  let au_build = req_build("PATCH", &url, Some(tsy), Some(&query), None)
    .cwl("Failed to build request for moving file")?;

  get_global_drive_limiter().until_ready().await;

  let res = au_build.send().await.cwl("Failed to send move file request")?;

  match res.status().as_u16() {
    200..=299 => {
      debug!("✓ Successfully moved file {} from folder {} to folder {}", file_id, current_parent_id, new_parent_id);
      Ok(())
    }
    status => {
      let error_text = res.text().await.unwrap_or_else(|_| "Unknown error".to_string());
      let clean_error = parse_google_api_error(&error_text);
      bail!("Failed to move file {} from {} to {} - Status: {} - {}", file_id, current_parent_id, new_parent_id, status, clean_error);
    }
  }
}

// ==================== SHARED DRIVE INDEXING FUNCTIONS ====================

/// Lists all items in a shared drive
///
/// # Arguments
/// * `drive_id` - ID of the shared drive
/// * `tsy` - Bearer token with subject for authentication (domain-wide delegation)
///
/// # Returns
/// Vector of SharedDriveItem structs
pub async fn index_shared_drive_contents(
  drive_id: &str,
  tsy: &str,
) -> AppResult<Vec<SharedDriveItem>> {
  debug!("Indexing contents of shared drive: {}", drive_id);

  let mut all_items = Vec::new();
  let mut page_token: Option<String> = None;

  loop {
    let query_str = format!("'{}' in parents", drive_id);

    let mut query = json!({
      "q": query_str,
      "supportsAllDrives": "true",
      "includeItemsFromAllDrives": "true",
      "fields": "nextPageToken,files(id,name,mimeType,driveId,parents,webViewLink,createdTime)",
      "pageSize": 100
    });

    if let Some(ref token_val) = page_token {
      query["pageToken"] = json!(token_val);
    }

    let url = Ep::Files.base_url();
    let au_build = req_build("GET", url, Some(tsy), Some(&query), None)
      .cwl("Failed to build request for indexing shared drive")?;

    get_global_drive_limiter().until_ready().await;

    let res = au_build.send().await.cwl("Failed to send index shared drive request")?;

    match res.status().as_u16() {
      200..=299 => {
        let rfin: Value = res.json().await.cwl("Failed to parse index shared drive response")?;

        // Extract files
        if let Some(files) = rfin.get("files").and_then(|f| f.as_array()) {
          debug!("Found {} items in current page for shared drive {}", files.len(), drive_id);
          for file in files {
            if let (Some(id), Some(name), Some(mime_type)) = (
              ex_str_val(file, &["id"]),
              ex_str_val(file, &["name"]),
              ex_str_val(file, &["mimeType"])
            ) {
              let parent_id = file
                .get("parents")
                .and_then(|p| p.as_array())
                .and_then(|arr| arr.first())
                .and_then(|p| p.as_str())
                .map(|s| s.to_string());

              let item_type = if mime_type.contains("folder") { "Folder" } else { "File" };
              debug!("  - {}: {} (ID: {}, Type: {})", item_type, name, id, mime_type);

              all_items.push(SharedDriveItem {
                id,
                name,
                mime_type,
                drive_id: ex_str_val(file, &["driveId"]),
                parent_folder_id: parent_id,
                web_view_link: ex_str_val(file, &["webViewLink"]),
                created_time: ex_str_val(file, &["createdTime"]),
              });
            }
          }
        }

        // Check for next page
        page_token = rfin
          .get("nextPageToken")
          .and_then(|t| t.as_str())
          .map(|s| s.to_string());

        if page_token.is_none() {
          break;
        } else {
          debug!("Fetching next page of items for shared drive {}", drive_id);
        }
      }
      status => {
        let error_text = res.text().await.unwrap_or_else(|_| "Unknown error".to_string());
        let clean_error = parse_google_api_error(&error_text);
        bail!("Failed to index shared drive - Status: {} - {}", status, clean_error);
      }
    }
  }

  debug!("Found {} items in shared drive {}", all_items.len(), drive_id);
  Ok(all_items)
}
