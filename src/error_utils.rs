use anyhow::{Result as AppResult, bail};
use serde_json::Value;

/// Parse Google API error JSON and extract clean error information
pub fn parse_google_api_error(error_text: &str) -> String {
  if let Ok(error_json) = serde_json::from_str::<Value>(error_text)
    && let Some(error_obj) = error_json.get("error")
  {
    let code = error_obj
      .get("code")
      .and_then(|c| c.as_u64())
      .map(|c| format!("Code: {}", c))
      .unwrap_or_default();

    let status = error_obj
      .get("status")
      .and_then(|s| s.as_str())
      .map(|s| format!("Status: {}", s))
      .unwrap_or_default();

    let message = error_obj
      .get("message")
      .and_then(|m| m.as_str())
      .map(|m| format!("Message: {}", m))
      .unwrap_or_default();

    let parts: Vec<String> = [code, status, message]
      .into_iter()
      .filter(|s| !s.is_empty())
      .collect();

    if !parts.is_empty() {
      return parts.join("\n");
    }
  }

  // Fallback to original error text if parsing fails
  error_text.to_string()
}

/// Enhanced error message formatting with status codes and descriptive reasons
pub fn format_http_error(
  operation: &str,
  status: u16,
  error_text: &str,
  context: Option<&str>,
) -> String {
  let status_description = match status {
    400 => "BAD_REQUEST - Invalid parameters or malformed request",
    401 => "UNAUTHORIZED - Authentication required or invalid credentials",
    403 => "FORBIDDEN - Insufficient permissions",
    404 => "NOT_FOUND - Resource does not exist",
    409 => "CONFLICT - Resource already exists or conflicting state",
    412 => {
      "PRECONDITION_FAILED - Operation cannot be completed due to current state"
    }
    429 => "RATE_LIMIT_EXCEEDED - Too many requests, please slow down",
    500 => "INTERNAL_SERVER_ERROR - Server error occurred",
    502 => "BAD_GATEWAY - Service temporarily unavailable",
    503 => "SERVICE_UNAVAILABLE - Service temporarily unavailable",
    _ => "UNKNOWN_ERROR - Unexpected status code",
  };

  let context_info = context.map(|c| format!(" ({})", c)).unwrap_or_default();

  format!(
    "{} failed - Status: {} {}{} - Details: {}",
    operation, status, status_description, context_info, error_text
  )
}

/// Enhanced error handling for specific Google API operations
pub fn format_google_api_error(
  operation: &str,
  api: &str,
  status: u16,
  error_text: &str,
  resource_info: Option<&str>,
) -> String {
  let specific_reason = match (api, status) {
    ("classroom", 400) => "Student cannot be enrolled due to domain settings",
    ("classroom", 403) => "Classroom API disabled or insufficient permissions",
    ("classroom", 409) => "Student already enrolled in class",
    ("classroom", 412) => "Class state doesn't allow this operation",
    ("admin", 400) => "Invalid user or group parameters",
    ("admin", 409) => "User or group already exists",
    ("admin", 412) => {
      if error_text.to_lowercase().contains("dynamic group") {
        "Cannot modify dynamic group members - these are managed automatically"
      } else if error_text.to_lowercase().contains("member")
        && (error_text.to_lowercase().contains("not found")
          || error_text.to_lowercase().contains("does not exist"))
      {
        "Member not found in group (may have been removed externally)"
      } else {
        "Operation failed due to current group/user state"
      }
    }
    ("admin", 429) => "Google Admin API rate limit exceeded",
    ("drive", 429) => "Google Drive API rate limit exceeded",
    ("sheets", 429) => "Google Sheets API rate limit exceeded",
    ("classroom", 429) => "Google Classroom API rate limit exceeded",
    (_, 429) => "Google API rate limit exceeded",
    ("admin", 503) => {
      "Google Admin API service temporarily unavailable (backend error)"
    }
    ("drive", 503) => {
      "Google Drive API service temporarily unavailable (backend error)"
    }
    ("sheets", 503) => {
      "Google Sheets API service temporarily unavailable (backend error)"
    }
    ("classroom", 503) => {
      "Google Classroom API service temporarily unavailable (backend error)"
    }
    (_, 503) => "Google API service temporarily unavailable (backend error)",
    _ => "",
  };

  let resource_info = resource_info
    .map(|r| format!(" for {}", r))
    .unwrap_or_default();

  let clean_error_details = parse_google_api_error(error_text);

  if specific_reason.is_empty() {
    format!(
      "{}{} failed:\n{}",
      operation, resource_info, clean_error_details
    )
  } else {
    format!(
      "{}{} failed - {}\n{}",
      operation, resource_info, specific_reason, clean_error_details
    )
  }
}

/// Get human-readable status code name
pub fn get_status_code_name(status: u16) -> &'static str {
  match status {
    400 => "BAD_REQUEST",
    401 => "UNAUTHORIZED",
    403 => "FORBIDDEN",
    404 => "NOT_FOUND",
    409 => "CONFLICT",
    412 => "PRECONDITION_FAILED",
    429 => "RATE_LIMIT_EXCEEDED",
    500 => "INTERNAL_SERVER_ERROR",
    502 => "BAD_GATEWAY",
    503 => "SERVICE_UNAVAILABLE",
    _ => "UNKNOWN_ERROR",
  }
}

//Helper function to create bail! with enhanced error messages

#[allow(dead_code)]
pub fn bail_with_enhanced_error(
  operation: &str,
  status: u16,
  error_text: &str,
  context: Option<&str>,
) -> AppResult<()> {
  bail!(format_http_error(operation, status, error_text, context))
}

/// Helper function to create bail! with Google API specific error messages
#[allow(dead_code)]
pub fn bail_with_google_api_error(
  operation: &str,
  api: &str,
  status: u16,
  error_text: &str,
  resource_info: Option<&str>,
) -> AppResult<()> {
  bail!(format_google_api_error(
    operation,
    api,
    status,
    error_text,
    resource_info
  ))
}
