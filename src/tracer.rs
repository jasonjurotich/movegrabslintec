use tracing_appender::rolling;
use tracing_appender::{self, non_blocking, non_blocking::WorkerGuard};
use tracing_error::ErrorLayer;
use tracing_subscriber::{
  EnvFilter, layer::SubscriberExt, util::SubscriberInitExt,
};

use std::fs::{self};
use std::sync::{Arc, Mutex};

static LAST_ERROR: std::sync::OnceLock<Arc<Mutex<Option<String>>>> =
  std::sync::OnceLock::new();

pub fn set_last_error(error_msg: String) {
  let last_error = LAST_ERROR.get_or_init(|| Arc::new(Mutex::new(None)));
  if let Ok(mut guard) = last_error.lock() {
    *guard = Some(error_msg);
  }
}

// pub fn get_last_error() -> Option<String> {
//   let last_error = LAST_ERROR.get_or_init(|| Arc::new(Mutex::new(None)));
//   if let Ok(mut guard) = last_error.lock() {
//     guard.take()
//   } else {
//     None
//   }
// }

#[macro_export]
macro_rules! debug {
    ($($arg:tt)*) => {
        tracing::debug!($($arg)*)
    };
}

#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {
        tracing::info!($($arg)*)
    };
}

#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => {
        tracing::warn!($($arg)*)
    };
}

#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => {
        tracing::error!($($arg)*)
    };
}

#[macro_export]
macro_rules! cwl {
  ($result:expr, $msg:expr) => {
    $result.map_err(|e| {
      let file_path = file!();
      let file_name = std::path::Path::new(file_path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown");

      let error_from_e = anyhow::Error::from(e);
      error_from_e.context(format!("{} {}:{}", $msg, file_name, line!()))
    })
  };
}

#[macro_export]
macro_rules! bail {
  ($msg:expr) => {
    return Err(anyhow::anyhow!("{} {}:{}", $msg,
      std::path::Path::new(file!())
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown"),
      line!()
    ))
  };
  ($fmt:expr, $($arg:tt)*) => {
    return Err(anyhow::anyhow!("{} {}:{}",
      format!($fmt, $($arg)*),
      std::path::Path::new(file!())
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown"),
      line!()
    ))
  };
}

pub trait ContextExt<T> {
  fn cwl(self, msg: &str) -> anyhow::Result<T>;
}

impl<T, E> ContextExt<T> for Result<T, E>
where
  E: Into<anyhow::Error>,
{
  #[track_caller]
  fn cwl(self, msg: &str) -> anyhow::Result<T> {
    let location = std::panic::Location::caller();
    let file_path = location.file();
    let file_name = std::path::Path::new(file_path)
      .file_name()
      .and_then(|name| name.to_str())
      .unwrap_or("unknown");

    match self {
      Ok(value) => Ok(value),
      Err(e) => {
        let error_info = format!("{} {}:{}", msg, file_name, location.line());
        let anyhow_error = e.into();
        tracing::error!("{} - underlying error: {}", error_info, anyhow_error);

        // Capture the last error for potential use in error reporting
        let full_error_msg =
          format!("{error_info} - underlying error: {anyhow_error}");
        set_last_error(full_error_msg);

        Err(anyhow_error.context(error_info))
      }
    }
  }
}

impl<T> ContextExt<T> for Option<T> {
  #[track_caller]
  fn cwl(self, msg: &str) -> anyhow::Result<T> {
    let location = std::panic::Location::caller();
    let file_path = location.file();
    let file_name = std::path::Path::new(file_path)
      .file_name()
      .and_then(|name| name.to_str())
      .unwrap_or("unknown");

    match self {
      Some(value) => Ok(value),
      None => {
        let error_info = format!("{} {}:{}", msg, file_name, location.line());
        tracing::error!("cwl error: {}", error_info);

        // Capture the last error for potential use in error reporting
        let full_error_msg = format!("cwl error: {error_info}");
        set_last_error(full_error_msg);

        Err(anyhow::anyhow!(error_info))
      }
    }
  }
}

pub fn setup_panic_hook() {
  std::panic::set_hook(Box::new(|panic_info| {
    // --- All of your excellent, detailed info gathering remains the same ---
    let thread_handle = std::thread::current();
    let thread_name = thread_handle.name().unwrap_or("unnamed");

    let location = if let Some(location) = panic_info.location() {
      let file_name = std::path::Path::new(location.file())
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown");
      format!("{}:{}", file_name, location.line())
    } else {
      "unknown location".to_string()
    };

    let message = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
      s.to_string()
    } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
      s.clone()
    } else {
      "unknown panic message".to_string()
    };

    // --- THE KEY CHANGE IS HERE ---
    // Instead of using the async `tracing::error!`, we use a standard, blocking
    // `eprintln!` to write directly to standard error. This is guaranteed to
    // execute before the process is aborted.

    eprintln!(
      "\n\n================================================\n\
     A FATAL PANIC OCCURRED. APPLICATION TERMINATING.\n\
     ================================================\n\n\
     Panic Details:\n\
     > Message:  {message}\n\
     > Location: {location}\n\
     > Thread:   {thread_name}\n"
    );

    // Your logic to capture the last error is still perfectly fine to keep.
    let full_panic_msg = format!(
      "TOKIO PANIC: {message} thread={thread_name} location={location}"
    );
    set_last_error(full_panic_msg);
  }));
}

pub fn init_logging() -> Option<WorkerGuard> {
  let crate_name = env!("CARGO_PKG_NAME");

  // Derive log file prefix directly from crate name
  let log_file_prefix = crate_name;

  // Derive filter target name (replaces hyphens with underscores)
  let crate_name_target = crate_name.replace('-', "_");

  // Hardcode default log level for the crate
  let default_crate_level = "debug";

  // Derive the environment variable name: CRATE_NAME_LOG (uppercase, underscores)
  let env_filter_var_name =
    format!("{}_LOG", crate_name.to_uppercase().replace('-', "_"));

  // Construct the *only* default filter directive (no extra directives here)
  let default_filter = format!("{crate_name_target}={default_crate_level}");

  // --- 1. Create the Filter ---
  let env_filter = EnvFilter::new(
    std::env::var(&env_filter_var_name).unwrap_or(default_filter),
  );

  // --- 2. Create the Stderr Layer (Always) ---
  // This layer formats messages and writes them to stderr.
  // NOTE you must use stderr and not stdout, let everything be an err
  let stderr_layer = tracing_subscriber::fmt::layer()
    .with_writer(std::io::stderr)
    .with_ansi(true)
    .with_target(true)
    .with_level(true)
    .with_file(true)
    .with_line_number(true)
    .with_timer(tracing_subscriber::fmt::time::LocalTime::rfc_3339());

  // --- 3. Conditionally Create File Layer Components ---
  let mut file_layer_option = None; // Will hold the configured file layer if successful
  let mut log_guard: Option<WorkerGuard> = None; // Holds the guard needed for the file writer

  let log_dir = "logs";
  match fs::create_dir_all(log_dir) {
    Ok(_) => {
      let log_file_name = format!("{log_file_prefix}.log");
      let file_appender = rolling::daily(log_dir, log_file_name);
      let (non_blocking_writer, guard) = non_blocking(file_appender);

      let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking_writer)
        .with_ansi(true)
        .with_target(true)
        .with_level(true)
        .with_file(true)
        .with_line_number(true);

      file_layer_option = Some(file_layer);
      log_guard = Some(guard);
    }
    Err(e) => {
      eprintln!(
        "[{log_file_prefix}] Failed to create log directory '{log_dir}'. File logging disabled. Error: {e}"
      );
    }
  }
  // --- 4. Build the Subscriber ---
  // Start with the registry and add layers common to both scenarios
  let registry = tracing_subscriber::registry()
    .with(env_filter)
    .with(ErrorLayer::default())
    .with(stderr_layer);

  // --- 5. Initialize Globally (Single Call) ---
  let init_result = match file_layer_option {
    Some(file_layer) => registry.with(file_layer).try_init(),
    None => registry.try_init(),
  };

  if init_result.is_err() {
    eprintln!("Failed to initialize tracing subscriber. Logging may not work.");
    // Clean up guard if init failed but guard was created
    if log_guard.is_some() {
      drop(log_guard.take());
    }
    return None;
  }

  eprintln!(
    "[{log_file_prefix}] Logging initialized. Log level '{default_crate_level}'. Override with env var '{env_filter_var_name}'"
  );

  log_guard
}
