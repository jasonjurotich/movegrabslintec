mod apis;
mod aux_process;
mod aux_sur;

mod error_utils;
mod goauth;
mod jsonresfil;
mod limiters;
mod lists;
mod mod_process;
mod mods;

mod sheets;
mod surrealstart;
mod tracer;

use goauth::*;
use tracer::*;

use axum::{http::StatusCode, response::IntoResponse};

use surrealstart::{
  EM, StaticEm, StaticFil, StaticKey, StaticPid, StaticSdir, initdb,
};

use std::{
  env,
  io::{self, Write},
};

pub type AppResult<T, E = anyhow::Error> = std::result::Result<T, E>;

fn format_error_chain(error: &anyhow::Error) -> String {
  let mut chain = Vec::new();
  chain.push(error.to_string());

  let mut source = error.source();
  while let Some(err) = source {
    chain.push(err.to_string());
    source = err.source();
  }

  chain.join("\n↳ ")
}

struct AppError(anyhow::Error);

// Implement conversion from anyhow::Error to our AppError
impl From<anyhow::Error> for AppError {
  fn from(err: anyhow::Error) -> Self {
    AppError(err)
  }
}

// Implement IntoResponse for our wrapper type
impl IntoResponse for AppError {
  fn into_response(self) -> axum::response::Response {
    (
      StatusCode::INTERNAL_SERVER_ERROR,
      format!("Something went wrong: {}", self.0),
    )
      .into_response()
  }
}

fn perform_health_check() -> bool {
  println!("Performing health check... status: OK.");
  true
}

async fn run_app() -> AppResult<()> {
  println!("=== MOVEVIDS STARTING ===");
  let _log_guard = init_logging();
  println!("=== LOGGING INITIALIZED ===");

  StaticEm::set("jason.jurotich@ieducando.com");
  StaticKey::set("");
  StaticPid::set("");
  info!("This is em {:?}", &*EM);
  StaticFil::set("avisosbot");

  let secrets_dir = env::var("DIR").unwrap_or_else(|_| {
    "/Users/jasonjurotich/Documents/RUSTDEV/JSONSECRETSAPI".to_string()
  });
  StaticSdir::set(&secrets_dir);

  initdb().await?;

  movevideosmul().await.cwl("Could not run  movevideosmul")?;

  Ok(())
}

#[tokio::main]
async fn main() {
  println!("=== MOVEVIDS STARTING ===");
  io::stdout().flush().unwrap();
  io::stderr().flush().unwrap();

  setup_panic_hook();

  let args: Vec<String> = env::args().collect();
  if args.len() > 1 && args[1] == "--health-check" {
    if perform_health_check() {
      std::process::exit(0);
    } else {
      std::process::exit(1);
    }
  }

  if let Err(e) = run_app().await {
    println!("\n--- APPLICATION FAILED ---\nError: {e}");
    eprintln!("\n--- APPLICATION FAILED ---\nError: {e}");
    io::stdout().flush().unwrap();
    io::stderr().flush().unwrap();
    std::process::exit(1);
  }

  println!("=== MOVEVIDS COMPLETED SUCCESSFULLY ===");
  io::stdout().flush().unwrap();
  io::stderr().flush().unwrap();
}

async fn movevideosmul() -> AppResult<()> {
  let grabfol1 = "grabacionesmeet";
  let grabfol2 = "GRABACIONES RESPALDO";

  debug!("This is folders {:#?}", grabfol1);

  Ok(())
}
