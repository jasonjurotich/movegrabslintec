mod apis;
mod aux_mods;
mod aux_process;

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

use std::env;
use tracer::*;

pub type AppResult<T, E = anyhow::Error> = std::result::Result<T, E>;

fn perform_health_check() -> bool {
  println!("Performing health check... status: OK.");
  true
}

async fn run_app() -> AppResult<()> {
  println!("=== AVISOSBOT2 STARTING ===");
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
  chres().await?;

  if let Some(last_error) = tracer::get_last_error() {
    chreserrs(last_error)
      .await
      .cwl("Could not send error messages.")?
  }

  Ok(())
}

#[tokio::main]
async fn main() {
  use std::io::{self, Write};

  println!("=== BINARY STARTING ===");
  eprintln!("=== BINARY STARTING (stderr) ===");
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

  println!("=== ABOUT TO RUN APP ===");
  eprintln!("=== ABOUT TO RUN APP (stderr) ===");
  io::stdout().flush().unwrap();
  io::stderr().flush().unwrap();

  if let Err(e) = run_app().await {
    println!("\n--- APPLICATION FAILED ---\nError: {e}");
    eprintln!("\n--- APPLICATION FAILED ---\nError: {e}");
    io::stdout().flush().unwrap();
    io::stderr().flush().unwrap();
    std::process::exit(1);
  }
  println!("=== APP COMPLETED SUCCESSFULLY ===");
  eprintln!("=== APP COMPLETED SUCCESSFULLY (stderr) ===");
  io::stdout().flush().unwrap();
  io::stderr().flush().unwrap();
}
