mod apis;
mod aux_drive;
mod aux_process;
mod aux_sur;

mod error_utils;
mod goauth2;
mod limiters;
mod lists;
mod mod_process;
mod mods;

mod sheets;
mod surrealstart;
mod tracer;

use apis::Ep;
use aux_drive::*;
use goauth2::*;
use limiters::create_rate_limiter;
use lists::list_users;
use tracer::*;

// Commented out - may be needed for future Axum integration
// use axum::{http::StatusCode, response::IntoResponse};

use surrealstart::{
  EM, Pets, StaticEm, StaticFil, StaticKey, StaticPid, StaticSdir,
  check_key, extract_record_parts, gdatabase_to_sheetsdb, initdb,
};

use std::{
  env,
  io::{self, Write},
};

pub type AppResult<T, E = anyhow::Error> = std::result::Result<T, E>;

// Commented out - not currently used but may be needed for future Axum integration
// fn format_error_chain(error: &anyhow::Error) -> String {
//   let mut chain = Vec::new();
//   chain.push(error.to_string());

//   let mut source = error.source();
//   while let Some(err) = source {
//     chain.push(err.to_string());
//     source = err.source();
//   }

//   chain.join("\n↳ ")
// }

// struct AppError(anyhow::Error);

// // Implement conversion from anyhow::Error to our AppError
// impl From<anyhow::Error> for AppError {
//   fn from(err: anyhow::Error) -> Self {
//     AppError(err)
//   }
// }

// // Implement IntoResponse for our wrapper type
// impl IntoResponse for AppError {
//   fn into_response(self) -> axum::response::Response {
//     (
//       StatusCode::INTERNAL_SERVER_ERROR,
//       format!("Something went wrong: {}", self.0),
//     )
//       .into_response()
//   }
// }

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
  StaticPid::set("adminbot-iedu-prod");
  info!("This is em {:?}", &*EM);
  StaticFil::set("ieducandocom");

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
  let group_email_base = "grabacionesmeet";
  let shared_drive_name = "GRABACIONES RESPALDO";
  let domain = "linguatec.com.mx"; // TODO: Make this configurable
  let abr = "lintec"; // TODO: Make this configurable from static config
  let admin_email = "ieducando@linguatec.com.mx";

  info!("=== Starting movevideosmul workflow ===");
  info!("Group: {}@{}", group_email_base, domain);
  info!("Shared drive: {}", shared_drive_name);
  info!("Admin email: {}", admin_email);

  // Get admin token with subject (tsy) for Drive/Workspace API calls
  // NOTE: tsy (token subject yes) is used for all Google Workspace operations
  // that require domain-wide delegation (Drive, Groups, etc.)
  let tsy = get_tok_impersonated(admin_email, "yes")
    .await
    .cwl("Failed to get tsy admin token")?;

  // Step 1: Find or create the main shared drive
  info!(
    "Step 1: Finding or creating shared drive '{}'",
    shared_drive_name
  );
  let shared_drive_id =
    match find_shared_drive_by_name(shared_drive_name, &tsy).await? {
      Some(id) => {
        info!("Found existing shared drive: {}", id);
        id
      }
      None => {
        info!("Shared drive not found, creating new one");
        let id = create_shared_drive(shared_drive_name, &tsy).await?;
        info!("Created new shared drive: {}", id);
        id
      }
    };

  // Step 2: Add temporary permission for grabacionesmeet group
  let group_email_full = format!("{}@{}", group_email_base, domain);
  info!("Step 2: Adding writer permission for {}", group_email_full);
  add_permission_to_file(&shared_drive_id, &group_email_full, "writer", &tsy)
    .await
    .cwl("Failed to add group permission")?;

  // Step 3: Populate database with users from Google Workspace
  info!("Step 3: Populating database with users from Google Workspace");

  let pets = Pets {
    tsn: String::new(), // Not needed for this operation
    tsy: tsy.clone(),
    sp: String::new(),  // Not needed
    cmd: String::new(), // Not needed
    abr: abr.to_string(),
    spshid: "1lOar2kxcKQVyQhQX5_fOPgUOe1dWtZO60O_ZHOKApgA".to_string(),
    dom: domain.to_string(),
    params: String::new(), // Not needed
  };

  let limiter = create_rate_limiter(100); // 100 requests per second
  list_users(limiter, &pets)
    .await
    .cwl("Failed to list users from Google Workspace")?;

  info!("Successfully populated database with users (in gusuarios table)");

  // Step 3.5: Transform data from gusuarios to usuarios
  info!("Step 3.5: Transforming user data from gusuarios to usuarios");
  gdatabase_to_sheetsdb(abr.to_string(), Ep::Users)
    .await
    .cwl("Failed to transform user data from gusuarios to usuarios")?;
  info!("Successfully transformed user data to usuarios table");

  // Step 4: Get list of professors from /COLABORADORES/PROFESORES org
  info!("Step 4: Getting list of professors from database");

  let org_path = "/COLABORADORES/PROFESORES";

  // Option: Filter by group members (uncomment if needed)
  // let group_members = get_group_members(&group_email_full, &tsy).await?;
  // let professors = get_professors(abr, org_path, Some(group_members)).await?;

  // Or get all professors from the org:
  let professors = get_professors(abr, org_path, None).await?;
  info!("Found {} professors in {}", professors.len(), org_path);

  // Step 4: For each professor, find their Meet Recordings folder and get videos
  info!("Step 4: Finding Meet Recordings folders and collecting videos");

  let mut all_videos: Vec<(Professor, Vec<VideoFile>)> = Vec::new();

  // Process professors in chunks to avoid overwhelming the API
  use futures::stream::{self, StreamExt};

  let chunk_size = 30;
  let chunks: Vec<_> = professors.chunks(chunk_size).collect();

  for (chunk_idx, chunk) in chunks.iter().enumerate() {
    info!(
      "Processing professor chunk {}/{}",
      chunk_idx + 1,
      chunks.len()
    );

    let results = stream::iter(chunk.iter())
      .map(|prof| async {
        // NOTE: Using tsy (admin token with subject) for all Drive API operations
        // This token has domain-wide delegation and can access all users' files
        let token = &tsy;

        // Find Meet Recordings folder
        match find_meet_recordings_folder(&prof.email, token).await {
          Ok(Some(folder)) => {
            debug!("Found Meet folder for {}: {}", prof.email, folder.id);

            // Get videos from folder
            match get_videos_from_folder(&folder.id, &prof.email, token).await {
              Ok(videos) => {
                info!("Found {} videos for {}", videos.len(), prof.email);
                Some((prof.clone(), videos))
              }
              Err(e) => {
                warn!("Failed to get videos for {}: {}", prof.email, e);
                None
              }
            }
          }
          Ok(None) => {
            debug!("No Meet Recordings folder for {}", prof.email);
            None
          }
          Err(e) => {
            warn!("Error finding Meet folder for {}: {}", prof.email, e);
            None
          }
        }
      })
      .buffer_unordered(chunk_size)
      .collect::<Vec<_>>()
      .await;

    all_videos.extend(results.into_iter().flatten());
  }

  info!("Total professors with videos: {}", all_videos.len());
  let total_videos: usize =
    all_videos.iter().map(|(_, videos)| videos.len()).sum();
  info!("Total videos to move: {}", total_videos);

  // Step 5: Create a folder in the shared drive for each professor
  info!("Step 5: Creating professor-specific folders in shared drive");

  use std::collections::HashMap;
  let mut professor_folders: HashMap<String, String> = HashMap::new();

  for (prof, videos) in &all_videos {
    if videos.is_empty() {
      continue;
    }

    let folder_name = format!("GRABS RESPALDO {}", prof.email);

    match create_folder_in_shared_drive(&folder_name, &shared_drive_id, &tsy)
      .await
    {
      Ok(folder_id) => {
        info!("Created folder '{}': {}", folder_name, folder_id);
        professor_folders.insert(prof.email.clone(), folder_id);
      }
      Err(e) => {
        warn!("Failed to create folder for {}: {}", prof.email, e);
      }
    }
  }

  // Step 6: Index the shared drive contents to verify folders
  info!("Step 6: Indexing shared drive contents");
  let drive_items = index_shared_drive_contents(&shared_drive_id, &tsy).await?;
  info!("Found {} items in shared drive", drive_items.len());

  // Step 7: Move videos to their respective professor folders
  info!("Step 7: Moving videos to professor folders");

  let mut move_count = 0;
  let mut error_count = 0;

  for (prof, videos) in &all_videos {
    if let Some(dest_folder_id) = professor_folders.get(&prof.email) {
      info!("Moving {} videos for {}", videos.len(), prof.email);

      for video in videos {
        if let Some(ref current_parent) = video.parent_folder_id {
          // NOTE: Using tsy (admin token with subject) for moving files
          // This token has domain-wide delegation to move files across users
          let token = &tsy;

          match move_video_file(
            &video.id,
            current_parent,
            dest_folder_id,
            token,
          )
          .await
          {
            Ok(_) => {
              move_count += 1;
              debug!("Moved video: {}", video.name);
            }
            Err(e) => {
              error_count += 1;
              warn!("Failed to move video {}: {}", video.name, e);
            }
          }
        } else {
          warn!("Video {} has no parent folder, skipping", video.name);
        }
      }
    }
  }

  info!("Successfully moved {} videos", move_count);
  if error_count > 0 {
    warn!("Failed to move {} videos", error_count);
  }

  // Step 8: Remove the group permission
  info!("Step 8: Removing group permission from shared drive");
  delete_group_permission(&shared_drive_id, &group_email_full, &tsy)
    .await
    .cwl("Failed to remove group permission")?;

  info!("=== movevideosmul workflow completed successfully ===");
  Ok(())
}
