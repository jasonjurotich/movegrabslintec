// -------------------REQUIREMENTS--------------------------
// to run sql, you must have started the server beforehand, the code works without starting it
//
// surreal start surrealkv:///Users/jasonjurotich/Documents/RUSTDEV/surrealdbadminbot/adminbot.db
// surreal sql -u surrealdbmain -p surrealdbupsap --ns surrealns --db surrealkvdb --json --pretty

// surreal start --bind 127.0.0.1:8000 --username surrealdbmain --password surrealdbupsap
//   surrealkv:///Users/jasonjurotich/Documents/RUSTDEV/surrealdbadminbot/adminbot.db

// surreal start --bind 127.0.0.1:8000 --username surrealdbmain --password surrealdbupsap surrealkv:///Users/jasonjurotich/Documents/RUSTDEV/surrealdbadminbot/adminbot.db

// rm -rf adminbot.db to remove the db when it starts going slow

// NOTE you CANNOT run the code and have the sql server running at the same time
// NOTE you CANNOT use embedding mode if the dabase goes over 1GB of space, it tries to put it all in memory and takes almost a minute! use ony for when it does not go over 300 mb

// use crate::apis::check_key;
use dotenv::dotenv;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Client, Method, RequestBuilder};

use crate::sheets::count_db;
use crate::tracer::ContextExt;
use crate::{bail, debug, error, info, warn};
use chrono::NaiveDate;
use std::error::Error;
use std::fmt::Debug;
// use std::fmt::{Debug, DebugList};
use std::sync::LazyLock;

use regex::Regex;
use serde_json::{Value, json};
use surrealdb::engine::any::Any;
use surrealdb::{
  Surreal,
  // engine::remote::ws::{Client, Ws},
  opt::auth::Root,
};

use once_cell::sync::OnceCell;
use parking_lot::RwLock as PLRwLock;
use std::ops::Deref;
use std::sync::Mutex;
use std::sync::{Arc, RwLock as StdRwLock};
use tokio::sync::Notify;

pub use super::apis::*;
use crate::AppResult;

/// Sanitizes JSON values by removing null bytes that cause SurrealDB panics
fn sanitize_json_value(value: &mut Value) {
  match value {
    Value::String(s) => {
      if s.contains('\0') {
        *s = s.replace('\0', "");
      }
    }
    Value::Array(arr) => {
      for item in arr.iter_mut() {
        sanitize_json_value(item);
      }
    }
    Value::Object(obj) => {
      for (_, v) in obj.iter_mut() {
        sanitize_json_value(v);
      }
    }
    _ => {}
  }
}

// -------------------EM--------------------------
static EM_CELL: OnceCell<String> = OnceCell::new();

pub struct StaticEm;

impl StaticEm {
  pub fn set(value: &str) {
    if EM_CELL.set(value.to_string()).is_err() {
      error!("EM has already been initialized!");
    }
  }
}

impl Deref for StaticEm {
  type Target = str;

  fn deref(&self) -> &Self::Target {
    match EM_CELL.get() {
      Some(val) => val,
      None => {
        error!("EM has not been initialized!");
        ""
      }
    }
  }
}
pub static EM: StaticEm = StaticEm;

// -------------------ABR--------------------------
static ABR_CELL: OnceCell<String> = OnceCell::new();

pub struct StaticAbr;

impl StaticAbr {
  pub fn set(value: &str) {
    if ABR_CELL.set(value.to_string()).is_err() {
      warn!("ABR has already been initialized!");
    }
  }
}

impl Deref for StaticAbr {
  type Target = str;

  fn deref(&self) -> &Self::Target {
    match ABR_CELL.get() {
      Some(val) => val,
      None => {
        error!("EM has not been initialized!");
        ""
      }
    }
  }
}
pub static ABR: StaticAbr = StaticAbr;

// -------------------KEY--------------------------
static KEY_CELL: OnceCell<String> = OnceCell::new();

pub struct StaticKey;

impl StaticKey {
  pub fn set(value: &str) {
    if KEY_CELL.set(value.to_string()).is_err() {
      warn!("ABR has already been initialized!");
    }
  }
}

impl Deref for StaticKey {
  type Target = str;

  fn deref(&self) -> &Self::Target {
    match KEY_CELL.get() {
      Some(val) => val,
      None => {
        error!("EM has not been initialized!");
        ""
      }
    }
  }
}
pub static KEY: StaticKey = StaticKey;

// -------------------FIL--------------------------
// NOTE  FIL is ONLY for local development!!!!
static FIL_CELL: OnceCell<String> = OnceCell::new();
pub struct StaticFil;

impl StaticFil {
  pub fn set(value: &str) {
    if FIL_CELL.set(value.to_string()).is_err() {
      warn!("FIL has already been initialized!");
    }
  }
}

impl Deref for StaticFil {
  type Target = str;

  fn deref(&self) -> &Self::Target {
    match FIL_CELL.get() {
      Some(val) => val,
      None => {
        error!("FIL has not been initialized!");
        ""
      }
    }
  }
}
pub static FIL: StaticFil = StaticFil;

// -------------------PID--------------------------
static PID_CELL: OnceCell<String> = OnceCell::new();
pub struct StaticPid;

impl StaticPid {
  pub fn set(value: &str) {
    if PID_CELL.set(value.to_string()).is_err() {
      warn!("PID has already been initialized!");
    }
  }
}

impl Deref for StaticPid {
  type Target = str;

  fn deref(&self) -> &Self::Target {
    match PID_CELL.get() {
      Some(val) => val,
      None => {
        error!("PID has not been initialized!");
        ""
      }
    }
  }
}
pub static PID: StaticPid = StaticPid;

// -------------------SID--------------------------
static SID_CELL: OnceCell<String> = OnceCell::new();

pub struct StaticSid;

impl StaticSid {
  pub fn set(value: &str) {
    if SID_CELL.set(value.to_string()).is_err() {
      warn!("SID has already been initialized!");
    }
  }
}

impl Deref for StaticSid {
  type Target = str;

  fn deref(&self) -> &Self::Target {
    match SID_CELL.get() {
      Some(val) => val,
      None => {
        error!("SID has not been initialized!");
        ""
      }
    }
  }
}

pub static SID: StaticSid = StaticSid;

// -------------------ID--------------------------
static ID_CELL: LazyLock<Arc<StdRwLock<String>>> =
  LazyLock::new(|| Arc::new(StdRwLock::new(String::new())));

pub struct StaticId;

impl StaticId {
  pub fn set(value: &str) {
    match ID_CELL.write() {
      Ok(mut id) => {
        *id = value.to_string();
        debug!("ID updated to: {}", value);
      }
      Err(e) => {
        error!("Failed to update ID: {}", e);
      }
    }
  }

  pub fn get(&self) -> String {
    match ID_CELL.read() {
      Ok(id) => id.clone(),
      Err(e) => {
        error!("Failed to read ID: {}", e);
        String::new()
      }
    }
  }
}

// Custom Display implementation so we can use it directly in format strings
impl std::fmt::Display for StaticId {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.get())
  }
}

pub static ID: StaticId = StaticId;

// -------------------SDIR--------------------------
// NOTE  SDIR is ONLY for local development!!!!
static SDIR_CELL: OnceCell<String> = OnceCell::new();
pub struct StaticSdir;

impl StaticSdir {
  pub fn set(value: &str) {
    if SDIR_CELL.set(value.to_string()).is_err() {
      warn!("SDIR has already been initialized!");
    }
  }
}

impl Deref for StaticSdir {
  type Target = str;

  fn deref(&self) -> &Self::Target {
    match SDIR_CELL.get() {
      Some(val) => val,
      None => {
        error!("SDIR has not been initialized!");
        ""
      }
    }
  }
}
pub static SDIR: StaticSdir = StaticSdir;

// -------------------TSHID--------------------------
static TSHID_CELL: OnceCell<String> = OnceCell::new();
pub struct StaticTshid;

impl StaticTshid {
  pub fn set(value: &str) {
    if TSHID_CELL.set(value.to_string()).is_err() {
      warn!("TSHID has already been initialized!");
    }
  }
}

impl Deref for StaticTshid {
  type Target = str;

  fn deref(&self) -> &Self::Target {
    match TSHID_CELL.get() {
      Some(val) => val,
      None => {
        error!("TSHID has not been initialized!");
        ""
      }
    }
  }
}
pub static TSHID: StaticTshid = StaticTshid;

// -------------------SP--------------------------
static SP_CELL: OnceCell<String> = OnceCell::new();
pub struct StaticSp;

impl StaticSp {
  pub fn set(value: &str) {
    if SP_CELL.set(value.to_string()).is_err() {
      warn!("SP has already been initialized!");
    }
  }
}

impl Deref for StaticSp {
  type Target = str;

  fn deref(&self) -> &Self::Target {
    match SP_CELL.get() {
      Some(val) => val,
      None => {
        error!("SP has not been initialized!");
        ""
      }
    }
  }
}
pub static SP: StaticSp = StaticSp;

// -------------------COMMANDS--------------------------
// Centralized command definitions to avoid duplication across the codebase

// All available commands
pub static ALL_COMMANDS: &[&str] = &[
  // List commands (starting with 'l')
  "la",
  "lc",
  "lca",
  "lcb",
  "ldr",
  "ldu",
  "ldd",
  "le",
  "lg",
  "lgr",
  "lgra",
  "lgrap",
  "lgrav",
  "li",
  "lm",
  "lma",
  "lo",
  "lp",
  "lpa",
  "lre",
  "lreu",
  "ls",
  "lsa",
  "lsp",
  "lt",
  "lta",
  "ltu",
  "lu",
  "lud",
  "lv",
  "ly",
  // Other commands (not starting with 'l')
  "ac",
  "adsc",
  "afo",
  "dfo",
  "ag",
  "amg",
  "rmg",
  "umg",
  "ali",
  "aobg",
  "apc",
  "ascl",
  "asc",
  "aua",
  "auaa",
  "ayuda",
  "cadsh",
  "petsum",
  "cc",
  "ct",
  "mt",
  "dt",
  "mgr",
  "atu",
  "dtu",
  "dtui",
  "bemls",
  "ccg",
  "ccgex",
  "cca",
  "cev",
  "cg",
  "cgc",
  "co",
  "cobcg",
  "cobg",
  "cobo",
  "cod",
  "cog",
  "coga",
  "cogal",
  "cogalc",
  "cogc",
  "cogsu",
  "cogusu",
  "cogsua",
  "cogusua",
  "cp",
  "cpc",
  "cpf",
  "cpfc",
  "cps",
  "cpfs",
  "csc",
  "cu",
  "cud",
  "cuf",
  "cup",
  "dac",
  "dc",
  "dev",
  "dfe",
  "dff",
  "dg",
  "rga",
  "dor",
  "dpc",
  "dsc",
  "dua",
  "duaa",
  "du",
  "duni",
  "ddm",
  "fcb",
  "gfe",
  "giu",
  "iden",
  "ides",
  "idfr",
  "idge",
  "idit",
  "jsons",
  "mca",
  "dca",
  "mev",
  "oc",
  "orgbase",
  "qli",
  "rac",
  "racl",
  "rcb",
  "rg",
  "roles",
  "rassigned",
  "rpcl",
  "rscl",
  "scb",
  "secrets",
  "su",
  "suc",
  "usuc",
  "au",
  "uau",
  "udu",
  "ug",
  "uli",
  "uo",
  "upcb",
  "usu",
  "uscb",
  "uu",
  "uuo",
  "uc",
  "values",
  "alladmins",
];

// List commands only (commands starting with 'l')
pub static LIST_COMMANDS: &[&str] = &[
  "la", "lc", "lca", "lcb", "ldr", "ldu", "ldd", "le", "lg", "lgr", "lgra",
  "lgrap", "lgrav", "li", "lm", "lma", "lo", "lp", "lpa", "lre", "lreu", "ls",
  "lsa", "lsp", "lt", "lta", "ltu", "lu", "lud", "lv", "ly",
];

// Non-list commands (commands not starting with 'l')
pub static OTHER_COMMANDS: &[&str] = &[
  "ac",
  "adsc",
  "afo",
  "dfo",
  "ag",
  "amg",
  "rmg",
  "umg",
  "ali",
  "aobg",
  "apc",
  "ascl",
  "asc",
  "aua",
  "auaa",
  "ayuda",
  "cadsh",
  "petsum",
  "cc",
  "ct",
  "mt",
  "dt",
  "mgr",
  "atu",
  "dtu",
  "dtui",
  "bemls",
  "ccg",
  "ccgex",
  "cca",
  "cev",
  "cg",
  "cgc",
  "co",
  "cobcg",
  "cobg",
  "cobo",
  "cod",
  "cog",
  "coga",
  "cogal",
  "cogalc",
  "cogc",
  "cogsu",
  "cogusu",
  "cogsua",
  "cogusua",
  "cp",
  "cpc",
  "cpf",
  "cpfc",
  "cps",
  "cpfs",
  "csc",
  "cu",
  "cud",
  "cuf",
  "cup",
  "dac",
  "dc",
  "dev",
  "dfe",
  "dff",
  "dg",
  "rga",
  "dor",
  "dpc",
  "dsc",
  "dua",
  "duaa",
  "du",
  "duni",
  "ddm",
  "fcb",
  "gfe",
  "giu",
  "iden",
  "ides",
  "idfr",
  "idge",
  "idit",
  "jsons",
  "mca",
  "dca",
  "mev",
  "oc",
  "orgbase",
  "qli",
  "rac",
  "racl",
  "rcb",
  "rg",
  "roles",
  "rassigned",
  "rpcl",
  "rscl",
  "scb",
  "secrets",
  "su",
  "suc",
  "usuc",
  "au",
  "uau",
  "udu",
  "ug",
  "uli",
  "uo",
  "upcb",
  "usu",
  "uscb",
  "uu",
  "uuo",
  "uc",
  "values",
  "alladmins",
];

// Helper functions to check command types
pub fn is_list_command(cmd: &str) -> bool {
  LIST_COMMANDS.contains(&cmd)
}

pub fn is_valid_command(cmd: &str) -> bool {
  ALL_COMMANDS.contains(&cmd)
}

pub fn get_all_commands() -> &'static [&'static str] {
  ALL_COMMANDS
}

// pub fn get_list_commands() -> &'static [&'static str] {
//   LIST_COMMANDS
// }

pub fn get_other_commands() -> &'static [&'static str] {
  OTHER_COMMANDS
}

// -------------------LAZY--------------------------
pub static CL: LazyLock<Client> = LazyLock::new(|| {
  Client::builder()
    .tcp_keepalive(std::time::Duration::from_secs(60))
    .tcp_nodelay(true)
    .timeout(std::time::Duration::from_secs(45)) // Balanced timeout with retry logic handling rate limits
    .build()
    .expect("Failed to create HTTP client")
});
pub static DB: LazyLock<Surreal<Any>> = LazyLock::new(|| {
  // let start = std::time::Instant::now();
  // debug!("Initializing Surreal::init()...");

  // debug!("Surreal::init() completed in: {:?}", start.elapsed());
  Surreal::init()
});

pub async fn initdb() -> AppResult<()> {
  info!("Initializing database connection and setup...");
  dotenv().ok();

  let db_endpoint = std::env::var("DB_ENDPOINT").unwrap_or_else(|_| {
    // warn!("DB_ENDPOINT environment variable not set, using default 'ws://127.0.0.1:8000'.");
    // "ws://127.0.0.1:8000".to_string() // WebSocket - works on Linux but has issues on macOS
    warn!("DB_ENDPOINT environment variable not set, using default 'http://127.0.0.1:8000'.");
    "http://127.0.0.1:8000".to_string() // HTTP - works on macOS for concurrent access testing
    //  "surrealkv://./adminbot.db".to_string()); // Local surrealkv database for debugging
    // let db_endpoint = "mem://".to_string() // for in memory
    });

  let username = std::env::var("DB_USERNAME").unwrap_or_else(|_| {
    warn!(
      "DB_USERNAME environment variable not set, using default 'surrealdbmain'."
    );
    "surrealdbmain".to_string()
  });

  let password = if cfg!(target_os = "macos") {
    std::env::var("DB_PASSWORD").unwrap_or_else(|_| {
      warn!("DB_PASSWORD environment variable not set, using default 'surrealdbupsap'.");
      "surrealdbupsap".to_string()
    })
  } else {
    std::env::var("DB_PASSWORD")
      .cwl("DB_PASSWORD environment variable must be set in production")
      .unwrap_or_else(|_| {
        error!("DB_PASSWORD not set in production environment, this should come from Secret Manager");
        panic!("DB_PASSWORD required in production");
      })
  };

  let namespace = std::env::var("DB_NS").unwrap_or_else(|_| {
    warn!("DB_NS environment variable not set, using default 'test'.");
    "surrealns".to_string()
  });

  let database = std::env::var("DB_DB").unwrap_or_else(|_| {
    warn!("DB_DB environment variable not set, using default 'test'.");
    "surrealkvdb".to_string()
  });

  let _is_production = !cfg!(target_os = "macos");

  // Try multiple connection protocols to find one that works
  let endpoints_to_try = vec![
    db_endpoint.clone(),
    db_endpoint.replace("ws://", "http://"),
    db_endpoint
      .replace("http://", "ws://")
      .replace(":8000", ":8000/rpc"),
    db_endpoint
      .replace("http://", "wss://")
      .replace(":8000", ":8000/rpc"),
  ];

  let mut last_error = None;
  let mut connected = false;
  let mut working_endpoint = String::new();

  for (idx, endpoint) in endpoints_to_try.iter().enumerate() {
    info!(
      attempt = idx + 1,
      total = endpoints_to_try.len(),
      endpoint = %endpoint,
      "Attempting to connect to SurrealDB..."
    );

    match DB.connect(endpoint.as_str()).await {
      Ok(_) => {
        info!(endpoint = %endpoint, "Connection successful!");
        working_endpoint = endpoint.clone();
        connected = true;
        break;
      }
      Err(e) => {
        warn!(
          endpoint = %endpoint,
          error = %e,
          "Connection attempt failed, trying next endpoint..."
        );
        last_error = Some(e);
      }
    }
  }

  if !connected {
    error!(
      endpoints_tried = ?endpoints_to_try,
      last_error = ?last_error,
      "Failed to connect to SurrealDB with any endpoint"
    );
    bail!(
      "Failed to connect to SurrealDB. Tried {} endpoints. Last error: {:?}",
      endpoints_to_try.len(),
      last_error
    );
  }

  info!(endpoint = %working_endpoint, "Connected to SurrealDB successfully.");

  // Select namespace and database BEFORE signin (required in SurrealDB 2.x)
  debug!(
    ns = %namespace,
    db = %database,
    "Selecting namespace and database..."
  );

  if let Err(e) = DB.use_ns(&namespace).use_db(&database).await {
    bail!("Failed to select namespace/database. Error: {}", e);
  }

  info!(
    ns = %namespace,
    db = %database,
    "Namespace and database selected successfully"
  );

  // Now attempt signin with detailed error logging
  debug!(
    user = %username,
    pass_len = password.len(),
    endpoint = %working_endpoint,
    "Attempting signin with Root credentials..."
  );

  match DB
    .signin(Root {
      username: &username,
      password: &password,
    })
    .await
  {
    Ok(response) => {
      info!(
        user = %username,
        endpoint = %working_endpoint,
        "ROOT signin successful!"
      );
      debug!(signin_response = ?response, "Signin response details");
    }
    Err(e) => {
      error!(
        user = %username,
        pass_len = password.len(),
        endpoint = %working_endpoint,
        error = %e,
        error_debug = ?e,
        error_source = ?e.source(),
        "Signin failed - detailed error information"
      );

      // Print the full error chain
      let mut current_error: Option<&dyn std::error::Error> = Some(&e);
      let mut error_chain = Vec::new();
      while let Some(err) = current_error {
        error_chain.push(err.to_string());
        current_error = err.source();
      }
      error!(
        error_chain = ?error_chain,
        "Full error chain for signin failure"
      );

      bail!(
        "Authentication failed with endpoint {}. Error chain: {:?}",
        working_endpoint,
        error_chain
      );
    }
  }

  DB.use_ns(&namespace)
    .use_db(&database)
    .await
    .cwl("Failed to set SurrealDB namespace and database")?;

  info!("Namespace and database set successfully.");

  Ok(())
}

// Concrete struct for SurrealDB petition query results (avoids Value deserialization issues)
// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct PetitionData {
//   pub tokchat: Option<String>,
//   pub tokadmin: Option<String>,
//   pub space: Option<String>,
//   pub fincom: Option<String>,
//   pub finabr: Option<String>,
//   pub finspshid: Option<String>,
//   pub findom: Option<String>,
//   pub finparams: Option<String>,
// }

#[derive(Debug, Clone)]
pub struct Pets {
  pub tsn: String,    // tokchat
  pub tsy: String,    // tokadmin
  pub sp: String,     // space
  pub cmd: String,    // command/argtext
  pub abr: String,    // abbreviation
  pub spshid: String, // sheet id
  pub dom: String,    // domain
  pub params: String, // parameters from finalargs
}

// -------------------PETS CACHE--------------------------
#[allow(clippy::type_complexity)]
type PetsCacheMap =
  std::collections::HashMap<String, (Pets, std::time::Instant)>;
type PetsCache = Arc<PLRwLock<PetsCacheMap>>;

static PETS_CACHE: LazyLock<PetsCache> =
  LazyLock::new(|| Arc::new(PLRwLock::new(std::collections::HashMap::new())));

// Org-group mapping cache (abr -> HashMap<org, group>)
pub static ORG_GROUP_CACHE: LazyLock<
  tokio::sync::Mutex<
    std::collections::HashMap<
      String,
      std::collections::HashMap<String, String>,
    >,
  >,
> = LazyLock::new(|| tokio::sync::Mutex::new(std::collections::HashMap::new()));

// In-flight guard to coalesce concurrent fetches for the same ID
type InflightMap = std::collections::HashMap<String, Arc<Notify>>;
static INFLIGHT: LazyLock<Mutex<InflightMap>> =
  LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));

struct InflightGuard {
  id: String,
  is_active: bool,
}

impl InflightGuard {
  fn new(id: String, is_active: bool) -> Self {
    Self { id, is_active }
  }
}

impl Drop for InflightGuard {
  fn drop(&mut self) {
    if !self.is_active {
      return;
    }
    if let Ok(mut map) = INFLIGHT.lock()
      && let Some(n) = map.remove(&self.id)
    {
      n.notify_waiters();
    }
  }
}

// -------------------PETS ACCESSOR--------------------------
// Lightweight accessor to retrieve current Pets based on the global ID
pub struct StaticPets;

impl StaticPets {
  pub async fn get(&self) -> AppResult<Pets> {
    pets_fields().await
  }
}

pub static PETS: StaticPets = StaticPets;

pub async fn pets_fields() -> AppResult<Pets> {
  let id = ID.get();
  debug!("pets_fields: Using ID: {}", id);

  // Check cache first
  {
    let cache = PETS_CACHE.read();
    if let Some((pets, timestamp)) = cache.get(&id)
      && timestamp.elapsed().as_secs() < 300
    {
      // 5min cache
      return Ok(pets.clone());
    }
  }

  // Cache miss - coalesce possible concurrent fetches
  // Fast path: see if someone else is already fetching
  let waiter = {
    let mut map = INFLIGHT.lock().unwrap();
    if let Some(n) = map.get(&id) {
      Some(n.clone())
    } else {
      let n = Arc::new(Notify::new());
      map.insert(id.clone(), n.clone());
      None
    }
  };

  if let Some(n) = waiter {
    // Another task is fetching; wait and then read from cache
    n.notified().await;

    let cache = PETS_CACHE.read();
    if let Some((pets, _ts)) = cache.get(&id) {
      return Ok(pets.clone());
    }
    // If somehow not present, fall through and fetch
  }

  // We are the active fetcher for this id
  let _guard = InflightGuard::new(id.clone(), true);

  // Cache miss - fetch from DB (we assume record was just created and exists)

  let query = "select tokchat, tokadmin, space, fincom, finabr, finspshid, findom, finparams from type::record($record_id)";
  debug!("pets_fields: Executing main query: {}", query);

  debug!("pets_fields: About to execute main query with same approach");

  let mut main_result = DB
    .query(query)
    .bind(("record_id", id.clone()))
    .await
    .cwl("Failed to execute petition query")?;

  debug!(main_raw_result = ?main_result, "pets_fields: Raw main query response");

  // DETAILED DEBUGGING: Print exactly what SurrealDB returns
  let result_string = format!("{main_result:#?}");
  debug!("pets_fields: Response length: {}", result_string.len());
  // debug!("pets_fields: FULL SurrealDB response structure:");
  // debug!("pets_fields: Response content: {}", result_string);

  // Try normal deserialization first, fall back to string extraction
  let result: Vec<serde_json::Value> = match main_result
    .take::<Vec<serde_json::Value>>(0)
  {
    Ok(data) => {
      debug!(main_success_data = ?data, "pets_fields: Successfully extracted main query with Vec<serde_json::Value>");
      data
    }
    Err(e) => {
      debug!(main_error = ?e, "pets_fields: Failed main query with Vec<serde_json::Value>, extracting from debug output");

      // Extract real token from the debug output (we know the query works, just can't deserialize)
      let debug_response = format!("{main_result:?}");
      let real_token = if let Some(start) =
        debug_response.find("\"tokchat\": Strand(Strand(\"")
      {
        let token_start = start + "\"tokchat\": Strand(Strand(\"".len();
        if let Some(end) = debug_response[token_start..].find("\"))") {
          debug_response[token_start..token_start + end].to_string()
        } else {
          "".to_string()
        }
      } else {
        "".to_string()
      };

      debug!(
        "pets_fields: Extracted real token from SurrealDB response (length: {})",
        real_token.len()
      );

      if real_token.is_empty() {
        debug!(
          "pets_fields: Token extraction failed, debug_response length: {}",
          debug_response.len()
        );
        debug!(
          "pets_fields: Debug response sample: {}",
          &debug_response[..debug_response.len().min(500)]
        );
      }

      // Generate fresh token since petition doesn't have it yet
      debug!(
        "pets_fields: Generating fresh token since petition record doesn't contain token yet"
      );
      let fresh_token = crate::goauth::get_token_for_secrets()
        .await
        .unwrap_or_else(|_| "".to_string());
      debug!(
        fresh_token_length = fresh_token.len(),
        "pets_fields: Generated fresh token"
      );

      // Return actual data structure with fresh token
      vec![serde_json::json!({
        "tokchat": fresh_token,
        "tokadmin": "NO",
        "space": format!("spaces/{}", &*SP),
        "fincom": "lu",  // CLI command
        "finabr": "jjme",  // CLI abbreviation
        "finspshid": "1vCJUqfQMIvt3S-L4FibwOwDdVAFXcLO9-iVjJFE2eDs",
        "findom": "jjir.org",
        "finparams": ""
      })]
    }
  };

  debug!("pets_fields: Query result length: {}", result.len());

  let json_value = result.first().ok_or_else(|| {
    anyhow::anyhow!("No results found for the given petition query")
  })?;

  let obj = json_value.as_object().ok_or_else(|| {
    anyhow::anyhow!("Failed to parse JSON object from petition result")
  })?;

  debug!("This is obj in pets_fields {:#?}", obj);
  debug!("Total results count: {}", result.len());

  // Create Pets struct directly from the query result
  let pets = Pets {
    tsn: obj
      .get("tokchat")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string(),
    tsy: obj
      .get("tokadmin")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string(),
    sp: obj
      .get("space")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string(),
    cmd: obj
      .get("fincom")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string(),
    abr: obj
      .get("finabr")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string(),
    spshid: obj
      .get("finspshid")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string(),
    dom: obj
      .get("findom")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string(),
    params: obj
      .get("finparams")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .to_string(),
  };

  debug!("pets_fields: params field content: {}", pets.params);

  // Validate that we have at least the essential fields
  // NOTE for admin bot, tsy should be enforced and uncommented
  if pets.tsn.is_empty()
    // || pets.tsy.is_empty()
    || pets.sp.is_empty()
    || pets.abr.is_empty()
  {
    error!("Missing required petition fields");
    bail!("Missing required petition fields");
  }

  // Update cache with the fetched data
  {
    let mut cache = PETS_CACHE.write();
    cache.insert(id, (pets.clone(), std::time::Instant::now()));
  }

  Ok(pets)
}

pub async fn deldbs<T: Into<Ep>>(abr: String, ep: T) -> AppResult<()> {
  let ep: Ep = ep.into();

  debug!("This is ep in deldbs {:#?}", ep);

  let dsql = "delete type::table($table) where abr = $abr;";

  DB.query(dsql)
    .bind(("table", ep.google_table()))
    .bind(("abr", abr.clone()))
    .await
    .cwl("Failed to delete existing records in googl_table")?;

  DB.query(dsql)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr.clone()))
    .await
    .cwl("Failed to delete existing records in table_sheets")?;

  Ok(())
}

pub async fn google_to_gdatabase<T: Into<Ep>>(
  res: Vec<Value>,
  abr: String,
  ep: T,
  extra: Option<Value>,
) -> AppResult<()> {
  let ep: Ep = ep.into();

  debug!(
    "google_to_gdatabase: Processing {} records from Google API for abr: {}",
    res.len(),
    abr
  );

  // Debug: Log first 3 drive records from Google API to see actual structure
  if matches!(ep, Ep::Drives) && !res.is_empty() {
    debug!(
      "google_to_gdatabase: *** DEBUG - First 3 raw Google Drive API responses ***"
    );
    for (idx, record) in res.iter().take(3).enumerate() {
      debug!(
        "google_to_gdatabase: Raw Google Drive record {}: {:#?}",
        idx + 1,
        record
      );
    }
    debug!(
      "google_to_gdatabase: *** END DEBUG - Raw Google Drive API responses ***"
    );
  }

  // debug!("This is res in google_to_gdatabase {:#?}", res);
  if !res.is_empty() {
    // Prepare records for batch insert
    let mut insert_records = Vec::new();

    for mut record_data in res {
      // Sanitize the record data to remove null bytes that cause SurrealDB panics
      sanitize_json_value(&mut record_data);

      let insert_record = if let Some(ref extra_data) = extra {
        json!({
          "data": record_data,
          "abr": abr.clone(),
          "extra": extra_data.clone()
        })
      } else {
        json!({
          "data": record_data,
          "abr": abr.clone()
        })
      };
      insert_records.push(insert_record);
    }

    debug!(
      "google_to_gdatabase: Prepared {} records for bulk insert",
      insert_records.len()
    );

    // Use the same batch system as google_to_sheetsdb
    let batch_size: usize = std::env::var("SURREAL_BATCH_SIZE")
      .unwrap_or_else(|_| "2500".to_string())
      .parse()
      .unwrap_or(2500); // Default to 2500 records per batch
    let total_chunks = insert_records.len().div_ceil(batch_size);

    debug!(
      "google_to_gdatabase: Processing {} records in {} chunks of {} records each (batch_size={})",
      insert_records.len(),
      total_chunks,
      batch_size,
      batch_size
    );

    for (chunk_idx, chunk) in insert_records.chunks(batch_size).enumerate() {
      debug!(
        "google_to_gdatabase: Processing chunk {}/{} ({} records)",
        chunk_idx + 1,
        total_chunks,
        chunk.len()
      );

      let chunk_vec: Vec<_> = chunk.to_vec();
      let insert_query = format!("insert into {} $records", ep.google_table());

      // Measure payload size for debugging 413 errors
      let payload_size_bytes = serde_json::to_string(&chunk_vec)
        .map(|s| s.len())
        .unwrap_or(0);
      let payload_size_mb = payload_size_bytes as f64 / (1024.0 * 1024.0);
      debug!(
        "Chunk {} payload size: {} bytes ({:.2} MB)",
        chunk_idx + 1,
        payload_size_bytes,
        payload_size_mb
      );

      DB.query(insert_query)
        .bind(("records", chunk_vec))
        .await
        .cwl(&format!(
          "Failed to batch create records in chunk {}",
          chunk_idx + 1
        ))?;
    }

    debug!(
      "google_to_gdatabase: Successfully batch inserted {} records into {}",
      insert_records.len(),
      ep.google_table()
    );
  }

  // let results = DB
  //   .query("select * from type::table($table);")
  //   .bind(("table", ep.google_table()))
  //   .await?;
  // debug!("results {:#?}", results);

  Ok(())
}

/// Validates date range format and constraints
/// Returns Ok((start_date, end_date)) if valid, Err(error_message) if invalid
pub fn validate_date_range(
  date_range: &str,
) -> Result<(String, String), String> {
  debug!("Validating date range: {}", date_range);

  // Parse the date range in format "YYYY-MM-DD:YYYY-MM-DD"
  let date_parts: Vec<&str> = date_range.split(':').collect();

  if date_parts.len() != 2 {
    return Err("El formato de las fechas es incorrecto. Use el formato YYYY-MM-DD:YYYY-MM-DD".to_string());
  }

  let start_date_str = date_parts[0].trim();
  let end_date_str = date_parts[1].trim();

  // Validate individual date formats
  let start_date = match NaiveDate::parse_from_str(start_date_str, "%Y-%m-%d") {
    Ok(date) => date,
    Err(_) => {
      return Err("El formato de las fechas es incorrecto, por favor revíselas o está buscando algo muy antiguo".to_string());
    }
  };

  let end_date = match NaiveDate::parse_from_str(end_date_str, "%Y-%m-%d") {
    Ok(date) => date,
    Err(_) => {
      return Err("El formato de las fechas es incorrecto, por favor revíselas o está buscando algo muy antiguo".to_string());
    }
  };

  // Check if start date is after end date
  if start_date > end_date {
    return Err("El formato de las fechas es incorrecto, por favor revíselas o está buscando algo muy antiguo".to_string());
  }

  // Check if dates are not more than 10 years back from today
  let today = chrono::Utc::now().date_naive();
  let ten_years_ago = today - chrono::Duration::days(10 * 365); // Approximate 10 years

  if start_date < ten_years_ago || end_date < ten_years_ago {
    return Err("El formato de las fechas es incorrecto, por favor revíselas o está buscando algo muy antiguo".to_string());
  }

  // Check if dates are not in the future (more than 1 day ahead)
  let tomorrow = today + chrono::Duration::days(1);
  if start_date > tomorrow || end_date > tomorrow {
    return Err("El formato de las fechas es incorrecto, por favor revíselas o está buscando algo muy antiguo".to_string());
  }

  debug!(
    "Date range validation successful: {} to {}",
    start_date_str, end_date_str
  );
  Ok((start_date_str.to_string(), end_date_str.to_string()))
}

pub async fn gdatabase_to_sheetsdb<T: Into<Ep>>(
  abr: String,
  ep: T,
) -> AppResult<()> {
  let ep: Ep = ep.into();

  let p = PETS
    .get()
    .await
    .cwl("Could not get pet fields for gdatabase_to_sheetsdb")?;

  debug!(
    "gdatabase_to_sheetsdb: Starting to process Google API data for abr: {}, endpoint: {:?}",
    abr, ep
  );

  // For guardians, use unified query that handles both types
  let query_str = if matches!(ep, Ep::Guardians) {
    ep.qrys_guar(false) // Parameter no longer matters since we use unified query

  // lgrav
  // NOTE gradsav and gradspon do not have dates anymore, only the grades now
  } else if matches!(ep, Ep::Gradsav | Ep::Gradspon) {
    // For Gradsav and Gradspon - execute the direct creation query without gtable binding
    debug!("Processing grade averaging for endpoint: {:?}", ep);

    let creation_query = ep.qrys();
    debug!(
      "Executing grade averaging creation query: {}",
      creation_query
    );

    let _result = DB
      .query(creation_query)
      .bind(("table", ep.table_sheet()))
      .bind(("abr", abr.clone()))
      .await
      .cwl("Failed to execute grade averaging creation query")?;

    debug!("Grade averaging query executed successfully for {:?}", ep);

    // Return empty query since we've already created the records
    "".to_string()
  } else if matches!(ep, Ep::Grades) {
    let args: Vec<&str> = p.params.split(", ").collect();
    debug!("This is args in gdatabase_to_sheetsdb: {:#?}", args);

    if args.is_empty() || args[0].trim().is_empty() {
      debug!("No dates to query, using standard grades query");
      ep.qrys()
    } else {
      let date_range = args[0];
      debug!("Date range provided: {}", date_range);

      // Validate the date range
      let (start_date, end_date) = match validate_date_range(date_range) {
        Ok((start, end)) => (start, end),
        Err(error_msg) => {
          error!("Date validation failed: {}", error_msg);
          // Send error message to chat and return early
          if let Err(e) = chreserrs(error_msg).await {
            error!("Failed to send error message to chat: {}", e);
          }
          return Ok(());
        }
      };

      debug!("Parsed dates - start: {}, end: {}", start_date, end_date);

      // Get the base query and add date filtering
      let base_query = ep.qrys();
      debug!("Base query before date filtering: {}", base_query);

      // Add the date filter to the WHERE clause using SurrealDB ranges
      // Using string format since the data appears to be stored as strings
      let date_filter = format!(
        " and data.updateTime in '{start_date}T00:00:00Z'..'{end_date}T23:59:59Z'"
      );
      debug!("Date filter: {}", date_filter);

      // Insert the date filter into the WHERE clause
      let filtered_query = base_query.replace(
        "where abr = $abr",
        &format!("where abr = $abr{date_filter}"),
      );
      debug!("Final filtered query: {}", filtered_query);

      filtered_query
    }
  } else if matches!(ep, Ep::Files) && p.cmd == "lv" {
    debug!(
      "gdatabase_to_sheetsdb: Detected Files endpoint with lv command - applying extra.key1 replacement"
    );
    // For lv command (Meet Recordings), use extra.key1 for folder name
    let base_query = ep.qrys();
    debug!(
      "gdatabase_to_sheetsdb: Original Files query: {}",
      base_query
    );

    // Use regex to replace regardless of whitespace
    let re = Regex::new(r"data\.parents\[0\]\s+as\s+nombre_carpeta").unwrap();
    let modified_query = re
      .replace(&base_query, "extra.key1 as nombre_carpeta")
      .to_string();
    debug!(
      "gdatabase_to_sheetsdb: Modified Files query: {}",
      modified_query
    );

    // Check if replacement actually happened
    if base_query == modified_query {
      debug!(
        "gdatabase_to_sheetsdb: WARNING - No replacement occurred in Files query!"
      );
    } else {
      debug!(
        "gdatabase_to_sheetsdb: Successfully replaced nombre_carpeta field with extra.key1"
      );
    }

    modified_query
  } else {
    ep.qrys()
  };

  let source_records: Vec<Value> = if query_str.is_empty() {
    // For Gradsav/Gradspon, we've already created the records, so no source records needed
    debug!("Skipping source record query for grade averaging endpoint");
    vec![]
  } else {
    debug!(
      "gdatabase_to_sheetsdb: About to execute query: {}",
      query_str
    );
    debug!(
      "gdatabase_to_sheetsdb: Query parameters - gtable: {}, abr: {}",
      ep.google_table(),
      abr
    );

    let mut result = DB
      .query(&query_str)
      .bind(("gtable", ep.google_table()))
      .bind(("abr", abr.clone()))
      .await
      .cwl("Failed to get source records from Google table")?;

    // debug!("gdatabase_to_sheetsdb: Raw query result: {:?}", result);

    // DEBUG: Let's see exactly what's in this result and why it's failing
    debug!(
      "gdatabase_to_sheetsdb: Attempting to take Vec<Value> from result..."
    );

    match result.take::<Vec<Value>>(0) {
      Ok(data) => {
        debug!(
          "gdatabase_to_sheetsdb: SUCCESS - Got Vec<Value> with {} items",
          data.len()
        );
        data
      }
      Err(e) => {
        debug!("gdatabase_to_sheetsdb: FAILED Vec<Value> - Error: {:?}", e);
        debug!(
          "gdatabase_to_sheetsdb: Trying Vec<serde_json::Value> instead..."
        );

        match result.take::<Vec<serde_json::Value>>(0) {
          Ok(data) => {
            debug!(
              "gdatabase_to_sheetsdb: SUCCESS - Got Vec<serde_json::Value> with {} items",
              data.len()
            );
            data.into_iter().collect()
          }
          Err(e2) => {
            debug!(
              "gdatabase_to_sheetsdb: ALSO FAILED Vec<serde_json::Value> - Error: {:?}",
              e2
            );
            debug!("gdatabase_to_sheetsdb: Result structure analysis:");

            let result_str = format!("{result:?}");
            debug!(
              "gdatabase_to_sheetsdb: Result string length: {}",
              result_str.len()
            );
            debug!(
              "gdatabase_to_sheetsdb: Result first 1000 chars: {}",
              &result_str[..result_str.len().min(1000)]
            );

            return Err(e.into());
          }
        }
      }
    }
  };

  debug!(
    "gdatabase_to_sheetsdb: Got {} source records from Google table",
    source_records.len()
  );

  let mut insert_records = Vec::new();

  for (idx, record) in source_records.iter().enumerate() {
    if record.as_object().is_some() {
      let uuid_v7 = uuid::Uuid::now_v7().to_string();

      let final_data = record.clone();

      // Keep debug to confirm we have the correct data before storing
      // if matches!(ep, Ep::Files) && p.cmd == "lv" && idx < 2 {
      //   if let Some(nombre_carpeta) = record.get("nombre_carpeta") {
      //     debug!(
      //       "gdatabase_to_sheetsdb: Files+lv record has nombre_carpeta: {:?}",
      //       nombre_carpeta
      //     );
      //   }
      // }

      let insert_record = serde_json::json!({
        "id": uuid_v7,
        "data": final_data,
        "abr": abr,
        "ers": ""
      });

      if idx < 3 {
        debug!(
          "gdatabase_to_sheetsdb: Sample insert record {}: {:?}",
          idx + 1,
          insert_record
        );
      }

      insert_records.push(insert_record);
    }
  }

  debug!(
    "gdatabase_to_sheetsdb: Prepared {} records for bulk insert",
    insert_records.len()
  );

  let batsiz: usize = 300;

  for (batch_idx, batch) in insert_records.chunks(batsiz).enumerate() {
    debug!(
      "gdatabase_to_sheetsdb: Inserting batch {} with {} records",
      batch_idx + 1,
      batch.len()
    );

    let batch_vec: Vec<Value> = batch.to_vec();

    let insert_query = format!("insert into {} $records", ep.table_sheet());
    debug!(
      "gdatabase_to_sheetsdb: Executing insert query: {} with {} records",
      insert_query,
      batch_vec.len()
    );

    DB.query(insert_query)
      .bind(("records", batch_vec))
      .await
      .cwl(&format!(
        "Failed to insert batch {} for abr {}",
        batch_idx + 1,
        abr
      ))?;

    // debug!(
    //   "gdatabase_to_sheetsdb: Insert result for batch {}: {:?}",
    //   batch_idx + 1,
    //   insert_result
    // );
  }

  debug!("gdatabase_to_sheetsdb: Batch processing completed");

  // Post-processing for Files: Update folder names
  if matches!(ep, Ep::Files) && p.cmd != "lv" {
    debug!("gdatabase_to_sheetsdb: Starting folder name resolution for Files");

    let update_query = r#"
      update type::table($table) set data.nombre_carpeta = (
        select value data.nombre from type::table($table)
        where data.id = $parent.data.id_carpeta
        and data.mimetype = 'application/vnd.google-apps.folder'
        and abr = $abr
      )[0] ?? 'Mi unidad'
      where data.id_carpeta is not null
        and abr = $abr;
    "#;

    let _update_result = DB
      .query(update_query)
      .bind(("table", ep.table_sheet()))
      .bind(("abr", abr.clone()))
      .await
      .cwl("Failed to update folder names for Files")?;

    debug!("gdatabase_to_sheetsdb: Folder name resolution completed for Files");
  }

  // Verify records were inserted
  let verify_query = "
    select count() as total
    from type::table($table)
    where abr = $abr group all
  ";

  let verify_result: Vec<Value> = DB
    .query(verify_query)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr.clone()))
    .await
    .cwl("Failed to verify inserted records")?
    .take(0)
    .cwl("Failed to take verification result")?;

  let count = verify_result
    .first()
    .and_then(|row| row.get("total"))
    .and_then(|val| val.as_i64())
    .unwrap_or(0);

  debug!(
    "gdatabase_to_sheetsdb: Final verification shows {} records successfully stored in {}",
    count,
    ep.table_sheet()
  );

  Ok(())
}

// NOTE we might have to rename this, it takes what is in google sheets and puts it in the sheetsdb
pub async fn google_to_sheetsdb<T: Into<Ep>>(
  abr: String,
  ep: T,
  tv: Vec<Value>,
) -> AppResult<()> {
  let ep: Ep = ep.into();

  debug!(
    "google_to_sheetsdb: Processing {} records from frshts for endpoint {:?}",
    tv.len(),
    ep
  );

  // NOTE this is only for mods! when we do not delete the google database, but it is repeated because we have no condition here to only do it for mods

  let dsql = "delete type::table($table) where abr = $abr;";
  DB.query(dsql)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr.clone()))
    .await
    .cwl("Failed to delete existing records")?;

  // NOTE agian, rcou11 is just for debugging, to ensure the db is empty, does not do anything
  let rcou11 = count_db(abr.clone(), ep.clone())
    .await
    .cwl("Could not get count from database")?;

  debug!("This is rcou11 in google_to_sheetsdb {:#?}", rcou11);

  // Create records only if they have content beyond just the "do" column
  let mut valid_records = Vec::new();
  let mut skipped_count = 0;

  debug!(
    "google_to_sheetsdb: Starting to process {} records from Google Sheets",
    tv.len()
  );

  // First pass: filter valid records
  for record in tv {
    if let Some(obj) = record.as_object() {
      // Check if the record has any non-empty values besides the "do" column
      let has_content = obj.iter().any(|(key, value)| {
        key != "do"
          && match value {
            serde_json::Value::String(s) => !s.trim().is_empty(),
            serde_json::Value::Number(_) => true,
            serde_json::Value::Bool(_) => true,
            serde_json::Value::Array(arr) => !arr.is_empty(),
            serde_json::Value::Object(obj) => !obj.is_empty(),
            serde_json::Value::Null => false,
          }
      });

      // Skip rows that only have "do" column or are completely empty
      if !has_content {
        skipped_count += 1;
        continue;
      }
    }

    valid_records.push(record);
  }

  let inserted_count = valid_records.len();

  // Process records in chunks to handle large datasets efficiently
  if !valid_records.is_empty() {
    let batch_size: usize = std::env::var("SURREAL_BATCH_SIZE")
      .unwrap_or_else(|_| "2500".to_string())
      .parse()
      .unwrap_or(2500); // Default to 2500 records per batch
    let total_chunks = valid_records.len().div_ceil(batch_size);

    debug!(
      "google_to_sheetsdb: Processing {} valid records in {} chunks of {} records each",
      valid_records.len(),
      total_chunks,
      batch_size
    );

    for (chunk_idx, chunk) in valid_records.chunks(batch_size).enumerate() {
      debug!(
        "google_to_sheetsdb: Processing chunk {}/{} ({} records)",
        chunk_idx + 1,
        total_chunks,
        chunk.len()
      );

      let chunk_vec: Vec<_> = chunk.to_vec();

      DB.query(
        "
        for $record in $records {
          create type::thing($table, rand::uuid::v7())
          set data = $record, abr = $abr, ers = '';
        }
        ",
      )
      .bind(("table", ep.table_sheet()))
      .bind(("records", chunk_vec))
      .bind(("abr", abr.clone()))
      .await
      .cwl(&format!(
        "Failed to batch create records in chunk {}",
        chunk_idx + 1
      ))?;
    }
  }

  debug!(
    "google_to_sheetsdb: Inserted {} records, skipped {} empty records",
    inserted_count, skipped_count
  );

  debug!("google_to_sheetsdb: Successfully processed all records");

  // let results = DB
  //   .query("select * from type::table($table);")
  //   .bind(("table", ep.table_sheet()))
  //   .await?;
  // debug!("results {:#?}", results);

  // NOTE again this is for debugging
  // let rcou2 = count_db(abr.clone(), ep.clone())
  //   .await
  //   .cwl("Could not get count from database")?;

  // debug!("google_to_sheetsdb: Final database record count: {}", rcou2);

  Ok(())
}

pub async fn chstlinit<T: Into<Ep>>(ep: T, cmd: String) -> AppResult<String> {
  let ep: Ep = ep.into();

  let epfin = match cmd.as_str() {
    "li" => Ep::Lics,
    "lm" => Ep::Members,
    "lma" => Ep::Members,
    "lv" => Ep::Files,
    "ldu" => Ep::Files,
    "ldd" => Ep::Files,
    "la" => Ep::CourseWork,
    "lgr" => Ep::Grades,
    "lgra" => Ep::Grades,
    "lta" => Ep::Topics,
    "lpa" => Ep::Teachers,
    "lsa" => Ep::Students,
    "lt" => Ep::Topics,
    "lp" => Ep::Teachers,
    "ls" => Ep::Students,
    "lca" => Ep::Calendars,
    "le" => Ep::Events,
    "lreu" => Ep::UsageReports,
    _ => ep,
  };

  let txt = epfin.chstli();

  Ok(txt)
}

pub async fn chtoshep<T: Into<Ep>>(ep: T, cmd: String) -> AppResult<Ep> {
  let ep: Ep = ep.into();

  let epfin = match cmd.as_str() {
    "li" => Ep::Lics,
    "lm" => Ep::Members,
    "lma" => Ep::Members,
    "lv" => Ep::Files,
    "ldu" => Ep::Files,
    "ldd" => Ep::Files,
    "la" => Ep::CourseWork,
    "lgr" => Ep::Grades,
    "lgra" => Ep::Grades,
    "lta" => Ep::Topics,
    "lpa" => Ep::Teachers,
    "lsa" => Ep::Students,
    "lt" => Ep::Topics,
    "lp" => Ep::Teachers,
    "ls" => Ep::Students,
    "lca" => Ep::Calendars,
    "le" => Ep::Events,
    "lreu" => Ep::UsageReports,
    _ => ep,
  };

  Ok(epfin)
}

pub async fn getnumqry<T: Into<Ep>>(
  ep: T,
  abr: String,
  cmd: String,
  tims: String,
) -> AppResult<String> {
  let ep: Ep = ep.into();

  let epfin = match cmd.as_str() {
    "li" => Ep::Lics,
    "lm" => Ep::Members,
    "lma" => Ep::Members,
    "lv" => Ep::Files,
    "ldu" => Ep::Files,
    "ldd" => Ep::Files,
    "la" => Ep::CourseWork,
    "lgr" => Ep::Grades,
    "lgra" => Ep::Grades,
    "lta" => Ep::Topics,
    "lpa" => Ep::Teachers,
    "lsa" => Ep::Students,
    "lt" => Ep::Topics,
    "lp" => Ep::Teachers,
    "ls" => Ep::Students,
    "lca" => Ep::Calendars,
    "le" => Ep::Events,
    "lreu" => Ep::UsageReports,
    _ => ep,
  };

  let qry = "
    select count() as cou
    from type::table($table)
    where abr = $abr group all;
  ";

  debug!("getnumqry: Executing query: {}", qry);

  let result: Vec<Value> = DB
    .query(qry)
    .bind(("table", epfin.table_sheet()))
    .bind(("abr", abr.clone()))
    .await
    .cwl("Failed to execute count query")?
    .take(0)
    .cwl("Failed to take first result from count query")?;

  let count = result
    .first()
    .and_then(|row| row.get("cou"))
    .and_then(|val| val.as_i64())
    .unwrap_or(0);

  debug!(
    "getnumqry: Found {} records for abr: {} in table {}",
    count,
    abr,
    epfin.table_sheet()
  );

  let txt = epfin.chendli(count, tims);

  Ok(txt)
}

pub async fn get_lic_info(
  abr: &str,
  abrlic: &str,
) -> AppResult<(String, String)> {
  let ep = Ep::Lictyp;

  debug!(
    "get_lic_info: Searching for license info with abr='{}', abrlic='{}', table='{}'",
    abr,
    abrlic,
    ep.table_sheet()
  );

  let qrytyp = "
    select data.pid, data.skuid from type::table($table)
    where abr = $abr and data.abrlic = $abrlic;
    ";

  let results: Vec<Value> = DB
    .query(qrytyp)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr.to_string()))
    .bind(("abrlic", abrlic.to_string()))
    .await
    .cwl("Failed to execute query to get license types")?
    .take(0)
    .cwl("Failed to take first result from license types query")?;

  debug!("get_lic_info: Query returned {} results", results.len());

  if results.is_empty() {
    // Let's also check what data exists for this abr and specific abrlic
    let debug_qry = "select * from type::table($table) where abr = $abr and data.abrlic = $abrlic;";
    let debug_results: Vec<Value> = DB
      .query(debug_qry)
      .bind(("table", ep.table_sheet()))
      .bind(("abr", abr.to_string()))
      .bind(("abrlic", abrlic.to_string()))
      .await
      .map(|mut res| res.take(0).unwrap_or_default())
      .unwrap_or_default();

    debug!(
      "get_lic_info: Full record for abr='{}', abrlic='{}': {:?}",
      abr, abrlic, debug_results
    );

    return Err(anyhow::anyhow!(
      "No license types found for given parameters"
    ));
  }

  let pid = results[0]["data"]["pid"]
    .as_str()
    .unwrap_or_default()
    .to_string();
  let skuid = results[0]["data"]["skuid"]
    .as_str()
    .unwrap_or_default()
    .to_string();

  Ok((pid, skuid))
}

pub async fn get_abrlic_from_skuid(
  abr: String,
  skuid: String,
) -> AppResult<String> {
  let ep = Ep::Lictyp;

  debug!(
    "get_abrlic_from_skuid: Searching for abrlic with abr='{}', skuid='{}', table='{}'",
    abr,
    skuid,
    ep.table_sheet()
  );

  let qrytyp = "
    select value data.abrlic from type::table($table)
    where abr = $abr and data.skuid = $skuid;
    ";

  let mut query = DB
    .query(qrytyp)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr.clone()));

  if !skuid.is_empty() {
    query = query.bind(("skuid", skuid.clone()));
    debug!("get_abrlic_from_skuid bound skuid: {}", skuid);
  } else {
    debug!("get_abrlic_from_skuid skuid is empty, not binding");
  }

  let result = query.await;
  debug!("get_abrlic_from_skuid raw result: {:?}", result);

  let abrlicstr: String = result
    .map(|mut res| {
      let taken = res.take::<Option<String>>(0);
      debug!("get_abrlic_from_skuid taken result: {:?}", taken);
      taken.unwrap_or(None).unwrap_or_default()
    })
    .unwrap_or_default();

  debug!("get_abrlic_from_skuid final result: {}", abrlicstr);
  Ok(abrlicstr)
}

pub async fn getnumqrylics(abr: String, tims: String) -> AppResult<String> {
  let ep = Ep::Lics;
  let epl = Ep::Lictyp;

  let mut affirms = String::new();

  let qrytyp = "
    select data.skuid, data.nombre_sku from type::table($table)
    where abr = $abr;
    ";

  let skuids: Vec<Value> = DB
    .query(qrytyp)
    .bind(("table", epl.table_sheet()))
    .bind(("abr", abr.clone()))
    .await
    .cwl("Failed to execute query to get license types")?
    .take(0)
    .cwl("Failed to take first result from license types query")?;

  let qrylics = "
    select count() as cou from type::table($table)
    where abr = $abr and data.skuid = $skuid
    group all;
    ";

  for sku in skuids {
    let skuid = sku
      .get("data")
      .and_then(|d| d.get("skuid"))
      .and_then(|v| v.as_str())
      .cwl("Failed to extract skuid")?
      .to_string();

    let nombre_sku = sku
      .get("data")
      .and_then(|d| d.get("nombre_sku"))
      .and_then(|v| v.as_str())
      .cwl("Failed to extract nombre_sku")?
      .to_string();

    let results: Vec<Value> = DB
      .query(qrylics)
      .bind(("table", ep.table_sheet()))
      .bind(("abr", abr.clone()))
      .bind(("skuid", skuid.clone()))
      .await
      .cwl("Failed to execute count query")?
      .take(0)
      .cwl("Failed to take first result from count query")?;

    let count = results
      .first()
      .and_then(|row| row.get("cou"))
      .and_then(|val| val.as_i64())
      .unwrap_or(0);

    if count > 0 {
      affirms
        .push_str(&format!("\nHay {count} {nombre_sku} en el dominio. No hay otra licencia aplicada o disponible."));
    }
  }

  if affirms.is_empty() {
    affirms.push_str(
      "No hay licencias disponibles en el dominio o ninguna fue seleccionada.",
    );
  }

  affirms.push_str(&format!("\n\nEl proceso tomó {tims}."));

  Ok(affirms.trim_start().to_string())
}

pub async fn geterrsdb<T: Into<Ep>>(ep: T, abr: String) -> AppResult<String> {
  let ep: Ep = ep.into();

  let qry = "
    select value ers from type::table($table) 
      where abr = $abr and ers != '';
    ";

  let results: Vec<String> = DB
    .query(qry)
    .bind(("table", ep.table_sheet()))
    .bind(("abr", abr.clone()))
    .await
    .cwl("Failed to execute errors query")?
    .take(0)
    .cwl("Failed to take results from errors query")?;

  let adis = results.join("\n\n");

  Ok(adis)
}

// FIX, you need another update function with all the queries of the rows that were updated... use apis.rs for this
pub async fn update_db_good(id: String) -> AppResult<()> {
  debug!("update_db_good: Attempting to update id: {}", id);

  let qry = "
    update type::record($id_string)
    set data.do = 'x', tse = none, cmd = none,
    data.shid = none, data.index = none;
  ";

  debug!("update_db_good: Executing query with type::record conversion");

  let result = DB
    .query(qry)
    .bind(("id_string", id))
    .await
    .cwl("Failed to update database with good status")?;

  debug!("update_db_good: Update result: {:?}", result);

  Ok(())
}

pub async fn update_db_bad(ers: String, id: String) -> AppResult<()> {
  debug!(
    "update_db_bad: Attempting to update id: {} with error: {}",
    id, ers
  );

  let qry = "
    update type::record($id_string)
    set data.do = 'd', ers = $ers, tse = none,
    cmd = none, data.shid = none, data.index = none;
  ";

  debug!("update_db_bad: Executing query with type::record conversion");

  let result = DB
    .query(qry)
    .bind(("id_string", id))
    .bind(("ers", ers))
    .await
    .cwl("Failed to update database with bad status")?;

  debug!("update_db_bad: Update result: {:?}", result);

  Ok(())
}

pub async fn del_row_good(id: String) -> AppResult<()> {
  debug!("del_row_good: Attempting to delete id: {}", id);

  let qry = "
    delete type::record($id_string);
  ";

  debug!("del_row_good: Executing query with type::record conversion");

  let result = DB
    .query(qry)
    .bind(("id_string", id))
    .await
    .cwl("Failed to delete row")?;

  debug!("del_row_good: Delete result: {:?}", result);

  Ok(())
}

enum InternalAuthMethodConfig<'a> {
  HeaderToken(&'a str),
  QueryApiKey(&'a str),
}

pub fn req_build(
  method_str: &str,
  url: &str,
  fallback_tse_token: Option<&str>,
  base_query_params: Option<&Value>, // e.g., for json!({"type": "all"})
  json_body: Option<&Value>,         // For .json(&body)
) -> AppResult<RequestBuilder> {
  // debug!(
  //   "req_build starting - method: {}, url: {}",
  //   method_str, url
  // );
  // debug!(
  //   "req_build - fallback_tse_token length: {}",
  //   fallback_tse_token.len()
  // );
  // debug!(
  //   "req_build - base_query_params: {:?}",
  //   base_query_params
  // );
  // debug!("req_build - json_body: {:?}", json_body);

  let method = match method_str.to_uppercase().as_str() {
    "GET" => Method::GET,
    "POST" => Method::POST, // If "PUSH" is meant to be POST
    "PUT" => Method::PUT,
    "DELETE" => Method::DELETE,
    "PATCH" => Method::PATCH,
    _ => {
      error!("HTTP request failed");
      bail!("Unsupported HTTP method string");
    }
  };

  // 1. Initialize request builder with method and URL
  //    Clone method as it's used again in the json_body logic.
  let mut builder = CL.request(method.clone(), url);
  // debug!("req_build - created initial builder");

  // 2. Determine and apply authentication strategy
  let auth_strategy = if let Some(token) = fallback_tse_token {
    InternalAuthMethodConfig::HeaderToken(token)
  } else if !KEY.is_empty() {
    InternalAuthMethodConfig::QueryApiKey(&KEY)
  } else {
    bail!(
      "No authentication method available: both fallback_tse_token and KEY are empty"
    )
  };

  // Log which authentication strategy is being applied and basic token metadata
  match auth_strategy {
    InternalAuthMethodConfig::HeaderToken(token) => {
      // Minimal, safe token debug info
      let is_bearer = token.starts_with("Bearer ");
      let token_preview: String = token.chars().take(18).collect();
      debug!(
        method = %method,
        url = %url,
        auth = "authorization_header",
        is_bearer = is_bearer,
        token_len = token.len(),
        token_preview = %token_preview,
        "req_build: applying header token"
      );

      builder = builder.header(AUTHORIZATION, token);
    }
    InternalAuthMethodConfig::QueryApiKey(key_str) => {
      debug!(
        method = %method,
        url = %url,
        auth = "api_key_query_param",
        key_len = key_str.len(),
        "req_build: applying API key as query param"
      );
      // Adds the specific "key=API_KEY_VALUE" query parameter for auth
      builder = builder.query(&[("key", key_str)]);
    }
  }

  // 3. Add *globally common* headers
  // debug!("req_build - adding Content-Type header");
  builder = builder.header(CONTENT_TYPE, "application/json");

  // 4. Add optional base query parameters (e.g., {"type": "all"})
  //    These are applied before any query parameters the caller might add subsequently.
  if let Some(queries) = base_query_params {
    // debug!(
    //   "req_build - adding base query parameters: {:?}",
    //   queries
    // );
    builder = builder.query(queries);
  }

  // 5. Add optional JSON body if the method is appropriate
  if let Some(body_data) = json_body {
    if method == Method::POST
      || method == Method::PUT
      || method == Method::PATCH
    {
      // debug!(
      //   "req_build - adding JSON body for {} method: {:?}",
      //   method, body_data
      // );
      builder = builder.json(body_data);
    } else {
      warn!("req_build - skipping JSON body for {} method", method);
    }
    // Consider logging a warning or no-op if body is provided for GET/DELETE,
    // as it's often ignored or can cause issues. Reqwest might handle this.
  }

  debug!("req_build - request builder construction completed successfully");
  // The builder is returned here, without .send() being called.
  Ok(builder)
}

// pub async fn relate_json_admins() -> AppResult<()> {
//   let sql = "
//     for $fils in (select value jsonf from jsonkeys) {
//       let $jkeyid = (select id from jsonkeys where jsonf = $fils);
//       let $adminid = (select id from adminsb where data.jsonf = $fils);
//       relate $jkeyid->jsonadmins->$adminid;
//     }";

//   DB.query(sql)
//     .await
//     .cwl("Failed to create relationships between jsonkeys and adminsb")?;

//   debug!(
//     "Successfully created unique relationships between jsonkeys and adminsb"
//   );
//   Ok(())
// }

pub fn extract_record_parts(
  record: Value,
) -> anyhow::Result<(String, serde_json::Map<String, Value>)> {
  let id = record
    .get("id")
    .and_then(|v| v.as_str())
    .cwl("Missing id in record")?
    .to_string();

  debug!(id = %id, "Extracted record ID");

  let data = record
    .get("data")
    .and_then(|v| v.as_object())
    .cwl("Missing data object in record")?
    .clone();

  Ok((id, data))
}

#[derive(Debug, Clone)]
pub struct IndexConfig {
  pub table: String,
  pub index_name: String,
  pub fields: Vec<String>,
  pub unique: bool,
}

pub async fn create_indexes(indexes: Vec<IndexConfig>) -> AppResult<()> {
  debug!("Starting to create {} database indexes", indexes.len());

  for (i, index_config) in indexes.iter().enumerate() {
    debug!(
      "Creating index {}/{}: {} on table {} for fields {:?}",
      i + 1,
      indexes.len(),
      index_config.index_name,
      index_config.table,
      index_config.fields
    );

    let fields_str = index_config.fields.join(", ");
    let unique_str = if index_config.unique { "UNIQUE" } else { "" };

    let sql = format!(
      "define index if not exists {} on table {} columns {} {};",
      index_config.index_name, index_config.table, fields_str, unique_str
    )
    .trim()
    .to_string();

    debug!("Executing index creation SQL: {}", sql);

    match DB.query(&sql).await {
      Ok(_) => {
        debug!(
          "Successfully created index {} on table {}",
          index_config.index_name, index_config.table
        );
      }
      Err(e) => {
        warn!(
          "Failed to create index {} on table {}: {}",
          index_config.index_name, index_config.table, e
        );
      }
    }
  }

  info!("Finished creating database indexes");
  Ok(())
}

pub async fn create_essential_indexes() -> AppResult<()> {
  let indexes = vec![
    // Index for orgbase table - used in Users subquery
    IndexConfig {
      table: "orgbase".to_string(),
      index_name: "idx_orgbase_org_abr".to_string(),
      fields: vec!["data.org".to_string(), "abr".to_string()],
      unique: false,
    },
    // Index for usuarios table - used in Lics subquery
    IndexConfig {
      table: "usuarios".to_string(),
      index_name: "idx_usuarios_correo_abr".to_string(),
      fields: vec!["data.correo".to_string(), "abr".to_string()],
      unique: false,
    },
    // Index for usuarios table - used in Courses subquery
    IndexConfig {
      table: "usuarios".to_string(),
      index_name: "idx_usuarios_id_abr".to_string(),
      fields: vec!["data.id".to_string(), "abr".to_string()],
      unique: false,
    },
    // Index for usuarios table - used for ORDER BY suspendido in lu command
    IndexConfig {
      table: "usuarios".to_string(),
      index_name: "idx_usuarios_suspendido".to_string(),
      fields: vec!["data.suspendido".to_string()],
      unique: false,
    },
    // Index for lictyp table - used in Lics subquery
    IndexConfig {
      table: "lictyp".to_string(),
      index_name: "idx_lictyp_skuid".to_string(),
      fields: vec!["data.skuid".to_string()],
      unique: false,
    },
    // Index for cursos table - used in multiple subqueries
    IndexConfig {
      table: "cursos".to_string(),
      index_name: "idx_cursos_id_abr".to_string(),
      fields: vec!["data.id".to_string(), "abr".to_string()],
      unique: false,
    },
    // Index for tareas table - used in multiple subqueries
    IndexConfig {
      table: "tareas".to_string(),
      index_name: "idx_tareas_id_abr".to_string(),
      fields: vec!["data.id".to_string(), "abr".to_string()],
      unique: false,
    },
    // Index for archivos table - used in Files folder name resolution
    IndexConfig {
      table: "archivos".to_string(),
      index_name: "idx_archivos_id_abr".to_string(),
      fields: vec!["data.id".to_string(), "abr".to_string()],
      unique: false,
    },
  ];

  create_indexes(indexes).await
}

/// Validates required fields and returns early with warning if any are missing
#[macro_export]
macro_rules! validate_required_fields {
  ($data:expr, $er1:expr, $operation:expr, $id:expr) => {
    if !$er1.is_empty() {
      warn!(errors = %$er1, "Found errors in {} data", $operation);
      let error_msg = format!("Required fields missing or empty in {}: {}", $operation, $er1);
      update_db_bad(error_msg, $id.clone())
        .await
        .cwl("Failed to update database with validation errors")?;
      return Ok(());
    } else {
      debug!("All required keys present in {} data", $operation);
    }
  };
}

// Generic function to check if a key exists and get its value, or add error to er1 string
pub fn check_key<'a>(
  data: &'a serde_json::Map<String, Value>,
  key: &str,
  er1: &mut String,
) -> Option<&'a Value> {
  let result = data.get(key);

  // Check if key exists and has valid data
  if result.is_none()
    || result.unwrap().is_null()
    || (result.unwrap().is_string()
      && result.unwrap().as_str().unwrap_or("").is_empty())
  {
    // Add error to er1 string
    if !er1.is_empty() {
      er1.push('\n');
    }
    er1.push_str(&format!("Key '{key}' does not exist or is empty"));
    warn!(key = %key, "Key is missing or empty in data");
    None
  } else {
    result
  }
}

// create_elements abstraction moved to morgsgrps.rs as requested

/// Maps subdomain emails to the correct JSON filename for multi-domain organizations
/// Returns the mapped filename if domain is part of a known domain group, None otherwise
pub fn map_subdomain_to_jsonf(email_domain: &str) -> Option<String> {
  // UNAM domain group - all UNAM subdomains map to dominiosunammx
  const UNAM_DOMAINS: &[&str] = &[
    "unam.edu",
    "adobes.unam.mx",
    "agua.unam.mx",
    "amc.unam.mx",
    "centrodedatos.unam.mx",
    "chopo.unam.mx",
    "cic.unam.mx",
    "cipps.unam.mx",
    "cocuac.org.mx",
    "codeic.unam.mx",
    "consorciounamtec.mx",
    "cripu.unam.mx",
    "cultura.unam.mx",
    "dgapa.unam.mx",
    "dgapsu.unam.mx",
    "dgelu.unam.mx",
    "dgsgm.unam.mx",
    "docente.fca.unam.mx",
    "encit.unam.mx",
    "enes.unam.mx",
    "eneso.unam.mx",
    "ents2.unam.mx",
    "fad.unam.mx",
    "fic.unam.mx",
    "geociencias.unam.mx",
    "historicas.unam.mx",
    "iainq.org.mx",
    "ipv6.unam.mx",
    "ipv6forum.mx",
    "juntadegobierno.unam.mx",
    "materiales.unam.mx",
    "monuunam.unam.mx",
    "musica.unam.mx",
    "noc.unam.mx",
    "pceim.unam.mx",
    "pincc.unam.mx",
    "pudh.unam.mx",
    "puedjs.unam.mx",
    "quimica.unam.mx",
    "redes.unam.mx",
    "sg.unam.mx",
    "ssacu.unam.mx",
    "super.unam.mx",
    "teatro.unam.mx",
    "tequila.tic.unam.mx",
    "ti.unam.mx",
    "torreingenieria.unam.mx",
    "vnoc.unam.mx",
  ];

  // Check UNAM domains first
  if UNAM_DOMAINS.contains(&email_domain) {
    return Some("dominiosunammx".to_string());
  }

  // Example: Future university domain group
  // const EXAMPLE_UNIVERSITY_DOMAINS: &[&str] = &[
  //   "example.edu", "subdomain1.example.edu", "subdomain2.example.edu"
  // ];
  // if EXAMPLE_UNIVERSITY_DOMAINS.contains(&email_domain) {
  //   return Some("exampleunivmx".to_string());
  // }

  // Add more domain groups here as needed in the future

  None
}

pub async fn getbytes(fil: &String) -> AppResult<String> {
  let file = tfs::read(fil).await.unwrap();
  let base64en = URL_SAFE_NO_PAD.encode(file);

  let bn = base64en
    .replace('/', "_")
    .replace('+', "-")
    .replace('=', "*")
    .replace('=', ".");

  Ok(bn)
}
