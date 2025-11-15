use crate::{debug, info};
use governor::clock::DefaultClock;
use governor::state::InMemoryState;
use governor::state::NotKeyed;
use governor::{Quota, RateLimiter};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};

pub fn create_rate_limiter(
  qps: usize,
) -> Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>> {
  let qps_value = std::num::NonZeroU32::new(qps.max(1).try_into().unwrap_or(1))
    .unwrap_or(std::num::NonZeroU32::new(1).unwrap());

  Arc::new(RateLimiter::direct(Quota::per_second(qps_value)))
}

#[derive(Debug)]
struct TokenBucket {
  tokens: f64,
  capacity: f64,
  refill_rate: f64,
  last_refill: Instant,
}

pub struct GlobalRateLimiter {
  tokens: Arc<Mutex<TokenBucket>>,
  api_name: String,
  sleep_duration: Duration,
}

impl GlobalRateLimiter {
  pub fn new(
    api_name: String,
    initial_tokens: f64,
    capacity: f64,
    refill_rate: f64,
    sleep_duration_millis: u64,
  ) -> Self {
    info!(
      "Creating global {} API rate limiter: {} requests/second",
      api_name, refill_rate
    );
    Self {
      tokens: Arc::new(Mutex::new(TokenBucket {
        tokens: initial_tokens,
        capacity,
        refill_rate,
        last_refill: Instant::now(),
      })),
      api_name,
      sleep_duration: Duration::from_millis(sleep_duration_millis),
    }
  }

  pub async fn until_ready(&self) {
    loop {
      let mut bucket = self.tokens.lock().await;

      let now = Instant::now();
      let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
      bucket.tokens =
        (bucket.tokens + elapsed * bucket.refill_rate).min(bucket.capacity);
      bucket.last_refill = now;

      if bucket.tokens >= 1.0 {
        bucket.tokens -= 1.0;
        debug!(
          "{} API token acquired, remaining: {:.2}",
          self.api_name, bucket.tokens
        );
        break;
      } else {
        if self.api_name == "Drive" {
          debug!("Drive API rate limited, waiting...");
        }
        drop(bucket);
        tokio::time::sleep(self.sleep_duration).await;
      }
    }
  }
}

lazy_static::lazy_static! {
  pub static ref GLOBAL_DRIVE_LIMITER: GlobalRateLimiter = GlobalRateLimiter::new(
    "Drive".to_string(), 166.0, 166.0, 166.0, 50
  );
  pub static ref GLOBAL_OAUTH_LIMITER: GlobalRateLimiter = GlobalRateLimiter::new(
    "OAuth".to_string(), 50.0, 200.0, 200.0, 50
  );
  pub static ref GLOBAL_DELETE_LIMITER: GlobalRateLimiter = GlobalRateLimiter::new(
    "Delete".to_string(), 1.0, 1.0, 1.0, 1000
  );
  pub static ref GLOBAL_EMAIL_DELETE_LIMITER: GlobalRateLimiter = GlobalRateLimiter::new(
    "Email_Delete".to_string(), 4.0, 4.0, 4.0, 500
  );
  pub static ref GLOBAL_CLASSROOM_LIMITER: GlobalRateLimiter = GlobalRateLimiter::new(
    "Classroom".to_string(), 7.0, 7.0, 7.0, 143
  );

  // Global limiters for sub-operations to prevent rate multiplication in concurrent operations
  pub static ref GLOBAL_GROUPS_LIST_LIMITER: GlobalRateLimiter = GlobalRateLimiter::new(
    "Groups_List".to_string(), 22.0, 22.0, 22.0, 50
  );
  pub static ref GLOBAL_GROUPS_DELETE_LIMITER: GlobalRateLimiter = GlobalRateLimiter::new(
    "Groups_Delete".to_string(), 5.0, 5.0, 5.0, 200
  );
  pub static ref GLOBAL_COURSES_LIST_LIMITER: GlobalRateLimiter = GlobalRateLimiter::new(
    "Courses_List".to_string(), 40.0, 40.0, 40.0, 50
  );
  pub static ref GLOBAL_COURSES_DELETE_LIMITER: GlobalRateLimiter = GlobalRateLimiter::new(
    "Courses_Delete".to_string(), 7.0, 7.0, 7.0, 150
  );
  pub static ref GLOBAL_LICENSING_LIMITER: GlobalRateLimiter = GlobalRateLimiter::new(
    "Licensing".to_string(), 1.0, 1.0, 1.0, 625
  );
  pub static ref GLOBAL_SHEETS_LIMITER: GlobalRateLimiter = GlobalRateLimiter::new(
    "Sheets".to_string(), 1.0, 1.0, 1.0, 500
  );
}

pub fn get_global_drive_limiter() -> &'static GlobalRateLimiter {
  &GLOBAL_DRIVE_LIMITER
}

pub fn get_global_oauth_limiter() -> &'static GlobalRateLimiter {
  &GLOBAL_OAUTH_LIMITER
}

pub fn get_global_delete_limiter() -> &'static GlobalRateLimiter {
  &GLOBAL_DELETE_LIMITER
}

pub fn get_global_email_delete_limiter() -> &'static GlobalRateLimiter {
  &GLOBAL_EMAIL_DELETE_LIMITER
}

pub fn get_global_classroom_limiter() -> &'static GlobalRateLimiter {
  &GLOBAL_CLASSROOM_LIMITER
}

pub fn get_global_groups_list_limiter() -> &'static GlobalRateLimiter {
  &GLOBAL_GROUPS_LIST_LIMITER
}

pub fn get_global_groups_delete_limiter() -> &'static GlobalRateLimiter {
  &GLOBAL_GROUPS_DELETE_LIMITER
}

// pub fn get_global_courses_list_limiter() -> &'static GlobalRateLimiter {
//   &GLOBAL_COURSES_LIST_LIMITER
// }

// pub fn get_global_courses_delete_limiter() -> &'static GlobalRateLimiter {
//   &GLOBAL_COURSES_DELETE_LIMITER
// }

pub fn get_global_licensing_limiter() -> &'static GlobalRateLimiter {
  &GLOBAL_LICENSING_LIMITER
}

pub fn get_global_sheets_limiter() -> &'static GlobalRateLimiter {
  &GLOBAL_SHEETS_LIMITER
}
