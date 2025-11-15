use crate::{debug, info};
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



pub struct GlobalDriveRateLimiter {
  tokens: Arc<Mutex<TokenBucket>>,
}

#[derive(Debug)]
struct TokenBucket {
  tokens: f64,
  capacity: f64,
  refill_rate: f64,
  last_refill: Instant,
}

impl GlobalDriveRateLimiter {
  pub fn new() -> Self {
    info!(
      "Creating global Drive and Calendar API rate limiter: 166 requests/second"
    );
    Self {
      tokens: Arc::new(Mutex::new(TokenBucket {
        tokens: 166.0,
        capacity: 166.0,
        refill_rate: 166.0,
        last_refill: Instant::now(),
      })),
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
        debug!("Drive API token acquired, remaining: {:.2}", bucket.tokens);
        break;
      } else {
        debug!("Drive API rate limited, waiting...");
        drop(bucket);
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
    }
  }
}

lazy_static::lazy_static! {
    pub static ref GLOBAL_DRIVE_LIMITER: GlobalDriveRateLimiter = GlobalDriveRateLimiter::new();
}

pub fn get_global_drive_limiter() -> &'static GlobalDriveRateLimiter {
  &GLOBAL_DRIVE_LIMITER
}
