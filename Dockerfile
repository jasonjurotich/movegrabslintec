FROM rust:1.91-bookworm AS builder

RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  pkg-config \
  libssl-dev \
  clang \
  tzdata \
  && rm -rf /var/lib/apt/lists/* \
  && apt-get clean

WORKDIR /surrealdbadminbot

# either use just this one line COPY . . or if you want to have a certain cache to reduce biuld times later, comment it out and use what is below it that is presently commented out
COPY . .

# COPY Cargo.toml Cargo.lock ./
# RUN mkdir src && echo "fn main() {}" > src/main.rs
# RUN cargo build --release
# RUN rm -rf src
# COPY src ./src

ENV RUSTFLAGS="-C link-arg=-s"
RUN cargo build --release --target x86_64-unknown-linux-gnu

FROM gcr.io/distroless/cc-debian12 AS runtime
WORKDIR /surrealdbadminbot

COPY --from=builder /surrealdbadminbot/target/x86_64-unknown-linux-gnu/release/surrealdbadminbot /usr/local/bin/surrealdbadminbot
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
# COPY --from=builder /etc/passwd /etc/passwd
# COPY --from=builder /etc/group /etc/group

USER 65534:65534
ENV PORT=8080
EXPOSE 8080

CMD ["/usr/local/bin/surrealdbadminbot"]


