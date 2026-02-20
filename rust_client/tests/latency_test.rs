// Copyright 2024-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

//! Stress and latency tests for the Rust subspace client.
//!
//! These tests measure publish/subscribe throughput and latency under
//! various conditions: reliable vs unreliable, single-threaded vs
//! multi-threaded, varying slot counts, and with checksums.

#[cfg(not(server_ffi))]
use std::process::{Child, Command, Stdio};

use subspace_client::options::{PublisherOptions, SubscriberOptions};
use subspace_client::{Client, ReadMode};

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;

// ── FFI server bindings (same as client_test.rs) ─────────────────────────────

#[cfg(server_ffi)]
extern "C" {
    fn subspace_server_create(
        socket_name: *const std::ffi::c_char,
        notify_fd: std::ffi::c_int,
    ) -> *mut std::ffi::c_void;
    fn subspace_server_run(handle: *mut std::ffi::c_void) -> std::ffi::c_int;
    fn subspace_server_stop(handle: *mut std::ffi::c_void);
    fn subspace_server_cleanup_after_session(handle: *mut std::ffi::c_void);
    fn subspace_server_destroy(handle: *mut std::ffi::c_void);
}

#[cfg(server_ffi)]
struct RawServerHandle(*mut std::ffi::c_void);
#[cfg(server_ffi)]
unsafe impl Send for RawServerHandle {}

#[cfg(server_ffi)]
struct ServerGuard {
    handle: *mut std::ffi::c_void,
    socket_path: String,
    thread: Option<std::thread::JoinHandle<()>>,
    notify_read_fd: std::ffi::c_int,
}

#[cfg(server_ffi)]
unsafe impl Send for ServerGuard {}
#[cfg(server_ffi)]
unsafe impl Sync for ServerGuard {}

#[cfg(server_ffi)]
impl ServerGuard {
    fn start() -> Self {
        let tmp = std::env::temp_dir().join(format!(
            "subspace_rust_latency_{}",
            std::process::id()
        ));
        let socket_path = tmp.to_str().unwrap().to_string();

        let mut pipe_fds = [0 as libc::c_int; 2];
        assert_eq!(unsafe { libc::pipe(pipe_fds.as_mut_ptr()) }, 0);
        let read_fd = pipe_fds[0];
        let write_fd = pipe_fds[1];

        let c_socket = std::ffi::CString::new(socket_path.clone()).unwrap();
        let handle = unsafe { subspace_server_create(c_socket.as_ptr(), write_fd) };
        assert!(!handle.is_null(), "subspace_server_create returned null");

        let raw = RawServerHandle(handle);
        let thread = std::thread::spawn(move || {
            let raw = raw;
            let ret = unsafe { subspace_server_run(raw.0) };
            if ret != 0 {
                eprintln!("ERROR: subspace_server_run returned {}", ret);
            }
        });

        let mut pollfd = libc::pollfd {
            fd: read_fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let poll_ret = unsafe { libc::poll(&mut pollfd, 1, 10_000) };
        if poll_ret <= 0 {
            panic!(
                "Server did not become ready within 10s (poll returned {}, errno={})",
                poll_ret,
                std::io::Error::last_os_error()
            );
        }
        let mut buf = [0u8; 8];
        let n =
            unsafe { libc::read(read_fd, buf.as_mut_ptr() as *mut libc::c_void, 8) };
        assert_eq!(n, 8, "failed to read server ready notification");

        ServerGuard {
            handle,
            socket_path,
            thread: Some(thread),
            notify_read_fd: read_fd,
        }
    }

    fn socket(&self) -> &str {
        &self.socket_path
    }
}

#[cfg(server_ffi)]
impl Drop for ServerGuard {
    fn drop(&mut self) {
        unsafe { subspace_server_stop(self.handle) };
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
        let mut buf = [0u8; 8];
        unsafe {
            libc::read(
                self.notify_read_fd,
                buf.as_mut_ptr() as *mut libc::c_void,
                8,
            );
            libc::close(self.notify_read_fd);
            subspace_server_cleanup_after_session(self.handle);
            subspace_server_destroy(self.handle);
        }
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

// ── Subprocess fallback ──────────────────────────────────────────────────────

#[cfg(not(server_ffi))]
fn find_server_binary() -> String {
    let manifest_dir =
        std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let workspace = std::path::Path::new(&manifest_dir)
        .parent()
        .unwrap_or(std::path::Path::new("."));

    let candidates = [
        workspace.join("bazel-bin/server/subspace_server"),
        workspace.join("server/subspace_server"),
        std::path::PathBuf::from("../bazel-bin/server/subspace_server"),
    ];
    for c in &candidates {
        if c.exists() {
            return c.to_string_lossy().to_string();
        }
    }
    "subspace_server".to_string()
}

#[cfg(not(server_ffi))]
struct ServerGuard {
    child: Child,
    socket_path: String,
}

#[cfg(not(server_ffi))]
impl ServerGuard {
    fn start() -> Self {
        let tmp = std::env::temp_dir().join(format!(
            "subspace_rust_latency_{}",
            std::process::id()
        ));
        let socket_path = tmp.to_str().unwrap().to_string();
        let binary = find_server_binary();
        let child = Command::new(&binary)
            .arg(format!("--socket={}", socket_path))
            .arg("--local")
            .arg("--cleanup_filesystem=false")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("Failed to start server '{}': {}", binary, e));

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if std::time::Instant::now() > deadline {
                panic!("Server did not become ready within 10 seconds");
            }
            match Client::new(&socket_path, "readiness_probe") {
                Ok(_) => break,
                Err(_) => std::thread::sleep(std::time::Duration::from_millis(50)),
            }
        }
        ServerGuard { child, socket_path }
    }

    fn socket(&self) -> &str {
        &self.socket_path
    }
}

#[cfg(not(server_ffi))]
impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

// ── Common infrastructure ────────────────────────────────────────────────────

static SERVER: std::sync::OnceLock<ServerGuard> = std::sync::OnceLock::new();

fn server_socket() -> &'static str {
    SERVER.get_or_init(ServerGuard::start).socket()
}

fn new_client(name: &str) -> Client {
    Client::new(server_socket(), name).expect("Failed to connect to server")
}

fn now_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn show_histogram(label: &str, latencies: &mut [u64]) {
    if latencies.is_empty() {
        eprintln!("{label}: no samples");
        return;
    }
    latencies.sort_unstable();
    let n = latencies.len();
    let sum: u64 = latencies.iter().sum();
    eprintln!(
        "{label}: min={} median={} p99={} max={} avg={} ns  (n={n})",
        latencies[0],
        latencies[n / 2],
        latencies[n * 99 / 100],
        latencies[n - 1],
        sum / n as u64,
    );
}

// ── Stress: multithreaded unreliable single channel ──────────────────────────

/// Blast messages through a single unreliable channel from a publisher thread
/// to a subscriber thread.  Verifies ordinal ordering and counts drops.
#[test]
fn stress_multithreaded_unreliable() {
    const NUM_MESSAGES: i64 = 100_000;

    let pub_client = new_client("stress_unrel_p");
    let sub_client = new_client("stress_unrel_s");

    let pub_opts = PublisherOptions::new()
        .set_slot_size(256)
        .set_num_slots(10);
    let publisher = pub_client
        .create_publisher("stress_unrel", &pub_opts)
        .unwrap();

    let sub_opts = SubscriberOptions::new().set_log_dropped_messages(false);
    let subscriber = sub_client
        .create_subscriber("stress_unrel", &sub_opts)
        .unwrap();

    let done = Arc::new(AtomicBool::new(false));
    let num_dropped = Arc::new(AtomicI64::new(0));
    let num_received = Arc::new(AtomicI64::new(0));

    // Subscriber thread.
    let sub_done = done.clone();
    let sub_dropped = num_dropped.clone();
    let sub_received = num_received.clone();
    let sub_thread = std::thread::spawn(move || {
        let mut last_ordinal: u64 = 0;
        subscriber.wait(Some(5000)).ok();
        while !sub_done.load(Ordering::Relaxed) {
            let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
            if msg.length > 0 {
                if last_ordinal > 0 && msg.ordinal > last_ordinal + 1 {
                    sub_dropped.fetch_add(
                        (msg.ordinal - last_ordinal - 1) as i64,
                        Ordering::Relaxed,
                    );
                }
                last_ordinal = msg.ordinal;
                sub_received.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    // Publisher thread.
    let pub_thread = std::thread::spawn(move || {
        for i in 0..NUM_MESSAGES {
            let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
            let payload = format!("msg {i}");
            unsafe {
                std::ptr::copy_nonoverlapping(
                    payload.as_ptr(),
                    buf,
                    payload.len(),
                );
            }
            publisher.publish_message(payload.len() as i64).unwrap();
        }
    });

    pub_thread.join().unwrap();

    // Give subscriber a moment to drain, then signal stop.
    std::thread::sleep(std::time::Duration::from_millis(100));
    done.store(true, Ordering::Relaxed);
    sub_thread.join().unwrap();

    let received = num_received.load(Ordering::Relaxed);
    let dropped = num_dropped.load(Ordering::Relaxed);
    eprintln!(
        "stress_multithreaded_unreliable: sent={NUM_MESSAGES} received={received} dropped={dropped}"
    );
    assert!(received > 0, "Should have received at least some messages");
}

// ── Stress: multithreaded reliable single channel ────────────────────────────

/// Reliable pub/sub: publisher blocks when out of slots, subscriber processes
/// everything.  Verifies zero drops and correct ordinal ordering.
#[test]
fn stress_multithreaded_reliable() {
    const NUM_MESSAGES: i64 = 50_000;

    let pub_client = new_client("stress_rel_p");
    let sub_client = new_client("stress_rel_s");

    let pub_opts = PublisherOptions::new()
        .set_slot_size(256)
        .set_num_slots(25)
        .set_reliable(true);
    let publisher = pub_client
        .create_publisher("stress_rel", &pub_opts)
        .unwrap();

    let sub_opts = SubscriberOptions::new().set_reliable(true);
    let subscriber = sub_client
        .create_subscriber("stress_rel", &sub_opts)
        .unwrap();

    let start = Instant::now();

    // Subscriber thread.
    let sub_thread = std::thread::spawn(move || {
        let mut last_ordinal: u64 = 0;
        let mut received: i64 = 0;
        subscriber.wait(Some(5000)).ok();
        while received < NUM_MESSAGES {
            let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
            if msg.length > 0 {
                if last_ordinal > 0 {
                    assert_eq!(
                        msg.ordinal,
                        last_ordinal + 1,
                        "Reliable ordinal gap: got {} expected {}",
                        msg.ordinal,
                        last_ordinal + 1
                    );
                }
                last_ordinal = msg.ordinal;
                received += 1;
            } else {
                subscriber.wait(Some(1000)).ok();
            }
        }
    });

    // Publisher thread.
    let pub_thread = std::thread::spawn(move || {
        let mut sent: i64 = 0;
        while sent < NUM_MESSAGES {
            let result = publisher.get_message_buffer(256).unwrap();
            if result.is_none() {
                publisher.wait(Some(1000)).unwrap();
                continue;
            }
            let (buf, _) = result.unwrap();
            let payload = format!("rel {sent}");
            unsafe {
                std::ptr::copy_nonoverlapping(
                    payload.as_ptr(),
                    buf,
                    payload.len(),
                );
            }
            publisher.publish_message(payload.len() as i64).unwrap();
            sent += 1;
        }
    });

    pub_thread.join().unwrap();
    sub_thread.join().unwrap();

    let elapsed = start.elapsed();
    let avg_ns = elapsed.as_nanos() as u64 / NUM_MESSAGES as u64;
    eprintln!(
        "stress_multithreaded_reliable: {NUM_MESSAGES} messages in {:.2}ms, avg {avg_ns} ns/msg",
        elapsed.as_secs_f64() * 1000.0,
    );
}

// ── Latency: reliable pub/sub round-trip ─────────────────────────────────────

/// Measures end-to-end reliable latency: publisher embeds a timestamp in each
/// message; subscriber computes the delta.
#[test]
fn latency_reliable_round_trip() {
    const NUM_MESSAGES: i64 = 50_000;

    let pub_client = new_client("lat_rel_p");
    let sub_client = new_client("lat_rel_s");

    let pub_opts = PublisherOptions::new()
        .set_slot_size(256)
        .set_num_slots(10)
        .set_reliable(true);
    let publisher = pub_client
        .create_publisher("lat_rel_rt", &pub_opts)
        .unwrap();

    let sub_opts = SubscriberOptions::new().set_reliable(true);
    let subscriber = sub_client
        .create_subscriber("lat_rel_rt", &sub_opts)
        .unwrap();

    // Collect latencies in the subscriber thread.
    let sub_thread = std::thread::spawn(move || {
        let mut latencies = Vec::with_capacity(NUM_MESSAGES as usize);
        let mut received: i64 = 0;
        subscriber.wait(Some(5000)).ok();
        while received < NUM_MESSAGES {
            let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
            if msg.length > 0 {
                let recv_time = now_ns();
                let send_time = unsafe { *(msg.buffer as *const u64) };
                latencies.push(recv_time.saturating_sub(send_time));
                received += 1;
            } else {
                subscriber.wait(Some(1000)).ok();
            }
        }
        latencies
    });

    // Publisher thread embeds send timestamp in every message.
    let pub_thread = std::thread::spawn(move || {
        let mut sent: i64 = 0;
        while sent < NUM_MESSAGES {
            let result = publisher.get_message_buffer(256).unwrap();
            if result.is_none() {
                publisher.wait(Some(1000)).unwrap();
                continue;
            }
            let (buf, _) = result.unwrap();
            let send_time = now_ns();
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &send_time as *const u64 as *const u8,
                    buf,
                    std::mem::size_of::<u64>(),
                );
            }
            publisher
                .publish_message(std::mem::size_of::<u64>() as i64)
                .unwrap();
            sent += 1;
        }
    });

    pub_thread.join().unwrap();
    let mut latencies = sub_thread.join().unwrap();

    show_histogram("latency_reliable_round_trip", &mut latencies);
    assert!(!latencies.is_empty());
}

// ── Latency: unreliable pub/sub round-trip ───────────────────────────────────

/// Measures unreliable end-to-end latency.  The publisher sends as fast as it
/// can; the subscriber measures timestamp deltas.  Drops are expected.
#[test]
fn latency_unreliable_round_trip() {
    const NUM_MESSAGES: i64 = 200_000;

    let pub_client = new_client("lat_unrel_p");
    let sub_client = new_client("lat_unrel_s");

    let pub_opts = PublisherOptions::new()
        .set_slot_size(256)
        .set_num_slots(100);
    let publisher = pub_client
        .create_publisher("lat_unrel_rt", &pub_opts)
        .unwrap();

    let sub_opts = SubscriberOptions::new().set_log_dropped_messages(false);
    let subscriber = sub_client
        .create_subscriber("lat_unrel_rt", &sub_opts)
        .unwrap();

    let total_dropped = Arc::new(AtomicI64::new(0));

    let sub_dropped = total_dropped.clone();
    let sub_thread = std::thread::spawn(move || {
        let mut latencies = Vec::with_capacity(NUM_MESSAGES as usize);
        let mut last_ordinal: u64 = 0;
        let mut received: i64 = 0;
        subscriber.wait(Some(5000)).ok();
        while received + sub_dropped.load(Ordering::Relaxed) < NUM_MESSAGES {
            let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
            if msg.length >= std::mem::size_of::<u64>() {
                let recv_time = now_ns();
                if last_ordinal > 0 && msg.ordinal > last_ordinal + 1 {
                    sub_dropped.fetch_add(
                        (msg.ordinal - last_ordinal - 1) as i64,
                        Ordering::Relaxed,
                    );
                }
                last_ordinal = msg.ordinal;
                let send_time = unsafe { *(msg.buffer as *const u64) };
                latencies.push(recv_time.saturating_sub(send_time));
                received += 1;
            }
        }
        latencies
    });

    let pub_thread = std::thread::spawn(move || {
        for _ in 0..NUM_MESSAGES {
            let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
            let send_time = now_ns();
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &send_time as *const u64 as *const u8,
                    buf,
                    std::mem::size_of::<u64>(),
                );
            }
            publisher
                .publish_message(std::mem::size_of::<u64>() as i64)
                .unwrap();
        }
    });

    pub_thread.join().unwrap();
    // Flush extra messages to unstick subscriber if it missed the tail.
    let flush_pub = new_client("lat_unrel_flush_p");
    let flush_publisher = flush_pub
        .create_publisher(
            "lat_unrel_rt",
            &PublisherOptions::new().set_slot_size(256).set_num_slots(100),
        )
        .unwrap();
    for _ in 0..200 {
        let (buf, _) = flush_publisher
            .get_message_buffer(256)
            .unwrap()
            .unwrap();
        let t = now_ns();
        unsafe {
            std::ptr::copy_nonoverlapping(
                &t as *const u64 as *const u8,
                buf,
                std::mem::size_of::<u64>(),
            );
        }
        flush_publisher
            .publish_message(std::mem::size_of::<u64>() as i64)
            .unwrap();
    }

    let mut latencies = sub_thread.join().unwrap();
    let dropped = total_dropped.load(Ordering::Relaxed);
    eprintln!(
        "latency_unreliable_round_trip: received={} dropped={dropped}",
        latencies.len()
    );
    show_histogram("latency_unreliable_round_trip", &mut latencies);
    assert!(!latencies.is_empty());
}

// ── Latency: publish-only (no subscriber backpressure) ───────────────────────

/// Measures how fast the publisher can fire messages while a subscriber
/// immediately consumes them (single-threaded, with retired slots).
/// Then fills the channel and measures publish latency with full channel
/// (publisher recycling old slots).
#[test]
fn latency_publisher_with_retirement() {
    const NUM_MESSAGES: usize = 20_000;

    let pub_client = new_client("lat_pub_ret_p");
    let sub_client = new_client("lat_pub_ret_s");

    eprintln!("num_slots,retired_avg_ns,full_avg_ns");

    let mut num_slots = 10;
    while num_slots < 1000 {
        let pub_opts = PublisherOptions::new()
            .set_slot_size(256)
            .set_num_slots(num_slots);
        let publisher = pub_client
            .create_publisher("lat_pub_ret", &pub_opts)
            .unwrap();

        let sub_opts = SubscriberOptions::new().set_log_dropped_messages(false);
        let subscriber = sub_client
            .create_subscriber("lat_pub_ret", &sub_opts)
            .unwrap();

        // Phase 1: publish+read in lockstep (slots always retired).
        let mut total_ns: u64 = 0;
        for _ in 0..NUM_MESSAGES {
            let t0 = Instant::now();
            let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
            unsafe { *buf = 0xAA };
            publisher.publish_message(100).unwrap();
            total_ns += t0.elapsed().as_nanos() as u64;

            let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
            assert_eq!(msg.length, 100);
        }
        let retired_avg = total_ns / NUM_MESSAGES as u64;

        // Phase 2: fill the channel completely.
        for _ in 0..num_slots {
            let result = publisher.get_message_buffer(256).unwrap();
            if result.is_none() {
                break;
            }
            publisher.publish_message(100).unwrap();
        }

        // Phase 3: publish into a full channel (forces recycling).
        total_ns = 0;
        for _ in 0..NUM_MESSAGES {
            let t0 = Instant::now();
            let (_, _) = publisher.get_message_buffer(256).unwrap().unwrap();
            publisher.publish_message(100).unwrap();
            total_ns += t0.elapsed().as_nanos() as u64;
        }
        let full_avg = total_ns / NUM_MESSAGES as u64;

        eprintln!("{num_slots},{retired_avg},{full_avg}");

        num_slots = num_slots * 3 / 2;
    }
}

// ── Latency: publisher with checksum ─────────────────────────────────────────

/// Same as publisher retirement latency but with checksums enabled on both
/// publisher and subscriber.
#[test]
fn latency_publisher_checksum() {
    const NUM_MESSAGES: usize = 20_000;

    let pub_client = new_client("lat_pub_csum_p");
    let sub_client = new_client("lat_pub_csum_s");

    let pub_opts = PublisherOptions::new()
        .set_slot_size(256)
        .set_num_slots(100)
        .set_checksum(true);
    let publisher = pub_client
        .create_publisher("lat_pub_csum", &pub_opts)
        .unwrap();

    let sub_opts = SubscriberOptions::new()
        .set_checksum(true)
        .set_log_dropped_messages(false);
    let subscriber = sub_client
        .create_subscriber("lat_pub_csum", &sub_opts)
        .unwrap();

    let mut latencies = Vec::with_capacity(NUM_MESSAGES);

    for _ in 0..NUM_MESSAGES {
        let t0 = Instant::now();
        let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
        unsafe { *buf = 0xBB };
        publisher.publish_message(100).unwrap();
        latencies.push(t0.elapsed().as_nanos() as u64);

        let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
        assert_eq!(msg.length, 100);
        assert!(!msg.checksum_error);
    }

    show_histogram("latency_publisher_checksum", &mut latencies);
}

// ── Latency: pub+sub round-trip, single thread ──────────────────────────────

/// Measures combined publish+read latency in a tight single-threaded loop
/// (no contention, no waiting).
#[test]
fn latency_pub_sub_single_thread() {
    const MAX_MESSAGES: usize = 10_000;

    let pub_client = new_client("lat_ps_st_p");
    let sub_client = new_client("lat_ps_st_s");

    eprintln!("num_messages,avg_latency_ns");

    let mut num_messages = 100;
    while num_messages <= MAX_MESSAGES {
        let pub_opts = PublisherOptions::new()
            .set_slot_size(256)
            .set_num_slots(10);
        let publisher = pub_client
            .create_publisher("lat_ps_st", &pub_opts)
            .unwrap();

        let sub_opts = SubscriberOptions::new().set_log_dropped_messages(false);
        let subscriber = sub_client
            .create_subscriber("lat_ps_st", &sub_opts)
            .unwrap();

        let start = Instant::now();
        for _ in 0..num_messages {
            let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
            unsafe { *buf = 0xCC };
            publisher.publish_message(100).unwrap();

            let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
            assert_eq!(msg.length, 100);
        }
        let elapsed = start.elapsed();
        let avg = elapsed.as_nanos() as u64 / num_messages as u64;
        eprintln!("{num_messages},{avg}");

        num_messages += 100;
    }
}

// ── Latency: subscriber drain ────────────────────────────────────────────────

/// Publishes many messages into a channel, then measures how fast the
/// subscriber can drain them all.
#[test]
fn latency_subscriber_drain() {
    let pub_client = new_client("lat_sub_drain_p");
    let sub_client = new_client("lat_sub_drain_s");

    eprintln!("num_slots,avg_read_ns");

    let mut num_slots = 100;
    while num_slots <= 5000 {
        let pub_opts = PublisherOptions::new()
            .set_slot_size(256)
            .set_num_slots(num_slots);
        let publisher = pub_client
            .create_publisher("lat_sub_drain", &pub_opts)
            .unwrap();

        let sub_opts = SubscriberOptions::new().set_log_dropped_messages(false);
        let subscriber = sub_client
            .create_subscriber("lat_sub_drain", &sub_opts)
            .unwrap();

        // Fill channel.
        let published = num_slots - 1; // one slot held by publisher as current
        for _ in 0..published {
            let (_, _) = publisher.get_message_buffer(256).unwrap().unwrap();
            publisher.publish_message(100).unwrap();
        }

        // Drain and measure.
        let start = Instant::now();
        for _ in 0..published {
            let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
            assert_eq!(msg.length, 100);
        }
        let elapsed = start.elapsed();
        let avg = elapsed.as_nanos() as u64 / published as u64;
        eprintln!("{num_slots},{avg}");

        num_slots += 500;
    }
}

// ── Latency: reliable latency histogram across slot counts ───────────────────

/// Runs reliable pub/sub across different slot counts to show how latency
/// scales with the number of slots.
#[test]
fn latency_reliable_histogram_by_slots() {
    const NUM_MESSAGES: i64 = 20_000;

    let pub_client = new_client("lat_rel_hist_p");
    let sub_client = new_client("lat_rel_hist_s");

    let mut results: Vec<(i32, u64)> = Vec::new();

    let mut num_slots = 3;
    while num_slots < 1000 {
        let pub_opts = PublisherOptions::new()
            .set_slot_size(256)
            .set_num_slots(num_slots)
            .set_reliable(true);
        let publisher = pub_client
            .create_publisher("lat_rel_hist", &pub_opts)
            .unwrap();

        let sub_opts = SubscriberOptions::new().set_reliable(true);
        let subscriber = sub_client
            .create_subscriber("lat_rel_hist", &sub_opts)
            .unwrap();

        let start = Instant::now();

        let sub_thread = std::thread::spawn(move || {
            let mut received: i64 = 0;
            subscriber.wait(Some(5000)).ok();
            while received < NUM_MESSAGES {
                let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
                if msg.length > 0 {
                    received += 1;
                } else {
                    subscriber.wait(Some(1000)).ok();
                }
            }
        });

        let pub_thread = std::thread::spawn(move || {
            let mut sent: i64 = 0;
            while sent < NUM_MESSAGES {
                let result = publisher.get_message_buffer(256).unwrap();
                if result.is_none() {
                    publisher.wait(Some(1000)).unwrap();
                    continue;
                }
                publisher.publish_message(1).unwrap();
                sent += 1;
            }
        });

        pub_thread.join().unwrap();
        sub_thread.join().unwrap();

        let elapsed = start.elapsed();
        let avg_ns = elapsed.as_nanos() as u64 / NUM_MESSAGES as u64;
        results.push((num_slots, avg_ns));

        num_slots *= 2;
    }

    eprintln!("slots,avg_ns");
    let mut prev: u64 = 0;
    for (slots, avg) in &results {
        let scale = if prev == 0 {
            0.0
        } else {
            *avg as f64 / prev as f64
        };
        eprintln!("{slots}: {avg} ns  (scale: {scale:.2}x)");
        prev = *avg;
    }
}

// ── Stress: publisher with multiple subscribers ──────────────────────────────

/// Single publisher, multiple subscribers all reading in lockstep.
/// Measures publish latency scaling with subscriber count.
#[test]
fn latency_publisher_multi_subscriber() {
    const NUM_MESSAGES: usize = 10_000;
    const NUM_SLOTS: i32 = 100;

    let pub_client = new_client("lat_pub_msub_p");
    let sub_client = new_client("lat_pub_msub_s");

    eprintln!("num_subs,avg_pub_ns");

    for num_subs in [1, 2, 4, 8] {
        let pub_opts = PublisherOptions::new()
            .set_slot_size(256)
            .set_num_slots(NUM_SLOTS);
        let publisher = pub_client
            .create_publisher("lat_pub_msub", &pub_opts)
            .unwrap();

        let mut subscribers = Vec::new();
        for _ in 0..num_subs {
            let sub_opts = SubscriberOptions::new().set_log_dropped_messages(false);
            subscribers.push(
                sub_client
                    .create_subscriber("lat_pub_msub", &sub_opts)
                    .unwrap(),
            );
        }

        let mut total_ns: u64 = 0;
        for _ in 0..NUM_MESSAGES {
            let t0 = Instant::now();
            let (_, _) = publisher.get_message_buffer(256).unwrap().unwrap();
            publisher.publish_message(100).unwrap();
            total_ns += t0.elapsed().as_nanos() as u64;

            for sub in &subscribers {
                let msg = sub.read_message(ReadMode::ReadNext).unwrap();
                assert_eq!(msg.length, 100);
            }
        }
        let avg = total_ns / NUM_MESSAGES as u64;
        eprintln!("{num_subs},{avg}");
    }
}

// ── Latency histogram: publisher retirement vs full channel ──────────────────

/// Detailed histogram of publish latency — retired (lockstep with subscriber)
/// and full-channel (recycling) — across slot counts.
#[test]
fn latency_publisher_histogram() {
    const NUM_MESSAGES: usize = 20_000;

    let pub_client = new_client("lat_pub_hist_p");
    let sub_client = new_client("lat_pub_hist_s");

    eprintln!("num_slots, phase, min, median, p99, max, avg");

    let mut num_slots = 10;
    while num_slots < 1000 {
        let pub_opts = PublisherOptions::new()
            .set_slot_size(256)
            .set_num_slots(num_slots);
        let publisher = pub_client
            .create_publisher("lat_pub_hist", &pub_opts)
            .unwrap();

        let sub_opts = SubscriberOptions::new().set_log_dropped_messages(false);
        let subscriber = sub_client
            .create_subscriber("lat_pub_hist", &sub_opts)
            .unwrap();

        // Phase 1: retired slots (lockstep).
        let mut latencies = Vec::with_capacity(NUM_MESSAGES);
        for _ in 0..NUM_MESSAGES {
            let t0 = Instant::now();
            let (_, _) = publisher.get_message_buffer(256).unwrap().unwrap();
            publisher.publish_message(100).unwrap();
            latencies.push(t0.elapsed().as_nanos() as u64);

            let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
            assert_eq!(msg.length, 100);
        }
        show_histogram(
            &format!("{num_slots},retired"),
            &mut latencies,
        );

        // Fill channel.
        for _ in 0..num_slots {
            if publisher.get_message_buffer(256).unwrap().is_none() {
                break;
            }
            publisher.publish_message(100).unwrap();
        }

        // Phase 2: full channel (recycling).
        latencies.clear();
        for _ in 0..NUM_MESSAGES {
            let t0 = Instant::now();
            let (_, _) = publisher.get_message_buffer(256).unwrap().unwrap();
            publisher.publish_message(100).unwrap();
            latencies.push(t0.elapsed().as_nanos() as u64);
        }
        show_histogram(
            &format!("{num_slots},full"),
            &mut latencies,
        );

        num_slots = num_slots * 3 / 2;
    }
}
