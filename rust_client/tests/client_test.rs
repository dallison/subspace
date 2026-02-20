// Copyright 2024-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#[cfg(not(server_ffi))]
use std::process::{Child, Command, Stdio};

use subspace_client::bitset::DynamicBitSet;
use subspace_client::channel::{
    aligned, aligned64, build_refs_bit_field, ORDINAL_MASK, ORDINAL_SHIFT, PUB_OWNED,
    RETIRED_REFS_MASK, RETIRED_REFS_SHIFT, VCHAN_ID_MASK, VCHAN_ID_SHIFT,
};
use subspace_client::checksum::{calculate_checksum, subspace_crc32, verify_checksum};
use subspace_client::options::{PublisherOptions, SubscriberOptions};
use subspace_client::{Client, ReadMode, SubspaceError};

// ── Options builder tests ────────────────────────────────────────────────────

#[test]
fn publisher_options_defaults() {
    let opts = PublisherOptions::new();
    assert_eq!(opts.slot_size, 0);
    assert_eq!(opts.num_slots, 0);
    assert!(!opts.local);
    assert!(!opts.reliable);
    assert!(!opts.bridge);
    assert!(!opts.fixed_size);
    assert!(!opts.activate);
    assert!(!opts.checksum);
    assert_eq!(opts.vchan_id, -1);
    assert!(opts.channel_type.is_empty());
    assert!(opts.mux.is_empty());
}

#[test]
fn publisher_options_builder_chain() {
    let opts = PublisherOptions::new()
        .set_slot_size(4096)
        .set_num_slots(16)
        .set_reliable(true)
        .set_local(true)
        .set_fixed_size(true)
        .set_checksum(true)
        .set_type("sensor".into())
        .set_mux("bus0".into())
        .set_vchan_id(3)
        .set_activate(true);

    assert_eq!(opts.slot_size, 4096);
    assert_eq!(opts.num_slots, 16);
    assert!(opts.reliable);
    assert!(opts.local);
    assert!(opts.fixed_size);
    assert!(opts.checksum);
    assert!(opts.activate);
    assert_eq!(opts.channel_type, "sensor");
    assert_eq!(opts.mux, "bus0");
    assert_eq!(opts.vchan_id, 3);
}

#[test]
fn subscriber_options_defaults() {
    let opts = SubscriberOptions::new();
    assert!(!opts.reliable);
    assert!(!opts.bridge);
    assert_eq!(opts.max_active_messages, 1);
    assert!(opts.log_dropped_messages);
    assert!(!opts.pass_activation);
    assert!(!opts.read_write);
    assert!(!opts.checksum);
    assert!(!opts.pass_checksum_errors);
    assert!(!opts.keep_active_message);
    assert_eq!(opts.vchan_id, -1);
}

#[test]
fn subscriber_options_builder_chain() {
    let opts = SubscriberOptions::new()
        .set_reliable(true)
        .set_max_active_messages(8)
        .set_log_dropped_messages(false)
        .set_pass_activation(true)
        .set_checksum(true)
        .set_pass_checksum_errors(true)
        .set_keep_active_message(true)
        .set_vchan_id(7)
        .set_type("image".into());

    assert!(opts.reliable);
    assert_eq!(opts.max_active_messages, 8);
    assert!(!opts.log_dropped_messages);
    assert!(opts.pass_activation);
    assert!(opts.checksum);
    assert!(opts.pass_checksum_errors);
    assert!(opts.keep_active_message);
    assert_eq!(opts.vchan_id, 7);
    assert_eq!(opts.channel_type, "image");
}

#[test]
fn subscriber_set_max_shared_ptrs_adds_one() {
    let opts = SubscriberOptions::new().set_max_shared_ptrs(4);
    assert_eq!(opts.max_active_messages, 5);
}

// ── CRC32 / Checksum tests ──────────────────────────────────────────────────

#[test]
fn crc32_empty_data() {
    let crc = subspace_crc32(0xFFFFFFFF, &[]);
    assert_eq!(crc, 0xFFFFFFFF);
}

#[test]
fn crc32_known_value() {
    let data = b"hello";
    let crc = !subspace_crc32(0xFFFFFFFF, data);
    // CRC32 of "hello" is a well-known value.
    assert_eq!(crc, 0x3610A686);
}

#[test]
fn crc32_incremental_matches_one_shot() {
    let data = b"hello world";
    let one_shot = subspace_crc32(0xFFFFFFFF, data);

    let partial = subspace_crc32(0xFFFFFFFF, b"hello ");
    let incremental = subspace_crc32(partial, b"world");

    assert_eq!(one_shot, incremental);
}

#[test]
fn calculate_and_verify_checksum() {
    let data1 = b"subspace";
    let data2 = b"ipc";
    let spans: Vec<&[u8]> = vec![data1, data2];

    let checksum = calculate_checksum(&spans);
    assert_ne!(checksum, 0);
    assert!(verify_checksum(&spans, checksum));
    assert!(!verify_checksum(&spans, checksum ^ 1));
}

#[test]
fn checksum_differs_for_different_data() {
    let c1 = calculate_checksum(&[b"aaa"]);
    let c2 = calculate_checksum(&[b"bbb"]);
    assert_ne!(c1, c2);
}

#[test]
fn checksum_single_span_matches_multi_span() {
    let whole = calculate_checksum(&[b"foobar"]);
    let split = calculate_checksum(&[b"foo", b"bar"]);
    assert_eq!(whole, split);
}

// ── Bit field helpers ────────────────────────────────────────────────────────

#[test]
fn build_refs_zero_ordinal_omits_vchan() {
    let field = build_refs_bit_field(0, 5, 0);
    let vchan = (field >> VCHAN_ID_SHIFT) & VCHAN_ID_MASK;
    assert_eq!(vchan, 0);
}

#[test]
fn build_refs_nonzero_ordinal_includes_vchan() {
    let field = build_refs_bit_field(42, 7, 0);
    let vchan = (field >> VCHAN_ID_SHIFT) & VCHAN_ID_MASK;
    assert_eq!(vchan, 7);

    let ord = (field >> ORDINAL_SHIFT) & ORDINAL_MASK;
    assert_eq!(ord, 42);
}

#[test]
fn build_refs_round_trips_all_fields() {
    let ordinal = 12345u64;
    let vchan_id = 99i32;
    let retired_refs = 3i32;

    let field = build_refs_bit_field(ordinal, vchan_id, retired_refs);

    let got_ordinal = (field >> ORDINAL_SHIFT) & ORDINAL_MASK;
    let got_vchan = (field >> VCHAN_ID_SHIFT) & VCHAN_ID_MASK;
    let got_retired = (field >> RETIRED_REFS_SHIFT) & RETIRED_REFS_MASK;

    assert_eq!(got_ordinal, ordinal);
    assert_eq!(got_vchan, vchan_id as u64);
    assert_eq!(got_retired, retired_refs as u64);
}

#[test]
fn build_refs_pub_owned_flag() {
    let field = build_refs_bit_field(1, 0, 0) | PUB_OWNED;
    assert_ne!(field & PUB_OWNED, 0);
}

// ── Alignment helpers ────────────────────────────────────────────────────────

#[test]
fn aligned64_already_aligned() {
    assert_eq!(aligned64(128), 128);
    assert_eq!(aligned64(0), 0);
    assert_eq!(aligned64(64), 64);
}

#[test]
fn aligned64_rounds_up() {
    assert_eq!(aligned64(1), 64);
    assert_eq!(aligned64(63), 64);
    assert_eq!(aligned64(65), 128);
    assert_eq!(aligned64(100), 128);
}

#[test]
fn aligned_generic() {
    assert_eq!(aligned::<8>(1), 8);
    assert_eq!(aligned::<8>(8), 8);
    assert_eq!(aligned::<8>(9), 16);
    assert_eq!(aligned::<16>(15), 16);
    assert_eq!(aligned::<16>(17), 32);
}

// ── DynamicBitSet tests ──────────────────────────────────────────────────────

#[test]
fn dynamic_bitset_new_is_empty() {
    let bs = DynamicBitSet::new(128);
    assert!(bs.is_empty());
    for i in 0..128 {
        assert!(!bs.is_set(i));
    }
}

#[test]
fn dynamic_bitset_set_and_clear() {
    let mut bs = DynamicBitSet::new(256);

    bs.set(0);
    bs.set(63);
    bs.set(64);
    bs.set(127);
    bs.set(255);

    assert!(bs.is_set(0));
    assert!(bs.is_set(63));
    assert!(bs.is_set(64));
    assert!(bs.is_set(127));
    assert!(bs.is_set(255));
    assert!(!bs.is_set(1));
    assert!(!bs.is_set(128));

    bs.clear(63);
    assert!(!bs.is_set(63));
    assert!(bs.is_set(64));
}

#[test]
fn dynamic_bitset_clear_all() {
    let mut bs = DynamicBitSet::new(128);
    for i in 0..128 {
        bs.set(i);
    }
    assert!(!bs.is_empty());
    bs.clear_all();
    assert!(bs.is_empty());
}

#[test]
fn dynamic_bitset_resize_preserves_low_bits() {
    let mut bs = DynamicBitSet::new(64);
    bs.set(10);
    bs.set(50);

    bs.resize(256);
    assert!(bs.is_set(10));
    assert!(bs.is_set(50));
    assert!(!bs.is_set(100));

    bs.set(200);
    assert!(bs.is_set(200));
}

// ── ReadMode tests ───────────────────────────────────────────────────────────

#[test]
fn read_mode_equality() {
    assert_eq!(ReadMode::ReadNext, ReadMode::ReadNext);
    assert_eq!(ReadMode::ReadNewest, ReadMode::ReadNewest);
    assert_ne!(ReadMode::ReadNext, ReadMode::ReadNewest);
}

#[test]
fn read_mode_clone() {
    let mode = ReadMode::ReadNext;
    let cloned = mode;
    assert_eq!(mode, cloned);
}

// ── Error type tests ─────────────────────────────────────────────────────────

#[test]
fn error_display_internal() {
    let err = SubspaceError::Internal("something went wrong".into());
    let msg = format!("{}", err);
    assert!(msg.contains("something went wrong"));
}

#[test]
fn error_display_server() {
    let err = SubspaceError::ServerError("channel not found".into());
    assert!(format!("{}", err).contains("channel not found"));
}

#[test]
fn error_from_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
    let sub_err: SubspaceError = io_err.into();
    assert!(matches!(sub_err, SubspaceError::Io(_)));
}

#[test]
fn error_checksum_display() {
    let err = SubspaceError::ChecksumError;
    assert_eq!(format!("{}", err), "checksum error");
}

// ── Client connection failure tests ──────────────────────────────────────────

#[test]
fn client_connect_nonexistent_socket_fails() {
    let result = Client::new("/nonexistent/socket/path_that_does_not_exist", "test_client");
    assert!(result.is_err());
}

// ── Shared memory struct size checks ─────────────────────────────────────────
// These verify binary compatibility with the C++ layout.

#[test]
fn message_prefix_size() {
    assert_eq!(
        std::mem::size_of::<subspace_client::channel::MessagePrefix>(),
        64
    );
}

#[test]
fn channel_counters_size() {
    assert_eq!(
        std::mem::size_of::<subspace_client::channel::ChannelCounters>(),
        14
    );
}

// ══════════════════════════════════════════════════════════════════════════════
// Integration tests: run a real subspace server and communicate via the Rust
// client.  When built with --cfg=server_ffi (the Bazel rust_test target does
// this automatically), the server runs in-process in a thread via FFI.
// Otherwise (plain cargo test), a subprocess is spawned as a fallback.
// ══════════════════════════════════════════════════════════════════════════════

// ── FFI-based in-process server (used when linked with C++ server library) ──

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

// The C++ Server is designed to be used across threads (Run in one thread,
// Stop from another).  The handle is only accessed from one thread at a time.
#[cfg(server_ffi)]
unsafe impl Send for ServerGuard {}
#[cfg(server_ffi)]
unsafe impl Sync for ServerGuard {}

#[cfg(server_ffi)]
impl ServerGuard {
    fn start() -> Self {
        let tmp = std::env::temp_dir()
            .join(format!("subspace_rust_test_{}", std::process::id()));
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
            // Bind `raw` as a whole so edition-2021 captures the Send wrapper,
            // not the inner *mut c_void field.
            let raw = raw;
            let ret = unsafe { subspace_server_run(raw.0) };
            if ret != 0 {
                eprintln!("ERROR: subspace_server_run returned {}", ret);
            }
        });

        // Wait for the server's 8-byte "ready" notification with a timeout.
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
        let n = unsafe {
            libc::read(read_fd, buf.as_mut_ptr() as *mut libc::c_void, 8)
        };
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

        // Drain the "stopped" notification that Run() wrote before returning.
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

// ── Subprocess-based server (fallback for cargo test without C++ linkage) ───

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
        let tmp = std::env::temp_dir()
            .join(format!("subspace_rust_test_{}", std::process::id()));
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
            .unwrap_or_else(|e| {
                panic!("Failed to start server binary '{}': {}", binary, e)
            });

        let deadline =
            std::time::Instant::now() + std::time::Duration::from_secs(10);
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

// ── Common test infrastructure ──────────────────────────────────────────────

static SERVER: std::sync::OnceLock<ServerGuard> = std::sync::OnceLock::new();

fn server_socket() -> &'static str {
    SERVER.get_or_init(ServerGuard::start).socket()
}

fn new_client(name: &str) -> Client {
    Client::new(server_socket(), name).expect("Failed to connect to server")
}

// ── Init / Connection ────────────────────────────────────────────────────────

#[test]
fn integration_init_client() {
    let _client = new_client("test_init");
}

#[test]
fn integration_create_publisher() {
    let client = new_client("test_create_pub");
    let opts = PublisherOptions::new().set_slot_size(256).set_num_slots(10);
    let _pub = client.create_publisher("rust_pub0", &opts).unwrap();
}

#[test]
fn integration_create_publisher_then_subscriber() {
    let client = new_client("test_pub_sub");
    let pub_opts = PublisherOptions::new().set_slot_size(256).set_num_slots(10);
    let _pub = client.create_publisher("rust_ps1", &pub_opts).unwrap();

    let sub_opts = SubscriberOptions::new();
    let _sub = client.create_subscriber("rust_ps1", &sub_opts).unwrap();
}

#[test]
fn integration_create_subscriber_then_publisher() {
    let client = new_client("test_sub_pub");
    let sub_opts = SubscriberOptions::new();
    let _sub = client.create_subscriber("rust_sp1", &sub_opts).unwrap();

    let pub_opts = PublisherOptions::new().set_slot_size(300).set_num_slots(10);
    let _pub = client.create_publisher("rust_sp1", &pub_opts).unwrap();
}

// ── Publish single message and read ──────────────────────────────────────────

#[test]
fn integration_publish_single_message_and_read() {
    let pub_client = new_client("test_pub_read_p");
    let sub_client = new_client("test_pub_read_s");

    let pub_opts = PublisherOptions::new().set_slot_size(256).set_num_slots(10);
    let publisher = pub_client.create_publisher("rust_rw1", &pub_opts).unwrap();

    // Write a message.
    let (buf_ptr, _cap) = publisher.get_message_buffer(256).unwrap().unwrap();
    let payload = b"hello from rust";
    unsafe {
        std::ptr::copy_nonoverlapping(payload.as_ptr(), buf_ptr, payload.len());
    }
    let pub_msg = publisher.publish_message(payload.len() as i64).unwrap();
    assert!(pub_msg.ordinal > 0);

    // Subscribe and read.
    let sub_opts = SubscriberOptions::new();
    let subscriber = sub_client.create_subscriber("rust_rw1", &sub_opts).unwrap();

    let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(msg.length as usize, payload.len());
    let received = unsafe { std::slice::from_raw_parts(msg.buffer, msg.length as usize) };
    assert_eq!(received, payload);

    // A second read should return an empty message (length 0).
    let msg2 = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(msg2.length, 0);
}

// ── Publish multiple messages and read them all ──────────────────────────────

#[test]
fn integration_publish_multiple_messages() {
    let pub_client = new_client("test_multi_p");
    let sub_client = new_client("test_multi_s");

    let num_messages = 9;
    let pub_opts = PublisherOptions::new().set_slot_size(256).set_num_slots(num_messages + 4);
    let publisher = pub_client.create_publisher("rust_multi1", &pub_opts).unwrap();

    let sub_opts = SubscriberOptions::new();
    let subscriber = sub_client.create_subscriber("rust_multi1", &sub_opts).unwrap();
    let mut sent: Vec<String> = Vec::new();
    for i in 0..num_messages {
        let text = format!("message_{}", i);
        let (buf_ptr, _) = publisher.get_message_buffer(256).unwrap().unwrap();
        unsafe {
            std::ptr::copy_nonoverlapping(text.as_ptr(), buf_ptr, text.len());
        }
        publisher.publish_message(text.len() as i64).unwrap();
        sent.push(text);
    }

    // Read all messages.
    let mut received: Vec<String> = Vec::new();
    loop {
        let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
        if msg.length == 0 {
            break;
        }
        let data = unsafe { std::slice::from_raw_parts(msg.buffer, msg.length as usize) };
        received.push(String::from_utf8_lossy(data).to_string());
    }

    assert_eq!(sent.len(), received.len());
    for (s, r) in sent.iter().zip(received.iter()) {
        assert_eq!(s, r);
    }
}

// ── Read newest skips intermediate messages ──────────────────────────────────

#[test]
fn integration_read_newest() {
    let pub_client = new_client("test_newest_p");
    let sub_client = new_client("test_newest_s");

    let pub_opts = PublisherOptions::new().set_slot_size(256).set_num_slots(10);
    let publisher = pub_client.create_publisher("rust_newest1", &pub_opts).unwrap();

    // Publish two messages before subscriber attaches.
    for text in &[b"first".as_slice(), b"second"] {
        let (buf_ptr, _) = publisher.get_message_buffer(256).unwrap().unwrap();
        unsafe {
            std::ptr::copy_nonoverlapping(text.as_ptr(), buf_ptr, text.len());
        }
        publisher.publish_message(text.len() as i64).unwrap();
    }

    let sub_opts = SubscriberOptions::new();
    let subscriber = sub_client.create_subscriber("rust_newest1", &sub_opts).unwrap();

    // ReadNewest should skip to the latest.
    let msg = subscriber.read_message(ReadMode::ReadNewest).unwrap();
    assert_eq!(msg.length as usize, b"second".len());
    let received = unsafe { std::slice::from_raw_parts(msg.buffer, msg.length as usize) };
    assert_eq!(received, b"second");

    // No more messages.
    let msg2 = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(msg2.length, 0);
}

// ── Two publishers on the same channel ───────────────────────────────────────

#[test]
fn integration_two_publishers_same_channel() {
    let client = new_client("test_two_pub");

    let opts = PublisherOptions::new().set_slot_size(256).set_num_slots(10);
    let pub1 = client.create_publisher("rust_2pub", &opts).unwrap();
    let pub2 = client.create_publisher("rust_2pub", &opts).unwrap();

    let sub_opts = SubscriberOptions::new().set_max_active_messages(2);
    let subscriber = client.create_subscriber("rust_2pub", &sub_opts).unwrap();

    // Publish from pub1.
    let (buf, _) = pub1.get_message_buffer(256).unwrap().unwrap();
    unsafe { std::ptr::copy_nonoverlapping(b"from_pub1".as_ptr(), buf, 9); }
    pub1.publish_message(9).unwrap();

    // Publish from pub2.
    let (buf, _) = pub2.get_message_buffer(256).unwrap().unwrap();
    unsafe { std::ptr::copy_nonoverlapping(b"from_pub2".as_ptr(), buf, 9); }
    pub2.publish_message(9).unwrap();

    // Read both messages.
    let msg1 = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(msg1.length, 9);
    let data1 = unsafe { std::slice::from_raw_parts(msg1.buffer, 9) };
    assert_eq!(data1, b"from_pub1");

    let msg2 = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(msg2.length, 9);
    let data2 = unsafe { std::slice::from_raw_parts(msg2.buffer, 9) };
    assert_eq!(data2, b"from_pub2");

    let msg3 = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(msg3.length, 0);
}

// ── Separate publisher and subscriber clients ────────────────────────────────

#[test]
fn integration_separate_clients() {
    let pub_client = new_client("test_sep_p");
    let sub_client = new_client("test_sep_s");

    let pub_opts = PublisherOptions::new().set_slot_size(256).set_num_slots(10);
    let _pub = pub_client.create_publisher("rust_sep1", &pub_opts).unwrap();
    let _sub = sub_client.create_subscriber("rust_sep1", &SubscriberOptions::new()).unwrap();
}

// ── Poll-based subscriber wait ───────────────────────────────────────────────

#[test]
fn integration_subscriber_wait() {
    let pub_client = new_client("test_wait_p");
    let sub_client = new_client("test_wait_s");

    let sub_opts = SubscriberOptions::new();
    let subscriber = sub_client.create_subscriber("rust_wait1", &sub_opts).unwrap();

    let pub_opts = PublisherOptions::new().set_slot_size(256).set_num_slots(10);
    let publisher = pub_client.create_publisher("rust_wait1", &pub_opts).unwrap();

    // Publish a message.
    let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
    unsafe { std::ptr::copy_nonoverlapping(b"ping".as_ptr(), buf, 4); }
    publisher.publish_message(4).unwrap();

    // Wait with a reasonable timeout (should succeed immediately since a
    // message has already been published).
    subscriber.wait(Some(5000)).unwrap();

    let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(msg.length, 4);
    let data = unsafe { std::slice::from_raw_parts(msg.buffer, 4) };
    assert_eq!(data, b"ping");
}

// ── Threaded publisher / subscriber ──────────────────────────────────────────

#[test]
fn integration_threaded_pub_sub() {
    let num_messages: usize = 20;
    let num_slots = num_messages as i32 + 4;

    let pub_client = new_client("thread_pub");
    let pub_opts = PublisherOptions::new().set_slot_size(256).set_num_slots(num_slots);
    let publisher = pub_client.create_publisher("rust_thread1", &pub_opts).unwrap();

    let sub_client = new_client("thread_sub");
    let sub_opts = SubscriberOptions::new();
    let subscriber = sub_client
        .create_subscriber("rust_thread1", &sub_opts)
        .unwrap();

    // Publish all messages from a separate thread.
    let pub_handle = std::thread::spawn(move || {
        for i in 0..num_messages {
            let text = format!("thread_msg_{}", i);
            let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
            unsafe {
                std::ptr::copy_nonoverlapping(text.as_ptr(), buf, text.len());
            }
            publisher.publish_message(text.len() as i64).unwrap();
        }
    });

    // Read all messages from a separate thread.
    let sub_handle = std::thread::spawn(move || {
        let mut count = 0;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while count < num_messages && std::time::Instant::now() < deadline {
            let _ = subscriber.wait(Some(1000));
            loop {
                let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
                if msg.length == 0 {
                    break;
                }
                count += 1;
            }
        }
        assert_eq!(count, num_messages);
    });

    pub_handle.join().unwrap();
    sub_handle.join().unwrap();
}

// ── Large messages ───────────────────────────────────────────────────────────

#[test]
fn integration_large_message() {
    let pub_client = new_client("test_large_p");
    let sub_client = new_client("test_large_s");

    let msg_size = 64 * 1024;
    let pub_opts = PublisherOptions::new()
        .set_slot_size(msg_size as i32)
        .set_num_slots(4);
    let publisher = pub_client.create_publisher("rust_large1", &pub_opts).unwrap();

    let sub_opts = SubscriberOptions::new();
    let subscriber = sub_client.create_subscriber("rust_large1", &sub_opts).unwrap();

    // Fill with a pattern.
    let payload: Vec<u8> = (0..msg_size).map(|i| (i % 251) as u8).collect();
    let (buf, _) = publisher.get_message_buffer(msg_size as i32).unwrap().unwrap();
    unsafe {
        std::ptr::copy_nonoverlapping(payload.as_ptr(), buf, payload.len());
    }
    publisher.publish_message(msg_size as i64).unwrap();

    let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(msg.length as usize, msg_size);
    let received = unsafe { std::slice::from_raw_parts(msg.buffer, msg.length as usize) };
    assert_eq!(received, payload.as_slice());
}

// ── Publisher type matching ──────────────────────────────────────────────────

#[test]
fn integration_publisher_type_mismatch() {
    let client = new_client("test_type_mm");

    let opts1 = PublisherOptions::new()
        .set_slot_size(256)
        .set_num_slots(10)
        .set_type("sensor_data".into());
    let _pub1 = client.create_publisher("rust_type1", &opts1).unwrap();

    let opts2 = PublisherOptions::new()
        .set_slot_size(256)
        .set_num_slots(10)
        .set_type("wrong_type".into());
    let result = client.create_publisher("rust_type1", &opts2);
    assert!(result.is_err());
}

// ── Subscriber type matching ─────────────────────────────────────────────────

#[test]
fn integration_subscriber_type_match() {
    let client = new_client("test_type_m");

    let pub_opts = PublisherOptions::new()
        .set_slot_size(256)
        .set_num_slots(10)
        .set_type("image".into());
    let _pub = client.create_publisher("rust_type2", &pub_opts).unwrap();

    let sub_ok = SubscriberOptions::new().set_type("image".into());
    let _sub = client.create_subscriber("rust_type2", &sub_ok).unwrap();

    let sub_bad = SubscriberOptions::new().set_type("video".into());
    let result = client.create_subscriber("rust_type2", &sub_bad);
    assert!(result.is_err());
}

// ── Multiple subscribers reading the same messages ───────────────────────────

#[test]
fn integration_multiple_subscribers() {
    let client = new_client("test_multi_sub");

    let pub_opts = PublisherOptions::new().set_slot_size(256).set_num_slots(10);
    let publisher = client.create_publisher("rust_msub1", &pub_opts).unwrap();

    let sub1 = client.create_subscriber("rust_msub1", &SubscriberOptions::new()).unwrap();
    let sub2 = client.create_subscriber("rust_msub1", &SubscriberOptions::new()).unwrap();

    let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
    unsafe { std::ptr::copy_nonoverlapping(b"shared".as_ptr(), buf, 6); }
    publisher.publish_message(6).unwrap();

    let m1 = sub1.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(m1.length, 6);

    let m2 = sub2.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(m2.length, 6);

    let d1 = unsafe { std::slice::from_raw_parts(m1.buffer, 6) };
    let d2 = unsafe { std::slice::from_raw_parts(m2.buffer, 6) };
    assert_eq!(d1, b"shared");
    assert_eq!(d2, b"shared");
}

// ── Subscriber created before any publisher ──────────────────────────────────

#[test]
fn integration_subscriber_before_publisher() {
    let client = new_client("test_sub_first");

    let sub_opts = SubscriberOptions::new();
    let subscriber = client.create_subscriber("rust_subfirst1", &sub_opts).unwrap();

    // No publisher yet, read should return empty.
    let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(msg.length, 0);

    // Now create publisher and publish.
    let pub_opts = PublisherOptions::new().set_slot_size(256).set_num_slots(10);
    let publisher = client.create_publisher("rust_subfirst1", &pub_opts).unwrap();

    let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
    unsafe { std::ptr::copy_nonoverlapping(b"late".as_ptr(), buf, 4); }
    publisher.publish_message(4).unwrap();

    // Subscriber should pick up the message now.
    let msg2 = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(msg2.length, 4);
    let data = unsafe { std::slice::from_raw_parts(msg2.buffer, 4) };
    assert_eq!(data, b"late");
}

// ── Slot release on message drop ─────────────────────────────────────────────

#[test]
fn integration_active_message_count_tracks_live_messages() {
    let pub_client = new_client("test_am_count_p");
    let sub_client = new_client("test_am_count_s");

    let pub_opts = PublisherOptions::new().set_slot_size(256).set_num_slots(10);
    let publisher = pub_client.create_publisher("rust_amcount", &pub_opts).unwrap();

    let sub_opts = SubscriberOptions::new().set_max_active_messages(4);
    let subscriber = sub_client.create_subscriber("rust_amcount", &sub_opts).unwrap();

    assert_eq!(subscriber.num_active_messages(), 0);

    // Publish and read two messages, keeping both alive.
    for text in &[b"aaa".as_slice(), b"bbb"] {
        let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
        unsafe { std::ptr::copy_nonoverlapping(text.as_ptr(), buf, text.len()); }
        publisher.publish_message(text.len() as i64).unwrap();
    }

    let msg1 = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(subscriber.num_active_messages(), 1);

    let msg2 = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(subscriber.num_active_messages(), 2);

    // Drop the first message -- count should go back to 1.
    drop(msg1);
    assert_eq!(subscriber.num_active_messages(), 1);

    // Drop the second -- count should go back to 0.
    drop(msg2);
    assert_eq!(subscriber.num_active_messages(), 0);
}

#[test]
fn integration_clone_shares_slot_last_drop_releases() {
    let pub_client = new_client("test_clone_p");
    let sub_client = new_client("test_clone_s");

    let pub_opts = PublisherOptions::new().set_slot_size(256).set_num_slots(10);
    let publisher = pub_client.create_publisher("rust_clone1", &pub_opts).unwrap();

    let sub_opts = SubscriberOptions::new().set_max_active_messages(2);
    let subscriber = sub_client.create_subscriber("rust_clone1", &sub_opts).unwrap();

    let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
    unsafe { std::ptr::copy_nonoverlapping(b"shared".as_ptr(), buf, 6); }
    publisher.publish_message(6).unwrap();

    let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(subscriber.num_active_messages(), 1);

    // Clone the message -- slot is shared, active count stays at 1.
    let msg_clone = msg.clone();
    assert_eq!(subscriber.num_active_messages(), 1);

    // Drop the original -- clone still holds the slot.
    drop(msg);
    assert_eq!(subscriber.num_active_messages(), 1);

    // Drop the clone -- slot is now released.
    drop(msg_clone);
    assert_eq!(subscriber.num_active_messages(), 0);
}

#[test]
fn integration_slots_reused_after_message_drop() {
    let pub_client = new_client("test_reuse_p");
    let sub_client = new_client("test_reuse_s");

    // Only 4 slots: tight enough that we must release to keep publishing.
    let pub_opts = PublisherOptions::new().set_slot_size(256).set_num_slots(4);
    let publisher = pub_client.create_publisher("rust_reuse1", &pub_opts).unwrap();

    let sub_opts = SubscriberOptions::new();
    let subscriber = sub_client.create_subscriber("rust_reuse1", &sub_opts).unwrap();

    // Publish-read-drop cycles well beyond the slot count.  Each iteration
    // consumes a slot; without proper release this would exhaust them.
    for i in 0..20 {
        let text = format!("msg_{}", i);
        let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
        unsafe { std::ptr::copy_nonoverlapping(text.as_ptr(), buf, text.len()); }
        publisher.publish_message(text.len() as i64).unwrap();

        let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
        assert_eq!(msg.length, text.len());
        let data = unsafe { std::slice::from_raw_parts(msg.buffer, msg.length) };
        assert_eq!(data, text.as_bytes());
        // msg drops here, releasing the slot for reuse.
    }

    assert_eq!(subscriber.num_active_messages(), 0);
}

#[test]
fn integration_fill_slots_then_free_and_reuse() {
    let pub_client = new_client("test_fill_p");
    let sub_client = new_client("test_fill_s");

    // 6 slots with max_active_messages of 3.
    let pub_opts = PublisherOptions::new().set_slot_size(256).set_num_slots(6);
    let publisher = pub_client.create_publisher("rust_fill1", &pub_opts).unwrap();

    let sub_opts = SubscriberOptions::new().set_max_active_messages(3);
    let subscriber = sub_client.create_subscriber("rust_fill1", &sub_opts).unwrap();

    // Publish 3 messages and read all of them, keeping them alive.
    let mut messages = Vec::new();
    for i in 0..3 {
        let text = format!("batch1_{}", i);
        let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
        unsafe { std::ptr::copy_nonoverlapping(text.as_ptr(), buf, text.len()); }
        publisher.publish_message(text.len() as i64).unwrap();

        let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
        assert!(msg.length > 0);
        messages.push(msg);
    }
    assert_eq!(subscriber.num_active_messages(), 3);

    // Drop all held messages, freeing the slots.
    messages.clear();
    assert_eq!(subscriber.num_active_messages(), 0);

    // Publish and read another batch -- proves the freed slots are reusable.
    for i in 0..3 {
        let text = format!("batch2_{}", i);
        let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
        unsafe { std::ptr::copy_nonoverlapping(text.as_ptr(), buf, text.len()); }
        publisher.publish_message(text.len() as i64).unwrap();

        let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
        let data = unsafe { std::slice::from_raw_parts(msg.buffer, msg.length) };
        assert_eq!(data, text.as_bytes());
    }
}

// ── Dropped message callback ─────────────────────────────────────────────────

#[test]
fn integration_dropped_message_callback() {
    let client = new_client("test_drop_cb");

    // 5 slots: tight enough to force overwrites.
    let pub_opts = PublisherOptions::new().set_slot_size(256).set_num_slots(5);
    let publisher = client.create_publisher("rust_dropcb", &pub_opts).unwrap();

    // max_active_messages = 2: one for the held message, one for the
    // message being read in the drain loop.
    let sub_opts = SubscriberOptions::new()
        .set_log_dropped_messages(false)
        .set_max_active_messages(2);
    let subscriber = client.create_subscriber("rust_dropcb", &sub_opts).unwrap();

    let num_dropped = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
    let dropped_clone = num_dropped.clone();
    subscriber.register_dropped_message_callback(move |n| {
        dropped_clone.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    });

    // Fill 4 of the 5 slots (publisher holds the 5th).
    for _ in 0..4 {
        let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
        unsafe { std::ptr::copy_nonoverlapping(b"foobar".as_ptr(), buf, 6); }
        publisher.publish_message(6).unwrap();
    }

    // Read one message and keep it alive to pin slot 0 (ordinal 1).
    // This prevents the publisher from reusing slot 0.
    let _held = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(_held.length, 6);

    // Publish 4 more messages.  The publisher cycles through slots 4,1,2,3
    // (skipping pinned slot 0).  When the subscriber reads the next message
    // it sees ordinal 6 (expects 2) => 4 dropped messages.
    for _ in 0..4 {
        let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
        unsafe { std::ptr::copy_nonoverlapping(b"foobar".as_ptr(), buf, 6); }
        publisher.publish_message(6).unwrap();
    }

    // Drain remaining messages.
    loop {
        let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
        if msg.length == 0 {
            break;
        }
    }

    assert_eq!(num_dropped.load(std::sync::atomic::Ordering::Relaxed), 4);
}

// ── Checksum tests ───────────────────────────────────────────────────────────

#[test]
fn integration_checksum_valid_message() {
    let pub_client = new_client("test_csum_ok_p");
    let sub_client = new_client("test_csum_ok_s");

    let pub_opts = PublisherOptions::new()
        .set_slot_size(256)
        .set_num_slots(4)
        .set_checksum(true);
    let publisher = pub_client
        .create_publisher("rust_csum_ok", &pub_opts)
        .unwrap();

    let sub_opts = SubscriberOptions::new().set_checksum(true);
    let subscriber = sub_client
        .create_subscriber("rust_csum_ok", &sub_opts)
        .unwrap();

    let payload = b"checksum test data";
    let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
    unsafe {
        std::ptr::copy_nonoverlapping(payload.as_ptr(), buf, payload.len());
    }
    publisher.publish_message(payload.len() as i64).unwrap();

    let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(msg.length, payload.len());
    assert!(!msg.checksum_error);
    let data = unsafe { std::slice::from_raw_parts(msg.buffer, msg.length) };
    assert_eq!(data, payload);
}

#[test]
fn integration_checksum_error_returns_error() {
    let pub_client = new_client("test_csum_err_p");
    let sub_client = new_client("test_csum_err_s");

    let pub_opts = PublisherOptions::new()
        .set_slot_size(256)
        .set_num_slots(4)
        .set_checksum(true);
    let publisher = pub_client
        .create_publisher("rust_csum_err", &pub_opts)
        .unwrap();

    // Subscriber verifies checksums but does NOT pass errors through.
    // A mismatched checksum callback forces the verification to fail.
    let sub_opts = SubscriberOptions::new().set_checksum(true);
    let subscriber = sub_client
        .create_subscriber("rust_csum_err", &sub_opts)
        .unwrap();

    subscriber.set_checksum_callback(|_spans| 0xBADBAD);

    let payload = b"good data";
    let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
    unsafe {
        std::ptr::copy_nonoverlapping(payload.as_ptr(), buf, payload.len());
    }
    publisher.publish_message(payload.len() as i64).unwrap();

    let result = subscriber.read_message(ReadMode::ReadNext);
    assert!(
        matches!(result, Err(SubspaceError::ChecksumError)),
        "Expected ChecksumError, got {:?}",
        result
    );
}

#[test]
fn integration_checksum_error_passed_through() {
    let pub_client = new_client("test_csum_pass_p");
    let sub_client = new_client("test_csum_pass_s");

    let pub_opts = PublisherOptions::new()
        .set_slot_size(256)
        .set_num_slots(4)
        .set_checksum(true);
    let publisher = pub_client
        .create_publisher("rust_csum_pass", &pub_opts)
        .unwrap();

    // Subscriber verifies checksums but passes errors through to the caller
    // instead of returning Err.  A mismatched callback forces the mismatch.
    let sub_opts = SubscriberOptions::new()
        .set_checksum(true)
        .set_pass_checksum_errors(true);
    let subscriber = sub_client
        .create_subscriber("rust_csum_pass", &sub_opts)
        .unwrap();

    subscriber.set_checksum_callback(|_spans| 0xBADBAD);

    let payload = b"good data";
    let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
    unsafe {
        std::ptr::copy_nonoverlapping(payload.as_ptr(), buf, payload.len());
    }
    publisher.publish_message(payload.len() as i64).unwrap();

    // With pass_checksum_errors, the read succeeds but the message is
    // flagged with checksum_error = true.
    let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert!(msg.checksum_error);
    assert!(msg.length > 0);
}

#[test]
fn integration_custom_checksum_callback() {
    let pub_client = new_client("test_csum_cb_p");
    let sub_client = new_client("test_csum_cb_s");

    let pub_opts = PublisherOptions::new()
        .set_slot_size(256)
        .set_num_slots(4)
        .set_checksum(true);
    let publisher = pub_client
        .create_publisher("rust_csum_cb", &pub_opts)
        .unwrap();

    // Install a custom checksum callback on the publisher that always
    // returns a fixed value.
    publisher.set_checksum_callback(|_spans| 0xDEADBEEF);

    let sub_opts = SubscriberOptions::new()
        .set_checksum(true)
        .set_pass_checksum_errors(true);
    let subscriber = sub_client
        .create_subscriber("rust_csum_cb", &sub_opts)
        .unwrap();

    // Install the same custom checksum on the subscriber so verification
    // matches.
    subscriber.set_checksum_callback(|_spans| 0xDEADBEEF);

    let payload = b"custom checksum";
    let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
    unsafe {
        std::ptr::copy_nonoverlapping(payload.as_ptr(), buf, payload.len());
    }
    publisher.publish_message(payload.len() as i64).unwrap();

    {
        let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
        assert!(!msg.checksum_error);
        assert_eq!(msg.length, payload.len());
    } // msg dropped here, releasing the active slot

    // Now set a *mismatched* checksum callback on the subscriber.
    subscriber.set_checksum_callback(|_spans| 0xCAFEBABE);

    let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
    unsafe {
        std::ptr::copy_nonoverlapping(b"mismatch".as_ptr(), buf, 8);
    }
    publisher.publish_message(8).unwrap();

    let msg2 = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert!(msg2.checksum_error);
}

// ── Retirement trigger helpers ───────────────────────────────────────────────

/// Poll a retirement fd with the given timeout (milliseconds).
/// Returns true if the fd is readable.
fn retirement_fd_readable(fd: i32, timeout_ms: i32) -> bool {
    let mut pfd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    let ret = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
    ret > 0 && (pfd.revents & libc::POLLIN) != 0
}

/// Read one retired slot ID (i32) from the retirement fd.
fn read_retired_slot(fd: i32) -> i32 {
    let mut slot_id: i32 = -1;
    let n = unsafe {
        libc::read(
            fd,
            &mut slot_id as *mut i32 as *mut libc::c_void,
            std::mem::size_of::<i32>(),
        )
    };
    assert_eq!(n, std::mem::size_of::<i32>() as isize);
    slot_id
}

// ── Retirement trigger tests ─────────────────────────────────────────────────

/// One publisher with retirement notification and two subscribers.
/// Both subscribers read the same message and then drop it.  The last
/// drop triggers a retirement notification for slot 0.
#[test]
fn integration_retirement_trigger_subscriber_side() {
    let pub_client = new_client("test_ret1_p");
    let sub_client = new_client("test_ret1_s");

    let pub_opts = PublisherOptions::new()
        .set_slot_size(256)
        .set_num_slots(10)
        .set_notify_retirement(true);
    let publisher = pub_client
        .create_publisher("rust_ret1", &pub_opts)
        .unwrap();

    let retirement_fd = publisher.get_retirement_fd();
    assert!(retirement_fd >= 0);

    let payload = b"foobar";
    let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
    unsafe {
        std::ptr::copy_nonoverlapping(payload.as_ptr(), buf, payload.len());
    }
    publisher.publish_message(payload.len() as i64).unwrap();

    let sub1 = sub_client
        .create_subscriber("rust_ret1", &SubscriberOptions::new())
        .unwrap();
    let sub2 = sub_client
        .create_subscriber("rust_ret1", &SubscriberOptions::new())
        .unwrap();

    let msg1 = sub1.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(msg1.length, payload.len());

    let msg2 = sub2.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(msg2.length, payload.len());

    // Drop the first subscriber's message — slot still held by sub2.
    drop(msg1);

    // Retirement should NOT have fired yet.
    assert!(!retirement_fd_readable(retirement_fd, 0));

    // Drop the second subscriber's message — last ref, triggers retirement.
    drop(msg2);

    assert!(retirement_fd_readable(retirement_fd, 1000));
    let retired = read_retired_slot(retirement_fd);
    assert_eq!(retired, 0);

    // Pipe should be empty now.
    assert!(!retirement_fd_readable(retirement_fd, 0));
}

/// Publisher-side retirement via dropped messages.  Two subscribers are
/// created, one reads messages and the other never does.  When the
/// publisher runs out of slots it recycles old ones, triggering retirement.
#[test]
fn integration_retirement_trigger_publisher_side() {
    let pub_client = new_client("test_ret2_p");
    let sub_client = new_client("test_ret2_s");

    let pub_opts = PublisherOptions::new()
        .set_slot_size(256)
        .set_num_slots(10)
        .set_notify_retirement(true);
    let publisher = pub_client
        .create_publisher("rust_ret2", &pub_opts)
        .unwrap();

    let retirement_fd = publisher.get_retirement_fd();

    let sub1 = sub_client
        .create_subscriber(
            "rust_ret2",
            &SubscriberOptions::new().set_log_dropped_messages(false),
        )
        .unwrap();
    let _sub2 = sub_client
        .create_subscriber(
            "rust_ret2",
            &SubscriberOptions::new().set_log_dropped_messages(false),
        )
        .unwrap();

    // Fill 7 of 10 slots.
    for _ in 0..7 {
        let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
        unsafe {
            std::ptr::copy_nonoverlapping(b"foobar".as_ptr(), buf, 6);
        }
        publisher.publish_message(6).unwrap();
    }

    // sub1 reads 2 messages (slots 0 and 1).
    for _ in 0..2 {
        let msg = sub1.read_message(ReadMode::ReadNext).unwrap();
        assert_eq!(msg.length, 6);
        // msg dropped at end of iteration, releasing sub1's claim.
    }

    // Publish 2 more messages — takes the retired slots.
    for _ in 0..2 {
        let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
        unsafe {
            std::ptr::copy_nonoverlapping(b"foobar".as_ptr(), buf, 6);
        }
        publisher.publish_message(6).unwrap();
    }

    // No retirement yet.
    assert!(!retirement_fd_readable(retirement_fd, 0));

    // Publish one more — forces recycling of a slot that sub2 never read,
    // triggering publisher-side retirement.
    {
        let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
        unsafe {
            std::ptr::copy_nonoverlapping(b"foobar".as_ptr(), buf, 6);
        }
        publisher.publish_message(6).unwrap();
    }

    assert!(retirement_fd_readable(retirement_fd, 1000));
    let retired = read_retired_slot(retirement_fd);
    assert_eq!(retired, 0);

    // Publish 2 more — triggers 2 more publisher-side retirements.
    for _ in 0..2 {
        let (buf, _) = publisher.get_message_buffer(256).unwrap().unwrap();
        unsafe {
            std::ptr::copy_nonoverlapping(b"foobar".as_ptr(), buf, 6);
        }
        publisher.publish_message(6).unwrap();
    }

    assert!(retirement_fd_readable(retirement_fd, 1000));
    let r0 = read_retired_slot(retirement_fd);
    let r1 = read_retired_slot(retirement_fd);
    assert_eq!(r0, 1);
    assert_eq!(r1, 2);

    assert!(!retirement_fd_readable(retirement_fd, 0));
}

/// Two publishers on separate clients share a channel with retirement
/// notification.  Retirement triggered by subscriber drops is visible on
/// the second publisher's retirement fd.
#[test]
fn integration_retirement_trigger_two_publishers_subscriber_side() {
    let pub1_client = new_client("test_ret3_p1");
    let pub2_client = new_client("test_ret3_p2");
    let sub_client = new_client("test_ret3_s");

    let pub_opts = PublisherOptions::new()
        .set_slot_size(256)
        .set_num_slots(10)
        .set_notify_retirement(true);
    let publisher1 = pub1_client
        .create_publisher("rust_ret3", &pub_opts)
        .unwrap();
    let publisher2 = pub2_client
        .create_publisher("rust_ret3", &pub_opts)
        .unwrap();

    // Check retirement on the second publisher (not the one that publishes).
    let retirement_fd = publisher2.get_retirement_fd();
    assert!(retirement_fd >= 0);

    let payload = b"foobar";
    let (buf, _) = publisher1.get_message_buffer(256).unwrap().unwrap();
    unsafe {
        std::ptr::copy_nonoverlapping(payload.as_ptr(), buf, payload.len());
    }
    publisher1.publish_message(payload.len() as i64).unwrap();

    let sub1 = sub_client
        .create_subscriber("rust_ret3", &SubscriberOptions::new())
        .unwrap();
    let sub2 = sub_client
        .create_subscriber("rust_ret3", &SubscriberOptions::new())
        .unwrap();

    let msg1 = sub1.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(msg1.length, payload.len());
    let msg2 = sub2.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(msg2.length, payload.len());

    drop(msg1);
    drop(msg2);

    assert!(retirement_fd_readable(retirement_fd, 1000));
    let retired = read_retired_slot(retirement_fd);
    assert_eq!(retired, 0);

    assert!(!retirement_fd_readable(retirement_fd, 0));
}

/// Two publishers on separate clients.  Publisher-side retirement via
/// dropped messages is visible on the second publisher's retirement fd.
#[test]
fn integration_retirement_trigger_two_publishers_publisher_side() {
    let pub1_client = new_client("test_ret4_p1");
    let pub2_client = new_client("test_ret4_p2");
    let sub_client = new_client("test_ret4_s");

    let pub_opts = PublisherOptions::new()
        .set_slot_size(256)
        .set_num_slots(10)
        .set_notify_retirement(true);
    let publisher1 = pub1_client
        .create_publisher("rust_ret4", &pub_opts)
        .unwrap();
    let publisher2 = pub2_client
        .create_publisher("rust_ret4", &pub_opts)
        .unwrap();

    // Check retirement on the second publisher.
    let retirement_fd = publisher2.get_retirement_fd();

    let sub1 = sub_client
        .create_subscriber(
            "rust_ret4",
            &SubscriberOptions::new().set_log_dropped_messages(false),
        )
        .unwrap();
    let _sub2 = sub_client
        .create_subscriber(
            "rust_ret4",
            &SubscriberOptions::new().set_log_dropped_messages(false),
        )
        .unwrap();

    // Fill 6 of 10 slots.
    for _ in 0..6 {
        let (buf, _) = publisher1.get_message_buffer(256).unwrap().unwrap();
        unsafe {
            std::ptr::copy_nonoverlapping(b"foobar".as_ptr(), buf, 6);
        }
        publisher1.publish_message(6).unwrap();
    }

    // sub1 reads 2 messages.
    for _ in 0..2 {
        let msg = sub1.read_message(ReadMode::ReadNext).unwrap();
        assert_eq!(msg.length, 6);
    }

    // Publish 2 more using the retired slots.
    for _ in 0..2 {
        let (buf, _) = publisher1.get_message_buffer(256).unwrap().unwrap();
        unsafe {
            std::ptr::copy_nonoverlapping(b"foobar".as_ptr(), buf, 6);
        }
        publisher1.publish_message(6).unwrap();
    }

    assert!(!retirement_fd_readable(retirement_fd, 0));

    // One more publish triggers recycling + retirement.
    {
        let (buf, _) = publisher1.get_message_buffer(256).unwrap().unwrap();
        unsafe {
            std::ptr::copy_nonoverlapping(b"foobar".as_ptr(), buf, 6);
        }
        publisher1.publish_message(6).unwrap();
    }

    assert!(retirement_fd_readable(retirement_fd, 1000));
    let retired = read_retired_slot(retirement_fd);
    assert_eq!(retired, 0);

    // Two more publishes trigger two more retirements.
    for _ in 0..2 {
        let (buf, _) = publisher1.get_message_buffer(256).unwrap().unwrap();
        unsafe {
            std::ptr::copy_nonoverlapping(b"foobar".as_ptr(), buf, 6);
        }
        publisher1.publish_message(6).unwrap();
    }

    assert!(retirement_fd_readable(retirement_fd, 1000));
    let r0 = read_retired_slot(retirement_fd);
    let r1 = read_retired_slot(retirement_fd);
    assert_eq!(r0, 2);
    assert_eq!(r1, 3);

    assert!(!retirement_fd_readable(retirement_fd, 0));
}

// ── Reliable publisher / subscriber tests ────────────────────────────────────

/// Basic reliable pub/sub: publisher with 5 slots publishes until blocked,
/// subscriber reads to free a slot, publisher can then publish again.
#[test]
fn integration_reliable_publisher_basic() {
    let client = new_client("test_rel1");

    let pub_opts = PublisherOptions::new()
        .set_slot_size(32)
        .set_num_slots(5)
        .set_reliable(true);
    let publisher = client.create_publisher("rust_rel1", &pub_opts).unwrap();

    let sub_opts = SubscriberOptions::new().set_reliable(true);
    let subscriber = client.create_subscriber("rust_rel1", &sub_opts).unwrap();

    // Publish one reliable message.
    let (buf, _) = publisher.get_message_buffer(32).unwrap().unwrap();
    unsafe { std::ptr::copy_nonoverlapping(b"foobar".as_ptr(), buf, 6) };
    publisher.publish_message(6).unwrap();

    // Read it.
    let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(msg.length, 6);

    // Publish 4 more — fills all remaining slots (subscriber still holds one).
    for _ in 0..4 {
        let (buf, _) = publisher.get_message_buffer(32).unwrap().unwrap();
        unsafe { std::ptr::copy_nonoverlapping(b"foobar".as_ptr(), buf, 6) };
        publisher.publish_message(6).unwrap();
    }

    // Next get_message_buffer should return None — no slots available.
    let result = publisher.get_message_buffer(32).unwrap();
    assert!(result.is_none(), "Expected None when all slots exhausted");

    // Release the subscriber's held message.
    drop(msg);

    // Now the publisher can obtain a buffer again.
    let result = publisher.get_message_buffer(32).unwrap();
    assert!(result.is_some(), "Expected a buffer after releasing a slot");
}

/// Reliable subscriber created before publisher — the subscriber must fetch
/// the publisher's trigger fd lazily on first read.
#[test]
fn integration_reliable_subscriber_before_publisher() {
    let client = new_client("test_rel2");

    let sub_opts = SubscriberOptions::new().set_reliable(true);
    let subscriber = client.create_subscriber("rust_rel2", &sub_opts).unwrap();

    let pub_opts = PublisherOptions::new()
        .set_slot_size(32)
        .set_num_slots(5)
        .set_reliable(true);
    let publisher = client.create_publisher("rust_rel2", &pub_opts).unwrap();

    // Publish one reliable message.
    let (buf, _) = publisher.get_message_buffer(32).unwrap().unwrap();
    unsafe { std::ptr::copy_nonoverlapping(b"foobar".as_ptr(), buf, 6) };
    publisher.publish_message(6).unwrap();

    // Subscriber reads — this triggers a server call to discover the reliable
    // publisher's trigger fd.
    let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(msg.length, 6);

    // Fill remaining slots.
    for _ in 0..4 {
        let (buf, _) = publisher.get_message_buffer(32).unwrap().unwrap();
        unsafe { std::ptr::copy_nonoverlapping(b"foobar".as_ptr(), buf, 6) };
        publisher.publish_message(6).unwrap();
    }

    // All slots exhausted.
    let result = publisher.get_message_buffer(32).unwrap();
    assert!(result.is_none());

    // Release the held message.
    drop(msg);

    // Publisher can publish again.
    let result = publisher.get_message_buffer(32).unwrap();
    assert!(result.is_some());
}

/// Reliable publisher wait: after filling all slots, dropping the subscriber's
/// held message triggers the publisher's poll fd.  wait() returns immediately
/// if the fd is already signalled.
#[test]
fn integration_reliable_publisher_wait() {
    let client = new_client("test_rel_wait");

    let pub_opts = PublisherOptions::new()
        .set_slot_size(32)
        .set_num_slots(5)
        .set_reliable(true);
    let publisher = client.create_publisher("rust_rel_w", &pub_opts).unwrap();

    let sub_opts = SubscriberOptions::new().set_reliable(true);
    let subscriber = client.create_subscriber("rust_rel_w", &sub_opts).unwrap();

    // Publish one message and have the subscriber hold onto it.
    let (buf, _) = publisher.get_message_buffer(32).unwrap().unwrap();
    unsafe { std::ptr::copy_nonoverlapping(b"foobar".as_ptr(), buf, 6) };
    publisher.publish_message(6).unwrap();

    let held_msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(held_msg.length, 6);

    // Fill the remaining 4 slots.
    for _ in 0..4 {
        let (buf, _) = publisher.get_message_buffer(32).unwrap().unwrap();
        unsafe { std::ptr::copy_nonoverlapping(b"foobar".as_ptr(), buf, 6) };
        publisher.publish_message(6).unwrap();
    }

    // All slots exhausted.
    assert!(publisher.get_message_buffer(32).unwrap().is_none());

    // wait() with zero timeout should time out (no slots freed yet).
    let wait_result = publisher.wait(Some(0));
    assert!(wait_result.is_err());

    // Drop the held message — triggers the reliable publisher's poll fd.
    drop(held_msg);

    // wait() should now return immediately.
    publisher.wait(Some(1000)).unwrap();

    // Publisher can get a buffer again.
    assert!(publisher.get_message_buffer(32).unwrap().is_some());

    // Read and drop remaining 4 messages.
    for _ in 0..4 {
        let m = subscriber.read_message(ReadMode::ReadNext).unwrap();
        assert_eq!(m.length, 6);
    }

    // No more messages.
    let m = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(m.length, 0);
}

/// Reliable publisher with activation: subscriber with pass_activation reads
/// the activation message first, then normal messages.
#[test]
fn integration_reliable_publisher_activation() {
    let client = new_client("test_rel_act");

    let pub_opts = PublisherOptions::new()
        .set_slot_size(32)
        .set_num_slots(5)
        .set_reliable(true);
    let publisher = client.create_publisher("rust_rel_a", &pub_opts).unwrap();

    let sub_opts = SubscriberOptions::new()
        .set_reliable(true)
        .set_pass_activation(true);
    let subscriber = client.create_subscriber("rust_rel_a", &sub_opts).unwrap();

    // First read should be the activation message.
    let act_msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert!(act_msg.is_activation);
    assert!(act_msg.length > 0);
    drop(act_msg);

    // Publish a reliable message.
    let (buf, _) = publisher.get_message_buffer(32).unwrap().unwrap();
    unsafe { std::ptr::copy_nonoverlapping(b"foobar".as_ptr(), buf, 6) };
    publisher.publish_message(6).unwrap();

    // Read the real message.
    let msg = subscriber.read_message(ReadMode::ReadNext).unwrap();
    assert_eq!(msg.length, 6);

    // Fill the remaining slots.
    for _ in 0..4 {
        let (buf, _) = publisher.get_message_buffer(32).unwrap().unwrap();
        unsafe { std::ptr::copy_nonoverlapping(b"foobar".as_ptr(), buf, 6) };
        publisher.publish_message(6).unwrap();
    }

    // All slots exhausted.
    assert!(publisher.get_message_buffer(32).unwrap().is_none());

    // Release the held message.
    drop(msg);

    // Publisher can obtain a buffer again.
    assert!(publisher.get_message_buffer(32).unwrap().is_some());
}
