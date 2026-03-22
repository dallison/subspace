// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

//! Syscall fault injection tests for the Rust subspace client.
//! Mirrors the approach used by the C++ syscall_failure_test.

use subspace_client::options::{PublisherOptions, SubscriberOptions};
use subspace_client::syscall_shim::{self, SyscallShim};
use subspace_client::Client;
use std::sync::atomic::{AtomicI32, Ordering};

// ── FailingShim ─────────────────────────────────────────────────────────────
//
// Wraps a SyscallShim with countdown-based failure injection.
// A countdown of -1 means "never fail"; 0 means "fail on the next call";
// >0 means decrement and forward to the real syscall.

static MMAP_COUNTDOWN: AtomicI32 = AtomicI32::new(-1);
static MMAP_CALL_COUNT: AtomicI32 = AtomicI32::new(0);
static OPEN_COUNTDOWN: AtomicI32 = AtomicI32::new(-1);
static FTRUNCATE_COUNTDOWN: AtomicI32 = AtomicI32::new(-1);
static FSTAT_COUNTDOWN: AtomicI32 = AtomicI32::new(-1);
static STAT_COUNTDOWN: AtomicI32 = AtomicI32::new(-1);
static POLL_COUNTDOWN: AtomicI32 = AtomicI32::new(-1);

/// Set `errno` for the current thread (replaces the `errno` crate for Bazel compatibility).
#[cfg(any(target_os = "linux", target_os = "android"))]
unsafe fn set_thread_errno(e: libc::c_int) {
    *libc::__errno_location() = e;
}

#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "android")),
))]
unsafe fn set_thread_errno(e: libc::c_int) {
    *libc::__error() = e;
}

fn should_fail(countdown: &AtomicI32) -> bool {
    let val = countdown.load(Ordering::SeqCst);
    if val < 0 {
        return false;
    }
    if val == 0 {
        countdown.store(-1, Ordering::SeqCst);
        return true;
    }
    countdown.fetch_sub(1, Ordering::SeqCst);
    false
}

fn reset_counters() {
    MMAP_COUNTDOWN.store(-1, Ordering::SeqCst);
    MMAP_CALL_COUNT.store(0, Ordering::SeqCst);
    OPEN_COUNTDOWN.store(-1, Ordering::SeqCst);
    FTRUNCATE_COUNTDOWN.store(-1, Ordering::SeqCst);
    FSTAT_COUNTDOWN.store(-1, Ordering::SeqCst);
    STAT_COUNTDOWN.store(-1, Ordering::SeqCst);
    POLL_COUNTDOWN.store(-1, Ordering::SeqCst);
}

unsafe extern "C" fn failing_mmap(
    addr: *mut libc::c_void,
    len: libc::size_t,
    prot: libc::c_int,
    flags: libc::c_int,
    fd: libc::c_int,
    offset: libc::off_t,
) -> *mut libc::c_void {
    MMAP_CALL_COUNT.fetch_add(1, Ordering::SeqCst);
    if should_fail(&MMAP_COUNTDOWN) {
        set_thread_errno(libc::ENOMEM);
        return libc::MAP_FAILED;
    }
    libc::mmap(addr, len, prot, flags, fd, offset)
}

unsafe extern "C" fn failing_open(
    path: *const libc::c_char,
    flags: libc::c_int,
    mode: libc::mode_t,
) -> libc::c_int {
    if should_fail(&OPEN_COUNTDOWN) {
        set_thread_errno(libc::EACCES);
        return -1;
    }
    libc::open(path, flags, mode as libc::c_uint)
}

unsafe extern "C" fn failing_ftruncate(
    fd: libc::c_int,
    length: libc::off_t,
) -> libc::c_int {
    if should_fail(&FTRUNCATE_COUNTDOWN) {
        set_thread_errno(libc::ENOSPC);
        return -1;
    }
    libc::ftruncate(fd, length)
}

unsafe extern "C" fn failing_fstat(
    fd: libc::c_int,
    buf: *mut libc::stat,
) -> libc::c_int {
    if should_fail(&FSTAT_COUNTDOWN) {
        set_thread_errno(libc::EBADF);
        return -1;
    }
    libc::fstat(fd, buf)
}

unsafe extern "C" fn failing_stat(
    path: *const libc::c_char,
    buf: *mut libc::stat,
) -> libc::c_int {
    if should_fail(&STAT_COUNTDOWN) {
        set_thread_errno(libc::EACCES);
        return -1;
    }
    libc::stat(path, buf)
}

unsafe extern "C" fn failing_poll(
    fds: *mut libc::pollfd,
    nfds: libc::nfds_t,
    timeout: libc::c_int,
) -> libc::c_int {
    if should_fail(&POLL_COUNTDOWN) {
        set_thread_errno(libc::EINTR);
        return -1;
    }
    libc::poll(fds, nfds, timeout)
}

fn make_failing_shim() -> SyscallShim {
    SyscallShim {
        mmap_fn: failing_mmap,
        open_fn: failing_open,
        ftruncate_fn: failing_ftruncate,
        fstat_fn: failing_fstat,
        stat_fn: failing_stat,
        poll_fn: failing_poll,
        ..SyscallShim::default()
    }
}

struct ScopedShim;

impl ScopedShim {
    fn install() -> Self {
        syscall_shim::set_shim(make_failing_shim());
        ScopedShim
    }
}

impl Drop for ScopedShim {
    fn drop(&mut self) {
        syscall_shim::reset_shim();
    }
}

// ── Server infrastructure (same as client_test.rs) ──────────────────────────

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
        let socket_path = format!("/tmp/ss_sf_{}", std::process::id());

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

#[cfg(not(server_ffi))]
use std::process::{Child, Command, Stdio};

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
        let socket_path = format!("/tmp/ss_sf_{}", std::process::id());
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

static SERVER: std::sync::OnceLock<ServerGuard> = std::sync::OnceLock::new();

fn server_socket() -> &'static str {
    SERVER.get_or_init(ServerGuard::start).socket()
}

fn new_client(name: &str) -> Client {
    Client::new(server_socket(), name).expect("Failed to connect to server")
}

// ── Syscall failure tests ───────────────────────────────────────────────────

#[test]
fn mmap_fail_on_scb() {
    reset_counters();
    let client = new_client("mmap_scb");
    let _guard = ScopedShim::install();
    MMAP_COUNTDOWN.store(0, Ordering::SeqCst);
    let opts = PublisherOptions::new().set_slot_size(64).set_num_slots(4);
    let result = client.create_publisher("sf_mmap_scb", &opts);
    assert!(result.is_err(), "expected mmap failure on SCB");
}

#[test]
fn mmap_fail_on_ccb() {
    reset_counters();
    let client = new_client("mmap_ccb");
    let _guard = ScopedShim::install();
    MMAP_COUNTDOWN.store(1, Ordering::SeqCst);
    let opts = PublisherOptions::new().set_slot_size(64).set_num_slots(4);
    let result = client.create_publisher("sf_mmap_ccb", &opts);
    assert!(result.is_err(), "expected mmap failure on CCB");
}

#[test]
fn mmap_fail_on_bcb() {
    reset_counters();
    let client = new_client("mmap_bcb");
    let _guard = ScopedShim::install();
    MMAP_COUNTDOWN.store(2, Ordering::SeqCst);
    let opts = PublisherOptions::new().set_slot_size(64).set_num_slots(4);
    let result = client.create_publisher("sf_mmap_bcb", &opts);
    assert!(result.is_err(), "expected mmap failure on BCB");
}

#[test]
fn mmap_fail_on_buffer_map() {
    reset_counters();
    let client = new_client("mmap_buf");
    let _guard = ScopedShim::install();
    // SCB, CCB, BCB succeed (0,1,2), then buffer map (3) fails.
    MMAP_COUNTDOWN.store(3, Ordering::SeqCst);
    let opts = PublisherOptions::new().set_slot_size(64).set_num_slots(4);
    let result = client.create_publisher("sf_mmap_buf", &opts);
    assert!(result.is_err(), "expected mmap failure on buffer map");
}

#[test]
fn open_fail_on_create_shm() {
    reset_counters();
    let client = new_client("open_shm");
    let _guard = ScopedShim::install();
    // The first open call during create_publisher is for the buffer shared
    // memory.  The channel mapping uses FDs from the server (no local open).
    OPEN_COUNTDOWN.store(0, Ordering::SeqCst);
    let opts = PublisherOptions::new().set_slot_size(64).set_num_slots(4);
    let result = client.create_publisher("sf_open_shm", &opts);
    assert!(result.is_err(), "expected open failure on create_shm");
}

#[test]
fn poll_fail_wait_for_subscriber() {
    reset_counters();
    let client = new_client("poll_sub");
    let opts = PublisherOptions::new().set_slot_size(64).set_num_slots(16);
    let _pub = client.create_publisher("sf_poll_sub", &opts).unwrap();
    let sub_opts = SubscriberOptions::new();
    let sub = client.create_subscriber("sf_poll_sub", &sub_opts).unwrap();

    let _guard = ScopedShim::install();
    POLL_COUNTDOWN.store(0, Ordering::SeqCst);
    let result = sub.wait(Some(100));
    assert!(result.is_err(), "expected poll failure");
}

#[test]
fn poll_fail_wait_for_reliable_publisher() {
    reset_counters();
    let client = new_client("poll_rpub");
    let opts = PublisherOptions::new()
        .set_slot_size(64)
        .set_num_slots(16)
        .set_reliable(true);
    let pub_handle = client.create_publisher("sf_poll_rpub", &opts).unwrap();

    let _guard = ScopedShim::install();
    POLL_COUNTDOWN.store(0, Ordering::SeqCst);
    let result = pub_handle.wait(Some(100));
    assert!(result.is_err(), "expected poll failure on reliable pub wait");
}

#[test]
fn mmap_call_counting() {
    reset_counters();
    let client = new_client("mmap_count");
    let _guard = ScopedShim::install();
    MMAP_CALL_COUNT.store(0, Ordering::SeqCst);
    let opts = PublisherOptions::new().set_slot_size(64).set_num_slots(4);
    let _pub = client.create_publisher("sf_mmap_count", &opts).unwrap();
    let count = MMAP_CALL_COUNT.load(Ordering::SeqCst);
    // At minimum: SCB + CCB + BCB + 1 buffer = 4 mmap calls.
    assert!(count >= 4, "expected at least 4 mmap calls, got {}", count);
}

#[test]
fn shim_restored_after_scope() {
    reset_counters();
    {
        let _guard = ScopedShim::install();
        MMAP_COUNTDOWN.store(0, Ordering::SeqCst);
    }
    // After the guard is dropped, the shim should be restored.
    // Creating a publisher should succeed.
    let client = new_client("shim_restored");
    let opts = PublisherOptions::new().set_slot_size(64).set_num_slots(4);
    let result = client.create_publisher("sf_restored", &opts);
    assert!(result.is_ok(), "publisher creation should succeed after shim restore: {:?}", result.err());
}

#[test]
fn shim_countdown_decrements() {
    reset_counters();
    // Set countdown to 2; calls 0 and 1 should succeed, call 2 should fail.
    MMAP_COUNTDOWN.store(2, Ordering::SeqCst);
    assert!(!should_fail(&MMAP_COUNTDOWN));
    assert_eq!(MMAP_COUNTDOWN.load(Ordering::SeqCst), 1);
    assert!(!should_fail(&MMAP_COUNTDOWN));
    assert_eq!(MMAP_COUNTDOWN.load(Ordering::SeqCst), 0);
    assert!(should_fail(&MMAP_COUNTDOWN));
    assert_eq!(MMAP_COUNTDOWN.load(Ordering::SeqCst), -1);
    // Subsequent calls should not fail.
    assert!(!should_fail(&MMAP_COUNTDOWN));
}

#[test]
fn fstat_or_stat_fail_on_subscriber_attach() {
    reset_counters();
    let client = new_client("get_shm_size_sub");
    let opts = PublisherOptions::new().set_slot_size(64).set_num_slots(16);
    let _pub = client.create_publisher("sf_get_shm_size_sub", &opts).unwrap();

    let _guard = ScopedShim::install();
    // get_shm_size is called during subscriber buffer attachment.
    // On Linux it uses fstat; on macOS it uses stat on the shadow file.
    if cfg!(target_os = "linux") {
        FSTAT_COUNTDOWN.store(0, Ordering::SeqCst);
    } else {
        STAT_COUNTDOWN.store(0, Ordering::SeqCst);
    }
    let sub_opts = SubscriberOptions::new();
    let result = client.create_subscriber("sf_get_shm_size_sub", &sub_opts);
    assert!(
        result.is_err(),
        "expected get_shm_size (fstat/stat) failure during subscriber attach"
    );
}
