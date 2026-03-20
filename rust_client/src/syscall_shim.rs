// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

//! Thin indirection layer over POSIX system calls, enabling tests to inject
//! failures into error-handling paths that are otherwise impossible to reach.
//!
//! Production code calls `shim_mmap(...)`, `shim_open(...)`, etc.  The default
//! shim forwards every call to the real libc symbol.  Tests install a custom
//! shim via `set_shim()` to make individual calls return errors on demand.

use std::cell::RefCell;
use std::os::unix::io::RawFd;
use std::ptr::NonNull;

// `libc::open` is variadic and cannot be stored as a function pointer.
unsafe extern "C" fn real_open(path: *const libc::c_char, flags: libc::c_int, mode: libc::mode_t) -> libc::c_int {
    libc::open(path, flags, mode)
}

pub type MmapFn =
    unsafe extern "C" fn(*mut libc::c_void, libc::size_t, libc::c_int, libc::c_int, libc::c_int, libc::off_t) -> *mut libc::c_void;
pub type MunmapFn = unsafe extern "C" fn(*mut libc::c_void, libc::size_t) -> libc::c_int;
pub type OpenFn = unsafe extern "C" fn(*const libc::c_char, libc::c_int, libc::mode_t) -> libc::c_int;
pub type CloseFn = unsafe extern "C" fn(libc::c_int) -> libc::c_int;
pub type FtruncateFn = unsafe extern "C" fn(libc::c_int, libc::off_t) -> libc::c_int;
pub type WriteFn = unsafe extern "C" fn(libc::c_int, *const libc::c_void, libc::size_t) -> libc::ssize_t;
pub type ReadFn = unsafe extern "C" fn(libc::c_int, *mut libc::c_void, libc::size_t) -> libc::ssize_t;
pub type PollFn = unsafe extern "C" fn(*mut libc::pollfd, libc::nfds_t, libc::c_int) -> libc::c_int;
pub type FstatFn = unsafe extern "C" fn(libc::c_int, *mut libc::stat) -> libc::c_int;
pub type StatFn = unsafe extern "C" fn(*const libc::c_char, *mut libc::stat) -> libc::c_int;

pub struct SyscallShim {
    pub mmap_fn: MmapFn,
    pub munmap_fn: MunmapFn,
    pub open_fn: OpenFn,
    pub close_fn: CloseFn,
    pub ftruncate_fn: FtruncateFn,
    pub write_fn: WriteFn,
    pub read_fn: ReadFn,
    pub poll_fn: PollFn,
    pub fstat_fn: FstatFn,
    pub stat_fn: StatFn,
}

impl Default for SyscallShim {
    fn default() -> Self {
        Self {
            mmap_fn: libc::mmap,
            munmap_fn: libc::munmap,
            open_fn: real_open,
            close_fn: libc::close,
            ftruncate_fn: libc::ftruncate,
            write_fn: libc::write,
            read_fn: libc::read,
            poll_fn: libc::poll,
            fstat_fn: libc::fstat,
            stat_fn: libc::stat,
        }
    }
}

thread_local! {
    static ACTIVE_SHIM: RefCell<SyscallShim> = RefCell::new(SyscallShim::default());
}

/// Install a custom shim for the current thread.
pub fn set_shim(shim: SyscallShim) {
    ACTIVE_SHIM.with(|s| *s.borrow_mut() = shim);
}

/// Restore the default (real-libc) shim for the current thread.
pub fn reset_shim() {
    set_shim(SyscallShim::default());
}

// ── Convenience wrappers ────────────────────────────────────────────────────
// These are the functions that production code calls instead of libc/nix.

use nix::sys::mman::{MapFlags, ProtFlags};
use std::num::NonZeroUsize;
use std::os::fd::BorrowedFd;

/// Wrapper around mmap that routes through the shim.
pub fn shim_mmap(
    size: NonZeroUsize,
    prot: ProtFlags,
    flags: MapFlags,
    fd: BorrowedFd<'_>,
    offset: libc::off_t,
) -> nix::Result<NonNull<libc::c_void>> {
    use std::os::fd::AsRawFd;
    let ptr = ACTIVE_SHIM.with(|s| {
        let shim = s.borrow();
        unsafe {
            (shim.mmap_fn)(
                std::ptr::null_mut(),
                size.get(),
                prot.bits(),
                flags.bits(),
                fd.as_raw_fd(),
                offset,
            )
        }
    });
    if ptr == libc::MAP_FAILED {
        Err(nix::Error::last())
    } else {
        // SAFETY: mmap succeeded, ptr is non-null.
        Ok(unsafe { NonNull::new_unchecked(ptr) })
    }
}

/// Wrapper around munmap that routes through the shim.
pub fn shim_munmap(addr: NonNull<libc::c_void>, len: usize) -> nix::Result<()> {
    let ret = ACTIVE_SHIM.with(|s| {
        let shim = s.borrow();
        unsafe { (shim.munmap_fn)(addr.as_ptr(), len) }
    });
    if ret == -1 {
        Err(nix::Error::last())
    } else {
        Ok(())
    }
}

/// Wrapper around open that routes through the shim.
/// Accepts Rust string path; converts to CString internally.
pub fn shim_open(
    path: &str,
    oflag: nix::fcntl::OFlag,
    mode: nix::sys::stat::Mode,
) -> nix::Result<RawFd> {
    let c_path = std::ffi::CString::new(path).map_err(|_| nix::Error::EINVAL)?;
    let ret = ACTIVE_SHIM.with(|s| {
        let shim = s.borrow();
        unsafe { (shim.open_fn)(c_path.as_ptr(), oflag.bits(), mode.bits()) }
    });
    if ret < 0 {
        Err(nix::Error::last())
    } else {
        Ok(ret)
    }
}

/// Wrapper around close that routes through the shim.
pub fn shim_close(fd: RawFd) -> libc::c_int {
    ACTIVE_SHIM.with(|s| {
        let shim = s.borrow();
        unsafe { (shim.close_fn)(fd) }
    })
}

/// Wrapper around ftruncate that routes through the shim.
pub fn shim_ftruncate(fd: RawFd, length: libc::off_t) -> libc::c_int {
    ACTIVE_SHIM.with(|s| {
        let shim = s.borrow();
        unsafe { (shim.ftruncate_fn)(fd, length) }
    })
}

/// Wrapper around write that routes through the shim.
pub fn shim_write(fd: RawFd, buf: *const libc::c_void, count: libc::size_t) -> libc::ssize_t {
    ACTIVE_SHIM.with(|s| {
        let shim = s.borrow();
        unsafe { (shim.write_fn)(fd, buf, count) }
    })
}

/// Wrapper around read that routes through the shim.
pub fn shim_read(fd: RawFd, buf: *mut libc::c_void, count: libc::size_t) -> libc::ssize_t {
    ACTIVE_SHIM.with(|s| {
        let shim = s.borrow();
        unsafe { (shim.read_fn)(fd, buf, count) }
    })
}

/// Wrapper around poll that routes through the shim and returns Result.
pub fn shim_poll(fds: *mut libc::pollfd, nfds: libc::nfds_t, timeout: libc::c_int) -> crate::error::Result<libc::c_int> {
    let ret = ACTIVE_SHIM.with(|s| {
        let shim = s.borrow();
        unsafe { (shim.poll_fn)(fds, nfds, timeout) }
    });
    if ret < 0 {
        Err(crate::error::SubspaceError::Io(std::io::Error::last_os_error()))
    } else {
        Ok(ret)
    }
}

/// Wrapper around fstat that routes through the shim.
pub fn shim_fstat(fd: RawFd) -> nix::Result<libc::stat> {
    let mut stat_buf = unsafe { std::mem::zeroed::<libc::stat>() };
    let ret = ACTIVE_SHIM.with(|s| {
        let shim = s.borrow();
        unsafe { (shim.fstat_fn)(fd, &mut stat_buf) }
    });
    if ret < 0 {
        Err(nix::Error::last())
    } else {
        Ok(stat_buf)
    }
}

/// Wrapper around stat that routes through the shim.
pub fn shim_stat(path: &str) -> nix::Result<libc::stat> {
    let c_path = std::ffi::CString::new(path).map_err(|_| nix::Error::EINVAL)?;
    let mut stat_buf = unsafe { std::mem::zeroed::<libc::stat>() };
    let ret = ACTIVE_SHIM.with(|s| {
        let shim = s.borrow();
        unsafe { (shim.stat_fn)(c_path.as_ptr(), &mut stat_buf) }
    });
    if ret < 0 {
        Err(nix::Error::last())
    } else {
        Ok(stat_buf)
    }
}

/// Wrapper around shm_open (non-Linux only) that routes through the shim.
/// On Linux this is implemented via open on /dev/shm, which goes through
/// shim_open; on macOS we need the real shm_open, which we route through
/// the open_fn pointer with a special path prefix.
#[cfg(not(target_os = "linux"))]
pub fn shim_shm_open(
    name: &str,
    oflag: nix::fcntl::OFlag,
    mode: nix::sys::stat::Mode,
) -> nix::Result<RawFd> {
    // shm_open on macOS/BSD uses a name that starts with '/'.
    // We call through libc directly here since shm_open is not variadic.
    let c_name = std::ffi::CString::new(name).map_err(|_| nix::Error::EINVAL)?;
    let ret = unsafe { libc::shm_open(c_name.as_ptr(), oflag.bits(), mode.bits()) };
    if ret < 0 {
        Err(nix::Error::last())
    } else {
        Ok(ret)
    }
}
